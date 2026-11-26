"""存储层回归：原子事务、崩溃后重放、幂等、双时态补登隔离。"""
import json
import os
import subprocess
import sys
import unittest

from revokerace.model import mint_token_id
from revokerace.storage import (
    FAULT_AFTER_COMMIT,
    FAULT_AFTER_WAL,
    FAULT_BEFORE_WAL,
    Store,
)

from _base import REPO_ROOT, TempStoreMixin

RUNNER = os.path.join(REPO_ROOT, "revokerace_run.py")


def race_batch(family: str) -> list[dict]:
    root = mint_token_id("root|" + family)
    return [
        {"request_id": "r-issue", "op": "issue", "family": family,
         "biz_ts": "2026-11-27T09:00:00", "seq": 1},
        {"request_id": "r-pre", "op": "use", "family": family, "token": root,
         "biz_ts": "2026-11-27T09:02:00", "seq": 2, "session_id": "ses-pre"},
        {"request_id": "r-win", "op": "rotate", "family": family, "token": root,
         "biz_ts": "2026-11-27T09:05:00", "seq": 3},
        {"request_id": "r-lose", "op": "rotate", "family": family, "token": root,
         "biz_ts": "2026-11-27T09:06:00", "seq": 4},
    ]


def run_cli(*args: str) -> subprocess.CompletedProcess:
    # errors="replace" 仅用于容错：崩溃子进程的 stderr 在某些控制台代码页
    # 下可能不是 UTF-8；stdout 的业务 JSON 始终由程序保证为 UTF-8。
    env = dict(os.environ, PYTHONUTF8="1", PYTHONIOENCODING="utf-8")
    return subprocess.run(
        [sys.executable, "-I", RUNNER, *args],
        cwd=REPO_ROOT, capture_output=True, text=True, encoding="utf-8",
        errors="replace", env=env,
    )


class StorageTest(TempStoreMixin, unittest.TestCase):
    def test_commit_and_reopen_replay_match(self) -> None:
        store = Store(self.path("ledger"))
        result = store.commit(race_batch("fam-s1"))
        self.assertFalse(result["replayed"])
        self.assertEqual([r["status"] for r in result["receipts"]],
                         ["ok", "ok", "ok", "reuse"])
        self.assertEqual(len(result["isolations"]), 1)

        reopened = Store(self.path("ledger"))
        check = reopened.verify()
        self.assertTrue(check["ok"], json.dumps(check, ensure_ascii=False))
        self.assertEqual(
            reopened.explain()["first_reuse_witness"]["discovery_request_id"],
            "r-lose",
        )

    def test_same_batch_replay_is_idempotent(self) -> None:
        store = Store(self.path("ledger"))
        first = store.commit(race_batch("fam-s2"))
        again = store.commit(race_batch("fam-s2"))
        self.assertTrue(again["replayed"])
        self.assertEqual(first["txn_id"], again["txn_id"])
        self.assertEqual(first["receipts"], again["receipts"])
        self.assertEqual(len(os.listdir(self.path("ledger", "wal"))), 1)

    def _crash_in_subprocess(self, fault: str) -> None:
        events_path = self.path("events.json")
        with open(events_path, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(race_batch("fam-crash"), fh, ensure_ascii=False)
        proc = run_cli("apply", "--store", self.path("ledger"),
                       "--events", events_path, "--fault", fault)
        self.assertEqual(proc.returncode, 99, proc.stderr)

    def test_crash_before_wal_leaves_no_trace(self) -> None:
        self._crash_in_subprocess(FAULT_BEFORE_WAL)
        wal = self.path("ledger", "wal")
        self.assertEqual(os.listdir(wal), [])
        # 另起进程重放同一批：事务完整生效
        proc = run_cli("apply", "--store", self.path("ledger"),
                       "--events", self.path("events.json"))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        body = json.loads(proc.stdout)
        self.assertTrue(body["ok"])
        self.assertFalse(body["replayed"])
        check = run_cli("verify", "--store", self.path("ledger"))
        self.assertTrue(json.loads(check.stdout)["ok"])

    def test_crash_after_wal_tmp_is_rolled_back_then_replays(self) -> None:
        self._crash_in_subprocess(FAULT_AFTER_WAL)
        wal = self.path("ledger", "wal")
        self.assertTrue(any(n.endswith(".tmp") for n in os.listdir(wal)))
        # 新进程打开时清理残留，重放同一批成功且不重复
        proc = run_cli("apply", "--store", self.path("ledger"),
                       "--events", self.path("events.json"))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        body = json.loads(proc.stdout)
        self.assertFalse(body["replayed"])
        self.assertEqual(len(os.listdir(wal)), 1)
        self.assertEqual(Store(self.path("ledger")).verify()["ok"], True)

    def test_crash_after_commit_replay_returns_original_result(self) -> None:
        self._crash_in_subprocess(FAULT_AFTER_COMMIT)
        wal = self.path("ledger", "wal")
        self.assertEqual(len(os.listdir(wal)), 1)  # 事务已落盘
        proc = run_cli("apply", "--store", self.path("ledger"),
                       "--events", self.path("events.json"))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        body = json.loads(proc.stdout)
        self.assertTrue(body["replayed"])
        self.assertEqual([r["status"] for r in body["receipts"]],
                         ["ok", "ok", "ok", "reuse"])
        # 隔离记录随事务同时生效
        self.assertEqual(len(body["isolations"]), 1)
        self.assertEqual(Store(self.path("ledger")).verify()["ok"], True)

    def test_events_receipts_isolations_are_one_atomic_unit(self) -> None:
        # 三种故障点之后，台账要么没有该事务，要么事件+回执+隔离三者齐全
        for fault in (FAULT_BEFORE_WAL, FAULT_AFTER_WAL, FAULT_AFTER_COMMIT):
            store_dir = self.path("ledger-" + fault)
            events_path = self.path(f"events-{fault}.json")
            with open(events_path, "w", encoding="utf-8", newline="\n") as fh:
                json.dump(race_batch("fam-" + fault), fh, ensure_ascii=False)
            proc = run_cli("apply", "--store", store_dir,
                           "--events", events_path, "--fault", fault)
            self.assertEqual(proc.returncode, 99)
            wal = os.path.join(store_dir, "wal")
            committed = [n for n in os.listdir(wal) if n.endswith(".json")]
            self.assertLessEqual(len(committed), 1)
            if committed:
                with open(os.path.join(wal, committed[0]), encoding="utf-8") as fh:
                    payload = json.load(fh)
                # 原子事务里只有事件事实；回执与隔离由全库重放派生，
                # 因而“事件可见 => 回执+隔离同时可派生”，不存在半成品
                self.assertTrue(payload["events"])
                self.assertNotIn("receipts", payload)
                derived = Store(store_dir)
                answer = derived._answer(payload, replayed=True)
                self.assertTrue(answer["receipts"])
                # 该批含竞争，必然派生出隔离记录
                self.assertTrue(
                    any(iso["witness"]["discovery_request_id"] == "r-lose"
                        for iso in answer["isolations"])
                )
                self.assertTrue(derived.verify()["ok"])

    def test_backfill_cannot_rewrite_prior_historical_view(self) -> None:
        store = Store(self.path("ledger"))
        root = mint_token_id("root|fam-hist")
        store.commit([
            {"request_id": "h-issue", "op": "issue", "family": "fam-hist",
             "biz_ts": "2026-11-27T09:00:00", "seq": 1},
            {"request_id": "h-use", "op": "use", "family": "fam-hist",
             "token": root, "biz_ts": "2026-11-27T09:05:00", "seq": 2,
             "session_id": "ses-normal"},
        ])
        # 人工补登：业务时间 08:30（更早），但第 2 个事务才入库
        store.commit([
            {"request_id": "h-backfill", "op": "use", "family": "fam-hist",
             "token": root, "biz_ts": "2026-11-27T08:30:00", "seq": 100,
             "session_id": "ses-backfill", "label": "人工补登"},
        ])

        horizon_1 = store.asof("2026-11-27T23:59:59", known_txn="1")
        ids_1 = {s["session_id"] for s in horizon_1["sessions"]}
        self.assertEqual(ids_1, {"ses-normal"})  # 补登前的历史视图保持不变

        horizon_now = store.asof("2026-11-27T23:59:59")
        ids_now = {s["session_id"] for s in horizon_now["sessions"]}
        self.assertEqual(ids_now, {"ses-normal", "ses-backfill"})

        # 纯业务时间窗口（不带事务上界）：09:00 之前没有任何会话，
        # 但补登的 08:30 会话会出现——它与“历史视图不可改写”是两个维度，
        # 后者由上面的 known_txn 断言保证。
        early = store.asof("2026-11-27T09:00:00")
        self.assertEqual(
            {s["session_id"] for s in early["sessions"]}, {"ses-backfill"}
        )
        self.assertEqual(
            {f["family"] for f in early["families"]}, {"fam-hist"}
        )


if __name__ == "__main__":
    unittest.main()
