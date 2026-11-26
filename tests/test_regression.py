"""并发两客户端、分批提交与夹具端到端回归。"""
import json
import os
import subprocess
import sys
import threading
import unittest

from revokerace.model import mint_token_id
from revokerace.storage import Store

from _base import REPO_ROOT, TempStoreMixin

RUNNER = os.path.join(REPO_ROOT, "revokerace_run.py")
FIXTURE_DIR = os.path.join(REPO_ROOT, "fixtures")


def run_cli(*args: str) -> subprocess.CompletedProcess:
    env = dict(os.environ, PYTHONUTF8="1", PYTHONIOENCODING="utf-8")
    return subprocess.run(
        [sys.executable, "-I", RUNNER, *args],
        cwd=REPO_ROOT, capture_output=True, text=True, encoding="utf-8",
        errors="replace", env=env,
    )


class BatchAndConcurrencyTest(TempStoreMixin, unittest.TestCase):
    def _seed_family(self, family: str) -> str:
        root = mint_token_id("root|" + family)
        store = Store(self.path("ledger"))
        store.commit([
            {"request_id": "g-issue", "op": "issue", "family": family,
             "biz_ts": "2026-11-27T09:00:00", "seq": 1},
        ])
        return root

    def test_split_batches_match_single_batch(self) -> None:
        family = "fam-split"
        root = mint_token_id("root|" + family)
        all_events = [
            {"request_id": "g-issue", "op": "issue", "family": family,
             "biz_ts": "2026-11-27T09:00:00", "seq": 1},
            {"request_id": "g-pre", "op": "use", "family": family, "token": root,
             "biz_ts": "2026-11-27T09:02:00", "seq": 2, "session_id": "ses-pre"},
            {"request_id": "g-win", "op": "rotate", "family": family, "token": root,
             "biz_ts": "2026-11-27T09:05:00", "seq": 3},
            {"request_id": "g-lose", "op": "rotate", "family": family, "token": root,
             "biz_ts": "2026-11-27T09:06:00", "seq": 4},
            {"request_id": "g-post", "op": "use", "family": family, "token": root,
             "biz_ts": "2026-11-27T09:07:00", "seq": 5, "session_id": "ses-post"},
        ]

        split = Store(self.path("ledger-split"))
        for ev in all_events:  # 每事件一个事务
            split.commit([ev])

        one_shot = Store(self.path("ledger-once"))
        one_shot.commit(all_events)  # 同一事件集合，单事务

        self.assertTrue(split.verify()["ok"])
        self.assertTrue(one_shot.verify()["ok"])
        # 最终审计语义完全一致（哈希已剥除事务/顺序等物理坐标）
        self.assertEqual(split.explain()["report_hash"],
                         one_shot.explain()["report_hash"])

    def test_two_concurrent_clients_one_winner_one_reuse(self) -> None:
        # 真并发语义：两个独立 Store 实例（模拟两个客户端进程），
        # 各自打开时只看见种子事务；提交时在锁内 catch-up 对方写入。
        family = "fam-conc"
        root = self._seed_family(family)
        client_a = self.path("client_a.json")
        client_b = self.path("client_b.json")
        event_a = {"request_id": "c-a", "op": "rotate", "family": family,
                   "token": root, "biz_ts": "2026-11-27T09:10:00", "seq": 2}
        event_b = {"request_id": "c-b", "op": "rotate", "family": family,
                   "token": root, "biz_ts": "2026-11-27T09:11:00", "seq": 3}
        for path, event in ((client_a, event_a), (client_b, event_b)):
            with open(path, "w", encoding="utf-8", newline="\n") as fh:
                json.dump([event], fh, ensure_ascii=False)

        outcomes: dict[str, dict] = {}
        barrier = threading.Barrier(2)
        import time
        lock_order: list[str] = []

        def gate_a() -> None:
            # 让 B 先取得锁并完成落盘，A 再进入，确定性地触发 catch-up 路径
            while "B-locked" not in lock_order:
                time.sleep(0.005)
            lock_order.append("A-locked")

        def gate_b() -> None:
            lock_order.append("B-locked")
            time.sleep(0.1)

        def client(event: dict, name: str, gate) -> None:
            barrier.wait()
            store = Store(self.path("ledger"), lock_gate=gate)  # 独立实例
            outcomes[name] = store.commit([event])

        t1 = threading.Thread(target=client, args=(event_a, "A", gate_a))
        t2 = threading.Thread(target=client, args=(event_b, "B", gate_b))
        t1.start(); t2.start()
        t1.join(); t2.join()
        self.assertEqual(lock_order, ["B-locked", "A-locked"])

        # 事件溯源：客户端应答可能早于对端事务落盘；权威裁决以最终
        # 全库重放为准——seq=2 赢、seq=3 重用，与锁获取顺序无关。
        final = Store(self.path("ledger"))
        derived = {
            rid: rcp["status"]
            for rid, rcp in final.engine.request_index.items()
            if rid in ("c-a", "c-b")
        }
        self.assertEqual(derived, {"c-a": "ok", "c-b": "reuse"})
        # 两个客户端都得到了结构合法的应答（无异常、各一事务）
        self.assertEqual(
            sorted(outcomes), sorted(["A", "B"])
        )
        for body in outcomes.values():
            self.assertEqual(len(body["receipts"]), 1)

        self.assertTrue(final.verify()["ok"])
        self.assertEqual(len(final.committed), 3)  # 种子 + 两个客户端各一事务
        witness = final.explain()["first_reuse_witness"]
        self.assertEqual(witness["discovery_request_id"], "c-b")
        self.assertEqual(witness["winner_request_id"], "c-a")
        self.assertTrue(final.engine.families[family]["isolated"])

    def test_concurrent_clients_survive_reverse_lock_order(self) -> None:
        # 锁顺序反转（B 先拿锁）也不改变 seq 裁决：逻辑顺序与调度顺序解耦
        import time

        family = "fam-conc2"
        root = self._seed_family(family)
        barrier = threading.Barrier(2)
        lock_order: list[str] = []

        def worker(name: str, seq: int, gate) -> None:
            barrier.wait()
            event = [{"request_id": f"k-{name}", "op": "rotate",
                      "family": family, "token": root,
                      "biz_ts": f"2026-11-27T09:1{seq}:00", "seq": seq}]
            store = Store(self.path("ledger"), lock_gate=gate)  # 独立客户端
            store.commit(event)

        def gate_a() -> None:
            # A 阻塞到 B 已取得锁之后才尝试，确保物理锁顺序 B -> A
            while "B-locked" not in lock_order:
                time.sleep(0.01)
            lock_order.append("A-locked")

        def gate_b() -> None:
            lock_order.append("B-locked")
            time.sleep(0.15)  # 持锁窗口，放大反转效果

        t1 = threading.Thread(target=worker, args=("A", 2, gate_a))
        t2 = threading.Thread(target=worker, args=("B", 3, gate_b))
        t1.start(); t2.start()
        t1.join(); t2.join()
        self.assertEqual(lock_order, ["B-locked", "A-locked"])

        # 即便 B 先拿锁先落盘，最终全库重放仍按 seq 裁决
        final = Store(self.path("ledger"))
        derived = {
            rid: rcp["status"]
            for rid, rcp in final.engine.request_index.items()
            if rid in ("k-A", "k-B")
        }
        self.assertEqual(derived, {"k-A": "ok", "k-B": "reuse"})
        self.assertTrue(final.verify()["ok"])
        self.assertTrue(final.engine.families[family]["isolated"])

    def test_two_subprocess_clients_competing(self) -> None:
        # 真·两个独立进程客户端：各自 python -I 进程，靠文件字节锁串行
        family = "fam-proc"
        root = self._seed_family(family)
        ev_a = self.path("a.json")
        ev_b = self.path("b.json")
        with open(ev_a, "w", encoding="utf-8", newline="\n") as fh:
            json.dump([{"request_id": "p-a", "op": "rotate", "family": family,
                        "token": root, "biz_ts": "2026-11-27T09:20:00",
                        "seq": 2}], fh, ensure_ascii=False)
        with open(ev_b, "w", encoding="utf-8", newline="\n") as fh:
            json.dump([{"request_id": "p-b", "op": "rotate", "family": family,
                        "token": root, "biz_ts": "2026-11-27T09:21:00",
                        "seq": 3}], fh, ensure_ascii=False)

        procs = [
            subprocess.Popen(
                [sys.executable, "-I", RUNNER, "apply",
                 "--store", self.path("ledger"), "--events", path],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                env=dict(os.environ, PYTHONUTF8="1"),
            )
            for path in (ev_a, ev_b)
        ]
        for p in procs:
            out, err = p.communicate(timeout=30)
            self.assertEqual(p.returncode, 0, err.decode("utf-8", "replace"))

        final = Store(self.path("ledger"))
        self.assertTrue(final.verify()["ok"])
        derived = {
            rid: rcp["status"]
            for rid, rcp in final.engine.request_index.items()
            if rid in ("p-a", "p-b")
        }
        self.assertEqual(derived, {"p-a": "ok", "p-b": "reuse"})
        self.assertEqual(
            final.explain()["first_reuse_witness"]["discovery_request_id"], "p-b"
        )

    def test_shuffled_split_and_concurrent_yield_same_report_hash(self) -> None:
        # 同样输入的三种投法：整批有序、乱序、逐事件分批，审计哈希必须一致
        family = "fam-equiv"
        root = mint_token_id("root|" + family)
        events = [
            {"request_id": "q-issue", "op": "issue", "family": family,
             "biz_ts": "2026-11-27T09:00:00", "seq": 1},
            {"request_id": "q-pre", "op": "use", "family": family, "token": root,
             "biz_ts": "2026-11-27T09:02:00", "seq": 2, "session_id": "ses-q"},
            {"request_id": "q-win", "op": "rotate", "family": family, "token": root,
             "biz_ts": "2026-11-27T09:05:00", "seq": 3},
            {"request_id": "q-lose", "op": "rotate", "family": family, "token": root,
             "biz_ts": "2026-11-27T09:06:00", "seq": 4},
        ]
        one = Store(self.path("one"))
        one.commit(events)

        shuffled = Store(self.path("shuffled"))
        shuffled.commit(list(reversed(events)))

        split = Store(self.path("split"))
        for ev in events:
            split.commit([ev])

        hashes = {s.explain()["report_hash"]
                  for s in (one, shuffled, split)}
        self.assertEqual(len(hashes), 1)
        for s in (one, shuffled, split):
            self.assertTrue(s.verify()["ok"])


class FixtureCliTest(TempStoreMixin, unittest.TestCase):
    def test_fixtures_end_to_end(self) -> None:
        ledger = self.path("ledger")

        def apply(fixture: str) -> dict:
            proc = run_cli("apply", "--store", ledger,
                           "--events", os.path.join(FIXTURE_DIR, fixture))
            self.assertEqual(proc.returncode, 0, proc.stderr)
            return json.loads(proc.stdout)

        body_02 = apply("02_race.json")
        self.assertEqual([r["status"] for r in body_02["receipts"]],
                         ["ok", "ok", "ok", "reuse", "blocked_isolated"])

        body_03 = apply("03_indeterminate.json")
        self.assertTrue(all(r["status"] == "indeterminate"
                            for r in body_03["receipts"]))

        apply("04_backfill.json")

        explain = run_cli("explain", "--store", ledger)
        report = json.loads(explain.stdout)
        self.assertEqual(report["first_reuse_witness"]["discovery_request_id"],
                         "b-lose")
        iso = report["isolations"][0]
        self.assertEqual(
            [s["session_id"] for s in iso["pre_isolation_sessions"]],
            ["ses-pre"],
        )
        self.assertEqual(len(report["indeterminate_requests"]), 2)
        self.assertRegex(report["report_hash"], r"^[0-9a-f]{64}$")

        # 补登不改写补登前视图：事务 1 视角看不到 04 夹具
        asof_before = run_cli("asof", "--store", ledger,
                              "--biz-ts", "2026-11-27T23:59:59",
                              "--known-txn", "1")
        sessions = {s["session_id"] for s in json.loads(asof_before.stdout)["sessions"]}
        self.assertNotIn("ses-backfill", sessions)
        self.assertIn("ses-pre", sessions)

        verify = run_cli("verify", "--store", ledger)
        self.assertTrue(json.loads(verify.stdout)["ok"])

    def test_shuffled_fixture_matches_ordered_fixture(self) -> None:
        for fixture, store_name in (("02_race.json", "a"),
                                    ("05_race_shuffled.json", "b")):
            proc = run_cli("apply", "--store", self.path(store_name),
                           "--events", os.path.join(FIXTURE_DIR, fixture))
            self.assertEqual(proc.returncode, 0, proc.stderr)
            rep = run_cli("explain", "--store", self.path(store_name))
            hash_a_or_b = json.loads(rep.stdout)["report_hash"]
            if store_name == "a":
                hash_a = hash_a_or_b
            else:
                hash_b = hash_a_or_b
        self.assertEqual(hash_a, hash_b)

    def test_cli_rejects_bad_input_with_json_error(self) -> None:
        bad = self.path("bad.json")
        with open(bad, "w", encoding="utf-8", newline="\n") as fh:
            fh.write('{"op": "noop"}')
        proc = run_cli("apply", "--store", self.path("ledger"), "--events", bad)
        self.assertEqual(proc.returncode, 2)
        self.assertTrue(json.loads(proc.stdout)["error"])


if __name__ == "__main__":
    unittest.main()
