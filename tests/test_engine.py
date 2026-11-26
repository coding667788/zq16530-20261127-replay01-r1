"""引擎语义回归：轮换消耗、竞争隔离、发现前会话、缺序不确定性、幂等。"""
import unittest

from revokerace.engine import Engine
from revokerace.model import (
    ST_BLOCKED_ISOLATED,
    ST_INDETERMINATE,
    ST_OK,
    ST_REUSE,
    mint_token_id,
)

from _base import TempStoreMixin  # noqa: E402,F401  (保留统一基类风格)


def issue(family: str, seq: int, rid: str, ts: str = "2026-11-27T09:00:00") -> dict:
    return {"request_id": rid, "op": "issue", "family": family,
            "biz_ts": ts, "seq": seq}


class EngineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.family = "fam-eng"
        self.root = mint_token_id("root|" + self.family)

    def _eng_with_family(self) -> Engine:
        eng = Engine()
        eng.apply_batch([issue(self.family, 1, "e-issue")], "t1")
        return eng

    def test_rotate_consumes_token_and_mints_new_one(self) -> None:
        eng = self._eng_with_family()
        rcp = eng.apply_batch([{
            "request_id": "e-rot", "op": "rotate", "family": self.family,
            "token": self.root, "biz_ts": "2026-11-27T09:05:00", "seq": 2,
        }], "t2")
        self.assertEqual(rcp[0]["status"], ST_OK)
        self.assertEqual(rcp[0]["consumed_token"], self.root)
        self.assertIsNotNone(rcp[0]["new_token"])
        fam = eng.families[self.family]
        self.assertEqual(fam["token_state"][self.root], "consumed")
        self.assertEqual(fam["token_state"][rcp[0]["new_token"]], "active")
        # 再轮换新令牌，血缘可追溯
        rcp2 = eng.apply_batch([{
            "request_id": "e-rot2", "op": "rotate", "family": self.family,
            "token": rcp[0]["new_token"], "biz_ts": "2026-11-27T09:08:00", "seq": 3,
        }], "t3")
        self.assertEqual(rcp2[0]["status"], ST_OK)

    def test_two_competing_rotations_only_one_wins_and_family_isolated(self) -> None:
        eng = self._eng_with_family()
        # 隔离前先放行一个衍生会话
        eng.apply_batch([{
            "request_id": "e-ses-pre", "op": "use", "family": self.family,
            "token": self.root, "biz_ts": "2026-11-27T09:02:00", "seq": 2,
            "session_id": "ses-pre",
        }], "t2")
        rcps = eng.apply_batch([
            {"request_id": "e-win", "op": "rotate", "family": self.family,
             "token": self.root, "biz_ts": "2026-11-27T09:05:00", "seq": 3},
            {"request_id": "e-lose", "op": "rotate", "family": self.family,
             "token": self.root, "biz_ts": "2026-11-27T09:06:00", "seq": 4},
        ], "t3")
        self.assertEqual([r["status"] for r in rcps], [ST_OK, ST_REUSE])

        fam = eng.families[self.family]
        self.assertTrue(fam["isolated"])
        witness = fam["first_witness"]
        self.assertEqual(witness["discovery_request_id"], "e-lose")
        self.assertEqual(witness["winner_request_id"], "e-win")
        self.assertEqual(witness["reused_token"], self.root)

        # 隔离后使用被阻止
        blocked = eng.apply_batch([{
            "request_id": "e-ses-post", "op": "use", "family": self.family,
            "token": self.root, "biz_ts": "2026-11-27T09:07:00", "seq": 5,
            "session_id": "ses-post",
        }], "t4")
        self.assertEqual(blocked[0]["status"], ST_BLOCKED_ISOLATED)
        self.assertFalse(eng.sessions[-1]["admitted"])

        report = eng.explain()
        self.assertEqual(report["first_reuse_witness"]["discovery_request_id"], "e-lose")
        iso = report["isolations"][0]
        self.assertEqual([s["session_id"] for s in iso["pre_isolation_sessions"]],
                         ["ses-pre"])
        self.assertEqual([s["session_id"] for s in iso["blocked_sessions"]],
                         ["ses-post"])
        # 报告自带可复核哈希，且对同一状态稳定
        self.assertEqual(eng.explain()["report_hash"], report["report_hash"])

    def test_redelivery_returns_original_receipt(self) -> None:
        eng = self._eng_with_family()
        batch = [{
            "request_id": "e-rot", "op": "rotate", "family": self.family,
            "token": self.root, "biz_ts": "2026-11-27T09:05:00", "seq": 2,
        }]
        first = eng.apply_batch(batch, "t2")
        again = eng.apply_batch(batch, "t2")
        self.assertEqual(len(eng.events_log), 2)  # 重复不产生新事件
        self.assertTrue(again[0]["redelivered"])
        for key in ("status", "new_token", "consumed_token", "detail"):
            self.assertEqual(first[0][key], again[0][key])

    def test_missing_seq_same_second_is_indeterminate_not_input_ordered(self) -> None:
        eng = self._eng_with_family()
        # 两种输入次序都必须得到相同的不确定性裁决
        for order in (("m-revoke", "m-use"), ("m-use", "m-revoke")):
            fresh = Engine()
            fresh.apply_batch([issue(self.family, 1, "e-issue")], "t1")
            events = [
                {"request_id": "m-revoke", "op": "revoke", "family": self.family,
                 "token": self.root, "biz_ts": "2026-11-27T10:00:00", "seq": None},
                {"request_id": "m-use", "op": "use", "family": self.family,
                 "token": self.root, "biz_ts": "2026-11-27T10:00:00", "seq": None,
                 "session_id": "ses-m"},
            ]
            events.sort(key=lambda e: order.index(e["request_id"]))
            rcps = fresh.apply_batch(events, "t2")
            self.assertTrue(all(r["status"] == ST_INDETERMINATE for r in rcps))
            self.assertFalse(fresh.families[self.family]["revoked_at"])

    def test_trusted_seq_decides_same_second_order(self) -> None:
        # 同秒但带明确 seq：先注销后使用 -> use 被拒；顺序由 seq 而非输入决定
        eng = self._eng_with_family()
        events = [
            {"request_id": "m-use", "op": "use", "family": self.family,
             "token": self.root, "biz_ts": "2026-11-27T10:00:00", "seq": 11,
             "session_id": "ses-m"},
            {"request_id": "m-revoke", "op": "revoke", "family": self.family,
             "token": self.root, "biz_ts": "2026-11-27T10:00:00", "seq": 10},
        ]
        rcps = eng.apply_batch(events, "t2")
        by_id = {r["request_id"]: r["status"] for r in rcps}
        self.assertEqual(by_id["m-revoke"], ST_OK)
        self.assertEqual(by_id["m-use"], "revoked")

    def test_shuffled_input_replays_identically(self) -> None:
        eng_a = Engine()
        eng_b = Engine()
        events = [
            issue("fam-shuffle", 1, "s-issue"),
            {"request_id": "s-rot", "op": "rotate", "family": "fam-shuffle",
             "token": mint_token_id("root|fam-shuffle"),
             "biz_ts": "2026-11-27T09:05:00", "seq": 2},
            {"request_id": "s-rot2", "op": "rotate", "family": "fam-shuffle",
             "token": mint_token_id("root|fam-shuffle"),
             "biz_ts": "2026-11-27T09:06:00", "seq": 3},
        ]
        eng_a.apply_batch(events, "t1")
        eng_b.apply_batch(list(reversed(events)), "t1")
        self.assertEqual(eng_a.explain()["report_hash"],
                         eng_b.explain()["report_hash"])

    def test_use_after_consume_also_isolates(self) -> None:
        eng = self._eng_with_family()
        eng.apply_batch([{
            "request_id": "e-rot", "op": "rotate", "family": self.family,
            "token": self.root, "biz_ts": "2026-11-27T09:05:00", "seq": 2,
        }], "t2")
        rcp = eng.apply_batch([{
            "request_id": "e-reuse-use", "op": "use", "family": self.family,
            "token": self.root, "biz_ts": "2026-11-27T09:09:00", "seq": 3,
            "session_id": "ses-bad",
        }], "t3")
        self.assertEqual(rcp[0]["status"], ST_REUSE)
        self.assertTrue(eng.families[self.family]["isolated"])


if __name__ == "__main__":
    unittest.main()
