"""纯函数式事件重放引擎。

设计要点
========
1. 每个家族维护令牌生命周期（active -> consumed/revoked）、血缘边与
   隔离/注销状态；所有状态只由规范化事件重放得到。
2. 有序事件严格按 ``seq`` 裁决：同一父令牌的两次轮换，seq 小者成功，
   seq 大者构成“重用见证”，立即隔离整族。
3. 隔离从发现时刻起阻止后续使用；发现前已放行的衍生会话保留并在
   ``explain`` 审计中明确标出（pre_isolation=true）。
4. 缺序（seq=null）事件不按输入次序取胜：同家族同业务秒的缺序事件
   聚成冲突簇，簇内一律记 indeterminate 且不改动家族状态。
5. 重复投递（request_id 已见）直接返回首次回执。
"""
from __future__ import annotations

from typing import Any, Optional

from .model import (
    ST_BLOCKED_ISOLATED,
    ST_DUPLICATE,
    ST_INDETERMINATE,
    ST_OK,
    ST_REUSE,
    ST_REVOKED,
    ST_UNKNOWN_TOKEN,
    digest,
    mint_session_id,
    mint_token_id,
    normalize_event,
)


class Engine:
    def __init__(self) -> None:
        self.families: dict[str, dict[str, Any]] = {}
        self.request_index: dict[str, dict[str, Any]] = {}
        self.seen_seq: dict[int, str] = {}
        self.sessions: list[dict[str, Any]] = []
        self.events_log: list[dict[str, Any]] = []
        self.txn_ids: list[str] = []
        # 全库轮换注册表：(family, parent_token) -> 参与竞争的有序事件。
        # 在批次开始前由全库事件构建，_do_rotate 现场查表裁决，
        # 保证跨批次/跨物理顺序的重放都收敛到同一结果。
        self.rotate_registry: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self._order = 0

    # ------------------------------------------------------------------ #
    # 批次入口
    # ------------------------------------------------------------------ #
    def apply_batch(self, raw_events: list[dict[str, Any]], txn_id: str) -> list[dict[str, Any]]:
        """重放一批事件，返回与输入等长、同序的回执列表。"""
        if txn_id not in self.txn_ids:
            self.txn_ids.append(txn_id)
        events = [normalize_event(raw, i) for i, raw in enumerate(raw_events)]

        # 1) 先做全库 seq 唯一性检查（可信序列不允许两个请求占用同一编号）
        for ev in events:
            if ev["seq"] is not None and ev["request_id"] not in self.request_index:
                if ev["seq"] in self.seen_seq:
                    raise ValueError(
                        f"seq={ev['seq']} 同时被 "
                        f"{self.seen_seq[ev['seq']]} 与 {ev['request_id']} 占用"
                    )

        # 2) 预计算本批缺序冲突簇：同家族 + 同业务秒的缺序事件
        cluster: dict[tuple[str, str], list[str]] = {}
        for ev in events:
            if ev["seq"] is None and ev["request_id"] not in self.request_index:
                cluster.setdefault((ev["family"], ev["biz_ts"]), []).append(ev["request_id"])
        ambiguous = {
            rid
            for rids in cluster.values()
            if len(rids) > 1
            for rid in rids
        }

        # 3) 两阶段重放第一阶段：在“全库事件视图”上构建轮换注册表。
        #    赢家只由可信 seq 决定，与事件物理入库/锁获取顺序无关。
        self._build_rotate_registry(events)

        # 4) 有序事件先按 seq 排，缺序事件随后（它们本身不携带可信先后）
        ordered = sorted(
            events,
            key=lambda e: (e["seq"] is None, e["seq"] if e["seq"] is not None else 0),
        )

        receipts: dict[str, dict[str, Any]] = {}
        for ev in ordered:
            receipts[ev["request_id"]] = self._apply_one(ev, txn_id, ambiguous)
        return [receipts[ev["request_id"]] for ev in events]

    def _build_rotate_registry(self, new_events: list[dict[str, Any]]) -> None:
        """收集全库（历史日志 + 本批）带可信 seq 的轮换事件，按父令牌分组。"""
        self.rotate_registry = {}
        candidates = [
            e for e in self.events_log
            if e["op"] == "rotate" and e["seq"] is not None
        ] + [
            e for e in new_events
            if e["op"] == "rotate" and e["seq"] is not None
            and e["request_id"] not in self.request_index
        ]
        for ev in candidates:
            self.rotate_registry.setdefault(
                (ev["family"], ev["token"]), []
            ).append(ev)
        for group in self.rotate_registry.values():
            group.sort(key=lambda e: e["seq"])

    def _race_role(self, fam_id: str, tok: str, request_id: str) -> Optional[str]:
        """返回该轮换请求在全库竞争中的角色：'winner' / 'loser' / None。"""
        group = self.rotate_registry.get((fam_id, tok))
        if not group:
            return None
        if group[0]["request_id"] == request_id:
            return "winner"
        if any(e["request_id"] == request_id for e in group[1:]):
            return "loser"
        return None

    def _apply_one(self, ev: dict[str, Any], txn_id: str, ambiguous: set[str]) -> dict[str, Any]:
        rid = ev["request_id"]

        # 重复投递：原样返回首次回执（幂等）
        if rid in self.request_index:
            original = self.request_index[rid]
            dup = dict(original)
            dup["redelivered"] = True
            return dup

        logged = dict(ev)
        logged["txn_id"] = txn_id
        logged["order"] = self._order
        ev["txn_id"] = txn_id
        ev["order"] = self._order
        self._order += 1
        self.events_log.append(logged)

        if ev["seq"] is not None:
            self.seen_seq[ev["seq"]] = rid

        # 缺序冲突簇：保留不确定性，不改动任何家族状态
        if rid in ambiguous:
            return self._receipt(ev, ST_INDETERMINATE,
                                 detail="同业务秒存在多个缺序事件，可信先后无法确定")

        fam = self.families.get(ev["family"])

        # 与历史已生效的缺序事件同家族同秒：同样不可裁决
        if fam is not None and ev["seq"] is None and ev["biz_ts"] in fam["float_ts"]:
            return self._receipt(ev, ST_INDETERMINATE,
                                 detail="与历史缺序事件同秒，顺序不可信")

        if ev["op"] == "issue":
            return self._do_issue(ev, fam)
        if fam is None:
            return self._receipt(ev, ST_UNKNOWN_TOKEN, detail="家族不存在")
        if ev["seq"] is None:
            fam["float_ts"].add(ev["biz_ts"])

        if ev["op"] == "rotate":
            return self._do_rotate(ev, fam)
        if ev["op"] == "use":
            return self._do_use(ev, fam)
        if ev["op"] == "revoke":
            return self._do_revoke(ev, fam)
        raise AssertionError(ev["op"])  # normalize_event 已拦截

    # ------------------------------------------------------------------ #
    # 各操作语义
    # ------------------------------------------------------------------ #
    def _do_issue(self, ev: dict[str, Any], fam: Optional[dict[str, Any]]) -> dict[str, Any]:
        if fam is not None:
            # 家族已存在：issue 同 request 不会走到这里（已幂等），
            # 不同 request 对同家族再次签发属于协议外竞争。
            return self._receipt(ev, ST_REUSE, detail="家族已被签发过")
        fam = self._new_family(ev)
        fam["token_state"][ev["token"]] = "active"
        if ev["seq"] is None:
            fam["float_ts"].add(ev["biz_ts"])
        return self._receipt(ev, ST_OK, new_token=ev["token"], detail="受控签发根令牌")

    def _do_rotate(self, ev: dict[str, Any], fam: dict[str, Any]) -> dict[str, Any]:
        tok = ev["token"]
        state = fam["token_state"].get(tok)
        if state is None:
            return self._receipt(ev, ST_UNKNOWN_TOKEN, detail="令牌不属于该家族")

        role = self._race_role(fam["id"], tok, ev["request_id"])
        is_winner = role == "winner"
        is_loser = role == "loser"

        # 竞争赢家优先：即便失败者物理先到并已隔离家族，赢家仍按可信
        # seq 完成有效轮换；隔离只用于阻止发现点之后的“其他”使用。
        if is_winner and fam["isolated"]:
            fam["isolated"] = False

        # 隔离发现之后：非赢家的一切后续使用/轮换都被阻止
        if fam["isolated"] and not is_winner:
            return self._receipt(ev, ST_BLOCKED_ISOLATED,
                                 detail=f"家族已于 {fam['isolate_at']['biz_ts']} 隔离")
        if fam["revoked_at"] is not None:
            return self._receipt(ev, ST_REVOKED, detail="家族已注销")
        if state == "revoked":
            return self._receipt(ev, ST_REVOKED, detail="令牌已注销")

        if is_loser:
            # 全库注册表已裁决：竞争失败，无论物理先后或父令牌当前状态，
            # 一律记重用并隔离整族
            self._isolate(fam, ev, tok, reason="reuse_after_consume")
            return self._receipt(ev, ST_REUSE,
                                 detail="竞争同一父令牌的后到请求，记为重用并隔离整族")

        if state == "consumed" and is_winner:
            # 赢家物理晚到：父令牌已被失败者消耗，按可信 seq 纠正血缘，
            # 赢家产出的新令牌才是有效轮换
            winner_token = mint_token_id(f"{fam['id']}|{tok}|{ev['request_id']}")
            fam["token_state"][tok] = "consumed"
            fam["token_state"][winner_token] = "active"
            fam["parent"][winner_token] = tok
            fam["children"].setdefault(tok, []).append(winner_token)
            fam["consumed_by"][tok] = {
                "request_id": ev["request_id"],
                "biz_ts": ev["biz_ts"],
                "seq": ev["seq"],
            }
            return self._receipt(ev, ST_OK, new_token=winner_token,
                                 consumed_token=tok,
                                 detail="可信 seq 最小，重放纠正为竞争赢家")

        if state == "consumed":
            # 注册表之外的消耗令牌重用（如缺序场景）：记重用并隔离
            self._isolate(fam, ev, tok, reason="reuse_after_consume")
            return self._receipt(ev, ST_REUSE,
                                 detail="父令牌已被消耗，后到竞争记为重用并隔离整族")

        if state != "active":
            return self._receipt(ev, ST_UNKNOWN_TOKEN, detail="令牌状态不可轮换")

        # state == active：正常轮换，消耗当前令牌并产生新令牌
        new_token = mint_token_id(f"{fam['id']}|{tok}|{ev['request_id']}")
        fam["token_state"][tok] = "consumed"
        fam["token_state"][new_token] = "active"
        fam["parent"][new_token] = tok
        fam["children"].setdefault(tok, []).append(new_token)
        fam["consumed_by"][tok] = {
            "request_id": ev["request_id"],
            "biz_ts": ev["biz_ts"],
            "seq": ev["seq"],
        }
        return self._receipt(ev, ST_OK, new_token=new_token,
                             consumed_token=tok, detail="轮换成功")

    def _do_use(self, ev: dict[str, Any], fam: dict[str, Any]) -> dict[str, Any]:
        tok = ev["token"]
        state = fam["token_state"].get(tok)
        if state is None:
            return self._receipt(ev, ST_UNKNOWN_TOKEN, detail="令牌不属于该家族")

        session = {
            "session_id": ev["session_id"] or mint_session_id(ev["request_id"]),
            "family": fam["id"],
            "token": tok,
            "request_id": ev["request_id"],
            "biz_ts": ev["biz_ts"],
            "seq": ev["seq"],
            "txn_id": ev["txn_id"],
            "order": ev["order"],
        }

        if fam["isolated"]:
            session.update(admitted=False, pre_isolation=False,
                           reason=ST_BLOCKED_ISOLATED)
            self.sessions.append(session)
            return self._receipt(ev, ST_BLOCKED_ISOLATED,
                                 detail="隔离发现之后的使用，已阻止",
                                 session_id=session["session_id"])

        if state == "consumed":
            # 对已消耗令牌的使用同样是重用：隔离整族，且本次不放行
            self._isolate(fam, ev, tok, reason="use_after_consume")
            session.update(admitted=False, pre_isolation=False, reason=ST_REUSE)
            self.sessions.append(session)
            return self._receipt(ev, ST_REUSE,
                                 detail="使用已消耗令牌，记为重用并隔离整族",
                                 session_id=session["session_id"])
        if fam["revoked_at"] is not None or state == "revoked":
            session.update(admitted=False, pre_isolation=False, reason=ST_REVOKED)
            self.sessions.append(session)
            return self._receipt(ev, ST_REVOKED, detail="家族/令牌已注销",
                                 session_id=session["session_id"])

        # active 且未隔离：放行；是否属于“发现前已放行的衍生会话”由隔离时刻回溯
        session.update(admitted=True,
                       pre_isolation=bool(fam["isolated"]),
                       reason=None)
        self.sessions.append(session)
        return self._receipt(ev, ST_OK, detail="会话放行",
                             session_id=session["session_id"])

    def _do_revoke(self, ev: dict[str, Any], fam: dict[str, Any]) -> dict[str, Any]:
        if fam["isolated"]:
            return self._receipt(ev, ST_BLOCKED_ISOLATED, detail="家族已隔离，注销冗余登记")
        if fam["revoked_at"] is not None:
            return self._receipt(ev, ST_DUPLICATE, detail="家族此前已注销")
        fam["revoked_at"] = {
            "request_id": ev["request_id"],
            "biz_ts": ev["biz_ts"],
            "seq": ev["seq"],
        }
        fam["token_state"][ev["token"]] = "revoked"
        return self._receipt(ev, ST_OK, detail="受控注销")

    # ------------------------------------------------------------------ #
    # 隔离与见证
    # ------------------------------------------------------------------ #
    def _isolate(self, fam: dict[str, Any], ev: dict[str, Any],
                 reused_token: str, reason: str) -> None:
        fam["isolated"] = True
        winner = fam["consumed_by"].get(reused_token)
        witness = {
            "family": fam["id"],
            "reason": reason,
            "reused_token": reused_token,
            "discovery_request_id": ev["request_id"],
            "discovery_seq": ev["seq"],
            "discovery_biz_ts": ev["biz_ts"],
            "discovery_order": self._order - 1,
            "winner_request_id": winner["request_id"] if winner else None,
            "winner_seq": winner["seq"] if winner else None,
            "winner_biz_ts": winner["biz_ts"] if winner else None,
        }
        fam["isolate_at"] = {
            "request_id": ev["request_id"],
            "biz_ts": ev["biz_ts"],
            "seq": ev["seq"],
            "order": self._order - 1,
        }
        # 首个重用见证：只记第一次（后续重用不再覆盖）
        if fam["first_witness"] is None:
            fam["first_witness"] = witness

    # ------------------------------------------------------------------ #
    # 视图
    # ------------------------------------------------------------------ #
    def asof(self, biz_ts: str, known_txn: Optional[str] = None) -> dict[str, Any]:
        """双时态快照。

        biz_ts   : 业务时间上界（含）
        known_txn: 入库事务上界（含）。人工补登虽然带旧 biz_ts，但写入的
                   是新事务；给定旧的 known_txn 即可复现“补登之前”的历史
                   视图，补登无法改写它。
        """
        sub = Engine()
        # 先按双时态窗口过滤，再保持全库重放顺序（order）逐事件重放：
        # 业务时间窗口不能改变事件之间的可信先后，否则窗口内会重排 seq。
        visible = [
            logged for logged in self.events_log
            if logged["biz_ts"] <= biz_ts
            and (known_txn is None or logged["txn_id"] <= known_txn)
        ]
        for logged in sorted(visible, key=lambda e: e["order"]):
            ev = {k: logged[k] for k in (
                "request_id", "op", "family", "token", "biz_ts", "seq",
                "session_id", "label") if k in logged}
            sub.apply_batch([ev], logged["txn_id"])
        return {
            "as_of_biz_ts": biz_ts,
            "known_txn": known_txn,
            "families": [sub._family_summary(fid) for fid in sorted(sub.families)],
            "sessions": [dict(s) for s in sub.sessions],
        }

    def explain(self) -> dict[str, Any]:
        """最终审计：首个重用见证 + 影响清单 + 不确定性登记。"""
        witnesses = [
            fam["first_witness"]
            for fid in sorted(self.families)
            if (fam := self.families[fid])["first_witness"]
        ]
        global_first = min(witnesses, key=lambda w: w["discovery_order"]) if witnesses else None

        isolations = []
        for fid in sorted(self.families):
            fam = self.families[fid]
            if not fam["isolated"]:
                continue
            pre_sessions = [
                s for s in self.sessions
                if s["family"] == fid and s["admitted"]
                and self._session_before_isolation(s, fam)
            ]
            blocked = [
                s for s in self.sessions
                if s["family"] == fid and not s["admitted"]
            ]
            isolations.append({
                "family": fid,
                "witness": fam["first_witness"],
                "isolate_at": fam["isolate_at"],
                "pre_isolation_sessions": pre_sessions,
                "blocked_sessions": blocked,
                "all_tokens": self._lineage(fam),
            })

        indeterminate = [
            rcp for rcp in self.request_index.values()
            if rcp["status"] == ST_INDETERMINATE
        ]
        report = {
            "event_count": len(self.events_log),
            "txn_count": len(self.txn_ids),
            "family_count": len(self.families),
            "first_reuse_witness": global_first,
            "isolations": isolations,
            "indeterminate_requests": indeterminate,
            "families": [self._family_summary(fid) for fid in sorted(self.families)],
        }
        # 哈希只覆盖语义内容：物理入库坐标（txn_id、order）不同的等价重放
        # （乱序投递、分批/整批、并发锁顺序反转）必须得到同一哈希。
        report["report_hash"] = digest(_canonical_json(
            self._semantic_view(report)
        ))
        report["txn_ids"] = list(self.txn_ids)
        return report

    @staticmethod
    def _semantic_view(obj: Any) -> Any:
        """剥除物理坐标字段，保留可跨等价重放复核的语义内容。"""
        drop = {"txn_id", "order", "txn_count", "discovery_order"}
        if isinstance(obj, dict):
            return {k: Engine._semantic_view(v) for k, v in obj.items()
                    if k not in drop}
        if isinstance(obj, list):
            return [Engine._semantic_view(v) for v in obj]
        return obj

    def _session_before_isolation(self, session: dict[str, Any], fam: dict[str, Any]) -> bool:
        return session["order"] < fam["isolate_at"]["order"]

    # ------------------------------------------------------------------ #
    # 序列化辅助
    # ------------------------------------------------------------------ #
    def _new_family(self, ev: dict[str, Any]) -> dict[str, Any]:
        fam = {
            "id": ev["family"],
            "token_state": {},
            "parent": {},
            "children": {},
            "consumed_by": {},
            "created_request": ev["request_id"],
            "created_biz_ts": ev["biz_ts"],
            "created_seq": ev["seq"],
            "revoked_at": None,
            "isolated": False,
            "isolate_at": None,
            "first_witness": None,
            "float_ts": set(),
        }
        self.families[ev["family"]] = fam
        return fam

    def _lineage(self, fam: dict[str, Any]) -> list[dict[str, Any]]:
        out = []
        for tok in sorted(fam["token_state"]):
            out.append({
                "token": tok,
                "state": fam["token_state"][tok],
                "parent": fam["parent"].get(tok),
                "children": list(fam["children"].get(tok, [])),
            })
        return out

    def _family_summary(self, fid: str) -> dict[str, Any]:
        fam = self.families[fid]
        return {
            "family": fid,
            "isolated": fam["isolated"],
            "isolate_at": fam["isolate_at"],
            "revoked_at": fam["revoked_at"],
            "first_witness": fam["first_witness"],
            "tokens": self._lineage(fam),
            "sessions": [dict(s) for s in self.sessions if s["family"] == fid],
        }

    def _receipt(self, ev: dict[str, Any], status: str, *,
                 detail: str = "", new_token: Optional[str] = None,
                 consumed_token: Optional[str] = None,
                 session_id: Optional[str] = None) -> dict[str, Any]:
        rcp = {
            "request_id": ev["request_id"],
            "op": ev["op"],
            "family": ev["family"],
            "token": ev["token"],
            "biz_ts": ev["biz_ts"],
            "seq": ev["seq"],
            "status": status,
            "detail": detail,
            "new_token": new_token,
            "consumed_token": consumed_token,
            "session_id": session_id,
            "redelivered": False,
        }
        self.request_index[ev["request_id"]] = rcp
        return rcp


def _canonical_json(obj: Any) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def replay_all(batches: list[tuple[str, list[dict[str, Any]]]]) -> Engine:
    """按给定 (txn_id, events) 序列从头重放，供测试与恢复复核。"""
    eng = Engine()
    for txn_id, events in batches:
        eng.apply_batch(events, txn_id)
    return eng
