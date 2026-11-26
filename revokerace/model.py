"""基础常量、事件规范化与确定性合成 ID 生成。

事件字段（合成事件，全部本地数据）：
    request_id  请求标识，用于重复投递幂等
    op          issue | rotate | use | revoke
    family      令牌家族 ID（issue 时可省略，自动合成）
    token       父/目标令牌 ID（issue 时可省略，自动合成根令牌）
    biz_ts      业务时间（ISO-8601 字符串）
    seq         单调入库编号（可信序列）；显式 null 表示缺序
    session_id  仅 use 需要；省略时由系统合成
    label       可选人工备注（如“人工补登”），不参与排序

任何带 seq 的事件都按 seq 决定先后；缺序事件之间的同秒冲突按
``engine`` 中的不确定性规则处理，绝不按输入次序裁决。
"""
from __future__ import annotations

import hashlib
from typing import Any, Optional

OPS = ("issue", "rotate", "use", "revoke")

# 回执状态
ST_OK = "ok"                     # 成功（签发/轮换/放行/注销）
ST_DUPLICATE = "duplicate"       # 重复投递，返回原回执
ST_REUSE = "reuse"               # 重用竞争：后到者触发整族隔离
ST_BLOCKED_ISOLATED = "blocked_isolated"  # 隔离发现之后的使用，阻止
ST_REVOKED = "revoked"           # 家族已注销后的操作，阻止
ST_UNKNOWN_TOKEN = "unknown_token"        # 引用了不存在的令牌
ST_INDETERMINATE = "indeterminate"        # 缺序同秒冲突，顺序不可信


def digest(text: str) -> str:
    """对文本取 16 位十六进制摘要，用于合成 ID 与批次指纹。"""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def mint_family_id(seed: str) -> str:
    return "fam-" + digest("family|" + seed)[:10]


def mint_token_id(seed: str) -> str:
    return "tok-" + digest("token|" + seed)[:14]


def mint_session_id(seed: str) -> str:
    return "ses-" + digest("session|" + seed)[:12]


def normalize_event(raw: dict[str, Any], index: int) -> dict[str, Any]:
    """把外部 JSON 事件规范化为内部统一结构。

    index 仅用于在缺少 request_id 等种子时合成确定性 ID，不参与排序。
    """
    if not isinstance(raw, dict):
        raise ValueError("事件必须是 JSON 对象")
    op = raw.get("op")
    if op not in OPS:
        raise ValueError(f"非法 op={op!r}，允许 {OPS}")

    request_id = raw.get("request_id")
    if not request_id:
        request_id = "req-auto-" + digest(f"auto|{index}|{op}")[:12]

    biz_ts = raw.get("biz_ts")
    if not biz_ts:
        raise ValueError(f"事件 {request_id} 缺少 biz_ts")
    biz_ts = str(biz_ts)

    # 显式 None / 缺失都视为“缺序”；seq 必须为非负整数
    if "seq" not in raw or raw["seq"] is None:
        seq: Optional[int] = None
    else:
        seq = raw["seq"]
        if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
            raise ValueError(f"事件 {request_id} 的 seq 必须是非负整数或 null")

    family = raw.get("family")
    token = raw.get("token")
    if op == "issue":
        seed = str(raw.get("family") or request_id)
        family = family or mint_family_id(seed)
        token = token or mint_token_id("root|" + family)
    else:
        if not family:
            raise ValueError(f"事件 {request_id}（{op}）缺少 family")
        if not token:
            raise ValueError(f"事件 {request_id}（{op}）缺少 token")

    session_id = raw.get("session_id")
    if op == "use" and not session_id:
        session_id = mint_session_id(f"{request_id}|{family}|{token}")

    return {
        "request_id": str(request_id),
        "op": op,
        "family": str(family),
        "token": str(token),
        "biz_ts": biz_ts,
        "seq": seq,
        "session_id": str(session_id) if session_id else None,
        "label": raw.get("label"),
    }


def event_seed(ev: dict[str, Any]) -> str:
    """事件的确定性内容种子（不含 index/label），用于批次指纹。"""
    return "|".join([
        ev["request_id"], ev["op"], ev["family"], ev["token"],
        ev["biz_ts"], "" if ev["seq"] is None else str(ev["seq"]),
        ev["session_id"] or "",
    ])
