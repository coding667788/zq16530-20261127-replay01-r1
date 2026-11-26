"""命令行入口：apply / asof / explain，stdout 一律输出 UTF-8 JSON。"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .storage import FAULT_POINTS, Store


def _emit(obj: Any, code: int = 0) -> int:
    # 强制 UTF-8 输出，与控制台当前代码页解耦；newline 固定为 LF
    try:
        sys.stdout.reconfigure(encoding="utf-8", newline="\n")
    except (AttributeError, ValueError):
        pass
    json.dump(obj, sys.stdout, ensure_ascii=False, sort_keys=True, indent=2)
    sys.stdout.write("\n")
    return code


def _error(message: str) -> int:
    return _emit({"ok": False, "error": message}, code=2)


def _load_events(path: str) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if isinstance(data, dict) and "events" in data:
        data = data["events"]
    if not isinstance(data, list):
        raise ValueError("事件文件必须是 JSON 数组，或含 events 数组的对象")
    return data


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="revokerace",
        description="离线票据令牌家族重放台账（合成 ID，本地数据，无身份服务）",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_apply = sub.add_parser("apply", help="重放一批合成事件并原子落盘")
    p_apply.add_argument("--store", required=True, help="本地台账目录")
    p_apply.add_argument("--events", required=True, help="事件 JSON 文件路径")
    p_apply.add_argument("--fault", choices=FAULT_POINTS, default=None,
                         help="受控故障点（仅用于崩溃测试，子进程内 os._exit）")

    p_asof = sub.add_parser("asof", help="双时态历史视图（业务时间 + 入库事务上界）")
    p_asof.add_argument("--store", required=True)
    p_asof.add_argument("--biz-ts", required=True, help="业务时间上界（含），ISO-8601")
    p_asof.add_argument("--known-txn", default=None,
                        help="入库事务上界（完整 txn_id 或事务序号）；缺省为当前")

    p_explain = sub.add_parser("explain", help="最终审计：首个重用见证与影响清单")
    p_explain.add_argument("--store", required=True)

    p_verify = sub.add_parser("verify", help="从零重放复核回执与隔离记录")
    p_verify.add_argument("--store", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "apply":
            events = _load_events(args.events)
            store = Store(args.store)
            result = store.commit(events, fault=args.fault)
            result["ok"] = True
            return _emit(result)

        store = Store(args.store)
        if args.command == "asof":
            return _emit(store.asof(args.biz_ts, args.known_txn))
        if args.command == "explain":
            return _emit(store.explain())
        if args.command == "verify":
            return _emit(store.verify())
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
        return _error(f"{type(exc).__name__}: {exc}")
    return _error(f"未知命令 {args.command!r}")


if __name__ == "__main__":
    raise SystemExit(main())
