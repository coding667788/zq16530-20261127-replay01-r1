"""生成 revokerace 合成夹具。

所有 ID 均由内容确定性合成（model.mint_*），无外部身份服务。
直接运行：``python tools\\make_fixtures.py``，或通过 unittest 间接校验。
"""
from __future__ import annotations

import json
import os
import sys

# 允许在隔离模式（python -I）下直接运行：先把仓库根目录放进 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from revokerace.model import mint_session_id, mint_token_id

FIXTURE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "fixtures")


def write(name: str, events: list[dict]) -> str:
    path = os.path.join(FIXTURE_DIR, name)
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        json.dump({"fixture": name.removesuffix(".json"), "events": events},
                  fh, ensure_ascii=False, sort_keys=True, indent=2)
        fh.write("\n")
    return path


def build() -> list[str]:
    os.makedirs(FIXTURE_DIR, exist_ok=True)
    paths: list[str] = []

    # 01 基本生命周期：签发 -> 会话 -> 轮换 -> 再会话，无竞争
    root_a = mint_token_id("root|fam-alpha")
    paths.append(write("01_basic.json", [
        {"request_id": "a-issue", "op": "issue", "family": "fam-alpha",
         "biz_ts": "2026-11-27T09:00:00", "seq": 1},
        {"request_id": "a-use-1", "op": "use", "family": "fam-alpha",
         "token": root_a, "biz_ts": "2026-11-27T09:02:00", "seq": 2,
         "session_id": mint_session_id("a-use-1|fam-alpha|" + root_a)},
        {"request_id": "a-rotate-1", "op": "rotate", "family": "fam-alpha",
         "token": root_a, "biz_ts": "2026-11-27T09:05:00", "seq": 3},
    ]))

    # 02 同父令牌竞争：两个 rotate 竞争 root，后到者重用并隔离整族；
    #    隔离前已有放行会话 ses-pre，隔离后的使用被阻止
    root_b = mint_token_id("root|fam-bravo")
    paths.append(write("02_race.json", [
        {"request_id": "b-issue", "op": "issue", "family": "fam-bravo",
         "biz_ts": "2026-11-27T09:00:00", "seq": 1},
        {"request_id": "b-pre-session", "op": "use", "family": "fam-bravo",
         "token": root_b, "biz_ts": "2026-11-27T09:03:00", "seq": 2,
         "session_id": "ses-pre"},
        {"request_id": "b-win", "op": "rotate", "family": "fam-bravo",
         "token": root_b, "biz_ts": "2026-11-27T09:05:00", "seq": 3},
        {"request_id": "b-lose", "op": "rotate", "family": "fam-bravo",
         "token": root_b, "biz_ts": "2026-11-27T09:06:00", "seq": 4},
        {"request_id": "b-after-session", "op": "use", "family": "fam-bravo",
         "token": root_b, "biz_ts": "2026-11-27T09:07:00", "seq": 5,
         "session_id": "ses-post"},
    ]))

    # 03 同秒缺序：revoke 与 use 都缺 seq 且同业务秒 -> 双双 indeterminate
    paths.append(write("03_indeterminate.json", [
        {"request_id": "c-revoke", "op": "revoke", "family": "fam-bravo",
         "token": root_b, "biz_ts": "2026-11-27T10:00:00", "seq": None},
        {"request_id": "c-use", "op": "use", "family": "fam-bravo",
         "token": root_b, "biz_ts": "2026-11-27T10:00:00", "seq": None,
         "session_id": "ses-amb"},
    ]))

    # 04 人工补登：业务时间很早（08:30），但在第 2 个事务才入库；
    #    asof --known-txn 1 必须看不到它（历史视图不可被补登改写）
    paths.append(write("04_backfill.json", [
        {"request_id": "b-backfill-use", "op": "use", "family": "fam-bravo",
         "token": root_b, "biz_ts": "2026-11-27T08:30:00", "seq": 100,
         "session_id": "ses-backfill", "label": "人工补登"},
    ]))

    # 05 乱序投递版 02：事件故意打乱，seq 不变，结果必须与 02 完全一致
    paths.append(write("05_race_shuffled.json", [
        {"request_id": "b-lose", "op": "rotate", "family": "fam-bravo",
         "token": root_b, "biz_ts": "2026-11-27T09:06:00", "seq": 4},
        {"request_id": "b-issue", "op": "issue", "family": "fam-bravo",
         "biz_ts": "2026-11-27T09:00:00", "seq": 1},
        {"request_id": "b-after-session", "op": "use", "family": "fam-bravo",
         "token": root_b, "biz_ts": "2026-11-27T09:07:00", "seq": 5,
         "session_id": "ses-post"},
        {"request_id": "b-pre-session", "op": "use", "family": "fam-bravo",
         "token": root_b, "biz_ts": "2026-11-27T09:03:00", "seq": 2,
         "session_id": "ses-pre"},
        {"request_id": "b-win", "op": "rotate", "family": "fam-bravo",
         "token": root_b, "biz_ts": "2026-11-27T09:05:00", "seq": 3},
    ]))
    return paths


if __name__ == "__main__":
    for p in build():
        print(p)
