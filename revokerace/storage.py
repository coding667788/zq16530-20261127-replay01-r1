"""崩溃安全的本地批次存储（事件溯源）。

事务模型
========
WAL 事务文件只保存不可变的**事件事实**：先写 ``*.tmp`` 并 fsync，
再 ``os.replace`` 原子改名。一批指令的事件要么整体可见，要么完全不可见。

回执与隔离记录**不**作为独立事实存储，而是在打开/提交时由全部已入库
事件按可信 ``seq`` 确定性重放派生（``Engine``）。因此：

* 两个客户端以任意物理顺序、任意分批方式提交同一组事件，最终派生
  出的回执、隔离、审计哈希完全一致——裁决只依赖事件携带的可信序列；
* “事件、回执、隔离记录同时生效”：原子事务落盘后重新派生，三者
  一起出现；事务不可见时三者都不存在；
* 崩溃恢复只需重放 WAL，不存在“已存回执与重放不一致”的可能。

故障点（FAULT_*）以 ``os._exit`` 模拟进程被杀，仅用于子进程测试：
    before_wal    写 WAL 之前被杀（无任何痕迹）
    after_wal     WAL 暂存文件已 fsync、改名之前被杀（残留 .tmp）
    after_commit  事务已落盘、应答之前被杀（重放时幂等返回原结果）

并发：目录内锁文件 + 字节锁（Windows 走 msvcrt，POSIX 走 fcntl），
两个客户端进程的提交在文件系统层面串行化。
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any, Optional

from .engine import Engine
from .model import event_seed, normalize_event

WAL_DIRNAME = "wal"
LOCK_FILENAME = "store.lock"
TMP_SUFFIX = ".tmp"

FAULT_BEFORE_WAL = "before_wal"
FAULT_AFTER_WAL = "after_wal"
FAULT_AFTER_COMMIT = "after_commit"
FAULT_POINTS = (FAULT_BEFORE_WAL, FAULT_AFTER_WAL, FAULT_AFTER_COMMIT)


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class ExclusiveLock:
    """跨进程排他字节锁，上下文管理器。

    gate: 可选零参回调，在真正获取字节锁之前调用，测试用来制造
    确定性的锁获取顺序；生产路径保持 None。
    """

    def __init__(self, path: str, gate=None) -> None:
        self.path = path
        self.gate = gate
        self.fh: Any = None

    def __enter__(self) -> "ExclusiveLock":
        if self.gate is not None:
            self.gate()
        self.fh = open(self.path, "a+b")
        if os.name == "nt":  # Windows / msvcrt
            import msvcrt
            msvcrt.locking(self.fh.fileno(), msvcrt.LK_LOCK, 1)
        else:  # POSIX / fcntl
            import fcntl
            fcntl.flock(self.fh.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc: Any) -> None:
        try:
            if os.name == "nt":
                import msvcrt
                self.fh.seek(0)
                msvcrt.locking(self.fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.fh.fileno(), fcntl.LOCK_UN)
        finally:
            self.fh.close()
            self.fh = None


class Store:
    def __init__(self, root: str, lock_gate=None) -> None:
        self.root = os.path.abspath(root)
        self.wal_dir = os.path.join(self.root, WAL_DIRNAME)
        self.lock_path = os.path.join(self.root, LOCK_FILENAME)
        self.lock_gate = lock_gate
        os.makedirs(self.wal_dir, exist_ok=True)
        self.committed: list[dict[str, Any]] = []  # 已提交事务（事件事实）
        self.engine = Engine()
        self._recover()

    # ------------------------------------------------------------------ #
    # 恢复与派生
    # ------------------------------------------------------------------ #
    def _recover(self) -> None:
        # 1) 清理原子改名之前的崩溃残留
        for name in os.listdir(self.wal_dir):
            if name.endswith(TMP_SUFFIX):
                os.unlink(os.path.join(self.wal_dir, name))
        # 2) 按事务序号加载事件事实
        for name in sorted(n for n in os.listdir(self.wal_dir)
                           if n.endswith(".json")):
            self._load_one(name)
        # 3) 全库事件确定性重放，派生产前回执、隔离与审计状态
        self._derive()

    def _catch_up(self) -> None:
        """持锁状态下同步其他进程并发写入的 WAL 增量并重派生。"""
        known = {p["txn_id"] for p in self.committed}
        loaded = False
        for name in sorted(os.listdir(self.wal_dir)):
            if not name.endswith(".json"):
                continue
            txn_id = name.removesuffix(".json")
            if txn_id in known:
                continue
            self._load_one(name)
            loaded = True
        if loaded:
            self._derive()

    def _load_one(self, name: str) -> None:
        expect = len(self.committed) + 1
        path = os.path.join(self.wal_dir, name)
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        n = int(name.split("-", 1)[0])
        if n != expect:
            raise RuntimeError(f"WAL 事务序号不连续：期望 {expect}，实际 {name}")
        self.committed.append(payload)

    def _derive(self) -> None:
        """从全部已提交事件从头确定性重放，派生产前状态。"""
        self.engine = Engine()
        for payload in self.committed:
            self.engine.apply_batch(payload["events"], payload["txn_id"])

    def verify(self) -> dict[str, Any]:
        """独立引擎再重放一遍，比对派生回执/隔离与当前视图是否一致。"""
        fresh = Engine()
        for payload in self.committed:
            fresh.apply_batch(payload["events"], payload["txn_id"])
        current = self.engine
        mismatches: list[dict[str, Any]] = []
        if fresh.request_index != current.request_index:
            mismatches.append({"kind": "receipts"})
        fresh_iso = {
            fid: fam["first_witness"]
            for fid, fam in fresh.families.items() if fam["isolated"]
        }
        current_iso = {
            fid: fam["first_witness"]
            for fid, fam in current.families.items() if fam["isolated"]
        }
        if fresh_iso != current_iso:
            mismatches.append({"kind": "isolations"})
        if fresh.explain()["report_hash"] != current.explain()["report_hash"]:
            mismatches.append({"kind": "report_hash"})
        return {
            "ok": not mismatches,
            "txn_count": len(self.committed),
            "event_count": len(fresh.events_log),
            "mismatches": mismatches,
        }

    # ------------------------------------------------------------------ #
    # 提交
    # ------------------------------------------------------------------ #
    def commit(self, raw_events: list[dict[str, Any]],
               fault: Optional[str] = None) -> dict[str, Any]:
        if fault is not None and fault not in FAULT_POINTS:
            raise ValueError(f"未知故障点 {fault!r}，允许 {FAULT_POINTS}")
        normalized = [normalize_event(ev, i) for i, ev in enumerate(raw_events)]
        # 引擎会在事件对象上挂 txn_id/order，落盘只保留规范化业务字段
        clean_events = [dict(ev) for ev in normalized]
        batch_id = self._batch_id(normalized)

        with ExclusiveLock(self.lock_path, self.lock_gate):
            # 同步并发进程的增量，保证幂等判断与序号分配基于最新全库
            self._catch_up()

            # 幂等：同一批内容（规范化事件指纹）重放直接返回原事务与派生结果
            for payload in self.committed:
                if payload["batch_id"] == batch_id:
                    return self._answer(payload, replayed=True)

            if fault == FAULT_BEFORE_WAL:
                self._crash(FAULT_BEFORE_WAL)

            txn_no = len(self.committed) + 1
            txn_id = f"{txn_no:06d}-{batch_id[:12]}"
            payload = {
                "batch_id": batch_id,
                "txn_id": txn_id,
                "events": clean_events,
            }
            tmp_path = os.path.join(self.wal_dir, txn_id + ".json" + TMP_SUFFIX)
            final_path = os.path.join(self.wal_dir, txn_id + ".json")
            with open(tmp_path, "w", encoding="utf-8", newline="\n") as fh:
                json.dump(payload, fh, ensure_ascii=False, sort_keys=True, indent=2)
                fh.write("\n")
                fh.flush()
                os.fsync(fh.fileno())

            if fault == FAULT_AFTER_WAL:
                self._crash(FAULT_AFTER_WAL)

            os.replace(tmp_path, final_path)
            self.committed.append(payload)
            self._derive()  # 事件落盘后，回执与隔离原子地一起派生出来

            if fault == FAULT_AFTER_COMMIT:
                self._crash(FAULT_AFTER_COMMIT)

            return self._answer(payload, replayed=False)

    # ------------------------------------------------------------------ #
    # 视图与应答
    # ------------------------------------------------------------------ #
    def _answer(self, payload: dict[str, Any], replayed: bool) -> dict[str, Any]:
        """从事务事件在当前全库视图中取出派生回执与本批隔离记录。"""
        rids = {e["request_id"] for e in payload["events"]}
        receipts = [
            self.engine.request_index[rid]
            for rid in rids
            if rid in self.engine.request_index
        ]
        # 保持与输入事件相同的回执顺序
        order = {e["request_id"]: i for i, e in enumerate(payload["events"])}
        receipts.sort(key=lambda r: order[r["request_id"]])
        isolations = [
            {
                "family": fid,
                "witness": fam["first_witness"],
                "isolate_at": fam["isolate_at"],
            }
            for fid, fam in sorted(self.engine.families.items())
            if fam["isolated"]
            and fam["isolate_at"]["request_id"] in rids
        ]
        return {
            "replayed": replayed,
            "batch_id": payload["batch_id"],
            "txn_id": payload["txn_id"],
            "receipts": receipts,
            "isolations": isolations,
        }

    def asof(self, biz_ts: str, known_txn: Optional[str] = None) -> dict[str, Any]:
        return self.engine.asof(biz_ts, self._resolve_txn(known_txn))

    def explain(self) -> dict[str, Any]:
        return self.engine.explain()

    def _resolve_txn(self, known_txn: Optional[str]) -> Optional[str]:
        if known_txn is None:
            return None
        if known_txn.isdigit():  # 允许只给事务序号
            for payload in self.committed:
                if payload["txn_id"].startswith(f"{int(known_txn):06d}-"):
                    return payload["txn_id"]
            raise ValueError(f"事务序号 {known_txn} 不存在")
        if not any(p["txn_id"] == known_txn for p in self.committed):
            raise ValueError(f"事务 {known_txn} 不存在")
        return known_txn

    @staticmethod
    def _batch_id(normalized: list[dict[str, Any]]) -> str:
        import hashlib
        seeds = [event_seed(ev) for ev in normalized]
        return hashlib.sha256(canonical_json(seeds).encode("utf-8")).hexdigest()

    @staticmethod
    def _crash(fault: str) -> None:
        # 模拟进程在故障点被杀：不跑任何 finally/缓冲刷新，直接退出。
        # 信息保持 ASCII，避免不同控制台代码页导致的管道解码问题。
        sys.stderr.write(f"[revokerace] controlled fault at {fault}, exiting\n")
        sys.stderr.flush()
        os._exit(99)
