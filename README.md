# revokerace — 离线票据令牌家族重放台账

按**带业务时间（`biz_ts`）和单调入库编号（`seq`）的合成事件**重放令牌家族的
离线台账。只使用合成令牌 ID 与本地文件数据，**不连接任何身份服务**，
纯 Python 3.12 标准库实现，可在 Windows 普通 CPU 上直接运行。

## 核心语义

### 令牌家族与轮换
- `issue` 受控签发一个家族的根令牌（ID 由内容确定性合成）。
- `rotate` 轮换：**消耗一个当前（active）令牌并产生一个新令牌**，
  父子血缘完整保留，可在 `explain` 中追溯。
- `use` 用令牌放行一个衍生会话；`revoke` 受控注销。

### 竞争、重用见证与整族隔离
- 两个不同请求竞争同一父令牌时，**只有 `seq` 最小的请求成功**；
  其余请求记为 `reuse`，并产生“首个重用见证”，**立即隔离整族**。
- 裁决只依赖事件携带的可信 `seq`，与物理入库顺序、锁获取顺序、
  分批方式全部解耦（见下方“事件溯源”说明）。
- 隔离**从发现时刻起**阻止后续使用（`blocked_isolated`）；
  发现前已经放行的衍生会话不会被抹掉，而会在 `explain` 的
  `pre_isolation_sessions` 中明确标出。

### 同秒生效与缺序不确定性
- 受控签发与注销可在同一业务秒生效：带明确 `seq` 时严格按 `seq` 排。
- `seq=null`（缺序）事件**不按输入次序取胜**：同家族、同业务秒的
  缺序事件聚成冲突簇，簇内一律记 `indeterminate`，且不改动家族状态，
  把不确定性原样保留下来。

### 幂等
- 同一 `request_id` 重复投递返回首次回执（`redelivered=true`），
  不产生新事件。
- 同一批事件（规范化内容指纹相同）重复提交返回原事务结果（`replayed=true`）。

## 三个入口

命令 stdout 一律输出 **UTF-8 JSON**（`ensure_ascii=False`，中文不转义）。

```powershell
# 1) apply：重放一批事件并原子落盘
python -I revokerace_run.py apply --store <台账目录> --events <事件.json>

# 2) asof：双时态历史视图
#    --biz-ts   业务时间上界（含）
#    --known-txn 入库事务上界（完整 txn_id 或事务序号，可省略=当前）
python -I revokerace_run.py asof --store <台账目录> `
    --biz-ts 2026-11-27T23:59:59 --known-txn 1

# 3) explain：最终审计（首个重用见证 + 影响清单 + 不确定性登记 + 报告哈希）
python -I revokerace_run.py explain --store <台账目录>

# 辅助：从零重放复核派生回执/隔离/审计哈希
python -I revokerace_run.py verify --store <台账目录>
```

### 双时态与“人工补登不能改写历史”
每个事件有两个时间维度：业务时间 `biz_ts` 与入库事务 `txn_id`。
人工补登可以带一个很早的 `biz_ts`，但它写入的是**新事务**。
`asof --known-txn 1` 只重放第 1 个事务及之前的事件，因此复现的是
“补登发生之前”的历史视图，补登无法改写它（测试
`test_backfill_cannot_rewrite_prior_historical_view` 固化此保证）。

## 事件溯源与崩溃一致性

WAL 事务文件（`<store>/wal/NNNNNN-<指纹>.json`）**只保存不可变的事件事实**；
回执、隔离记录、审计状态都在打开/提交时由全部已入库事件**确定性重放派生**。

- 提交：先写 `*.tmp` 并 `fsync`，再 `os.replace` 原子改名。
  一批指令的事件要么整体可见、要么完全不可见；事件一旦可见，
  回执与隔离就被一起派生——三者同时生效，不存在半成品。
- 三个可控故障点（`apply --fault`，子进程内 `os._exit(99)`）：
  - `before_wal`：写 WAL 前被杀，无任何痕迹；
  - `after_wal`：暂存文件已 fsync、改名前被杀，残留 `.tmp`，恢复时清理；
  - `after_commit`：事务已落盘、应答前被杀，重放时幂等返回原结果。
- 并发：目录锁文件 + 字节锁（Windows `msvcrt` / POSIX `fcntl`），
  两个客户端进程的提交在文件系统层面串行；提交锁内会 `catch-up`
  其他进程的 WAL 增量后再重派生。

## 目录结构

```
revokerace/
  model.py      常量、事件规范化、确定性合成 ID
  engine.py     纯函数式重放引擎（家族、竞争裁决、隔离、审计、as-of）
  storage.py    崩溃安全 WAL + 跨进程锁 + 故障点注入 + 复核
  cli.py        apply / asof / explain / verify 命令
revokerace_run.py   顶层启动器（支持 python -I）
run_tests.py        unittest 启动器（支持 python -I）
fixtures/           合成夹具（由 tools/make_fixtures.py 确定性生成）
tests/              可直接运行的 unittest
tools/make_fixtures.py
```

## 合成夹具

| 文件 | 场景 |
|---|---|
| `01_basic.json` | 签发 → 会话 → 轮换的正常生命周期 |
| `02_race.json` | 同父令牌竞争：赢家/重用/隔离 + 发现前会话 + 隔离后阻断 |
| `03_indeterminate.json` | 同秒双缺序事件，双双 indeterminate |
| `04_backfill.json` | 旧业务时间的人工补登（新事务），用于双时态验证 |
| `05_race_shuffled.json` | 与 02 同内容、输入乱序，审计哈希必须一致 |

重新生成：`python -I tools/make_fixtures.py`

## 运行测试

```powershell
python run_tests.py          # 普通模式
python -I run_tests.py       # 隔离模式（推荐，模拟安全约束）
# 或
python -m unittest discover -s tests
```

测试覆盖（20 个用例）：
- 轮换消耗/产出、血缘；竞争只一个赢家、整族隔离；发现前会话标注；
- 重复投递幂等；缺序同秒不确定性；可信 seq 同秒裁决；乱序输入哈希一致；
- 三个故障点的子进程崩溃 + 另起进程事务一致 + 原子性；
- 双时态补登不可改写历史；分批 vs 整批哈希一致；
- 两客户端并发（含反向锁获取顺序）最终裁决一致；CLI 端到端与错误处理。

## 回执状态

| status | 含义 |
|---|---|
| `ok` | 成功（签发/轮换/放行/注销） |
| `reuse` | 竞争失败或使用已消耗令牌，触发整族隔离 |
| `blocked_isolated` | 隔离发现之后的使用，被阻止 |
| `revoked` | 家族/令牌已注销后的操作 |
| `indeterminate` | 缺序同秒冲突，可信先后无法确定 |
| `duplicate` | 重复投递/重复注销 |
| `unknown_token` | 引用了不存在的令牌 |
