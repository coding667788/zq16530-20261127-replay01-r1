"""revokerace —— 离线票据令牌家族重放台账。

仅使用合成令牌 ID 与本地数据，不连接任何身份服务。
模块划分：
    model   常量、规范化与确定性 ID 生成
    engine  纯函数式事件重放（令牌家族、重用见证、隔离、审计）
    storage 崩溃安全的本地 WAL 批次存储（跨进程文件锁 + 原子 rename）
    cli     apply / asof / explain 三个命令入口
"""

__version__ = "1.0.0"
