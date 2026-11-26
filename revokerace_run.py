"""顶层启动脚本：允许在 Python 隔离模式（-I）下直接运行。

用法（Windows PowerShell）：
    python -I revokerace_run.py apply --store data/ledger --events fixtures/01_basic.json
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from revokerace.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
