"""unittest 启动器：兼容 Python 隔离模式（python -I）。

普通模式与隔离模式均可运行：
    python run_tests.py
    python -I run_tests.py
"""
import os
import sys
import unittest

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
TESTS_DIR = os.path.join(REPO_ROOT, "tests")
for path in (REPO_ROOT, TESTS_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)


def main() -> int:
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=TESTS_DIR, top_level_dir=REPO_ROOT)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
