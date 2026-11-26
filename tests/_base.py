"""unittest 公共辅助：路径引导与临时目录。"""
import os
import sys
import tempfile

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from revokerace.model import mint_token_id  # noqa: E402,F401


class TempStoreMixin:
    def setUp(self) -> None:  # noqa: N802 (unittest 命名)
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = self._tmp.name

    def tearDown(self) -> None:  # noqa: N802
        self._tmp.cleanup()

    def path(self, *parts: str) -> str:
        return os.path.join(self.tmp, *parts)
