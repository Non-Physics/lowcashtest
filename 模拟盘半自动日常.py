from __future__ import annotations

import runpy
from pathlib import Path


if __name__ == "__main__":
    runpy.run_path(
        str(Path(__file__).resolve().parent / "engines" / "trading" / "模拟盘半自动日常.py"),
        run_name="__main__",
    )
