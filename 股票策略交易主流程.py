from __future__ import annotations

import runpy
from pathlib import Path


if __name__ == "__main__":
    runpy.run_path(
        str(Path(__file__).resolve().parent / "engines" / "trading" / "股票策略交易主流程.py"),
        run_name="__main__",
    )
