from __future__ import annotations

import runpy
from pathlib import Path


if __name__ == "__main__":
    runpy.run_path(
        str(Path(__file__).resolve().parent / "diagnostics" / "ths" / "同花顺控件诊断.py"),
        run_name="__main__",
    )
