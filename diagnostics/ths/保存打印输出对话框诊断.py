from __future__ import annotations

import runpy
from pathlib import Path


if __name__ == "__main__":
    runpy.run_path(
        str(Path(__file__).resolve().with_name("保存打印输出对话框诊断_impl.py")),
        run_name="__main__",
    )
