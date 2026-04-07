from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_EXE_PATH = r"D:\量化\同花顺\同花顺\xiadan.exe"
DEFAULT_RUNTIME_ROOT = PROJECT_ROOT / "outputs" / "股票策略交易执行"
DEFAULT_PYTHON = PROJECT_ROOT / ".venv-trader32" / "Scripts" / "python.exe"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="统一的交易执行入口")
    parser.add_argument(
        "action",
        nargs="?",
        default="menu",
        choices=["menu", "eod", "preopen", "postcheck", "status", "stability"],
        help="执行动作，默认 menu",
    )
    parser.add_argument(
        "date",
        nargs="?",
        default="",
        help="eod 时表示 signal-date，其余模式表示 trade-date，可留空自动推断",
    )
    parser.add_argument("--python-exe", default=str(DEFAULT_PYTHON), help="交易环境 Python")
    parser.add_argument("--runtime-root", default=str(DEFAULT_RUNTIME_ROOT), help="交易运行目录")
    parser.add_argument("--exe-path", default=DEFAULT_EXE_PATH, help="同花顺下单端路径")
    parser.add_argument("--pdf-printer", default="Microsoft Print to PDF", help="PDF 打印机名称")
    parser.add_argument("--pdf-incoming-dir", default="", help="PDFCreator 自动保存接收目录")
    parser.add_argument("--disable-auto-export-pdf", action="store_true", help="关闭 preopen/postcheck 自动导出 PDF")
    parser.add_argument("--disable-sync-cash", action="store_true", help="关闭盘前空仓现金同步")
    parser.add_argument("--disable-factor-switch-gray", action="store_true", help="关闭 EOD 条件切换灰度报告")
    parser.add_argument("--factor-switch-candidate-version", default="factor_switch_v1")
    parser.add_argument("--factor-switch-policy", default="pullback_or_bull_highvol_to_core5")
    parser.add_argument("--rounds", type=int, default=3, help="stability 模式巡检轮数")
    return parser.parse_args()


def build_cmd(args: argparse.Namespace) -> list[str]:
    python_exe = Path(args.python_exe)
    if not python_exe.exists():
        raise FileNotFoundError(f"未找到交易环境 Python: {python_exe}")

    cmd = [
        str(python_exe),
        str(PROJECT_ROOT / "模拟盘懒人一键.py"),
        "--mode",
        args.action,
        "--runtime-root",
        args.runtime_root,
        "--exe-path",
        args.exe_path,
        "--python-exe",
        str(python_exe),
        "--pdf-printer",
        args.pdf_printer,
        "--rounds",
        str(args.rounds),
    ]
    if args.date:
        if args.action == "eod":
            cmd.extend(["--signal-date", args.date])
        elif args.action != "menu":
            cmd.extend(["--trade-date", args.date])
    if not args.disable_auto_export_pdf:
        cmd.append("--auto-export-pdf")
    if args.pdf_incoming_dir:
        cmd.extend(["--pdf-incoming-dir", args.pdf_incoming_dir])
    if not args.disable_sync_cash:
        cmd.append("--sync-cash")
    if not args.disable_factor_switch_gray:
        cmd.append("--enable-factor-switch-gray")
        cmd.extend(["--factor-switch-candidate-version", args.factor_switch_candidate_version])
        cmd.extend(["--factor-switch-policy", args.factor_switch_policy])
    return cmd


def main() -> None:
    args = parse_args()
    cmd = build_cmd(args)
    print("执行命令:")
    print(" ".join(f'"{part}"' if " " in part else part for part in cmd))
    raise SystemExit(subprocess.run(cmd).returncode)


if __name__ == "__main__":
    main()
