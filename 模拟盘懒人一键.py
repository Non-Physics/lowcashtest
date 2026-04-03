from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_RUNTIME_ROOT = PROJECT_ROOT / "outputs" / "股票策略交易执行"
DEFAULT_EXE_PATH = r"D:\量化\同花顺\同花顺\xiadan.exe"
DEFAULT_DATA_ROOT = PROJECT_ROOT / "data" / "stock_data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="模拟盘懒人一键入口")
    parser.add_argument("--runtime-root", default=str(DEFAULT_RUNTIME_ROOT), help="交易运行目录")
    parser.add_argument("--exe-path", default=DEFAULT_EXE_PATH, help="同花顺下单端 exe 路径")
    parser.add_argument("--python-exe", default=sys.executable, help="调用子脚本的 Python 解释器")
    parser.add_argument(
        "--mode",
        choices=["menu", "eod", "preopen", "postcheck", "status", "stability"],
        default="menu",
        help="执行模式，默认 menu 交互菜单",
    )
    parser.add_argument("--signal-date", default="", help="信号日期，可留空自动推断")
    parser.add_argument("--trade-date", default="", help="执行日期，可留空自动推断")
    parser.add_argument("--rounds", type=int, default=3, help="稳定性巡检轮数")
    parser.add_argument("--refresh-data", action="store_true", help="EOD 时先刷新数据")
    parser.add_argument("--sync-cash", action="store_true", help="盘前若空仓则同步券商现金")
    return parser.parse_args()


def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def load_open_days() -> list[pd.Timestamp]:
    path = DEFAULT_DATA_ROOT / "metadata" / "trading_calendar.csv"
    if not path.exists():
        return []
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"])
    return [pd.Timestamp(x).normalize() for x in df[df["is_open"] == 1]["date"].tolist()]


def infer_latest_signal_date() -> str | None:
    state_dir = Path(args.runtime_root) / "state"
    planned_files = sorted(state_dir.glob("planned_orders_*.json"))
    if not planned_files:
        return None
    latest = planned_files[-1]
    payload = load_json(latest, default={})
    signal_date = payload.get("signal_date")
    if signal_date:
        return str(signal_date)
    return None


def infer_due_trade_date(runtime_root: Path) -> str | None:
    state_dir = runtime_root / "state"
    due_dates: list[str] = []
    for path in sorted(state_dir.glob("planned_orders_*.json")):
        payload = load_json(path, default={"orders": []})
        for item in payload.get("orders", []):
            if item.get("status", "planned") == "planned" and item.get("execute_date"):
                due_dates.append(str(item["execute_date"]))
    if due_dates:
        return sorted(due_dates)[0]
    return None


def infer_signal_date(runtime_root: Path) -> str:
    explicit = args.signal_date.strip()
    if explicit:
        return explicit
    latest_signal = infer_latest_signal_date()
    if latest_signal:
        return latest_signal
    open_days = load_open_days()
    if not open_days:
        raise RuntimeError("无法推断 signal-date：未找到交易日历。")
    today = pd.Timestamp.today().normalize()
    candidates = [d for d in open_days if d <= today]
    if candidates:
        return str(candidates[-1].date())
    return str(open_days[-1].date())


def infer_trade_date(runtime_root: Path) -> str:
    explicit = args.trade_date.strip()
    if explicit:
        return explicit
    due_date = infer_due_trade_date(runtime_root)
    if due_date:
        return due_date
    latest_signal = infer_signal_date(runtime_root)
    open_days = load_open_days()
    current = pd.Timestamp(latest_signal).normalize()
    future = [d for d in open_days if d > current]
    if future:
        return str(future[0].date())
    raise RuntimeError("无法推断 trade-date：没有待执行订单，也找不到下一交易日。")


def run_and_print(cmd: list[str]) -> int:
    print("执行命令:")
    print(" ".join(f'"{c}"' if " " in c else c for c in cmd))
    result = subprocess.run(cmd, text=True, encoding="utf-8", errors="replace")
    return result.returncode


def build_daily_cmd(command: str, signal_date: str = "", trade_date: str = "") -> list[str]:
    base = [args.python_exe, str(PROJECT_ROOT / "模拟盘半自动日常.py"), "--runtime-root", args.runtime_root, "--exe-path", args.exe_path]
    if command == "eod":
        cmd = base + ["eod", "--signal-date", signal_date]
        if args.refresh_data:
            cmd.append("--refresh-data")
        return cmd
    if command == "preopen":
        cmd = base + ["preopen", "--trade-date", trade_date]
        if args.sync_cash:
            cmd.append("--sync-cash")
        return cmd
    if command == "postcheck":
        cmd = base + ["postcheck", "--trade-date", trade_date]
        manual_status = getattr(args, "_manual_status", "").strip()
        manual_note = getattr(args, "_manual_note", "").strip()
        if manual_status:
            cmd.extend(["--manual-status", manual_status])
        if manual_note:
            cmd.extend(["--manual-note", manual_note])
        return cmd
    if command == "status":
        cmd = base + ["status", "--trade-date", trade_date]
        if signal_date:
            cmd.extend(["--signal-date", signal_date])
        return cmd
    raise ValueError(command)


def build_stability_cmd(trade_date: str) -> list[str]:
    return [
        args.python_exe,
        str(PROJECT_ROOT / "模拟盘稳定性巡检.py"),
        "--runtime-root",
        args.runtime_root,
        "--exe-path",
        args.exe_path,
        "--date",
        trade_date,
        "--rounds",
        str(args.rounds),
    ]


def print_context(runtime_root: Path) -> None:
    due_trade_date = infer_due_trade_date(runtime_root)
    latest_signal = infer_latest_signal_date()
    print("=" * 72)
    print("模拟盘懒人一键入口")
    print(f"运行目录: {runtime_root}")
    print(f"同花顺路径: {args.exe_path}")
    print(f"最新信号日: {latest_signal or 'N/A'}")
    print(f"最近待执行日: {due_trade_date or 'N/A'}")
    print("=" * 72)


def menu(runtime_root: Path) -> int:
    print_context(runtime_root)
    signal_date = infer_signal_date(runtime_root)
    trade_date = infer_trade_date(runtime_root)
    print("1. 收盘后生成信号")
    print("2. 盘前对账 + 预览")
    print("3. 人工下单后复核")
    print("4. 查看当前摘要")
    print("5. 稳定性巡检")
    choice = input("输入序号 [1-5]: ").strip()

    if choice == "1":
        user_date = input(f"signal-date [{signal_date}]: ").strip() or signal_date
        return run_and_print(build_daily_cmd("eod", signal_date=user_date))
    if choice == "2":
        user_date = input(f"trade-date [{trade_date}]: ").strip() or trade_date
        if not args.sync_cash:
            sync = input("空仓时是否同步模拟盘现金到本地? [y/N]: ").strip().lower()
            if sync in {"y", "yes"}:
                args.sync_cash = True
        return run_and_print(build_daily_cmd("preopen", trade_date=user_date))
    if choice == "3":
        user_date = input(f"trade-date [{trade_date}]: ").strip() or trade_date
        args._manual_status = input("人工结果 [已成交/未成交/已撤单/部分成交，可留空]: ").strip()
        args._manual_note = input("人工备注 [可留空]: ").strip()
        return run_and_print(build_daily_cmd("postcheck", trade_date=user_date))
    if choice == "4":
        return run_and_print(build_daily_cmd("status", signal_date=signal_date, trade_date=trade_date))
    if choice == "5":
        user_date = input(f"trade-date [{trade_date}]: ").strip() or trade_date
        return run_and_print(build_stability_cmd(user_date))
    print("无效选择。")
    return 1


def main() -> None:
    global args
    args = parse_args()
    runtime_root = Path(args.runtime_root)
    runtime_root.mkdir(parents=True, exist_ok=True)

    signal_date = infer_signal_date(runtime_root)
    trade_date = infer_trade_date(runtime_root)

    if args.mode == "menu":
        raise SystemExit(menu(runtime_root))
    if args.mode == "eod":
        raise SystemExit(run_and_print(build_daily_cmd("eod", signal_date=signal_date)))
    if args.mode == "preopen":
        raise SystemExit(run_and_print(build_daily_cmd("preopen", trade_date=trade_date)))
    if args.mode == "postcheck":
        raise SystemExit(run_and_print(build_daily_cmd("postcheck", trade_date=trade_date)))
    if args.mode == "status":
        raise SystemExit(run_and_print(build_daily_cmd("status", signal_date=signal_date, trade_date=trade_date)))
    if args.mode == "stability":
        raise SystemExit(run_and_print(build_stability_cmd(trade_date)))


if __name__ == "__main__":
    main()
