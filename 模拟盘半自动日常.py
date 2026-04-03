from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from stock_trading.common import (
    build_runtime_paths,
    dump_json,
    ensure_runtime_dirs,
    load_json,
    load_state,
    save_state,
)
from stock_trading.signal_service import StrategySignalService

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_RUNTIME_ROOT = PROJECT_ROOT / "outputs" / "股票策略交易执行"
DEFAULT_EXE_PATH = r"D:\量化\同花顺\同花顺\xiadan.exe"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="模拟盘半自动日常脚本")
    parser.add_argument("--runtime-root", default=str(DEFAULT_RUNTIME_ROOT), help="交易运行目录")
    parser.add_argument("--exe-path", default=DEFAULT_EXE_PATH, help="同花顺下单端 exe 路径")
    parser.add_argument("--client-type", default="ths", help="easytrader client type")
    parser.add_argument("--python-exe", default=sys.executable, help="调用主流程脚本的 Python 解释器")
    parser.add_argument("--sleep-seconds", type=float, default=1.0, help="阶段内命令间等待秒数")

    subparsers = parser.add_subparsers(dest="command", required=True)

    eod = subparsers.add_parser("eod", help="收盘后：刷新数据、生成次日信号、输出摘要")
    eod.add_argument("--signal-date", required=True, help="信号日期，例如 2026-04-03")
    eod.add_argument("--refresh-data", action="store_true", help="先刷新并检查数据")
    eod.add_argument("--strategy-version", default="baseline_live_v1")
    eod.add_argument("--parameter-version", default="baseline_default")

    preopen = subparsers.add_parser("preopen", help="盘前：读取账户、同步现金、预览待提交订单")
    preopen.add_argument("--trade-date", required=True, help="执行日期，例如 2026-04-07")
    preopen.add_argument("--sync-cash", action="store_true", help="若本地空仓，则用模拟盘现金覆盖本地现金")

    post = subparsers.add_parser("postcheck", help="人工下单后：再次对账并生成复核摘要")
    post.add_argument("--trade-date", required=True, help="执行日期，例如 2026-04-07")
    post.add_argument("--manual-status", default="", help="人工执行结果，例如 已成交/未成交/已撤单/部分成交")
    post.add_argument("--manual-note", default="", help="人工备注，例如 实际买入了哪只股票、成交价、失败原因")

    status = subparsers.add_parser("status", help="读取最近的信号、预览和对账文件，输出当日摘要")
    status.add_argument("--trade-date", required=True, help="执行日期，例如 2026-04-07")
    status.add_argument("--signal-date", default="", help="可选，指定信号日期")

    return parser.parse_args()


def run_command(cmd: list[str]) -> dict[str, Any]:
    started_at = time.strftime("%Y-%m-%d %H:%M:%S")
    completed = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    finished_at = time.strftime("%Y-%m-%d %H:%M:%S")
    return {
        "command": cmd,
        "returncode": completed.returncode,
        "started_at": started_at,
        "finished_at": finished_at,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def build_flow_command(
    python_exe: str,
    runtime_root: Path,
    action: str,
    date_value: str,
    client_type: str,
    exe_path: str,
    strategy_version: str | None = None,
    parameter_version: str | None = None,
) -> list[str]:
    cmd = [
        python_exe,
        str(PROJECT_ROOT / "股票策略交易主流程.py"),
        "--runtime-root",
        str(runtime_root),
        action,
        "--date",
        date_value,
    ]
    if action == "generate-signals":
        if strategy_version is not None:
            cmd.extend(["--strategy-version", strategy_version])
        if parameter_version is not None:
            cmd.extend(["--parameter-version", parameter_version])
    if action.startswith("ths-"):
        cmd.extend(["--client-type", client_type, "--exe-path", exe_path])
    return cmd


def daily_report_path(runtime_root: Path, tag: str) -> Path:
    return runtime_root / "reports" / f"semi_auto_{tag}.json"


def log_path(runtime_root: Path, tag: str) -> Path:
    return runtime_root / "logs" / f"semi_auto_{tag}.jsonl"


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def summarize_signal(runtime_root: Path, signal_date: str) -> dict[str, Any]:
    tag = signal_date.replace("-", "")
    signal_report = load_json(runtime_root / "reports" / f"signal_report_{tag}.json", default={})
    planned_orders = load_json(runtime_root / "state" / f"planned_orders_{tag}.json", default={"orders": []})
    orders = planned_orders.get("orders", []) if isinstance(planned_orders, dict) else []
    execute_dates = sorted({item.get("execute_date") for item in orders if item.get("execute_date")})
    return {
        "signal_date": signal_date,
        "order_count": len(orders),
        "buy_count": sum(item.get("action") == "BUY" for item in orders),
        "sell_count": sum(item.get("action") == "SELL" for item in orders),
        "execute_dates": execute_dates,
        "risk_on": signal_report.get("risk_on"),
        "risk_off": signal_report.get("risk_off"),
        "estimated_equity": signal_report.get("estimated_equity"),
        "signal_report_path": str(runtime_root / "reports" / f"signal_report_{tag}.json"),
        "planned_orders_path": str(runtime_root / "state" / f"planned_orders_{tag}.json"),
    }


def summarize_trade(runtime_root: Path, trade_date: str) -> dict[str, Any]:
    tag = trade_date.replace("-", "")
    preview = load_json(runtime_root / "reports" / f"ths_preview_{tag}.json", default={})
    reconcile = load_json(runtime_root / "reports" / f"reconcile_ths_{tag}.json", default={})
    snapshot = load_json(runtime_root / "state" / f"account_snapshot_ths_{tag}.json", default={})
    orders = preview.get("orders", []) if isinstance(preview, dict) else []
    return {
        "trade_date": trade_date,
        "preview_order_count": len(orders),
        "buy_preview_count": sum(item.get("action") == "BUY" for item in orders),
        "sell_preview_count": sum(item.get("action") == "SELL" for item in orders),
        "broker_cash": reconcile.get("broker_cash"),
        "local_cash": reconcile.get("local_cash"),
        "position_diff_count": reconcile.get("position_diff_count"),
        "broker_position_count": len(snapshot.get("positions", [])) if isinstance(snapshot, dict) else None,
        "broker_entrust_count": len(snapshot.get("today_entrusts", [])) if isinstance(snapshot, dict) else None,
        "broker_trade_count": len(snapshot.get("today_trades", [])) if isinstance(snapshot, dict) else None,
        "preview_path": str(runtime_root / "reports" / f"ths_preview_{tag}.json"),
        "reconcile_path": str(runtime_root / "reports" / f"reconcile_ths_{tag}.json"),
        "snapshot_path": str(runtime_root / "state" / f"account_snapshot_ths_{tag}.json"),
    }


def try_sync_cash(runtime_root: Path, trade_date: str) -> dict[str, Any]:
    paths = build_runtime_paths(runtime_root)
    signal_service = StrategySignalService()
    state = load_state(paths, initial_capital=signal_service.baseline.INITIAL_CAPITAL)
    snapshot = load_json(paths.state_dir / f"account_snapshot_ths_{trade_date.replace('-', '')}.json", default={})
    reconcile = load_json(paths.reports_dir / f"reconcile_ths_{trade_date.replace('-', '')}.json", default={})
    broker_cash = snapshot.get("cash") if isinstance(snapshot, dict) else None
    position_diff_count = reconcile.get("position_diff_count") if isinstance(reconcile, dict) else None

    result = {
        "updated": False,
        "reason": "",
        "old_cash": state.cash,
        "new_cash": state.cash,
    }
    if broker_cash is None:
        result["reason"] = "未找到券商快照现金字段"
        return result
    if state.positions:
        result["reason"] = "本地存在持仓，拒绝只同步现金"
        return result
    snapshot_positions = snapshot.get("positions", []) if isinstance(snapshot, dict) else []
    if snapshot_positions:
        result["reason"] = "券商存在持仓，拒绝只同步现金"
        return result
    if position_diff_count not in (0, None):
        result["reason"] = "存在持仓差异，拒绝同步现金"
        return result

    state.cash = float(broker_cash)
    state.notes["last_cash_sync_date"] = trade_date
    save_state(state, paths)
    result["updated"] = True
    result["new_cash"] = state.cash
    result["reason"] = "本地空仓且券商空仓，已用券商现金覆盖本地现金"
    return result


def print_command_result(name: str, result: dict[str, Any]) -> None:
    print(f"[{name}] exit={result['returncode']}")
    stdout = result["stdout"].strip()
    stderr = result["stderr"].strip()
    if stdout:
        print(stdout)
    if stderr:
        print(stderr)


def handle_eod(args: argparse.Namespace) -> None:
    runtime_root = Path(args.runtime_root)
    ensure_runtime_dirs(build_runtime_paths(runtime_root))
    tag = f"eod_{args.signal_date.replace('-', '')}"
    day_log = log_path(runtime_root, tag)

    steps: list[tuple[str, dict[str, Any]]] = []

    if args.refresh_data:
        result = run_command(
            [
                args.python_exe,
                str(PROJECT_ROOT / "股票策略交易主流程.py"),
                "--runtime-root",
                str(runtime_root),
                "refresh-data",
                "--check",
            ]
        )
        steps.append(("refresh-data", result))
        append_jsonl(day_log, {"step": "refresh-data", "result": result})
        print_command_result("refresh-data", result)
        time.sleep(args.sleep_seconds)

    gen_result = run_command(
        build_flow_command(
            args.python_exe,
            runtime_root,
            "generate-signals",
            args.signal_date,
            args.client_type,
            args.exe_path,
            strategy_version=args.strategy_version,
            parameter_version=args.parameter_version,
        )
    )
    steps.append(("generate-signals", gen_result))
    append_jsonl(day_log, {"step": "generate-signals", "result": gen_result})
    print_command_result("generate-signals", gen_result)

    signal_summary = summarize_signal(runtime_root, args.signal_date)
    report = {
        "mode": "eod",
        "signal_summary": signal_summary,
        "steps": [{"name": name, "returncode": res["returncode"]} for name, res in steps],
    }
    report_path = daily_report_path(runtime_root, tag)
    dump_json(report, report_path)
    print(f"\nEOD 摘要: {report_path}")
    print(
        f"订单数={signal_summary['order_count']} "
        f"买入={signal_summary['buy_count']} 卖出={signal_summary['sell_count']} "
        f"执行日={signal_summary['execute_dates']}"
    )


def handle_preopen(args: argparse.Namespace) -> None:
    runtime_root = Path(args.runtime_root)
    ensure_runtime_dirs(build_runtime_paths(runtime_root))
    tag = f"preopen_{args.trade_date.replace('-', '')}"
    day_log = log_path(runtime_root, tag)
    steps = []

    for action in ["ths-reconcile", "ths-preview"]:
        result = run_command(
            build_flow_command(
                args.python_exe,
                runtime_root,
                action,
                args.trade_date,
                args.client_type,
                args.exe_path,
            )
        )
        steps.append((action, result))
        append_jsonl(day_log, {"step": action, "result": result})
        print_command_result(action, result)
        time.sleep(args.sleep_seconds)

    cash_sync = None
    if args.sync_cash:
        cash_sync = try_sync_cash(runtime_root, args.trade_date)
        append_jsonl(day_log, {"step": "sync-cash", "result": cash_sync})
        print(
            f"[sync-cash] updated={cash_sync['updated']} "
            f"old_cash={cash_sync['old_cash']} new_cash={cash_sync['new_cash']} "
            f"reason={cash_sync['reason']}"
        )

    trade_summary = summarize_trade(runtime_root, args.trade_date)
    report = {
        "mode": "preopen",
        "trade_summary": trade_summary,
        "cash_sync": cash_sync,
        "steps": [{"name": name, "returncode": res["returncode"]} for name, res in steps],
        "manual_actions": [
            "检查 ths_preview 文件中的买卖清单是否符合预期",
            "若验证码频繁失败，不要自动提交，改为手工在模拟盘下单",
            "人工下单后运行 postcheck 做复核",
        ],
    }
    report_path = daily_report_path(runtime_root, tag)
    dump_json(report, report_path)
    print(f"\n盘前摘要: {report_path}")
    print(
        f"预览订单={trade_summary['preview_order_count']} "
        f"买入={trade_summary['buy_preview_count']} 卖出={trade_summary['sell_preview_count']} "
        f"仓位差异={trade_summary['position_diff_count']}"
    )


def handle_postcheck(args: argparse.Namespace) -> None:
    runtime_root = Path(args.runtime_root)
    ensure_runtime_dirs(build_runtime_paths(runtime_root))
    tag = f"postcheck_{args.trade_date.replace('-', '')}"
    day_log = log_path(runtime_root, tag)

    result = run_command(
        build_flow_command(
            args.python_exe,
            runtime_root,
            "ths-reconcile",
            args.trade_date,
            args.client_type,
            args.exe_path,
        )
    )
    append_jsonl(day_log, {"step": "ths-reconcile", "result": result})
    print_command_result("ths-reconcile", result)

    trade_summary = summarize_trade(runtime_root, args.trade_date)
    manual_result = {
        "status": args.manual_status.strip(),
        "note": args.manual_note.strip(),
    }
    report = {
        "mode": "postcheck",
        "trade_summary": trade_summary,
        "step": {"name": "ths-reconcile", "returncode": result["returncode"]},
        "manual_result": manual_result,
        "manual_review": [
            "确认模拟盘持仓和本地预期是否一致",
            "若 broker_trade_count 仍为 0，检查是否实际未成交或未下单",
            "若 position_diff_count > 0，次日不要直接沿用本地状态继续交易",
        ],
    }
    append_jsonl(day_log, {"step": "manual-result", "result": manual_result})
    report_path = daily_report_path(runtime_root, tag)
    dump_json(report, report_path)
    print(f"\n复核摘要: {report_path}")
    print(
        f"券商成交数={trade_summary['broker_trade_count']} "
        f"券商委托数={trade_summary['broker_entrust_count']} "
        f"仓位差异={trade_summary['position_diff_count']}"
    )
    if manual_result["status"] or manual_result["note"]:
        print(f"人工结果={manual_result['status'] or '未填写'} | 备注={manual_result['note'] or '无'}")


def handle_status(args: argparse.Namespace) -> None:
    runtime_root = Path(args.runtime_root)
    report = {"mode": "status", "trade_summary": summarize_trade(runtime_root, args.trade_date)}
    if args.signal_date:
        report["signal_summary"] = summarize_signal(runtime_root, args.signal_date)
    print(json.dumps(report, ensure_ascii=False, indent=2))


def main() -> None:
    args = parse_args()
    handlers = {
        "eod": lambda: handle_eod(args),
        "preopen": lambda: handle_preopen(args),
        "postcheck": lambda: handle_postcheck(args),
        "status": lambda: handle_status(args),
    }
    handlers[args.command]()


if __name__ == "__main__":
    main()
