from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from stock_trading.common import (
    StrategyPosition,
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
    parser.add_argument("--pdf-root-dir", default="", help="PDF fallback 的根目录")

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

    sync_state = subparsers.add_parser("sync-state", help="把券商快照同步回本地状态，仅用于半自动模拟盘校准")
    sync_state.add_argument("--trade-date", required=True, help="快照交易日，例如 2026-04-07")
    sync_state.add_argument("--replace-cash", action="store_true", help="同步时用券商现金覆盖本地现金")

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
    pdf_root_dir: str,
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
        if pdf_root_dir:
            cmd.extend(["--pdf-root-dir", pdf_root_dir])
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


def _infer_market_from_code(code: str) -> str:
    if code.endswith(".SH"):
        return "SH"
    if code.endswith(".SZ"):
        return "SZ"
    if code.endswith(".BJ"):
        return "BJ"
    return ""


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_price_hints_from_ocr_line(raw_line: str, shares: int) -> dict[str, float | None]:
    if not raw_line or shares <= 0:
        return {"entry_price": None, "market_value": None}
    cleaned = re.sub(r"^\d{1,2}:\d{2}:\d{2}\s+\d{6}\s*", "", raw_line).strip()
    numbers = re.findall(r"\d+(?:\.\d+)?", cleaned)
    values = [float(item) for item in numbers]
    market_value = None
    entry_price = None
    positive_values = [value for value in values if value > 0]

    decimal_values = [value for value in positive_values if not float(value).is_integer()]
    decimal_amount_candidates = [value for value in decimal_values if shares <= value <= shares * 1000]
    if decimal_amount_candidates:
        market_value = decimal_amount_candidates[0]
    else:
        compact_amount_candidates = [
            value for value in positive_values
            if shares <= value <= shares * 1000 and value < 100000
        ]
        if compact_amount_candidates:
            market_value = compact_amount_candidates[0]

    if market_value and shares > 0:
        inferred_price = market_value / shares
        if 0 < inferred_price < 1000:
            entry_price = inferred_price

    # OCR occasionally drops decimal points, e.g. 4250 means 4.250.
    if entry_price is None:
        compact_candidates = [
            value / 1000.0
            for value in positive_values
            if float(value).is_integer() and 1000 <= value <= 99999
        ]
        compact_candidates = [value for value in compact_candidates if 0 < value < 1000]
        if compact_candidates:
            entry_price = compact_candidates[0]

    return {
        "entry_price": round(entry_price, 4) if entry_price is not None else None,
        "market_value": round(market_value, 4) if market_value is not None else None,
    }


def _infer_latest_price_from_snapshot(
    snapshot: dict[str, Any],
    snapshot_position: dict[str, Any],
    total_snapshot_shares: int,
) -> float | None:
    raw = snapshot_position.get("raw", {}) if isinstance(snapshot_position.get("raw"), dict) else {}
    for key in ("市价", "最新价"):
        latest_price = _safe_float(raw.get(key))
        if latest_price and latest_price > 0:
            return latest_price

    guess = _safe_float(snapshot_position.get("last_price_guess"))
    if guess and guess > 0:
        return guess

    raw_balance = snapshot.get("raw_balance", {}) if isinstance(snapshot.get("raw_balance"), dict) else {}
    stock_value = None
    for key in ("股票市值", "A股市值", "证券市值"):
        stock_value = _safe_float(raw_balance.get(key))
        if stock_value and stock_value > 0:
            break
    shares = int(snapshot_position.get("shares", 0) or 0)
    if stock_value and shares > 0 and total_snapshot_shares == shares:
        return round(stock_value / shares, 4)
    return None


def _find_trade_match(snapshot: dict[str, Any], code: str) -> dict[str, Any] | None:
    trades = snapshot.get("raw_today_trades") or snapshot.get("today_trades") or []
    target_digits = code.split(".")[0]
    for trade in trades:
        trade_code = str(
            trade.get("normalized_code")
            or trade.get("证券代码")
            or trade.get("股票代码")
            or trade.get("code")
            or ""
        ).strip().upper()
        if trade_code == code or trade_code.split(".")[0] == target_digits:
            return trade
    return None


def _is_suspicious_price(entry_price: float, latest_price: float) -> bool:
    if entry_price <= 0:
        return True
    if latest_price > 0 and (entry_price / latest_price >= 5 or latest_price / entry_price >= 5):
        return True
    return False


def _is_suspicious_cost_basis(cost_basis: float, shares: int, latest_price: float) -> bool:
    if cost_basis <= 0 or shares <= 0:
        return True
    avg_cost = cost_basis / shares
    if latest_price > 0 and (avg_cost / latest_price >= 5 or latest_price / avg_cost >= 5):
        return True
    return False


def _build_position_from_snapshot(
    snapshot: dict[str, Any],
    snapshot_position: dict[str, Any],
    snapshot_timestamp: str,
    total_snapshot_shares: int,
) -> StrategyPosition:
    raw = snapshot_position.get("raw", {}) if isinstance(snapshot_position.get("raw"), dict) else {}
    code = str(snapshot_position.get("code", ""))
    shares = int(snapshot_position.get("shares", 0) or 0)
    name = str(
        raw.get("证券名称")
        or raw.get("股票名称")
        or snapshot_position.get("name")
        or code
    )
    market = _infer_market_from_code(code)
    trade_match = _find_trade_match(snapshot, code)
    ocr_hints = _extract_price_hints_from_ocr_line(str(snapshot_position.get("raw_line", "")), shares)
    ocr_only_position = (
        snapshot_position.get("source") == "ocr_fallback"
        and not raw
        and trade_match is None
    )

    cost_price = (
        _safe_float(raw.get("成本价"))
        or _safe_float(raw.get("摊薄成本价"))
        or _safe_float((trade_match or {}).get("成交均价"))
        or _safe_float(snapshot_position.get("entry_price_guess"))
        or _safe_float(ocr_hints.get("entry_price"))
        or 0.0
    )
    latest_price = (
        _infer_latest_price_from_snapshot(snapshot, snapshot_position, total_snapshot_shares)
        or cost_price
    )
    cost_basis = (
        (_safe_float(raw.get("成本价")) or 0.0) * shares
        or _safe_float(raw.get("市值"))
        or _safe_float((trade_match or {}).get("成交金额"))
        or _safe_float(ocr_hints.get("market_value"))
        or cost_price * shares
    )

    if ocr_only_position:
        # OCR 适合确认股数，不适合恢复真实成交价。此时统一退回到“按当前市值近似”的保守口径。
        if latest_price <= 0 and shares > 0:
            latest_price = cost_price
        if latest_price > 0:
            cost_price = latest_price
            cost_basis = latest_price * shares

    highest_price = max(cost_price, latest_price, 0.0)
    return StrategyPosition(
        code=code,
        name=name,
        market=market,
        shares=shares,
        entry_dt=snapshot_timestamp,
        entry_price=cost_price if cost_price > 0 else latest_price,
        cost_basis=cost_basis,
        atr_at_entry=0.0,
        highest_price=highest_price,
        latest_price=latest_price if latest_price > 0 else cost_price,
    )


def sync_state_from_snapshot(runtime_root: Path, trade_date: str, replace_cash: bool = False) -> dict[str, Any]:
    paths = build_runtime_paths(runtime_root)
    signal_service = StrategySignalService()
    state = load_state(paths, initial_capital=signal_service.baseline.INITIAL_CAPITAL)
    snapshot = load_json(paths.state_dir / f"account_snapshot_ths_{trade_date.replace('-', '')}.json", default={})
    reconcile = load_json(paths.reports_dir / f"reconcile_ths_{trade_date.replace('-', '')}.json", default={})
    result = {
        "updated": False,
        "reason": "",
        "replace_cash": replace_cash,
        "old_cash": state.cash,
        "new_cash": state.cash,
        "old_position_count": len(state.positions),
        "new_position_count": len(state.positions),
        "synced_codes": [],
    }
    if not isinstance(snapshot, dict) or not snapshot:
        result["reason"] = "未找到券商快照"
        return result
    snapshot_positions = snapshot.get("positions", [])
    if not snapshot_positions:
        result["reason"] = "券商快照中没有持仓，拒绝同步"
        return result
    if not isinstance(reconcile, dict) or reconcile.get("position_diff_count", 0) <= 0:
        # 即便仓位数量已对齐，也允许做一次字段补全。
        pass

    snapshot_timestamp = str(snapshot.get("timestamp") or f"{trade_date}T15:00:00")
    total_snapshot_shares = sum(int(item.get("shares", 0) or 0) for item in snapshot_positions)
    snapshot_built_positions = {}
    for item in snapshot_positions:
        position = _build_position_from_snapshot(snapshot, item, snapshot_timestamp, total_snapshot_shares)
        snapshot_built_positions[position.code] = position

    if state.positions:
        local_map = {code: pos.shares for code, pos in state.positions.items()}
        broker_map = {code: pos.shares for code, pos in snapshot_built_positions.items()}
        if local_map != broker_map:
            result["reason"] = "本地已有持仓且与券商股数不一致，拒绝覆盖，请先人工确认"
            return result

        synced_positions = {}
        for code, old_pos in state.positions.items():
            new_pos = snapshot_built_positions[code]
            old_data = old_pos.to_dict()
            new_data = new_pos.to_dict()
            old_entry_price = float(old_data.get("entry_price", 0.0) or 0.0)
            new_entry_price = float(new_data.get("entry_price", 0.0) or 0.0)
            old_latest_price = float(old_data.get("latest_price", 0.0) or 0.0)
            new_latest_price = float(new_data.get("latest_price", 0.0) or 0.0)
            old_cost_basis = float(old_data.get("cost_basis", 0.0) or 0.0)
            new_cost_basis = float(new_data.get("cost_basis", 0.0) or 0.0)

            use_new_entry_price = (
                old_entry_price <= 0
                or (
                    new_entry_price > 0
                    and _is_suspicious_price(old_entry_price, old_latest_price or new_latest_price)
                    and not _is_suspicious_price(new_entry_price, new_latest_price or old_latest_price)
                )
            )
            use_new_cost_basis = (
                old_cost_basis <= 0
                or (
                    new_cost_basis > 0
                    and _is_suspicious_cost_basis(old_cost_basis, old_data["shares"], old_latest_price or new_latest_price)
                    and not _is_suspicious_cost_basis(new_cost_basis, new_data["shares"], new_latest_price or old_latest_price)
                )
            )
            use_new_latest_price = old_latest_price <= 0 < new_latest_price

            merged = StrategyPosition.from_dict(
                {
                    "code": code,
                    "name": old_data["name"] if old_data["name"] not in ("", code) else new_data["name"],
                    "market": old_data["market"] or new_data["market"],
                    "shares": old_data["shares"],
                    "entry_dt": old_data["entry_dt"] if old_data["entry_dt"] else new_data["entry_dt"],
                    "entry_price": new_entry_price if use_new_entry_price else old_entry_price,
                    "cost_basis": new_cost_basis if use_new_cost_basis else old_cost_basis,
                    "atr_at_entry": old_data.get("atr_at_entry", 0.0),
                    "highest_price": max(old_data.get("highest_price", 0.0), new_data["highest_price"]),
                    "latest_price": new_latest_price if use_new_latest_price else old_latest_price,
                }
            )
            synced_positions[code] = merged
    else:
        synced_positions = snapshot_built_positions

    state.positions = synced_positions
    if replace_cash and snapshot.get("cash") is not None:
        state.cash = float(snapshot["cash"])
    state.notes["last_broker_sync"] = {
        "trade_date": trade_date,
        "timestamp": snapshot_timestamp,
        "source": "ths_snapshot",
        "position_count": len(synced_positions),
        "replace_cash": replace_cash,
        "pricing_policy": "prefer_raw_then_trade_else_latest_price_approx",
    }
    save_state(state, paths)
    result["updated"] = True
    result["new_cash"] = state.cash
    result["new_position_count"] = len(state.positions)
    result["synced_codes"] = sorted(state.positions)
    result["reason"] = "已用券商快照同步本地持仓状态"
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
            args.pdf_root_dir,
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
                args.pdf_root_dir,
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
            args.pdf_root_dir,
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


def handle_sync_state(args: argparse.Namespace) -> None:
    runtime_root = Path(args.runtime_root)
    ensure_runtime_dirs(build_runtime_paths(runtime_root))
    tag = f"sync_state_{args.trade_date.replace('-', '')}"
    day_log = log_path(runtime_root, tag)
    sync_result = sync_state_from_snapshot(runtime_root, args.trade_date, replace_cash=args.replace_cash)
    append_jsonl(day_log, {"step": "sync-state", "result": sync_result})
    report = {
        "mode": "sync-state",
        "trade_date": args.trade_date,
        "sync_result": sync_result,
    }
    report_path = daily_report_path(runtime_root, tag)
    dump_json(report, report_path)
    print(f"状态同步摘要: {report_path}")
    print(
        f"updated={sync_result['updated']} "
        f"old_pos={sync_result['old_position_count']} new_pos={sync_result['new_position_count']} "
        f"cash={sync_result['new_cash']} reason={sync_result['reason']}"
    )
    if sync_result["synced_codes"]:
        print(f"synced_codes={sync_result['synced_codes']}")


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
        "sync-state": lambda: handle_sync_state(args),
        "status": lambda: handle_status(args),
    }
    handlers[args.command]()


if __name__ == "__main__":
    main()
