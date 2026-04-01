"""
可信基线版回测：
1. 信号只使用当日收盘前可见的日线和 30 分钟数据。
2. 所有订单统一在次日开盘执行，先卖后买。
3. 同一开盘批次的买单共享同一个开盘前权益快照，消除遍历顺序导致的 sizing 漂移。
4. 基线默认关闭盘中退出，先验证入场层是否真的能改变成交路径。
5. 基线默认要求市场 risk_on 才允许开仓，避免“弱市轻仓”与策略说明不一致。
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = Path(os.getenv("THS_STOCK_DATA_DIR", str(PROJECT_ROOT / "data" / "stock_data")))
BACKTEST_START = "2025-01-01"
BACKTEST_END = "2026-03-27"
INITIAL_CAPITAL = 10000.0

TOP_N = 4
MAX_POSITION_PCT = 0.24
MIN_DAILY_AMOUNT = 5e5
ATR_STOP_MULTIPLIER = 2.0
TAKE_PROFIT_PCT = 0.12
ENABLE_INTRADAY_EXIT = False
MAX_HOLDING_DAYS = 16
MAX_RET20 = 0.42
MAX_ATR_RATIO = 0.05
MIN_CLOSE_MA20_GAP = 0.012
MAX_CLOSE_MA20_GAP = 0.20
MIN_RET20 = 0.035
TRAILING_STOP_TRIGGER_PCT = 0.06
TRAILING_STOP_ATR_MULTIPLIER = 2.2
MIN_MA20_SLOPE_PCT = 0.008
MIN_HOLDING_DAYS_BEFORE_TREND_EXIT = 2
REENTRY_COOLDOWN_DAYS = 2
MIN_INTRADAY_CLOSE_FROM_LOW_PCT = 0.004
MAX_INTRADAY_CHASE_FROM_DAY_OPEN_PCT = 0.045
EARLY_WEAK_EXIT_CHECK_DAY = 4
EARLY_WEAK_EXIT_MIN_CLOSE_RET = -0.01
EARLY_WEAK_EXIT_MIN_HIGH_RET = 0.02

COMMISSION_RATE = 0.0000854
MIN_COMMISSION = 0.1
SLIPPAGE = 0.001
STAMP_TAX_RATE = 0.0005
TRANSFER_FEE_RATE = 0.00001

BENCHMARK_CODE = "000300.SH"
STRICT_RISK_ON_FOR_ENTRY = True


def infer_output_dir() -> Path:
    explicit_output_dir = os.getenv("THS_OUTPUT_DIR", "").strip()
    if explicit_output_dir:
        return Path(explicit_output_dir)

    default_root = PROJECT_ROOT / "outputs" / "股票策略回测_基线版"
    parts = DATA_ROOT.parts
    if "stock_splits" in parts:
        split_idx = parts.index("stock_splits")
        if split_idx + 1 < len(parts):
            split_name = parts[split_idx + 1]
            return default_root / split_name
    return default_root


OUTPUT_DIR = infer_output_dir()


def _load_base_strategy():
    strategy_path = PROJECT_ROOT / "股票策略回测.py"
    module_name = "stock_strategy_base_module"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, strategy_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载基础策略文件: {strategy_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_BASE = _load_base_strategy()


def _sync_config_to_base() -> None:
    sync_keys = [
        "DATA_ROOT",
        "BACKTEST_START",
        "BACKTEST_END",
        "INITIAL_CAPITAL",
        "TOP_N",
        "MAX_POSITION_PCT",
        "MIN_DAILY_AMOUNT",
        "ATR_STOP_MULTIPLIER",
        "TAKE_PROFIT_PCT",
        "ENABLE_INTRADAY_EXIT",
        "MAX_HOLDING_DAYS",
        "MAX_RET20",
        "MAX_ATR_RATIO",
        "MIN_CLOSE_MA20_GAP",
        "MAX_CLOSE_MA20_GAP",
        "MIN_RET20",
        "TRAILING_STOP_TRIGGER_PCT",
        "TRAILING_STOP_ATR_MULTIPLIER",
        "MIN_MA20_SLOPE_PCT",
        "MIN_HOLDING_DAYS_BEFORE_TREND_EXIT",
        "REENTRY_COOLDOWN_DAYS",
        "MIN_INTRADAY_CLOSE_FROM_LOW_PCT",
        "MAX_INTRADAY_CHASE_FROM_DAY_OPEN_PCT",
        "EARLY_WEAK_EXIT_CHECK_DAY",
        "EARLY_WEAK_EXIT_MIN_CLOSE_RET",
        "EARLY_WEAK_EXIT_MIN_HIGH_RET",
        "COMMISSION_RATE",
        "MIN_COMMISSION",
        "SLIPPAGE",
        "STAMP_TAX_RATE",
        "TRANSFER_FEE_RATE",
        "BENCHMARK_CODE",
        "OUTPUT_DIR",
    ]
    for key in sync_keys:
        setattr(_BASE, key, globals()[key])


def get_backtest_context() -> dict:
    _sync_config_to_base()
    return _BASE.get_backtest_context()


def export_backtest_outputs(
    daily_df: pd.DataFrame,
    trades_df: pd.DataFrame,
    sells_df: pd.DataFrame,
    metrics: dict[str, float],
):
    _sync_config_to_base()
    return _BASE.export_backtest_outputs(daily_df, trades_df, sells_df, metrics)


def print_trade_diagnostics(sells_df: pd.DataFrame) -> None:
    _BASE.print_trade_diagnostics(sells_df)


def run_backtest(
    verbose: bool = True,
    export_outputs: bool = True,
    preloaded_context: dict | None = None,
):
    _sync_config_to_base()
    context = preloaded_context if preloaded_context is not None else get_backtest_context()
    instruments = context["instruments"]
    st_status = context["st_status"]
    sample_codes = context["sample_codes"]
    daily_data = context["daily_data"]
    intraday_data = context["intraday_data"]
    benchmark = context["benchmark"]

    start_ts = pd.Timestamp(BACKTEST_START)
    end_ts = pd.Timestamp(BACKTEST_END)
    all_dates = [d for d in context["all_dates"] if start_ts <= d <= end_ts]

    portfolio = _BASE.Portfolio(INITIAL_CAPITAL, log_trades=verbose)
    pending_orders: list[_BASE.PendingOrder] = []
    daily_values: list[dict] = []
    cooldown_until: dict[str, pd.Timestamp] = {}
    last_close_prices: dict[str, float] = {}

    if verbose:
        print("=" * 70)
        print("🚀 股票策略可信基线回测")
        print(f"回测区间: {BACKTEST_START} 至 {BACKTEST_END}")
        print(f"样本股票数: {len(sample_codes)}")
        print(f"严格 risk_on 开仓: {STRICT_RISK_ON_FOR_ENTRY}")
        print(f"启用盘中退出: {ENABLE_INTRADAY_EXIT}")
        print("=" * 70)

    for current_date in all_dates:
        future_orders: list[_BASE.PendingOrder] = []
        due_orders: list[_BASE.PendingOrder] = []
        for order in pending_orders:
            if order.execute_dt.date() == current_date.date():
                due_orders.append(order)
            else:
                future_orders.append(order)

        start_of_day_equity = portfolio.total_value(last_close_prices)

        sell_orders = [order for order in due_orders if order.action == "SELL"]
        buy_orders = [order for order in due_orders if order.action == "BUY"]

        for order in sell_orders:
            code = order.code
            if code not in intraday_data:
                continue
            exec_row = intraday_data[code][intraday_data[code]["datetime"] == order.execute_dt]
            if exec_row.empty:
                continue
            price = float(exec_row.iloc[0]["open"])
            sold = portfolio.sell(order.execute_dt, code, price, order.reason)
            if sold:
                cooldown_until[code] = pd.Timestamp(order.execute_dt).normalize() + pd.Timedelta(days=REENTRY_COOLDOWN_DAYS)

        for order in buy_orders:
            code = order.code
            if code not in intraday_data:
                continue
            exec_row = intraday_data[code][intraday_data[code]["datetime"] == order.execute_dt]
            if exec_row.empty:
                continue
            daily_slice = daily_data.get(code)
            if daily_slice is None:
                continue
            daily_slice = daily_slice[daily_slice["date"] == current_date]
            if daily_slice.empty:
                continue
            price = float(exec_row.iloc[0]["open"])
            daily_row = daily_slice.iloc[0]
            atr = float(daily_row["atr14"])
            name = instruments.loc[instruments["code"] == code, "name"].iloc[0]
            market = _BASE.market_of(code)
            target_value = start_of_day_equity * MAX_POSITION_PCT
            portfolio.buy(order.execute_dt, code, name, market, price, target_value, order.reason, atr)

        pending_orders = future_orders

        risk_on = _BASE.benchmark_risk_on(benchmark, current_date)
        risk_off = _BASE.benchmark_risk_off(benchmark, current_date)

        for code in list(portfolio.positions.keys()):
            if code not in daily_data:
                continue
            daily_row = daily_data[code][daily_data[code]["date"] == current_date]
            if daily_row.empty:
                continue
            daily_row = daily_row.iloc[0]
            pos = portfolio.positions[code]
            pos["latest_price"] = float(daily_row["close"])
            pos["highest_price"] = max(pos["highest_price"], float(daily_row["close"]))
            stop_price, stop_reason = _BASE.calc_stop_price(pos, daily_row)
            holding_days = (current_date - pd.Timestamp(pos["entry_dt"]).normalize()).days
            code_df = daily_data[code]
            idx_list = code_df.index[code_df["date"] == current_date].tolist()
            prev_daily_row = code_df.iloc[idx_list[0] - 1] if idx_list and idx_list[0] > 0 else None

            if ENABLE_INTRADAY_EXIT:
                intraday_exit = _BASE.get_day_intraday_exit(intraday_data[code], current_date, pos)
                if intraday_exit and not any(order.code == code and order.action == "SELL" for order in pending_orders):
                    exit_dt = _BASE.get_next_session_open(intraday_data[code], current_date)
                    if exit_dt:
                        pending_orders.append(_BASE.PendingOrder(exit_dt[0], code, "SELL", intraday_exit[1]))
                        continue

            if daily_row["close"] <= stop_price:
                exit_dt = _BASE.get_next_session_open(intraday_data[code], current_date)
                if exit_dt:
                    pending_orders.append(_BASE.PendingOrder(exit_dt[0], code, "SELL", stop_reason))
                    continue
            if _BASE.should_exit_early_weakness(pos, holding_days):
                exit_dt = _BASE.get_next_session_open(intraday_data[code], current_date)
                if exit_dt:
                    pending_orders.append(_BASE.PendingOrder(exit_dt[0], code, "SELL", "早期弱势淘汰"))
                    continue
            if holding_days >= MAX_HOLDING_DAYS:
                exit_dt = _BASE.get_next_session_open(intraday_data[code], current_date)
                if exit_dt:
                    pending_orders.append(_BASE.PendingOrder(exit_dt[0], code, "SELL", "达到最大持有天数"))
                    continue

            trend_break = bool(
                holding_days >= MIN_HOLDING_DAYS_BEFORE_TREND_EXIT
                and (
                    daily_row["close"] < daily_row["ma20"] * 0.992
                    or (
                        prev_daily_row is not None
                        and prev_daily_row["close"] < prev_daily_row["ma20"]
                        and daily_row["close"] < daily_row["ma20"]
                    )
                )
            )
            market_exit = bool(
                risk_off
                and holding_days >= MIN_HOLDING_DAYS_BEFORE_TREND_EXIT
                and daily_row["close"] < daily_row["ma20"]
            )
            if trend_break or market_exit:
                exit_dt = _BASE.get_next_session_open(intraday_data[code], current_date)
                if exit_dt:
                    reason = "跌破20日线" if trend_break else "市场转弱"
                    pending_orders.append(_BASE.PendingOrder(exit_dt[0], code, "SELL", reason))

        if (not STRICT_RISK_ON_FOR_ENTRY) or risk_on:
            candidates = _BASE.select_candidates(current_date, daily_data, instruments, st_status, cooldown_until)
            holding_codes = set(portfolio.positions.keys())
            for code, _ in candidates:
                if code in holding_codes:
                    continue
                if any(order.code == code and order.action == "BUY" for order in pending_orders):
                    continue
                if code not in intraday_data:
                    continue
                entry_signal_dt = _BASE.find_intraday_entry(intraday_data[code], current_date)
                if entry_signal_dt is None:
                    continue
                execute = _BASE.get_next_session_open(intraday_data[code], current_date)
                if execute is None:
                    continue
                pending_orders.append(_BASE.PendingOrder(execute[0], code, "BUY", "日线趋势+30分钟回踩突破"))

        close_prices = {}
        for code, df in daily_data.items():
            row = df[df["date"] == current_date]
            if not row.empty:
                close_prices[code] = float(row.iloc[0]["close"])
        daily_values.append({"date": current_date, "value": portfolio.total_value(close_prices), "cash": portfolio.cash})
        last_close_prices = close_prices

    final_date = all_dates[-1]
    for code in list(portfolio.positions.keys()):
        row = daily_data[code][daily_data[code]["date"] == final_date]
        if not row.empty:
            portfolio.sell(pd.Timestamp(f"{final_date.date()} 15:00:00"), code, float(row.iloc[0]["close"]), "回测结束平仓")

    if daily_values:
        daily_values[-1]["value"] = portfolio.cash
        daily_values[-1]["cash"] = portfolio.cash

    daily_df = pd.DataFrame(daily_values)
    trades_df = pd.DataFrame(portfolio.trades)
    metrics = _BASE.performance_metrics(daily_df)
    sells_df = trades_df[trades_df["action"] == "卖出"].copy() if not trades_df.empty else pd.DataFrame()
    wins = sells_df[sells_df["profit"] > 0] if not sells_df.empty else pd.DataFrame()
    metrics.update(
        {
            "initial_capital": INITIAL_CAPITAL,
            "final_value": float(portfolio.cash),
            "sell_trade_count": int(len(sells_df)),
            "win_rate_pct": float(len(wins) / len(sells_df) * 100) if not sells_df.empty else 0.0,
            "early_weak_exit_check_day": EARLY_WEAK_EXIT_CHECK_DAY,
            "early_weak_exit_min_close_ret": EARLY_WEAK_EXIT_MIN_CLOSE_RET,
            "early_weak_exit_min_high_ret": EARLY_WEAK_EXIT_MIN_HIGH_RET,
            "enable_intraday_exit": ENABLE_INTRADAY_EXIT,
            "strict_risk_on_for_entry": STRICT_RISK_ON_FOR_ENTRY,
            "data_root": str(DATA_ROOT),
            "output_dir": str(OUTPUT_DIR),
            "backtest_start": BACKTEST_START,
            "backtest_end": BACKTEST_END,
        }
    )

    if export_outputs:
        equity_path, trades_path, summary_path, reason_summary_path, holding_summary_path = export_backtest_outputs(
            daily_df,
            trades_df,
            sells_df,
            metrics,
        )
    else:
        equity_path = None
        trades_path = None
        summary_path = None
        reason_summary_path = None
        holding_summary_path = None

    if verbose:
        print("\n" + "=" * 70)
        print("📊 股票策略可信基线报告")
        print("=" * 70)
        print(f"初始资金: {INITIAL_CAPITAL:.2f} 元")
        print(f"最终资产: {portfolio.cash:.2f} 元")
        print(f"总收益率: {metrics['return_pct']:+.2f}%")
        print(f"年化收益率: {metrics['annual_pct']:+.2f}%")
        print(f"最大回撤: {metrics['max_drawdown_pct']:.2f}%")
        print(f"夏普比率: {metrics['sharpe']:.2f}")
        print(f"交易次数: {len(sells_df)}")
        print(f"胜率: {len(wins) / len(sells_df) * 100:.1f}%" if not sells_df.empty else "胜率: N/A")
        print(f"净值导出: {equity_path}")
        print(f"交易导出: {trades_path}")
        print(f"摘要导出: {summary_path}")
        print(f"退出原因汇总导出: {reason_summary_path}")
        print(f"持有区间汇总导出: {holding_summary_path}")
        if not sells_df.empty:
            print_trade_diagnostics(sells_df)

    return {
        "daily_df": daily_df,
        "trades_df": trades_df,
        "sells_df": sells_df,
        "metrics": metrics,
        "paths": {
            "equity_path": equity_path,
            "trades_path": trades_path,
            "summary_path": summary_path,
            "reason_summary_path": reason_summary_path,
            "holding_summary_path": holding_summary_path,
        },
    }


if __name__ == "__main__":
    run_backtest()
