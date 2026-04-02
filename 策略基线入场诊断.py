from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent
BASELINE_PATH = PROJECT_ROOT / "股票策略回测_基线版.py"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "策略基线入场诊断"
OUTPUT_PATH = OUTPUT_DIR / "latest.json"

PARAM_SETS = {
    "loose": {
        "TOP_N": 2,
        "MAX_POSITION_PCT": 0.20,
        "MIN_RET20": 0.035,
        "MIN_MA20_SLOPE_PCT": 0.006,
        "MIN_INTRADAY_CLOSE_FROM_LOW_PCT": 0.002,
        "EARLY_WEAK_EXIT_CHECK_DAY": 4,
        "EARLY_WEAK_EXIT_MIN_CLOSE_RET": -0.01,
        "EARLY_WEAK_EXIT_MIN_HIGH_RET": 0.02,
        "TRAILING_STOP_ATR_MULTIPLIER": 2.2,
        "MAX_HOLDING_DAYS": 18,
    },
    "strict": {
        "TOP_N": 2,
        "MAX_POSITION_PCT": 0.20,
        "MIN_RET20": 0.06,
        "MIN_MA20_SLOPE_PCT": 0.012,
        "MIN_INTRADAY_CLOSE_FROM_LOW_PCT": 0.008,
        "EARLY_WEAK_EXIT_CHECK_DAY": 5,
        "EARLY_WEAK_EXIT_MIN_CLOSE_RET": -0.01,
        "EARLY_WEAK_EXIT_MIN_HIGH_RET": 0.02,
        "TRAILING_STOP_ATR_MULTIPLIER": 2.2,
        "MAX_HOLDING_DAYS": 20,
    },
}

MAX_DIFF_ROWS = 40


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def apply_params(module, params: dict) -> None:
    for key, value in params.items():
        setattr(module, key, value)


def diagnose_case(module, params: dict) -> list[dict]:
    apply_params(module, params)
    context = module.get_backtest_context()
    daily_data = context["daily_data"]
    instruments = context["instruments"]
    st_status = context["st_status"]
    intraday_data = context["intraday_data"]
    benchmark = context["benchmark"]

    start_ts = pd.Timestamp(module.BACKTEST_START)
    end_ts = pd.Timestamp(module.BACKTEST_END)
    all_dates = [d for d in context["all_dates"] if start_ts <= d <= end_ts]

    portfolio = module._BASE.Portfolio(module.INITIAL_CAPITAL, log_trades=False)
    pending_orders: list[module._BASE.PendingOrder] = []
    cooldown_until: dict[str, pd.Timestamp] = {}
    last_close_prices: dict[str, float] = {}
    records: list[dict] = []

    for current_date in all_dates:
        future_orders: list[module._BASE.PendingOrder] = []
        due_orders: list[module._BASE.PendingOrder] = []
        for order in pending_orders:
            if order.execute_dt.date() == current_date.date():
                due_orders.append(order)
            else:
                future_orders.append(order)

        sell_orders = [order for order in due_orders if order.action == "SELL"]
        buy_orders = [order for order in due_orders if order.action == "BUY"]
        start_of_day_equity = portfolio.total_value(last_close_prices)

        executed_buys: list[str] = []
        executed_sells: list[str] = []

        for order in sell_orders:
            code = order.code
            exec_row = intraday_data.get(code)
            if exec_row is None:
                continue
            exec_row = exec_row[exec_row["datetime"] == order.execute_dt]
            if exec_row.empty:
                continue
            price = float(exec_row.iloc[0]["open"])
            if portfolio.sell(order.execute_dt, code, price, order.reason):
                executed_sells.append(code)
                cooldown_until[code] = pd.Timestamp(order.execute_dt).normalize() + pd.Timedelta(days=module.REENTRY_COOLDOWN_DAYS)

        for order in buy_orders:
            code = order.code
            exec_row = intraday_data.get(code)
            if exec_row is None:
                continue
            exec_row = exec_row[exec_row["datetime"] == order.execute_dt]
            if exec_row.empty:
                continue
            daily_slice = daily_data.get(code)
            if daily_slice is None:
                continue
            daily_slice = daily_slice[daily_slice["date"] == current_date]
            if daily_slice.empty:
                continue
            daily_row = daily_slice.iloc[0]
            price = float(exec_row.iloc[0]["open"])
            atr = float(daily_row["atr14"])
            name = instruments.loc[instruments["code"] == code, "name"].iloc[0]
            market = module._BASE.market_of(code)
            target_value = start_of_day_equity * module.MAX_POSITION_PCT
            if portfolio.buy(order.execute_dt, code, name, market, price, target_value, order.reason, atr):
                executed_buys.append(code)

        pending_orders = future_orders
        risk_on = module._BASE.benchmark_risk_on(benchmark, current_date)
        risk_off = module._BASE.benchmark_risk_off(benchmark, current_date)

        for code in list(portfolio.positions.keys()):
            daily_row = daily_data[code][daily_data[code]["date"] == current_date]
            if daily_row.empty:
                continue
            daily_row = daily_row.iloc[0]
            pos = portfolio.positions[code]
            pos["latest_price"] = float(daily_row["close"])
            pos["highest_price"] = max(pos["highest_price"], float(daily_row["close"]))
            stop_price, stop_reason = module._BASE.calc_stop_price(pos, daily_row)
            holding_days = (current_date - pd.Timestamp(pos["entry_dt"]).normalize()).days
            code_df = daily_data[code]
            idx_list = code_df.index[code_df["date"] == current_date].tolist()
            prev_daily_row = code_df.iloc[idx_list[0] - 1] if idx_list and idx_list[0] > 0 else None

            if daily_row["close"] <= stop_price:
                exit_dt = module._BASE.get_next_session_open(intraday_data[code], current_date)
                if exit_dt:
                    pending_orders.append(module._BASE.PendingOrder(exit_dt[0], code, "SELL", stop_reason))
                    continue
            if module._BASE.should_exit_early_weakness(pos, holding_days):
                exit_dt = module._BASE.get_next_session_open(intraday_data[code], current_date)
                if exit_dt:
                    pending_orders.append(module._BASE.PendingOrder(exit_dt[0], code, "SELL", "早期弱势淘汰"))
                    continue
            if holding_days >= module.MAX_HOLDING_DAYS:
                exit_dt = module._BASE.get_next_session_open(intraday_data[code], current_date)
                if exit_dt:
                    pending_orders.append(module._BASE.PendingOrder(exit_dt[0], code, "SELL", "达到最大持有天数"))
                    continue

            trend_break = bool(
                holding_days >= module.MIN_HOLDING_DAYS_BEFORE_TREND_EXIT
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
                and holding_days >= module.MIN_HOLDING_DAYS_BEFORE_TREND_EXIT
                and daily_row["close"] < daily_row["ma20"]
            )
            if trend_break or market_exit:
                exit_dt = module._BASE.get_next_session_open(intraday_data[code], current_date)
                if exit_dt:
                    reason = "跌破20日线" if trend_break else "市场转弱"
                    pending_orders.append(module._BASE.PendingOrder(exit_dt[0], code, "SELL", reason))

        candidate_codes: list[str] = []
        signaled_codes: list[str] = []
        queued_buy_codes: list[str] = []
        holding_codes_before_entry = sorted(portfolio.positions.keys())

        if (not module.STRICT_RISK_ON_FOR_ENTRY) or risk_on:
            candidates = module._BASE.select_candidates(current_date, daily_data, instruments, st_status, cooldown_until)
            candidate_codes = [code for code, _ in candidates]
            holding_codes = set(portfolio.positions.keys())
            for code, _ in candidates:
                if code in holding_codes:
                    continue
                if any(order.code == code and order.action == "BUY" for order in pending_orders):
                    continue
                if code not in intraday_data:
                    continue
                entry_signal_dt = module._BASE.find_intraday_entry(intraday_data[code], current_date)
                if entry_signal_dt is None:
                    continue
                signaled_codes.append(code)
                execute = module._BASE.get_next_session_open(intraday_data[code], current_date)
                if execute is None:
                    continue
                pending_orders.append(module._BASE.PendingOrder(execute[0], code, "BUY", "日线趋势+30分钟回踩突破"))
                queued_buy_codes.append(code)

        close_prices = {}
        for code, df in daily_data.items():
            row = df[df["date"] == current_date]
            if not row.empty:
                close_prices[code] = float(row.iloc[0]["close"])
        last_close_prices = close_prices

        records.append(
            {
                "date": str(current_date.date()),
                "risk_on": bool(risk_on),
                "start_of_day_equity": round(float(start_of_day_equity), 4),
                "holding_codes_before_entry": holding_codes_before_entry,
                "candidate_codes": candidate_codes,
                "signaled_codes": signaled_codes,
                "queued_buy_codes": queued_buy_codes,
                "executed_buy_codes": executed_buys,
                "executed_sell_codes": executed_sells,
            }
        )

    return records


def compare_records(left: list[dict], right: list[dict]) -> list[dict]:
    diffs: list[dict] = []
    for left_row, right_row in zip(left, right):
        diff_payload = {"date": left_row["date"]}
        changed = False
        for key in [
            "risk_on",
            "holding_codes_before_entry",
            "candidate_codes",
            "signaled_codes",
            "queued_buy_codes",
            "executed_buy_codes",
            "executed_sell_codes",
        ]:
            if left_row[key] != right_row[key]:
                diff_payload[f"loose_{key}"] = left_row[key]
                diff_payload[f"strict_{key}"] = right_row[key]
                changed = True
        if changed:
            diffs.append(diff_payload)
        if len(diffs) >= MAX_DIFF_ROWS:
            break
    return diffs


def main() -> None:
    module = load_module(BASELINE_PATH, "baseline_entry_diag")
    module.BACKTEST_START = "2025-01-01"
    module.BACKTEST_END = "2026-03-27"

    print("[1/2] 运行 loose 基线诊断...")
    loose_records = diagnose_case(module, PARAM_SETS["loose"])
    print("[2/2] 运行 strict 基线诊断...")
    strict_records = diagnose_case(module, PARAM_SETS["strict"])

    diffs = compare_records(loose_records, strict_records)
    output = {
        "diff_count": len(diffs),
        "max_diff_rows": MAX_DIFF_ROWS,
        "first_diffs": diffs,
    }
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"结果已写入: {OUTPUT_PATH}")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
