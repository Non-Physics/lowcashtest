"""
股票版第一版回测：
1. 日线选股：趋势向上 + 流动性过滤 + 非 ST。
2. 市场过滤：沪深300 在 20/60 日均线上方才允许开仓。
3. 30 分钟择时：当日回踩短均线后重新站上，次个 30 分钟开盘买入。
4. 风控：ATR 止损 + 日线跌破 20 日线 + 市场转弱退出。

数据目录默认使用：
    <项目目录>/data/stock_data
可通过环境变量覆盖：
    THS_STOCK_DATA_DIR=/your/path

结果默认导出到：
    <项目目录>/outputs/股票策略回测
可通过环境变量覆盖：
    THS_OUTPUT_DIR=/your/path

如果数据目录位于：
    <项目目录>/data/datasets/stock_splits/<split_name>
则结果会自动输出到：
    <项目目录>/outputs/股票策略回测/<split_name>
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
import json

import numpy as np
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


def infer_output_dir() -> Path:
    explicit_output_dir = os.getenv("THS_OUTPUT_DIR", "").strip()
    if explicit_output_dir:
        return Path(explicit_output_dir)

    default_root = PROJECT_ROOT / "outputs" / "股票策略回测"
    parts = DATA_ROOT.parts
    if "stock_splits" in parts:
        split_idx = parts.index("stock_splits")
        if split_idx + 1 < len(parts):
            split_name = parts[split_idx + 1]
            return PROJECT_ROOT / "outputs" / "股票策略回测" / split_name
    return default_root


OUTPUT_DIR = infer_output_dir()
_DATA_CACHE: dict[str, dict] = {}


@dataclass
class PendingOrder:
    execute_dt: pd.Timestamp
    code: str
    action: str
    reason: str


class Portfolio:
    def __init__(self, initial_capital: float):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions: dict[str, dict] = {}
        self.trades: list[dict] = []

    def total_value(self, prices: dict[str, float]) -> float:
        return self.cash + sum(pos["shares"] * prices.get(code, 0.0) for code, pos in self.positions.items())

    def buy(self, dt, code, name, market, price, target_value, reason, atr):
        if code in self.positions:
            return False

        shares = int(target_value / price / 100) * 100
        while shares > 0:
            amount = shares * price
            total_cost, fee = calc_buy_cost(amount, market)
            if total_cost <= self.cash:
                self.cash -= total_cost
                self.positions[code] = {
                    "name": name,
                    "market": market,
                    "shares": shares,
                    "entry_dt": dt,
                    "entry_price": price,
                    "cost_basis": total_cost,
                    "atr_at_entry": atr,
                    "highest_price": price,
                }
                self.trades.append(
                    {
                        "datetime": dt,
                        "code": code,
                        "name": name,
                        "action": "买入",
                        "price": price,
                        "shares": shares,
                        "reason": reason,
                        "fee": fee,
                    }
                )
                print(f"  🔴 买入 {name}({code}) {shares}股 @{price:.2f} | 理由:{reason}")
                return True
            shares -= 100
        return False

    def sell(self, dt, code, price, reason):
        if code not in self.positions:
            return False
        pos = self.positions.pop(code)
        amount = pos["shares"] * price
        net_proceeds, fee = calc_sell_net(amount, pos["market"])
        profit = net_proceeds - pos["cost_basis"]
        profit_rate = profit / pos["cost_basis"] * 100 if pos["cost_basis"] else 0.0
        self.cash += net_proceeds
        self.trades.append(
            {
                "datetime": dt,
                "code": code,
                "name": pos["name"],
                "action": "卖出",
                "price": price,
                "shares": pos["shares"],
                "entry_dt": pos["entry_dt"],
                "entry_price": pos["entry_price"],
                "profit": profit,
                "profit_rate": profit_rate,
                "reason": reason,
                "fee": fee,
            }
        )
        print(f"  🟢 卖出 {pos['name']}({code}) {pos['shares']}股 @{price:.2f} | 盈亏:{profit:+.2f}({profit_rate:+.2f}%) | 理由:{reason}")
        return True


def calc_buy_cost(amount: float, market: str) -> tuple[float, float]:
    commission = max(amount * COMMISSION_RATE, MIN_COMMISSION)
    transfer_fee = amount * TRANSFER_FEE_RATE if market == "sh" else 0
    slippage = amount * SLIPPAGE
    fee = commission + transfer_fee + slippage
    return amount + fee, fee


def calc_sell_net(amount: float, market: str) -> tuple[float, float]:
    commission = max(amount * COMMISSION_RATE, MIN_COMMISSION)
    stamp_tax = amount * STAMP_TAX_RATE
    transfer_fee = amount * TRANSFER_FEE_RATE if market == "sh" else 0
    slippage = amount * SLIPPAGE
    fee = commission + stamp_tax + transfer_fee + slippage
    return amount - fee, fee


def market_of(code: str) -> str:
    return "sh" if code.endswith(".SH") else "sz"


def is_tradeable_a_share(code: str) -> bool:
    symbol = code.split(".")[0]
    if symbol.startswith(("300", "301", "688", "8", "4")):
        return False
    return True


DAILY_READ_COLUMNS = ["date", "open", "high", "low", "close", "volume", "amount", "pre_close"]
INTRADAY_READ_COLUMNS = ["datetime", "open", "high", "low", "close", "volume", "amount"]
NUMERIC_DTYPES = {
    "open": "float32",
    "high": "float32",
    "low": "float32",
    "close": "float32",
    "volume": "float32",
    "amount": "float32",
    "pre_close": "float32",
}


def add_daily_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma60"] = df["close"].rolling(60).mean()
    df["ma20_slope_5"] = df["ma20"] / df["ma20"].shift(5) - 1
    df["ma60_slope_5"] = df["ma60"] / df["ma60"].shift(5) - 1
    df["ret20"] = df["close"] / df["close"].shift(20) - 1
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            (df["high"] - df["low"]).abs(),
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr14"] = tr.rolling(14).mean()
    df["amount_ma20"] = df["amount"].rolling(20).mean()
    float_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "pre_close",
        "ma20",
        "ma60",
        "ma20_slope_5",
        "ma60_slope_5",
        "ret20",
        "atr14",
        "amount_ma20",
    ]
    df[float_cols] = df[float_cols].astype("float32")
    return df


def add_30m_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").reset_index(drop=True)
    df["ma3"] = df["close"].rolling(3).mean()
    df["ma8"] = df["close"].rolling(8).mean()
    df["bar_ret"] = df["close"] / df["open"] - 1
    df["day"] = df["datetime"].dt.normalize()
    float_cols = ["open", "high", "low", "close", "volume", "amount", "ma3", "ma8", "bar_ret"]
    df[float_cols] = df[float_cols].astype("float32")
    return df


def load_metadata() -> tuple[pd.DataFrame, pd.DataFrame]:
    instruments = pd.read_csv(DATA_ROOT / "metadata" / "instruments.csv")
    st_path = DATA_ROOT / "metadata" / "st_status.csv"
    st_status = pd.read_csv(st_path) if st_path.exists() else pd.DataFrame(columns=["date", "code", "is_st"])
    if not st_status.empty:
        st_status["date"] = pd.to_datetime(st_status["date"])
    return instruments, st_status


def load_daily_stock_data(codes: list[str]) -> dict[str, pd.DataFrame]:
    out = {}
    for code in codes:
        if not is_tradeable_a_share(code):
            continue
        path = DATA_ROOT / "daily" / "stock" / f"{code}.csv"
        if path.exists():
            df = pd.read_csv(path, usecols=DAILY_READ_COLUMNS, dtype=NUMERIC_DTYPES)
            out[code] = add_daily_indicators(df)
    return out


def load_intraday_stock_data(codes: list[str]) -> dict[str, pd.DataFrame]:
    out = {}
    for code in codes:
        if not is_tradeable_a_share(code):
            continue
        path = DATA_ROOT / "intraday_30m" / "stock" / f"{code}.csv"
        if path.exists():
            df = pd.read_csv(path, usecols=INTRADAY_READ_COLUMNS, dtype=NUMERIC_DTYPES)
            out[code] = add_30m_indicators(df)
    return out


def load_benchmark() -> pd.DataFrame:
    df = pd.read_csv(DATA_ROOT / "daily" / "index" / f"{BENCHMARK_CODE}.csv", usecols=DAILY_READ_COLUMNS, dtype=NUMERIC_DTYPES)
    return add_daily_indicators(df)


def get_backtest_context() -> dict:
    cache_key = str(DATA_ROOT)
    cached = _DATA_CACHE.get(cache_key)
    if cached is not None:
        return cached

    instruments, st_status = load_metadata()
    sample_codes = sorted([p.stem for p in (DATA_ROOT / "daily" / "stock").glob("*.csv")])
    daily_data = load_daily_stock_data(sample_codes)
    intraday_data = load_intraday_stock_data(sample_codes)
    benchmark = load_benchmark()
    all_dates = sorted(set(date for df in daily_data.values() for date in df["date"].tolist()))

    cached = {
        "instruments": instruments,
        "st_status": st_status,
        "sample_codes": sample_codes,
        "daily_data": daily_data,
        "intraday_data": intraday_data,
        "benchmark": benchmark,
        "all_dates": all_dates,
    }
    _DATA_CACHE[cache_key] = cached
    return cached


def is_st_on_date(st_status: pd.DataFrame, code: str, date: pd.Timestamp) -> bool:
    if st_status.empty:
        return False
    matched = st_status[(st_status["code"] == code) & (st_status["date"] == date)]
    return not matched.empty


def benchmark_risk_on(benchmark_df: pd.DataFrame, date: pd.Timestamp) -> bool:
    row = benchmark_df[benchmark_df["date"] == date]
    if row.empty:
        return False
    row = row.iloc[0]
    if pd.isna(row["ma20"]) or pd.isna(row["ma60"]):
        return False
    return bool(row["close"] > row["ma20"] > row["ma60"] and row["ret20"] > 0)


def benchmark_risk_off(benchmark_df: pd.DataFrame, date: pd.Timestamp) -> bool:
    row = benchmark_df[benchmark_df["date"] == date]
    if row.empty:
        return True
    row = row.iloc[0]
    if pd.isna(row["ma20"]) or pd.isna(row["ma60"]):
        return True
    return bool(row["close"] < row["ma20"] and row["ma20"] < row["ma60"] and row["ret20"] < -0.02)


def select_candidates(
    current_date: pd.Timestamp,
    daily_data: dict[str, pd.DataFrame],
    instruments: pd.DataFrame,
    st_status: pd.DataFrame,
    cooldown_until: dict[str, pd.Timestamp],
) -> list[tuple[str, float]]:
    candidates = []
    instrument_map = instruments.set_index("code")

    for code, df in daily_data.items():
        if not is_tradeable_a_share(code):
            continue
        row = df[df["date"] == current_date]
        if row.empty:
            continue
        row = row.iloc[0]
        if pd.isna(row["ma20"]) or pd.isna(row["ma60"]) or pd.isna(row["ret20"]) or pd.isna(row["atr14"]):
            continue
        if pd.isna(row["ma20_slope_5"]) or pd.isna(row["ma60_slope_5"]):
            continue
        if row["amount_ma20"] < MIN_DAILY_AMOUNT:
            continue
        if row["close"] <= row["ma20"] or row["ma20"] <= row["ma60"]:
            continue
        if row["ma20_slope_5"] <= MIN_MA20_SLOPE_PCT or row["ma60_slope_5"] <= -0.005:
            continue
        if row["ret20"] <= MIN_RET20:
            continue
        if row["ret20"] >= MAX_RET20:
            continue
        if row["atr14"] / row["close"] >= MAX_ATR_RATIO:
            continue
        close_ma20_gap = row["close"] / row["ma20"] - 1
        if close_ma20_gap <= MIN_CLOSE_MA20_GAP:
            continue
        if close_ma20_gap >= MAX_CLOSE_MA20_GAP:
            continue
        if code not in instrument_map.index:
            continue
        inst = instrument_map.loc[code]
        if inst["status"] != "L":
            continue
        if "ST" in str(inst["name"]).upper():
            continue
        if is_st_on_date(st_status, code, current_date):
            continue
        if code in cooldown_until and current_date <= cooldown_until[code]:
            continue

        score = float(row["ret20"] * 0.55 + close_ma20_gap * 0.20 + row["ma20_slope_5"] * 0.25)
        candidates.append((code, score))

    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[:TOP_N]


def find_intraday_entry(intraday_df: pd.DataFrame, day: pd.Timestamp) -> pd.Timestamp | None:
    day_df = intraday_df[intraday_df["day"] == day].copy()
    if len(day_df) < 6:
        return None
    day_open = float(day_df.iloc[0]["open"])

    for i in range(2, len(day_df)):
        prev2 = day_df.iloc[i - 2] if i >= 2 else None
        prev_row = day_df.iloc[i - 1]
        row = day_df.iloc[i]
        if (
            (prev2 is not None and pd.isna(prev2["ma3"]))
            or pd.isna(prev_row["ma3"])
            or pd.isna(prev_row["ma8"])
            or pd.isna(row["ma3"])
            or pd.isna(row["ma8"])
        ):
            continue

        pullback = bool(
            (prev2 is not None and prev2["close"] <= prev2["ma3"] * 1.004)
            or prev_row["close"] <= prev_row["ma3"] * 1.004
        )
        intraday_low = float(day_df.iloc[: i + 1]["low"].min())
        close_from_low = row["close"] / intraday_low - 1 if intraday_low > 0 else 0.0
        day_chase = row["close"] / day_open - 1 if day_open > 0 else 0.0
        breakout = bool(
            row["close"] > row["ma3"] >= row["ma8"]
            and row["close"] >= prev_row["high"] * 0.998
            and row["close"] > day_open * 0.998
            and row["bar_ret"] > 0.001
            and close_from_low >= MIN_INTRADAY_CLOSE_FROM_LOW_PCT
            and day_chase <= MAX_INTRADAY_CHASE_FROM_DAY_OPEN_PCT
        )
        if pullback and breakout:
            return row["datetime"]
    return None


def get_30m_open_after(intraday_df: pd.DataFrame, dt: pd.Timestamp) -> tuple[pd.Timestamp, float] | None:
    future = intraday_df[intraday_df["datetime"] > dt]
    if future.empty:
        return None
    row = future.iloc[0]
    return row["datetime"], float(row["open"])


def get_next_session_open(intraday_df: pd.DataFrame, day: pd.Timestamp) -> tuple[pd.Timestamp, float] | None:
    future = intraday_df[intraday_df["day"] > day]
    if future.empty:
        return None
    row = future.iloc[0]
    return row["datetime"], float(row["open"])


def get_day_intraday_exit(intraday_df: pd.DataFrame, day: pd.Timestamp, pos: dict) -> tuple[pd.Timestamp, str] | None:
    day_df = intraday_df[intraday_df["day"] == day].copy()
    if day_df.empty:
        return None

    entry_price = pos["entry_price"]
    for i in range(1, len(day_df) - 1):
        row = day_df.iloc[i]
        next_row = day_df.iloc[i + 1]
        if pd.isna(row["ma3"]) or pd.isna(row["ma8"]):
            continue

        # 盘中止盈：达到固定收益后下一根开盘退出
        if row["close"] >= entry_price * (1 + TAKE_PROFIT_PCT):
            return next_row["datetime"], "达到止盈目标"

        # 盘中转弱：跌回短均线下方且短线走坏
        if row["close"] < row["ma8"] and row["ma3"] < row["ma8"]:
            return next_row["datetime"], "30分钟转弱"

    return None


def calc_stop_price(pos: dict, daily_row: pd.Series) -> tuple[float, str]:
    atr_base = max(pos["atr_at_entry"], float(daily_row["atr14"]))
    base_stop = pos["entry_price"] - ATR_STOP_MULTIPLIER * atr_base
    if pos["highest_price"] >= pos["entry_price"] * (1 + TRAILING_STOP_TRIGGER_PCT):
        trailing_stop = pos["highest_price"] - TRAILING_STOP_ATR_MULTIPLIER * atr_base
        # 跟踪止损触发后，至少保护一点浮盈，减少盈利单回撤成亏损单。
        breakeven_stop = pos["entry_price"] * 1.015
        return max(base_stop, trailing_stop, breakeven_stop), "ATR跟踪止损"
    return base_stop, "ATR止损"


def should_exit_early_weakness(pos: dict, holding_days: int) -> bool:
    if holding_days < EARLY_WEAK_EXIT_CHECK_DAY:
        return False
    max_high_ret = pos["highest_price"] / pos["entry_price"] - 1
    latest_ret = pos["latest_price"] / pos["entry_price"] - 1
    return bool(
        latest_ret <= EARLY_WEAK_EXIT_MIN_CLOSE_RET
        and max_high_ret < EARLY_WEAK_EXIT_MIN_HIGH_RET
    )


def performance_metrics(daily_df: pd.DataFrame) -> dict[str, float]:
    values = daily_df["value"].astype(float)
    ret = values.iloc[-1] / INITIAL_CAPITAL - 1 if not values.empty else 0
    daily_ret = values.pct_change().fillna(0.0) if not values.empty else pd.Series(dtype=float)
    drawdown = values / values.cummax() - 1 if not values.empty else pd.Series(dtype=float)
    sharpe = 0.0
    if not daily_ret.empty and daily_ret.std() > 0:
        sharpe = np.sqrt(252) * daily_ret.mean() / daily_ret.std()
    annual = (1 + ret) ** (252 / len(values)) - 1 if len(values) > 0 and ret > -1 else -1
    return {
        "return_pct": ret * 100,
        "annual_pct": annual * 100,
        "max_drawdown_pct": float(drawdown.min() * 100) if not drawdown.empty else 0.0,
        "sharpe": float(sharpe),
    }


def build_trade_diagnostics(sells_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if sells_df.empty:
        empty_reason = pd.DataFrame(columns=["交易数", "总盈亏", "平均盈亏", "胜率"])
        empty_holding = pd.DataFrame(columns=["交易数", "总盈亏", "平均盈亏", "胜率"])
        return empty_reason, empty_holding

    reason_summary = (
        sells_df.groupby("reason", dropna=False)
        .agg(
            交易数=("profit", "count"),
            总盈亏=("profit", "sum"),
            平均盈亏=("profit", "mean"),
            胜率=("profit", lambda s: (s > 0).mean() * 100),
        )
        .sort_values("总盈亏")
        .round(2)
    )

    holding_df = sells_df.copy()
    holding_df["hold_days"] = (
        pd.to_datetime(holding_df["datetime"]).dt.normalize()
        - pd.to_datetime(holding_df["entry_dt"]).dt.normalize()
    ).dt.days
    holding_df["持有区间"] = pd.cut(
        holding_df["hold_days"],
        bins=[-1, 2, 4, 7, 10, 16, 999],
        labels=["0-2", "3-4", "5-7", "8-10", "11-16", "17+"],
    )
    holding_summary = (
        holding_df.groupby("持有区间", observed=False)
        .agg(
            交易数=("profit", "count"),
            总盈亏=("profit", "sum"),
            平均盈亏=("profit", "mean"),
            胜率=("profit", lambda s: (s > 0).mean() * 100),
        )
        .round(2)
    )
    return reason_summary, holding_summary


def export_backtest_outputs(
    daily_df: pd.DataFrame,
    trades_df: pd.DataFrame,
    sells_df: pd.DataFrame,
    metrics: dict[str, float],
) -> tuple[Path, Path, Path, Path, Path]:
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise RuntimeError(
            f"输出目录不可写: {OUTPUT_DIR}。请设置 THS_OUTPUT_DIR 到可写目录。"
        ) from exc
    equity_path = OUTPUT_DIR / "股票策略_净值曲线.csv"
    trades_path = OUTPUT_DIR / "股票策略_交易明细.csv"
    summary_path = OUTPUT_DIR / "股票策略_回测摘要.json"
    reason_summary_path = OUTPUT_DIR / "股票策略_退出原因汇总.csv"
    holding_summary_path = OUTPUT_DIR / "股票策略_持有区间汇总.csv"
    daily_df.to_csv(equity_path, index=False, encoding="utf-8-sig")
    trades_df.to_csv(trades_path, index=False, encoding="utf-8-sig")
    reason_summary, holding_summary = build_trade_diagnostics(sells_df)
    reason_summary.to_csv(reason_summary_path, encoding="utf-8-sig")
    holding_summary.to_csv(holding_summary_path, encoding="utf-8-sig")
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    return equity_path, trades_path, summary_path, reason_summary_path, holding_summary_path


def print_trade_diagnostics(sells_df: pd.DataFrame) -> None:
    if sells_df.empty:
        print("\n未发现已平仓交易，跳过诊断。")
        return

    print("\n亏损来源诊断:")

    reason_summary = (
        sells_df.groupby("reason", dropna=False)
        .agg(
            交易数=("profit", "count"),
            总盈亏=("profit", "sum"),
            平均盈亏=("profit", "mean"),
            胜率=("profit", lambda s: (s > 0).mean() * 100),
        )
        .sort_values("总盈亏")
    )
    print("\n按退出原因汇总:")
    print(reason_summary.round(2).to_string())

    loser_summary = (
        sells_df.groupby(["code", "name"], dropna=False)
        .agg(
            交易数=("profit", "count"),
            总盈亏=("profit", "sum"),
            平均盈亏=("profit", "mean"),
            胜率=("profit", lambda s: (s > 0).mean() * 100),
        )
        .sort_values("总盈亏")
        .head(10)
        .reset_index()
    )
    print("\n拖后腿最多的10只股票:")
    print(loser_summary.round(2).to_string(index=False))

    winner_summary = (
        sells_df.groupby(["code", "name"], dropna=False)
        .agg(
            交易数=("profit", "count"),
            总盈亏=("profit", "sum"),
            平均盈亏=("profit", "mean"),
            胜率=("profit", lambda s: (s > 0).mean() * 100),
        )
        .sort_values("总盈亏", ascending=False)
        .head(10)
        .reset_index()
    )
    print("\n贡献利润最多的10只股票:")
    print(winner_summary.round(2).to_string(index=False))


def run_backtest(
    verbose: bool = True,
    export_outputs: bool = True,
    preloaded_context: dict | None = None,
):
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

    portfolio = Portfolio(INITIAL_CAPITAL)
    pending_orders: list[PendingOrder] = []
    daily_values: list[dict] = []
    cooldown_until: dict[str, pd.Timestamp] = {}

    if verbose:
        print("=" * 70)
        print("🚀 股票版日线选股 + 30分钟择时回测")
        print(f"回测区间: {BACKTEST_START} 至 {BACKTEST_END}")
        print(f"样本股票数: {len(sample_codes)}")
        print("=" * 70)

    for idx, current_date in enumerate(all_dates):
        # 先执行挂单
        future_orders = []
        intraday_open_prices = {}
        for code, df in intraday_data.items():
            same_day = df[df["day"] == current_date]
            if not same_day.empty:
                intraday_open_prices[code] = float(same_day.iloc[-1]["close"])

        for order in pending_orders:
            if order.execute_dt.date() != current_date.date():
                future_orders.append(order)
                continue
            code = order.code
            if code not in intraday_data:
                continue
            exec_row = intraday_data[code][intraday_data[code]["datetime"] == order.execute_dt]
            if exec_row.empty:
                continue
            price = float(exec_row.iloc[0]["open"])
            name = instruments.loc[instruments["code"] == code, "name"].iloc[0]
            market = market_of(code)
            if order.action == "BUY":
                daily_slice = daily_data[code][daily_data[code]["date"] == current_date]
                if daily_slice.empty:
                    continue
                daily_row = daily_slice.iloc[0]
                atr = float(daily_row["atr14"])
                target_pct = MAX_POSITION_PCT * (0.4 if "弱市轻仓" in order.reason else 1.0)
                target_value = portfolio.total_value(intraday_open_prices) * target_pct
                portfolio.buy(order.execute_dt, code, name, market, price, target_value, order.reason, atr)
            else:
                sold = portfolio.sell(order.execute_dt, code, price, order.reason)
                if sold:
                    cooldown_until[code] = pd.Timestamp(order.execute_dt).normalize() + pd.Timedelta(days=REENTRY_COOLDOWN_DAYS)
        pending_orders = future_orders

        # 市场过滤
        risk_on = benchmark_risk_on(benchmark, current_date)
        risk_off = benchmark_risk_off(benchmark, current_date)

        # 出场检查
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
            stop_price, stop_reason = calc_stop_price(pos, daily_row)
            holding_days = (current_date - pd.Timestamp(pos["entry_dt"]).normalize()).days
            code_df = daily_data[code]
            idx_list = code_df.index[code_df["date"] == current_date].tolist()
            prev_daily_row = code_df.iloc[idx_list[0] - 1] if idx_list and idx_list[0] > 0 else None

            intraday_exit = get_day_intraday_exit(intraday_data[code], current_date, pos)
            if intraday_exit and not any(order.code == code and order.action == "SELL" for order in pending_orders):
                pending_orders.append(PendingOrder(intraday_exit[0], code, "SELL", intraday_exit[1]))
                continue

            if daily_row["close"] <= stop_price:
                exit_dt = get_next_session_open(intraday_data[code], current_date)
                if exit_dt:
                    pending_orders.append(PendingOrder(exit_dt[0], code, "SELL", stop_reason))
                    continue
            if should_exit_early_weakness(pos, holding_days):
                exit_dt = get_next_session_open(intraday_data[code], current_date)
                if exit_dt:
                    pending_orders.append(PendingOrder(exit_dt[0], code, "SELL", "早期弱势淘汰"))
                    continue
            if holding_days >= MAX_HOLDING_DAYS:
                exit_dt = get_next_session_open(intraday_data[code], current_date)
                if exit_dt:
                    pending_orders.append(PendingOrder(exit_dt[0], code, "SELL", "达到最大持有天数"))
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
                exit_dt = get_next_session_open(intraday_data[code], current_date)
                if exit_dt:
                    reason = "跌破20日线" if trend_break else "市场转弱"
                    pending_orders.append(PendingOrder(exit_dt[0], code, "SELL", reason))

        # 入场检查
        if True:
            candidates = select_candidates(current_date, daily_data, instruments, st_status, cooldown_until)
            holding_codes = set(portfolio.positions.keys())
            for code, _ in candidates:
                if code in holding_codes:
                    continue
                if any(order.code == code and order.action == "BUY" for order in pending_orders):
                    continue
                if code not in intraday_data:
                    continue
                entry_signal_dt = find_intraday_entry(intraday_data[code], current_date)
                if entry_signal_dt is None:
                    continue
                execute = get_next_session_open(intraday_data[code], current_date)
                if execute is None:
                    continue
                reason = "日线趋势+30分钟回踩突破"
                if not risk_on:
                    reason += "(弱市轻仓)"
                pending_orders.append(PendingOrder(execute[0], code, "BUY", reason))

        close_prices = {}
        for code, df in daily_data.items():
            row = df[df["date"] == current_date]
            if not row.empty:
                close_prices[code] = float(row.iloc[0]["close"])
        daily_values.append({"date": current_date, "value": portfolio.total_value(close_prices), "cash": portfolio.cash})

    # 期末平仓
    final_date = all_dates[-1]
    for code in list(portfolio.positions.keys()):
        row = daily_data[code][daily_data[code]["date"] == final_date]
        if not row.empty:
            portfolio.sell(pd.Timestamp(f"{final_date.date()} 15:00:00"), code, float(row.iloc[0]["close"]), "回测结束平仓")

    daily_df = pd.DataFrame(daily_values)
    trades_df = pd.DataFrame(portfolio.trades)
    metrics = performance_metrics(daily_df)
    sells_df = trades_df[trades_df["action"] == "卖出"].copy() if not trades_df.empty else pd.DataFrame()
    wins = sells_df[sells_df["profit"] > 0] if not sells_df.empty else pd.DataFrame()
    metrics.update(
        {
            "initial_capital": INITIAL_CAPITAL,
            "final_value": float(daily_df["value"].iloc[-1]) if not daily_df.empty else INITIAL_CAPITAL,
            "sell_trade_count": int(len(sells_df)),
            "win_rate_pct": float(len(wins) / len(sells_df) * 100) if not sells_df.empty else 0.0,
            "early_weak_exit_check_day": EARLY_WEAK_EXIT_CHECK_DAY,
            "early_weak_exit_min_close_ret": EARLY_WEAK_EXIT_MIN_CLOSE_RET,
            "early_weak_exit_min_high_ret": EARLY_WEAK_EXIT_MIN_HIGH_RET,
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
        print("📊 股票策略回测报告")
        print("=" * 70)
        print(f"初始资金: {INITIAL_CAPITAL:.2f} 元")
        print(f"最终资产: {daily_df['value'].iloc[-1]:.2f} 元")
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
            print("\n前10笔已平仓交易:")
            print(f"{'时间':<20} {'股票':<10} {'买入价':<10} {'卖出价':<10} {'盈亏':<12} {'原因':<20}")
            print("-" * 90)
            for _, trade in sells_df.head(10).iterrows():
                print(
                    f"{str(trade['datetime'])[:19]:<20} {trade['name']:<10} {trade['entry_price']:<10.2f} "
                    f"{trade['price']:<10.2f} {trade['profit']:<+12.2f} {trade['reason']:<20}"
                )
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
