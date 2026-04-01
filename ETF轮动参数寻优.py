"""
ETF 轮动策略参数寻优与分阶段回测

目标：
1. 不只看全样本收益，而是同时看训练区间、验证区间和全区间。
2. 优先选择收益/回撤/夏普更均衡的参数，而不是单段暴利参数。
"""

from __future__ import annotations

import itertools
import os
from dataclasses import dataclass

import numpy as np
import pandas as pd

TOTAL_CAPITAL = 10000
COMMISSION_RATE = 0.0000854
MIN_COMMISSION = 0.1
SLIPPAGE = 0.001
STAMP_TAX_RATE = 0.0005
TRANSFER_FEE_RATE = 0.00001

LOOKBACK_BUFFER_DAYS = 200
MIN_HISTORY_BARS = 120
BENCHMARK_CODE = "561380"

DEFAULT_DATA_DIRS = [
    os.environ.get("THS_BACKTEST_DATA_DIR"),
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "回测数据"),
    r"D:\量化\同花顺\回测数据",
    "/mnt/d/量化/同花顺/回测数据",
]

ETF_NAME_MAP = {
    "159206": "国证地产ETF",
    "159755": "电池ETF",
    "159796": "新能源车ETF",
    "159857": "光伏ETF",
    "159934": "黄金ETF",
    "159952": "创业板ETF",
    "159971": "传媒ETF",
    "161226": "国投白银LOF",
    "501018": "南方原油LOF",
    "512980": "传媒ETF",
    "515210": "钢铁ETF",
    "515230": "软件ETF",
    "561160": "电力ETF",
    "561380": "A500ETF",
    "563200": "港股科技ETF",
}


@dataclass(frozen=True)
class StrategyParams:
    top_n: int
    rebalance_days: int
    max_position_pct: float
    breadth_in: float
    breadth_out: float
    atr_stop: float
    atr_trail: float


def normalize_path(path: str | None) -> str | None:
    if not path:
        return None
    if os.path.exists(path):
        return path
    if ":" in path and "\\" in path:
        drive = path[0].lower()
        converted = f"/mnt/{drive}/" + path[3:].replace("\\", "/")
        if os.path.exists(converted):
            return converted
    return path


def resolve_data_dir() -> str:
    for candidate in DEFAULT_DATA_DIRS:
        normalized = normalize_path(candidate)
        if normalized and os.path.exists(normalized):
            return normalized
    raise FileNotFoundError("未找到回测数据目录，请检查 THS_BACKTEST_DATA_DIR 或项目目录。")


def market_of(code: str) -> str:
    return "sh" if code.startswith(("5", "6")) else "sz"


def calc_buy_cost(amount: float, market: str) -> tuple[float, float]:
    commission = max(amount * COMMISSION_RATE, MIN_COMMISSION)
    transfer_fee = amount * TRANSFER_FEE_RATE if market == "sh" else 0.0
    slippage = amount * SLIPPAGE
    total_fee = commission + transfer_fee + slippage
    return amount + total_fee, total_fee


def calc_sell_net(amount: float, market: str) -> tuple[float, float]:
    commission = max(amount * COMMISSION_RATE, MIN_COMMISSION)
    stamp_tax = amount * STAMP_TAX_RATE
    transfer_fee = amount * TRANSFER_FEE_RATE if market == "sh" else 0.0
    slippage = amount * SLIPPAGE
    total_fee = commission + stamp_tax + transfer_fee + slippage
    return amount - total_fee, total_fee


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma60"] = df["close"].rolling(60).mean()
    df["ret20"] = df["close"] / df["close"].shift(20) - 1
    df["ret60"] = df["close"] / df["close"].shift(60) - 1
    df["ret120"] = df["close"] / df["close"].shift(120) - 1
    prev_close = df["close"].shift(1)
    tr_components = pd.concat(
        [
            (df["high"] - df["low"]).abs(),
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    )
    df["atr14"] = tr_components.max(axis=1).rolling(14).mean()
    return df


def load_etf_universe(data_dir: str, start_date: str, end_date: str) -> dict[str, dict]:
    etf_dir = os.path.join(data_dir, "etfs")
    universe = {}
    history_start = pd.to_datetime(start_date) - pd.Timedelta(days=LOOKBACK_BUFFER_DAYS)
    backtest_start = pd.to_datetime(start_date)
    backtest_end = pd.to_datetime(end_date)

    for file_name in sorted(os.listdir(etf_dir)):
        if not file_name.endswith(".csv"):
            continue

        code = file_name[:-4]
        path = os.path.join(etf_dir, file_name)
        df = pd.read_csv(path)
        required = {"trade_date", "open", "high", "low", "close"}
        if required - set(df.columns):
            continue

        df["trade_date"] = pd.to_datetime(df["trade_date"])
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df[(df["trade_date"] >= history_start) & (df["trade_date"] <= backtest_end)].copy()
        df = df.dropna(subset=["trade_date", "open", "high", "low", "close"]).sort_values("trade_date").reset_index(drop=True)
        if len(df) < MIN_HISTORY_BARS:
            continue

        df = add_indicators(df)
        df = df[df["trade_date"] >= backtest_start].copy().reset_index(drop=True)
        if len(df) < 20 or df["ret120"].notna().sum() < 20:
            continue

        universe[code] = {
            "code": code,
            "name": ETF_NAME_MAP.get(code, code),
            "market": market_of(code),
            "df": df,
        }

    return universe


def compute_score(row: pd.Series) -> float:
    if pd.isna(row["ret20"]) or pd.isna(row["ret60"]) or pd.isna(row["ret120"]):
        return -np.inf
    trend_ok = (
        row["close"] > row["ma20"] > row["ma60"]
        and row["ret20"] > 0
        and row["ret60"] > 0
    )
    if not trend_ok:
        return -np.inf
    score = row["ret20"] * 0.5 + row["ret60"] * 0.35 + row["ret120"] * 0.15
    if row["ret20"] > 0.18:
        score -= 0.02
    return float(score)


def get_row_by_date(df: pd.DataFrame, current_date: pd.Timestamp) -> pd.Series | None:
    row = df[df["trade_date"] == current_date]
    if row.empty:
        return None
    return row.iloc[0]


def current_close_prices(universe: dict[str, dict], current_date: pd.Timestamp) -> dict[str, float]:
    prices = {}
    for code, info in universe.items():
        row = get_row_by_date(info["df"], current_date)
        if row is not None:
            prices[code] = float(row["close"])
    return prices


def next_trade_date(all_dates: list[pd.Timestamp], current_date: pd.Timestamp) -> pd.Timestamp | None:
    try:
        idx = all_dates.index(current_date)
    except ValueError:
        return None
    if idx + 1 >= len(all_dates):
        return None
    return all_dates[idx + 1]


def market_regime(universe: dict[str, dict], current_date: pd.Timestamp, params: StrategyParams) -> dict[str, float | bool]:
    trend_flags = []
    for info in universe.values():
        row = get_row_by_date(info["df"], current_date)
        if row is None or pd.isna(row["ma20"]) or pd.isna(row["ma60"]):
            continue
        trend_flags.append(bool(row["close"] > row["ma20"] > row["ma60"]))

    breadth = float(np.mean(trend_flags)) if trend_flags else 0.0
    benchmark_row = get_row_by_date(universe[BENCHMARK_CODE]["df"], current_date) if BENCHMARK_CODE in universe else None
    benchmark_up = False
    benchmark_down = False
    if benchmark_row is not None and not pd.isna(benchmark_row["ma20"]) and not pd.isna(benchmark_row["ma60"]):
        benchmark_up = bool(
            benchmark_row["close"] > benchmark_row["ma20"] > benchmark_row["ma60"]
            and benchmark_row["ret20"] > 0
        )
        benchmark_down = bool(benchmark_row["close"] < benchmark_row["ma20"])

    return {
        "breadth": breadth,
        "risk_on": breadth >= params.breadth_in and benchmark_up,
        "force_defense": breadth <= params.breadth_out and benchmark_down,
    }


class Portfolio:
    def __init__(self):
        self.available_cash = TOTAL_CAPITAL
        self.positions: dict[str, dict] = {}
        self.trades: list[dict] = []

    def total_value(self, current_prices: dict[str, float]) -> float:
        return self.available_cash + sum(
            pos["shares"] * current_prices.get(code, 0.0)
            for code, pos in self.positions.items()
        )

    def buy(self, date, code, info, price, target_value, atr):
        shares = int(target_value / price / 100) * 100
        while shares > 0:
            amount = shares * price
            total_cost, fee = calc_buy_cost(amount, info["market"])
            if total_cost <= self.available_cash:
                self.available_cash -= total_cost
                self.positions[code] = {
                    "name": info["name"],
                    "market": info["market"],
                    "shares": shares,
                    "entry_date": date,
                    "entry_price": price,
                    "cost_basis": total_cost,
                    "atr_at_entry": atr,
                    "highest_close": price,
                }
                self.trades.append(
                    {"date": date, "code": code, "name": info["name"], "action": "买入", "price": price, "shares": shares, "fee": fee}
                )
                return True
            shares -= 100
        return False

    def sell(self, date, code, price, reason):
        if code not in self.positions:
            return False
        pos = self.positions.pop(code)
        amount = pos["shares"] * price
        net_proceeds, fee = calc_sell_net(amount, pos["market"])
        profit = net_proceeds - pos["cost_basis"]
        profit_rate = profit / pos["cost_basis"] * 100 if pos["cost_basis"] else 0.0
        self.available_cash += net_proceeds
        self.trades.append(
            {
                "date": date,
                "code": code,
                "name": pos["name"],
                "action": "卖出",
                "price": price,
                "shares": pos["shares"],
                "entry_date": pos["entry_date"],
                "entry_price": pos["entry_price"],
                "profit": profit,
                "profit_rate": profit_rate,
                "reason": reason,
                "fee": fee,
            }
        )
        return True


def performance_metrics(daily_df: pd.DataFrame) -> dict[str, float]:
    if daily_df.empty:
        return {"return_pct": 0.0, "annual_pct": 0.0, "max_drawdown_pct": 0.0, "sharpe": 0.0}

    values = daily_df["value"].astype(float)
    returns = values.pct_change().fillna(0.0)
    total_return = values.iloc[-1] / TOTAL_CAPITAL - 1
    annual_return = (1 + total_return) ** (252 / len(values)) - 1 if total_return > -1 else -1
    drawdown = values / values.cummax() - 1
    sharpe = 0.0 if returns.std() == 0 else np.sqrt(252) * returns.mean() / returns.std()
    return {
        "return_pct": float(total_return * 100),
        "annual_pct": float(annual_return * 100),
        "max_drawdown_pct": float(drawdown.min() * 100),
        "sharpe": float(sharpe),
    }


def run_strategy(universe: dict[str, dict], params: StrategyParams) -> dict:
    portfolio = Portfolio()
    pending_orders: list[dict] = []
    daily_values: list[dict] = []
    all_dates = sorted(set(date for info in universe.values() for date in info["df"]["trade_date"].tolist()))

    for day_idx, current_date in enumerate(all_dates):
        future_orders = []
        for order in pending_orders:
            if order["execute_date"] != current_date:
                future_orders.append(order)
                continue
            info = universe.get(order["code"])
            if not info:
                continue
            row = get_row_by_date(info["df"], current_date)
            if row is None:
                continue
            open_price = float(row["open"])
            if order["action"] == "SELL":
                portfolio.sell(current_date, order["code"], open_price, order["reason"])
            else:
                target_value = portfolio.total_value(current_close_prices(universe, current_date)) * params.max_position_pct
                atr = float(row["atr14"]) if not pd.isna(row["atr14"]) else open_price * 0.03
                portfolio.buy(current_date, order["code"], info, open_price, target_value, atr)
        pending_orders = future_orders

        close_prices = current_close_prices(universe, current_date)
        regime = market_regime(universe, current_date, params)

        scored = []
        for code, info in universe.items():
            row = get_row_by_date(info["df"], current_date)
            if row is None:
                continue
            score = compute_score(row)
            if np.isfinite(score):
                scored.append((code, score))
        scored.sort(key=lambda x: x[1], reverse=True)
        top_codes = [code for code, _ in scored[: params.top_n]]

        execute_date = next_trade_date(all_dates, current_date)
        if execute_date is not None:
            for code in list(portfolio.positions.keys()):
                if code not in close_prices:
                    continue
                pos = portfolio.positions[code]
                close_price = close_prices[code]
                pos["highest_close"] = max(pos["highest_close"], close_price)
                row = get_row_by_date(universe[code]["df"], current_date)
                current_atr = float(row["atr14"]) if row is not None and not pd.isna(row["atr14"]) else pos["atr_at_entry"]
                atr_base = max(pos["atr_at_entry"], current_atr)
                stop_price = pos["entry_price"] - params.atr_stop * atr_base
                trail_price = pos["highest_close"] - params.atr_trail * atr_base

                if close_price <= stop_price:
                    if not any(o["code"] == code and o["action"] == "SELL" for o in pending_orders):
                        pending_orders.append({"execute_date": execute_date, "code": code, "action": "SELL", "reason": "ATR初始止损"})
                    continue

                if pos["highest_close"] > pos["entry_price"] + atr_base and close_price <= trail_price:
                    if not any(o["code"] == code and o["action"] == "SELL" for o in pending_orders):
                        pending_orders.append({"execute_date": execute_date, "code": code, "action": "SELL", "reason": "ATR移动止盈"})

            if regime["force_defense"]:
                for code in list(portfolio.positions.keys()):
                    if not any(o["code"] == code and o["action"] == "SELL" for o in pending_orders):
                        pending_orders.append({"execute_date": execute_date, "code": code, "action": "SELL", "reason": "市场状态转弱"})

            for code in list(portfolio.positions.keys()):
                row = get_row_by_date(universe[code]["df"], current_date)
                if row is None:
                    continue
                trend_broken = not (row["close"] > row["ma20"] > row["ma60"] and row["ret20"] > -0.02)
                if trend_broken and not any(o["code"] == code and o["action"] == "SELL" for o in pending_orders):
                    pending_orders.append({"execute_date": execute_date, "code": code, "action": "SELL", "reason": "趋势失效"})

            if day_idx % params.rebalance_days == 0 and regime["risk_on"]:
                holding_codes = set(portfolio.positions.keys())
                target_codes = set(top_codes)
                for code in sorted(holding_codes - target_codes):
                    if not any(o["code"] == code and o["action"] == "SELL" for o in pending_orders):
                        pending_orders.append({"execute_date": execute_date, "code": code, "action": "SELL", "reason": "轮动调出"})
                for code in top_codes:
                    if code not in holding_codes and not any(o["code"] == code and o["action"] == "BUY" for o in pending_orders):
                        pending_orders.append({"execute_date": execute_date, "code": code, "action": "BUY", "reason": "趋势+相对强弱入选"})

        daily_values.append({"date": current_date, "value": portfolio.total_value(close_prices)})

    if all_dates:
        final_date = all_dates[-1]
        final_prices = current_close_prices(universe, final_date)
        for code in list(portfolio.positions.keys()):
            if code in final_prices:
                portfolio.sell(final_date, code, final_prices[code], "回测结束平仓")

    daily_df = pd.DataFrame(daily_values)
    metrics = performance_metrics(daily_df)
    sells = [trade for trade in portfolio.trades if trade["action"] == "卖出"]
    metrics["trades"] = len(sells)
    metrics["win_rate_pct"] = float(sum(t["profit"] > 0 for t in sells) / len(sells) * 100) if sells else 0.0
    return metrics


def score_params(train: dict, valid: dict) -> float:
    trade_penalty = 0 if valid["trades"] >= 3 else -5
    dd_penalty = abs(valid["max_drawdown_pct"]) * 0.12
    train_dd_penalty = abs(train["max_drawdown_pct"]) * 0.05
    return (
        valid["return_pct"] * 0.35
        + valid["sharpe"] * 12
        + train["sharpe"] * 4
        - dd_penalty
        - train_dd_penalty
        + trade_penalty
    )


def build_output_path(file_name: str) -> str:
    for path in [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), file_name),
        os.path.join(os.getcwd(), file_name),
        os.path.join("/tmp", file_name),
    ]:
        parent = os.path.dirname(path)
        if os.path.isdir(parent) and os.access(parent, os.W_OK):
            return path
    return os.path.join("/tmp", file_name)


def run_optimization():
    data_dir = resolve_data_dir()
    train_range = ("2025-11-01", "2026-01-31")
    valid_range = ("2026-02-01", "2026-03-27")
    full_range = ("2025-11-01", "2026-03-27")

    grids = {
        "top_n": [1, 2],
        "rebalance_days": [5, 10],
        "max_position_pct": [0.30, 0.40],
        "breadth_in": [0.30, 0.40],
        "breadth_out": [0.20, 0.25],
        "atr_stop": [2.0, 2.5],
        "atr_trail": [3.2, 3.8],
    }

    train_universe = load_etf_universe(data_dir, *train_range)
    valid_universe = load_etf_universe(data_dir, *valid_range)
    full_universe = load_etf_universe(data_dir, *full_range)

    rows = []
    for values in itertools.product(*grids.values()):
        params = StrategyParams(*values)
        if params.breadth_out >= params.breadth_in:
            continue
        if params.atr_trail <= params.atr_stop:
            continue

        train_metrics = run_strategy(train_universe, params)
        valid_metrics = run_strategy(valid_universe, params)
        full_metrics = run_strategy(full_universe, params)
        objective = score_params(train_metrics, valid_metrics)

        rows.append(
            {
                "top_n": params.top_n,
                "rebalance_days": params.rebalance_days,
                "max_position_pct": params.max_position_pct,
                "breadth_in": params.breadth_in,
                "breadth_out": params.breadth_out,
                "atr_stop": params.atr_stop,
                "atr_trail": params.atr_trail,
                "train_return_pct": train_metrics["return_pct"],
                "train_dd_pct": train_metrics["max_drawdown_pct"],
                "train_sharpe": train_metrics["sharpe"],
                "train_trades": train_metrics["trades"],
                "valid_return_pct": valid_metrics["return_pct"],
                "valid_dd_pct": valid_metrics["max_drawdown_pct"],
                "valid_sharpe": valid_metrics["sharpe"],
                "valid_trades": valid_metrics["trades"],
                "full_return_pct": full_metrics["return_pct"],
                "full_dd_pct": full_metrics["max_drawdown_pct"],
                "full_sharpe": full_metrics["sharpe"],
                "full_trades": full_metrics["trades"],
                "objective": objective,
            }
        )

    result_df = pd.DataFrame(rows).sort_values(["objective", "valid_sharpe", "valid_return_pct"], ascending=False).reset_index(drop=True)
    best = result_df.iloc[0]

    monthly_universe = load_etf_universe(data_dir, *full_range)
    monthly_rows = []
    for month, month_df in pd.DataFrame({"date": sorted(set(date for info in monthly_universe.values() for date in info["df"]["trade_date"].tolist()))}).groupby(
        pd.Grouper(key="date", freq="M")
    ):
        if month_df.empty:
            continue
        month_start = str(month_df["date"].min().date())
        month_end = str(month_df["date"].max().date())
        month_universe = load_etf_universe(data_dir, month_start, month_end)
        month_metrics = run_strategy(
            month_universe,
            StrategyParams(
                int(best["top_n"]),
                int(best["rebalance_days"]),
                float(best["max_position_pct"]),
                float(best["breadth_in"]),
                float(best["breadth_out"]),
                float(best["atr_stop"]),
                float(best["atr_trail"]),
            ),
        )
        monthly_rows.append(
            {
                "month": str(month.date())[:7],
                "return_pct": month_metrics["return_pct"],
                "max_drawdown_pct": month_metrics["max_drawdown_pct"],
                "sharpe": month_metrics["sharpe"],
                "trades": month_metrics["trades"],
            }
        )

    best_summary = pd.DataFrame(
        [
            {"参数": "top_n", "数值": int(best["top_n"])},
            {"参数": "rebalance_days", "数值": int(best["rebalance_days"])},
            {"参数": "max_position_pct", "数值": float(best["max_position_pct"])},
            {"参数": "breadth_in", "数值": float(best["breadth_in"])},
            {"参数": "breadth_out", "数值": float(best["breadth_out"])},
            {"参数": "atr_stop", "数值": float(best["atr_stop"])},
            {"参数": "atr_trail", "数值": float(best["atr_trail"])},
            {"参数": "训练收益%", "数值": float(best["train_return_pct"])},
            {"参数": "验证收益%", "数值": float(best["valid_return_pct"])},
            {"参数": "全样本收益%", "数值": float(best["full_return_pct"])},
            {"参数": "全样本回撤%", "数值": float(best["full_dd_pct"])},
            {"参数": "全样本夏普", "数值": float(best["full_sharpe"])},
            {"参数": "目标函数", "数值": float(best["objective"])},
        ]
    )

    print("=" * 70)
    print("🔎 ETF 轮动参数寻优结果")
    print("=" * 70)
    print("训练区间: 2025-11-01 至 2026-01-31")
    print("验证区间: 2026-02-01 至 2026-03-27")
    print(f"共评估参数组合: {len(result_df)}")
    print("\n最优参数:")
    for _, row in best_summary.iloc[:7].iterrows():
        print(f"  {row['参数']}: {row['数值']}")
    print("\n分阶段表现:")
    print(f"  训练收益: {best['train_return_pct']:+.2f}% | 训练回撤: {best['train_dd_pct']:.2f}% | 训练夏普: {best['train_sharpe']:.2f}")
    print(f"  验证收益: {best['valid_return_pct']:+.2f}% | 验证回撤: {best['valid_dd_pct']:.2f}% | 验证夏普: {best['valid_sharpe']:.2f}")
    print(f"  全样本收益: {best['full_return_pct']:+.2f}% | 全样本回撤: {best['full_dd_pct']:.2f}% | 全样本夏普: {best['full_sharpe']:.2f}")

    output_path = build_output_path("ETF轮动参数寻优.xlsx")
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        best_summary.to_excel(writer, sheet_name="最优参数", index=False)
        result_df.head(50).to_excel(writer, sheet_name="Top50组合", index=False)
        pd.DataFrame(monthly_rows).to_excel(writer, sheet_name="月度分阶段", index=False)

    print(f"\n📁 寻优报告已保存: {output_path}")


if __name__ == "__main__":
    run_optimization()
