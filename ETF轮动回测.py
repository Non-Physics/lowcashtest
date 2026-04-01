"""
ETF 趋势过滤 + 相对强弱轮动回测

策略规则：
1. 只使用 ETF 数据池，避免个股噪声过大。
2. 当日收盘后计算趋势和相对强弱，次日开盘调仓。
3. 趋势过滤：close > ma20 > ma60，且 20/60 日收益为正。
4. 相对强弱：按 20/60/120 日动量加权打分，选前 2 名持有。
5. 每 5 个交易日轮动一次；若持仓跌破趋势、ATR 止损触发或市场状态转弱，则提前卖出。
"""

from __future__ import annotations

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

START_DATE = "2025-11-01"
END_DATE = "2026-03-27"
LOOKBACK_BUFFER_DAYS = 200

TOP_N = 2
REBALANCE_EVERY_N_DAYS = 10
MIN_HISTORY_BARS = 120
BENCHMARK_CODE = "561380"
MAX_POSITION_PCT = 0.30
MARKET_BREADTH_THRESHOLD = 0.40
MARKET_EXIT_BREADTH_THRESHOLD = 0.20
ATR_STOP_MULTIPLIER = 2.0
ATR_TRAIL_MULTIPLIER = 3.2

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


def market_of(code: str) -> str:
    return "sh" if code.startswith(("5", "6")) else "sz"


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


def load_etf_universe(data_dir: str) -> dict[str, dict]:
    etf_dir = os.path.join(data_dir, "etfs")
    universe = {}
    history_start = pd.to_datetime(START_DATE) - pd.Timedelta(days=LOOKBACK_BUFFER_DAYS)
    backtest_start = pd.to_datetime(START_DATE)
    backtest_end = pd.to_datetime(END_DATE)

    for file_name in sorted(os.listdir(etf_dir)):
        if not file_name.endswith(".csv"):
            continue

        code = file_name[:-4]
        path = os.path.join(etf_dir, file_name)
        df = pd.read_csv(path)
        if {"trade_date", "open", "high", "low", "close"} - set(df.columns):
            continue

        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df["open"] = pd.to_numeric(df["open"], errors="coerce")
        df["high"] = pd.to_numeric(df["high"], errors="coerce")
        df["low"] = pd.to_numeric(df["low"], errors="coerce")
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
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


@dataclass
class PendingOrder:
    execute_date: pd.Timestamp
    code: str
    action: str
    reason: str


class RotationPortfolio:
    def __init__(self, total_capital: float = TOTAL_CAPITAL):
        self.total_capital = total_capital
        self.available_cash = total_capital
        self.positions: dict[str, dict] = {}
        self.trades: list[dict] = []

    def position_value(self, current_prices: dict[str, float]) -> float:
        return sum(pos["shares"] * current_prices.get(code, 0.0) for code, pos in self.positions.items())

    def total_value(self, current_prices: dict[str, float]) -> float:
        return self.available_cash + self.position_value(current_prices)

    def target_shares(self, price: float, market: str, target_value: float) -> int:
        shares = int(target_value / price / 100) * 100
        while shares > 0:
            amount = price * shares
            total_cost, _ = calc_buy_cost(amount, market)
            if total_cost <= self.available_cash + 1e-9:
                return shares
            shares -= 100
        return 0

    def buy(self, date, code, name, market, price, target_value, reason, atr):
        if code in self.positions:
            return False

        shares = self.target_shares(price, market, target_value)
        if shares <= 0:
            return False

        amount = price * shares
        total_cost, fee = calc_buy_cost(amount, market)
        if total_cost > self.available_cash:
            return False

        self.available_cash -= total_cost
        self.positions[code] = {
            "name": name,
            "market": market,
            "shares": shares,
            "entry_date": date,
            "entry_price": price,
            "cost_basis": total_cost,
            "atr_at_entry": atr,
            "highest_close": price,
        }
        self.trades.append(
            {
                "date": date,
                "code": code,
                "name": name,
                "action": "买入",
                "price": price,
                "shares": shares,
                "reason": reason,
                "fee": fee,
            }
        )
        print(f"  🔴 买入 {name}({code}) {shares}股 @{price:.3f} | 理由:{reason}")
        return True

    def sell(self, date, code, price, reason):
        if code not in self.positions:
            return False

        pos = self.positions.pop(code)
        amount = price * pos["shares"]
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
        print(f"  🟢 卖出 {pos['name']}({code}) {pos['shares']}股 @{price:.3f} | 盈亏:{profit:+.2f}({profit_rate:+.2f}%) | 理由:{reason}")
        return True


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

    # 短中长期动量加权，并适当惩罚过热。
    score = row["ret20"] * 0.5 + row["ret60"] * 0.35 + row["ret120"] * 0.15
    if row["ret20"] > 0.18:
        score -= 0.02
    return float(score)


def get_row_by_date(df: pd.DataFrame, current_date: pd.Timestamp) -> pd.Series | None:
    row = df[df["trade_date"] == current_date]
    if row.empty:
        return None
    return row.iloc[0]


def market_regime(universe: dict[str, dict], current_date: pd.Timestamp) -> dict[str, float | bool]:
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

    risk_on = breadth >= MARKET_BREADTH_THRESHOLD and benchmark_up
    force_defense = breadth <= MARKET_EXIT_BREADTH_THRESHOLD and benchmark_down
    return {
        "breadth": breadth,
        "risk_on": risk_on,
        "force_defense": force_defense,
    }


def performance_metrics(daily_df: pd.DataFrame, initial_capital: float) -> dict[str, float]:
    if daily_df.empty:
        return {
            "final_value": initial_capital,
            "total_return_pct": 0.0,
            "annual_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "sharpe": 0.0,
        }

    values = daily_df["value"].astype(float)
    returns = values.pct_change().fillna(0.0)
    total_return = values.iloc[-1] / initial_capital - 1
    annual_return = (1 + total_return) ** (252 / len(values)) - 1 if total_return > -1 else -1
    drawdown = values / values.cummax() - 1
    sharpe = 0.0 if returns.std() == 0 else np.sqrt(252) * returns.mean() / returns.std()
    return {
        "final_value": float(values.iloc[-1]),
        "total_return_pct": float(total_return * 100),
        "annual_return_pct": float(annual_return * 100),
        "max_drawdown_pct": float(drawdown.min() * 100),
        "sharpe": float(sharpe),
    }


def build_output_path(file_name: str) -> str:
    candidates = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), file_name),
        os.path.join(os.getcwd(), file_name),
        os.path.join("/tmp", file_name),
    ]
    for path in candidates:
        parent = os.path.dirname(path)
        if os.path.isdir(parent) and os.access(parent, os.W_OK):
            return path
    return os.path.join("/tmp", file_name)


def run_backtest():
    data_dir = resolve_data_dir()
    universe = load_etf_universe(data_dir)
    print("=" * 70)
    print("🚀 ETF 趋势过滤 + 相对强弱轮动回测")
    print(f"回测周期: {START_DATE} 至 {END_DATE}")
    print(f"ETF 数量: {len(universe)}")
    print(f"调仓频率: 每 {REBALANCE_EVERY_N_DAYS} 个交易日")
    print("=" * 70)

    portfolio = RotationPortfolio()
    pending_orders: list[PendingOrder] = []
    daily_values: list[dict] = []

    all_dates = sorted(set(date for info in universe.values() for date in info["df"]["trade_date"].tolist()))
    rank_history: list[dict] = []
    regime_history: list[dict] = []

    for day_idx, current_date in enumerate(all_dates):
        # 执行订单
        future_orders = []
        for order in pending_orders:
            if order.execute_date != current_date:
                future_orders.append(order)
                continue

            info = universe.get(order.code)
            if not info:
                continue
            row = info["df"][info["df"]["trade_date"] == current_date]
            if row.empty:
                continue
            open_price = float(row.iloc[0]["open"])
            if order.action == "SELL":
                portfolio.sell(current_date, order.code, open_price, order.reason)
            elif order.action == "BUY":
                target_value = portfolio.total_value(current_close_prices(universe, current_date)) * MAX_POSITION_PCT
                atr = float(row.iloc[0]["atr14"]) if not pd.isna(row.iloc[0]["atr14"]) else open_price * 0.03
                portfolio.buy(current_date, order.code, info["name"], info["market"], open_price, target_value, order.reason, atr)
        pending_orders = future_orders

        close_prices = current_close_prices(universe, current_date)
        regime = market_regime(universe, current_date)
        regime_history.append(
            {
                "date": current_date,
                "breadth": regime["breadth"],
                "risk_on": regime["risk_on"],
                "force_defense": regime["force_defense"],
            }
        )

        # 收盘后检查是否需要调仓
        scored = []
        for code, info in universe.items():
            row = info["df"][info["df"]["trade_date"] == current_date]
            if row.empty:
                continue
            row = row.iloc[0]
            score = compute_score(row)
            if np.isfinite(score):
                scored.append((code, score, row))

        scored.sort(key=lambda item: item[1], reverse=True)
        top_codes = [code for code, _, _ in scored[:TOP_N]]

        if scored:
            rank_history.append(
                {
                    "date": current_date,
                    "leaders": ",".join(f"{universe[code]['name']}({score:.3f})" for code, score, _ in scored[:TOP_N]),
                }
            )

        should_rebalance = day_idx % REBALANCE_EVERY_N_DAYS == 0

        execute_date = next_trade_date(all_dates, current_date)
        if execute_date is not None:
            # 更新持仓最高收盘价并检查 ATR 风控
            for code in list(portfolio.positions.keys()):
                if code not in close_prices:
                    continue
                position = portfolio.positions[code]
                close_price = close_prices[code]
                position["highest_close"] = max(position["highest_close"], close_price)

                row = get_row_by_date(universe[code]["df"], current_date)
                current_atr = float(row["atr14"]) if row is not None and not pd.isna(row["atr14"]) else position["atr_at_entry"]
                atr_base = max(position["atr_at_entry"], current_atr)
                stop_price = position["entry_price"] - ATR_STOP_MULTIPLIER * atr_base
                trail_price = position["highest_close"] - ATR_TRAIL_MULTIPLIER * atr_base

                if close_price <= stop_price and not any(order.code == code and order.action == "SELL" for order in pending_orders):
                    pending_orders.append(PendingOrder(execute_date, code, "SELL", "ATR初始止损"))
                    continue

                if position["highest_close"] > position["entry_price"] + atr_base and close_price <= trail_price:
                    if not any(order.code == code and order.action == "SELL" for order in pending_orders):
                        pending_orders.append(PendingOrder(execute_date, code, "SELL", "ATR移动止盈"))

            if regime["force_defense"]:
                for code in list(portfolio.positions.keys()):
                    if not any(order.code == code and order.action == "SELL" for order in pending_orders):
                        pending_orders.append(PendingOrder(execute_date, code, "SELL", "市场状态转弱"))

            # 趋势失效先卖
            for code in list(portfolio.positions.keys()):
                info = universe[code]
                row = info["df"][info["df"]["trade_date"] == current_date]
                if row.empty:
                    continue
                row = row.iloc[0]
                trend_broken = not (
                    row["close"] > row["ma20"] > row["ma60"]
                    and row["ret20"] > -0.02
                )
                if trend_broken:
                    if not any(order.code == code and order.action == "SELL" for order in pending_orders):
                        pending_orders.append(PendingOrder(execute_date, code, "SELL", "趋势失效"))

            if should_rebalance and regime["risk_on"]:
                current_holding_codes = set(portfolio.positions.keys())
                target_codes = set(top_codes)

                for code in sorted(current_holding_codes - target_codes):
                    if not any(order.code == code and order.action == "SELL" for order in pending_orders):
                        pending_orders.append(PendingOrder(execute_date, code, "SELL", "轮动调出"))

                for code in top_codes:
                    if code not in current_holding_codes and not any(order.code == code and order.action == "BUY" for order in pending_orders):
                        pending_orders.append(PendingOrder(execute_date, code, "BUY", "趋势+相对强弱入选"))

        daily_values.append(
            {
                "date": current_date,
                "value": portfolio.total_value(close_prices),
                "cash": portfolio.available_cash,
                "positions": len(portfolio.positions),
                "leaders": ",".join(top_codes),
                "breadth": regime["breadth"],
                "risk_on": regime["risk_on"],
            }
        )

    # 回测结束平仓
    final_date = all_dates[-1]
    final_prices = current_close_prices(universe, final_date)
    for code in list(portfolio.positions.keys()):
        portfolio.sell(final_date, code, final_prices[code], "回测结束平仓")

    report(portfolio, pd.DataFrame(daily_values), pd.DataFrame(rank_history), pd.DataFrame(regime_history))


def current_close_prices(universe: dict[str, dict], current_date: pd.Timestamp) -> dict[str, float]:
    prices = {}
    for code, info in universe.items():
        row = info["df"][info["df"]["trade_date"] == current_date]
        if not row.empty:
            prices[code] = float(row.iloc[0]["close"])
    return prices


def next_trade_date(all_dates: list[pd.Timestamp], current_date: pd.Timestamp) -> pd.Timestamp | None:
    try:
        idx = all_dates.index(current_date)
    except ValueError:
        return None
    if idx + 1 >= len(all_dates):
        return None
    return all_dates[idx + 1]


def report(portfolio: RotationPortfolio, daily_df: pd.DataFrame, rank_df: pd.DataFrame, regime_df: pd.DataFrame):
    sells = [trade for trade in portfolio.trades if trade["action"] == "卖出"]
    metrics = performance_metrics(daily_df, portfolio.total_capital)
    total_profit = sum(trade.get("profit", 0.0) for trade in sells)
    wins = [trade for trade in sells if trade.get("profit", 0.0) > 0]
    losses = [trade for trade in sells if trade.get("profit", 0.0) <= 0]

    print("\n" + "=" * 70)
    print("📊 ETF 轮动回测报告")
    print("=" * 70)
    print(f"初始资金: {portfolio.total_capital:.2f} 元")
    print(f"最终资产: {metrics['final_value']:.2f} 元")
    print(f"总盈亏: {total_profit:+.2f} 元")
    print(f"总收益率: {metrics['total_return_pct']:+.2f}%")
    print(f"年化收益率: {metrics['annual_return_pct']:+.2f}%")
    print(f"最大回撤: {metrics['max_drawdown_pct']:.2f}%")
    print(f"夏普比率: {metrics['sharpe']:.2f}")
    print(f"交易次数: {len(sells)}")
    print(f"胜率: {len(wins) / len(sells) * 100:.1f}%" if sells else "胜率: N/A")

    if sells:
        print("\n🏆 前10大盈利交易:")
        print(f"{'日期':<12} {'标的':<14} {'买入价':<10} {'卖出价':<10} {'盈亏':<12} {'收益率':<10}")
        print("-" * 64)
        for trade in sorted(sells, key=lambda item: item["profit"], reverse=True)[:10]:
            print(
                f"{str(trade['date'])[:10]:<12} {trade['name']:<14} {trade['entry_price']:<10.3f} "
                f"{trade['price']:<10.3f} {trade['profit']:<+12.2f} {trade['profit_rate']:<+10.2f}%"
            )

    file_name = f"ETF轮动回测报告_{START_DATE[:7]}_{END_DATE[:7]}.xlsx"
    output_path = build_output_path(file_name)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        pd.DataFrame(
            [
                {"指标": "初始资金", "数值": f"{portfolio.total_capital:.2f}元"},
                {"指标": "最终资产", "数值": f"{metrics['final_value']:.2f}元"},
                {"指标": "总收益率", "数值": f"{metrics['total_return_pct']:+.2f}%"},
                {"指标": "年化收益率", "数值": f"{metrics['annual_return_pct']:+.2f}%"},
                {"指标": "最大回撤", "数值": f"{metrics['max_drawdown_pct']:.2f}%"},
                {"指标": "夏普比率", "数值": f"{metrics['sharpe']:.2f}"},
                {"指标": "交易次数", "数值": len(sells)},
                {"指标": "胜率", "数值": f"{len(wins) / len(sells) * 100:.1f}%"} if sells else {"指标": "胜率", "数值": "N/A"},
            ]
        ).to_excel(writer, sheet_name="汇总", index=False)

        pd.DataFrame(sells).to_excel(writer, sheet_name="交易明细", index=False)
        daily_df.to_excel(writer, sheet_name="每日权益", index=False)
        rank_df.to_excel(writer, sheet_name="轮动排名", index=False)
        regime_df.to_excel(writer, sheet_name="市场状态", index=False)

    print(f"\n📁 报告已保存: {output_path}")


if __name__ == "__main__":
    run_backtest()
