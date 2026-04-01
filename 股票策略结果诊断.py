from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "股票策略回测"
TIME_SPLIT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "股票策略时间切分"


def read_summary(summary_path: Path) -> dict:
    with summary_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def read_equity_curve(curve_path: Path) -> list[dict]:
    rows = []
    with curve_path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            row["value"] = float(row["value"])
            row["cash"] = float(row["cash"])
            rows.append(row)
    return rows


def read_sell_trades(trades_path: Path) -> list[dict]:
    sells = []
    with trades_path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            if row["action"] != "卖出":
                continue
            row["profit"] = float(row["profit"])
            row["profit_rate"] = float(row["profit_rate"])
            sells.append(row)
    return sells


def calc_curve_stats(rows: list[dict]) -> dict:
    peak_value = 0.0
    peak_date = None
    max_drawdown = 0.0
    drawdown_from = None
    drawdown_to = None
    monthly = {}

    for row in rows:
        value = row["value"]
        date = row["date"]
        month = date[:7]
        if month not in monthly:
            monthly[month] = {"start": value, "end": value}
        monthly[month]["end"] = value

        if value > peak_value:
            peak_value = value
            peak_date = date
        drawdown = value / peak_value - 1 if peak_value else 0.0
        if drawdown < max_drawdown:
            max_drawdown = drawdown
            drawdown_from = peak_date
            drawdown_to = date

    monthly_returns = {
        month: (item["end"] / item["start"] - 1) * 100
        for month, item in monthly.items()
    }
    return {
        "peak_value": peak_value,
        "peak_date": peak_date,
        "max_drawdown_pct": max_drawdown * 100,
        "drawdown_from": drawdown_from,
        "drawdown_to": drawdown_to,
        "monthly_returns_pct": monthly_returns,
    }


def calc_trade_stats(sells: list[dict]) -> dict:
    by_reason = defaultdict(lambda: {"count": 0, "profit": 0.0, "wins": 0})
    sorted_sells = sorted(sells, key=lambda item: item["profit"])

    for row in sells:
        reason = row["reason"]
        bucket = by_reason[reason]
        bucket["count"] += 1
        bucket["profit"] += row["profit"]
        if row["profit"] > 0:
            bucket["wins"] += 1

    reason_stats = []
    for reason, item in sorted(by_reason.items(), key=lambda kv: kv[1]["profit"]):
        reason_stats.append(
            {
                "reason": reason,
                "count": item["count"],
                "profit": item["profit"],
                "avg_profit": item["profit"] / item["count"] if item["count"] else 0.0,
                "win_rate_pct": item["wins"] / item["count"] * 100 if item["count"] else 0.0,
            }
        )

    return {
        "best_trade": sorted_sells[-1] if sorted_sells else None,
        "worst_trade": sorted_sells[0] if sorted_sells else None,
        "last_5_sells": sells[-5:],
        "reason_stats": reason_stats,
    }


def analyze_output_dir(output_dir: Path) -> dict:
    summary = read_summary(output_dir / "股票策略_回测摘要.json")
    curve_stats = calc_curve_stats(read_equity_curve(output_dir / "股票策略_净值曲线.csv"))
    trade_stats = calc_trade_stats(read_sell_trades(output_dir / "股票策略_交易明细.csv"))
    return {
        "output_dir": str(output_dir),
        "summary": summary,
        "curve_stats": curve_stats,
        "trade_stats": trade_stats,
    }


def print_analysis(title: str, analysis: dict) -> None:
    summary = analysis["summary"]
    curve_stats = analysis["curve_stats"]
    trade_stats = analysis["trade_stats"]

    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)
    print(
        f"区间: {summary['backtest_start']} -> {summary['backtest_end']} | "
        f"收益 {summary['return_pct']:+.2f}% | "
        f"年化 {summary['annual_pct']:+.2f}% | "
        f"回撤 {summary['max_drawdown_pct']:.2f}% | "
        f"Sharpe {summary['sharpe']:.2f}"
    )
    print(
        f"最终资产: {summary['final_value']:.2f} | "
        f"卖出笔数: {summary['sell_trade_count']} | "
        f"胜率: {summary['win_rate_pct']:.2f}%"
    )
    print(
        f"净值峰值: {curve_stats['peak_value']:.2f} @ {curve_stats['peak_date']} | "
        f"最大回撤区间: {curve_stats['drawdown_from']} -> {curve_stats['drawdown_to']}"
    )

    if trade_stats["best_trade"] is not None:
        best = trade_stats["best_trade"]
        worst = trade_stats["worst_trade"]
        print(
            f"最佳单笔: {best['name']}({best['code']}) {best['profit']:+.2f} "
            f"({best['profit_rate']:+.2f}%) | {best['reason']}"
        )
        print(
            f"最差单笔: {worst['name']}({worst['code']}) {worst['profit']:+.2f} "
            f"({worst['profit_rate']:+.2f}%) | {worst['reason']}"
        )

    print("\n按退出原因:")
    for item in trade_stats["reason_stats"]:
        print(
            f"{item['reason']}: "
            f"交易 {item['count']} | "
            f"总盈亏 {item['profit']:+.2f} | "
            f"平均 {item['avg_profit']:+.2f} | "
            f"胜率 {item['win_rate_pct']:.2f}%"
        )

    print("\n月度收益:")
    for month, value in curve_stats["monthly_returns_pct"].items():
        print(f"{month}: {value:+.2f}%")


def main() -> None:
    default_analysis = analyze_output_dir(DEFAULT_OUTPUT_DIR)
    print_analysis("全样本结果诊断", default_analysis)

    split_summary_path = TIME_SPLIT_OUTPUT_DIR / "时间切分汇总.json"
    if not split_summary_path.exists():
        print("\n未发现时间切分结果，跳过时间切分诊断。")
        return

    for split_dir in sorted(p for p in TIME_SPLIT_OUTPUT_DIR.iterdir() if p.is_dir()):
        analysis = analyze_output_dir(split_dir)
        print_analysis(f"时间切分诊断: {split_dir.name}", analysis)


if __name__ == "__main__":
    main()
