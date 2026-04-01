from __future__ import annotations

import importlib.util
from pathlib import Path
import json
import sys


PROJECT_ROOT = Path(__file__).resolve().parent
BASE_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "股票策略时间切分"
STRATEGY_FILE = PROJECT_ROOT / "股票策略回测.py"

TIME_SPLITS = [
    ("train_window", "2025-01-01", "2025-09-30"),
    ("validation_window", "2025-10-01", "2025-12-31"),
    ("test_window", "2026-01-01", "2026-03-27"),
]


def load_strategy_module():
    module_name = "stock_strategy_backtest_module"
    spec = importlib.util.spec_from_file_location(module_name, STRATEGY_FILE)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载策略文件: {STRATEGY_FILE}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def run_time_split_backtests() -> None:
    strategy = load_strategy_module()
    BASE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    split_summaries: list[dict] = []

    print("=" * 70)
    print("股票策略时间切分样本外回测")
    print(f"策略文件: {STRATEGY_FILE}")
    print(f"输出目录: {BASE_OUTPUT_DIR}")
    print("=" * 70)

    for split_name, start_date, end_date in TIME_SPLITS:
        output_dir = BASE_OUTPUT_DIR / split_name
        strategy.BACKTEST_START = start_date
        strategy.BACKTEST_END = end_date
        strategy.OUTPUT_DIR = output_dir

        print(f"\n[{split_name}] {start_date} -> {end_date}")
        strategy.run_backtest()

        summary_path = output_dir / "股票策略_回测摘要.json"
        with summary_path.open("r", encoding="utf-8") as f:
            summary = json.load(f)
        summary["split_name"] = split_name
        split_summaries.append(summary)

    combined_summary_path = BASE_OUTPUT_DIR / "时间切分汇总.json"
    with combined_summary_path.open("w", encoding="utf-8") as f:
        json.dump(split_summaries, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 70)
    print("时间切分回测完成")
    print("=" * 70)
    for summary in split_summaries:
        print(
            f"{summary['split_name']}: "
            f"收益 {summary['return_pct']:+.2f}% | "
            f"回撤 {summary['max_drawdown_pct']:.2f}% | "
            f"Sharpe {summary['sharpe']:.2f} | "
            f"胜率 {summary['win_rate_pct']:.1f}%"
        )
    print(f"汇总文件: {combined_summary_path}")


if __name__ == "__main__":
    run_time_split_backtests()
