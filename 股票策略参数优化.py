from __future__ import annotations

import hashlib
import importlib.util
import itertools
import json
from dataclasses import asdict, dataclass
from pathlib import Path
import sys

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent
STRATEGY_FILE = PROJECT_ROOT / "股票策略回测.py"
OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "股票策略参数优化_修正版夜跑版"
LEADERBOARD_PATH = OUTPUT_ROOT / "参数排行榜.csv"
PROGRESS_PATH = OUTPUT_ROOT / "参数排行榜_增量.csv"
BEST_PARAMS_PATH = OUTPUT_ROOT / "最佳参数.json"
BEST_SPLIT_SUMMARY_PATH = OUTPUT_ROOT / "最佳参数_时间切分汇总.json"
OPTIMIZE_WITH_INTRADAY_EXIT = False

TIME_SPLITS = [
    ("train_window", "2025-01-01", "2025-09-30"),
    ("validation_window", "2025-10-01", "2025-12-31"),
    ("test_window", "2026-01-01", "2026-03-27"),
]


@dataclass(frozen=True)
class StrategyParams:
    top_n: int
    max_position_pct: float
    min_ret20: float
    min_ma20_slope_pct: float
    min_intraday_close_from_low_pct: float
    early_weak_exit_check_day: int
    early_weak_exit_min_close_ret: float
    early_weak_exit_min_high_ret: float
    trailing_stop_atr_multiplier: float
    max_holding_days: int


PARAM_GRID = {
    "top_n": [2],
    "max_position_pct": [0.20],
    "min_ret20": [0.035, 0.04, 0.045, 0.05, 0.06],
    "min_ma20_slope_pct": [0.006, 0.008, 0.01, 0.012],
    "min_intraday_close_from_low_pct": [0.002, 0.004, 0.006, 0.008],
    "early_weak_exit_check_day": [4, 5],
    "early_weak_exit_min_close_ret": [-0.01],
    "early_weak_exit_min_high_ret": [0.02],
    "trailing_stop_atr_multiplier": [2.2],
    "max_holding_days": [18, 20],
}


def load_strategy_module():
    module_name = "stock_strategy_optimizer_module"
    spec = importlib.util.spec_from_file_location(module_name, STRATEGY_FILE)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载策略文件: {STRATEGY_FILE}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def build_param_candidates() -> list[StrategyParams]:
    keys = list(PARAM_GRID.keys())
    candidates = []
    for values in itertools.product(*(PARAM_GRID[key] for key in keys)):
        payload = dict(zip(keys, values))
        if payload["early_weak_exit_check_day"] > payload["max_holding_days"]:
            continue
        candidates.append(StrategyParams(**payload))
    return candidates


def apply_params(strategy, params: StrategyParams) -> None:
    strategy.TOP_N = params.top_n
    strategy.MAX_POSITION_PCT = params.max_position_pct
    strategy.MIN_RET20 = params.min_ret20
    strategy.MIN_MA20_SLOPE_PCT = params.min_ma20_slope_pct
    strategy.MIN_INTRADAY_CLOSE_FROM_LOW_PCT = params.min_intraday_close_from_low_pct
    strategy.EARLY_WEAK_EXIT_CHECK_DAY = params.early_weak_exit_check_day
    strategy.EARLY_WEAK_EXIT_MIN_CLOSE_RET = params.early_weak_exit_min_close_ret
    strategy.EARLY_WEAK_EXIT_MIN_HIGH_RET = params.early_weak_exit_min_high_ret
    strategy.TRAILING_STOP_ATR_MULTIPLIER = params.trailing_stop_atr_multiplier
    strategy.MAX_HOLDING_DAYS = params.max_holding_days


def digest_df(df: pd.DataFrame, cols: list[str]) -> str:
    if df.empty:
        return "empty"
    existing_cols = [col for col in cols if col in df.columns]
    if not existing_cols:
        return "missing_cols"
    payload = df[existing_cols].astype(str).to_csv(index=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def digest_exit_reasons(sells_df: pd.DataFrame) -> str:
    if sells_df.empty or "reason" not in sells_df.columns:
        return "empty"
    reason_counts = sells_df["reason"].value_counts().sort_index()
    payload = reason_counts.to_csv()
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def score_run(metrics: dict) -> float:
    return (
        metrics["return_pct"] * 0.7
        + metrics["sharpe"] * 18.0
        + metrics["win_rate_pct"] * 0.08
        + min(metrics["sell_trade_count"], 30) * 0.12
        + min(metrics["annual_pct"], 60.0) * 0.25
        + metrics["max_drawdown_pct"] * 0.55
    )


def aggregate_score(split_metrics: dict[str, dict]) -> float:
    train = split_metrics["train_window"]
    validation = split_metrics["validation_window"]
    test = split_metrics["test_window"]

    base = (
        score_run(train) * 0.20
        + score_run(validation) * 0.35
        + score_run(test) * 0.45
    )
    penalty = 0.0

    for split_name, metrics in split_metrics.items():
        if metrics["return_pct"] < 0:
            penalty += abs(metrics["return_pct"]) * 2.5
        if metrics["max_drawdown_pct"] < -12:
            penalty += abs(metrics["max_drawdown_pct"] + 12) * 1.8
        if metrics["sell_trade_count"] < 8:
            penalty += (8 - metrics["sell_trade_count"]) * 1.5
        if split_name in {"validation_window", "test_window"} and metrics["sharpe"] < 0.8:
            penalty += (0.8 - metrics["sharpe"]) * 12

    stability_penalty = abs(validation["return_pct"] - test["return_pct"]) * 0.6
    return base - penalty - stability_penalty


def run_split(strategy, params: StrategyParams, split_name: str, start: str, end: str) -> dict:
    apply_params(strategy, params)
    strategy.ENABLE_INTRADAY_EXIT = OPTIMIZE_WITH_INTRADAY_EXIT
    strategy.BACKTEST_START = start
    strategy.BACKTEST_END = end
    result = strategy.run_backtest(
        verbose=False,
        export_outputs=False,
        preloaded_context=strategy.get_backtest_context(),
    )
    metrics = dict(result["metrics"])
    metrics["enable_intraday_exit"] = bool(strategy.ENABLE_INTRADAY_EXIT)
    metrics["trade_digest"] = digest_df(
        result["trades_df"],
        ["datetime", "code", "action", "price", "shares", "reason"],
    )
    metrics["sell_digest"] = digest_df(
        result["sells_df"],
        ["datetime", "code", "price", "shares", "reason", "profit"],
    )
    metrics["reason_digest"] = digest_exit_reasons(result["sells_df"])
    metrics["split_name"] = split_name
    return metrics


def param_slug(params: StrategyParams) -> str:
    return (
        f"tn{params.top_n}"
        f"_pos{int(params.max_position_pct * 100)}"
        f"_r20{int(params.min_ret20 * 1000)}"
        f"_s5{int(params.min_ma20_slope_pct * 1000)}"
        f"_low{int(params.min_intraday_close_from_low_pct * 1000)}"
        f"_ewd{params.early_weak_exit_check_day}"
        f"_ecr{int(abs(params.early_weak_exit_min_close_ret) * 1000)}"
        f"_ehr{int(params.early_weak_exit_min_high_ret * 1000)}"
        f"_trail{int(params.trailing_stop_atr_multiplier * 10)}"
        f"_hold{params.max_holding_days}"
    )


def load_existing_progress() -> pd.DataFrame:
    if not PROGRESS_PATH.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(PROGRESS_PATH, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame()


def build_result_row(params: StrategyParams, split_metrics: dict[str, dict]) -> dict:
    row = asdict(params)
    row["param_slug"] = param_slug(params)
    for split_name, metrics in split_metrics.items():
        prefix = split_name.replace("_window", "")
        row[f"{prefix}_return_pct"] = metrics["return_pct"]
        row[f"{prefix}_annual_pct"] = metrics["annual_pct"]
        row[f"{prefix}_max_drawdown_pct"] = metrics["max_drawdown_pct"]
        row[f"{prefix}_sharpe"] = metrics["sharpe"]
        row[f"{prefix}_sell_trade_count"] = metrics["sell_trade_count"]
        row[f"{prefix}_win_rate_pct"] = metrics["win_rate_pct"]
        row[f"{prefix}_enable_intraday_exit"] = metrics["enable_intraday_exit"]
        row[f"{prefix}_trade_digest"] = metrics["trade_digest"]
        row[f"{prefix}_sell_digest"] = metrics["sell_digest"]
        row[f"{prefix}_reason_digest"] = metrics["reason_digest"]
    row["objective_score"] = aggregate_score(split_metrics)
    return row


def persist_progress(rows: list[dict]) -> pd.DataFrame:
    leaderboard = pd.DataFrame(rows).sort_values(
        by=["objective_score", "test_return_pct", "validation_return_pct", "test_sharpe"],
        ascending=False,
    ).reset_index(drop=True)
    leaderboard.to_csv(PROGRESS_PATH, index=False, encoding="utf-8-sig")
    leaderboard.to_csv(LEADERBOARD_PATH, index=False, encoding="utf-8-sig")
    return leaderboard


def optimize() -> None:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    strategy = load_strategy_module()
    original_data_root = strategy.DATA_ROOT
    preloaded_context = strategy.get_backtest_context()

    candidates = build_param_candidates()
    existing = load_existing_progress()
    rows: list[dict] = existing.to_dict("records") if not existing.empty else []
    completed = set(existing["param_slug"].astype(str)) if "param_slug" in existing.columns else set()

    print("=" * 70)
    print("股票策略参数优化")
    print(f"候选参数组数: {len(candidates)}")
    print(f"已完成参数组数: {len(completed)}")
    print(f"输出目录: {OUTPUT_ROOT}")
    print(f"优化阶段是否启用盘中退出: {OPTIMIZE_WITH_INTRADAY_EXIT}")
    print("=" * 70)

    for idx, params in enumerate(candidates, start=1):
        slug = param_slug(params)
        if slug in completed:
            print(f"[{idx}/{len(candidates)}] {slug} 已存在，跳过")
            continue

        strategy.DATA_ROOT = original_data_root
        split_metrics = {}
        print(f"[{idx}/{len(candidates)}] {slug}")
        for split_name, start, end in TIME_SPLITS:
            split_metrics[split_name] = run_split(strategy, params, split_name, start, end)

        row = build_result_row(params, split_metrics)
        rows.append(row)
        completed.add(slug)
        leaderboard = persist_progress(rows)
        current_best = leaderboard.iloc[0]
        unique_train_paths = leaderboard["train_sell_digest"].nunique() if "train_sell_digest" in leaderboard.columns else 0
        unique_validation_paths = leaderboard["validation_sell_digest"].nunique() if "validation_sell_digest" in leaderboard.columns else 0
        unique_test_paths = leaderboard["test_sell_digest"].nunique() if "test_sell_digest" in leaderboard.columns else 0
        print(
            f"已保存 {len(completed)}/{len(candidates)} | "
            f"当前最佳 {current_best['param_slug']} | "
            f"score {current_best['objective_score']:.2f} | "
            f"path(train/val/test)={unique_train_paths}/{unique_validation_paths}/{unique_test_paths}"
        )

    leaderboard = persist_progress(rows)

    best_row = leaderboard.iloc[0].to_dict()
    best_params = StrategyParams(
        top_n=int(best_row["top_n"]),
        max_position_pct=float(best_row["max_position_pct"]),
        min_ret20=float(best_row["min_ret20"]),
        min_ma20_slope_pct=float(best_row["min_ma20_slope_pct"]),
        min_intraday_close_from_low_pct=float(best_row["min_intraday_close_from_low_pct"]),
        early_weak_exit_check_day=int(best_row["early_weak_exit_check_day"]),
        early_weak_exit_min_close_ret=float(best_row["early_weak_exit_min_close_ret"]),
        early_weak_exit_min_high_ret=float(best_row["early_weak_exit_min_high_ret"]),
        trailing_stop_atr_multiplier=float(best_row["trailing_stop_atr_multiplier"]),
        max_holding_days=int(best_row["max_holding_days"]),
    )

    best_split_metrics = []
    best_runs_root = OUTPUT_ROOT / "best_runs"
    for split_name, start, end in TIME_SPLITS:
        apply_params(strategy, best_params)
        strategy.BACKTEST_START = start
        strategy.BACKTEST_END = end
        strategy.OUTPUT_DIR = best_runs_root / split_name
        result = strategy.run_backtest(
            verbose=False,
            export_outputs=True,
            preloaded_context=preloaded_context,
        )
        metrics = dict(result["metrics"])
        metrics["split_name"] = split_name
        best_split_metrics.append(metrics)

    apply_params(strategy, best_params)
    strategy.BACKTEST_START = "2025-01-01"
    strategy.BACKTEST_END = "2026-03-27"
    strategy.OUTPUT_DIR = best_runs_root / "full_sample"
    best_full = strategy.run_backtest(
        verbose=False,
        export_outputs=True,
        preloaded_context=preloaded_context,
    )

    with BEST_PARAMS_PATH.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "best_params": asdict(best_params),
                "objective_score": float(best_row["objective_score"]),
                "full_sample_metrics": best_full["metrics"],
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    with BEST_SPLIT_SUMMARY_PATH.open("w", encoding="utf-8") as f:
        json.dump(best_split_metrics, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 70)
    print("参数优化完成")
    print("=" * 70)
    print(f"排行榜: {LEADERBOARD_PATH}")
    print(f"最佳参数: {BEST_PARAMS_PATH}")
    print(f"最佳参数时间切分汇总: {BEST_SPLIT_SUMMARY_PATH}")
    print("最佳参数如下:")
    for key, value in asdict(best_params).items():
        print(f"{key} = {value}")
    print(
        f"最佳参数全样本: 收益 {best_full['metrics']['return_pct']:+.2f}% | "
        f"回撤 {best_full['metrics']['max_drawdown_pct']:.2f}% | "
        f"Sharpe {best_full['metrics']['sharpe']:.2f}"
    )


if __name__ == "__main__":
    optimize()
