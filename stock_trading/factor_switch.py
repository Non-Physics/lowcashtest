from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from typing import Callable, Any

import pandas as pd

from factor_research.data_access import ResearchContext, load_trading_calendar, resolve_split_name
from factor_research.evaluator import EvaluationConfig, evaluate_factor_panel, evaluate_multifactor_methods
from factor_research.factor_library import DEFAULT_FACTOR_NAMES, build_factor_panel
from factor_research.labels import build_next_open_to_n_close_labels
from factor_research.universe import UniverseConfig, build_universe_panel

PROJECT_ROOT = Path(__file__).resolve().parent.parent

CORE3_NAME = "core3_baseline_top8"
CORE5_NAME = "core5_downside80_top4"
DEFAULT_CANDIDATE_SOURCE = "factor_switch_candidate"
DEFAULT_CANDIDATE_VERSION = "factor_switch_v1"
DEFAULT_SWITCH_POLICY = "pullback_or_bull_highvol_to_core5"

CORE3_SPEC = {
    "group_name": CORE3_NAME,
    "variant_name": "core3_baseline",
    "factor_names": ["close_ma20_gap", "ret60", "amount_ma20"],
    "top_n": 8,
    "risk_filter_name": None,
}

CORE5_SPEC = {
    "group_name": CORE5_NAME,
    "variant_name": "core5_downside80",
    "factor_names": ["close_ma20_gap", "ret60", "amount_ma20", "ret5", "amount_ratio_5_20"],
    "top_n": 4,
    "risk_filter_name": "downside_vol20",
}


def _load_baseline_module():
    module_name = "stock_trading_factor_switch_baseline"
    if module_name in sys.modules:
        return sys.modules[module_name]
    strategy_path = PROJECT_ROOT / "股票策略回测_基线版.py"
    spec = importlib.util.spec_from_file_location(module_name, strategy_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载基线策略文件: {strategy_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _build_research_context_from_baseline(baseline) -> ResearchContext:
    context = baseline.get_backtest_context()
    trading_calendar = load_trading_calendar(baseline.DATA_ROOT)
    sample_codes = sorted(context["daily_data"].keys())
    return ResearchContext(
        data_root=Path(baseline.DATA_ROOT),
        split_name=resolve_split_name(baseline.DATA_ROOT),
        instruments=context["instruments"].copy(),
        st_status=context["st_status"].copy(),
        daily_data=context["daily_data"],
        intraday_data=context["intraday_data"],
        benchmark=context["benchmark"].copy(),
        all_dates=list(context["all_dates"]),
        trading_calendar=trading_calendar,
        sample_codes=sample_codes,
    )


def _apply_optional_risk_filter(day_df: pd.DataFrame, risk_filter_name: str | None) -> pd.DataFrame:
    if not risk_filter_name:
        return day_df
    if risk_filter_name not in day_df.columns:
        return day_df.iloc[0:0].copy()
    filtered = day_df.dropna(subset=[risk_filter_name]).copy()
    if filtered.empty:
        return filtered
    filtered["risk_rank_pct"] = filtered[risk_filter_name].rank(method="first", pct=True, ascending=True)
    filtered = filtered[filtered["risk_rank_pct"] <= 0.8].copy()
    return filtered.drop(columns=["risk_rank_pct"], errors="ignore")


def _build_regime_map(benchmark: pd.DataFrame) -> dict[pd.Timestamp, dict[str, str]]:
    frame = benchmark.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
    frame = frame.sort_values("date").reset_index(drop=True)
    frame["benchmark_atr_ratio"] = frame["atr14"] / frame["close"]
    atr_series = frame["benchmark_atr_ratio"].dropna()
    atr_median = float(atr_series.median()) if not atr_series.empty else None

    def classify_trend(row: pd.Series) -> str:
        close = row.get("close")
        ma20 = row.get("ma20")
        slope = row.get("ma20_slope_5")
        if pd.isna(close) or pd.isna(ma20) or pd.isna(slope):
            return "unknown"
        if close >= ma20 and slope > 0:
            return "bull_trend"
        if close < ma20 and slope < 0:
            return "bear_trend"
        if close >= ma20 and slope <= 0:
            return "rebound_phase"
        return "pullback_phase"

    def classify_vol(value: float) -> str:
        if atr_median is None or pd.isna(value):
            return "unknown"
        return "high_vol" if value >= atr_median else "low_vol"

    frame["trend_regime"] = frame.apply(classify_trend, axis=1)
    frame["vol_regime"] = frame["benchmark_atr_ratio"].apply(classify_vol)
    frame["combined_regime"] = frame["trend_regime"] + "__" + frame["vol_regime"]
    return {
        pd.Timestamp(row.date).normalize(): {
            "trend_regime": str(row.trend_regime),
            "vol_regime": str(row.vol_regime),
            "combined_regime": str(row.combined_regime),
        }
        for row in frame.itertuples()
    }


def _prefer_core5(policy: str, regime: dict[str, str]) -> bool:
    trend = regime.get("trend_regime")
    combined = regime.get("combined_regime")
    vol = regime.get("vol_regime")
    if policy == "always_core3":
        return False
    if policy == "always_core5":
        return True
    if policy == "pullback_to_core5":
        return trend == "pullback_phase"
    if policy == "pullback_highvol_to_core5":
        return combined == "pullback_phase__high_vol"
    if policy == "highvol_non_bear_to_core5":
        return vol == "high_vol" and trend != "bear_trend"
    if policy == "pullback_or_bull_highvol_to_core5":
        return trend == "pullback_phase" or combined == "bull_trend__high_vol"
    raise ValueError(f"未知切换策略: {policy}")


def _select_ranked_candidates(
    scored_df: pd.DataFrame,
    signal_date: pd.Timestamp,
    score_col: str,
    top_n: int,
    risk_filter_name: str | None,
    cooldown_until: dict[str, pd.Timestamp],
) -> list[dict[str, Any]]:
    day_df = scored_df[pd.to_datetime(scored_df["date"]).dt.normalize() == signal_date].copy()
    if day_df.empty or score_col not in day_df.columns:
        return []
    day_df = day_df.dropna(subset=[score_col]).copy()
    day_df = _apply_optional_risk_filter(day_df, risk_filter_name)
    if day_df.empty:
        return []
    day_df = day_df.sort_values(score_col, ascending=False)
    rows: list[dict[str, Any]] = []
    for row in day_df.itertuples():
        code = str(row.code)
        if code in cooldown_until and signal_date <= pd.Timestamp(cooldown_until[code]).normalize():
            continue
        rows.append(
            {
                "code": code,
                "score": float(getattr(row, score_col)),
                "time_split": str(getattr(row, "time_split", "")),
            }
        )
        if len(rows) >= top_n:
            break
    return rows


def build_factor_switch_decision(
    signal_date: str | pd.Timestamp,
    cooldown_until: dict[str, pd.Timestamp] | None = None,
    switch_policy: str = DEFAULT_SWITCH_POLICY,
    candidate_version: str = DEFAULT_CANDIDATE_VERSION,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    baseline = _load_baseline_module()
    ctx = _build_research_context_from_baseline(baseline)
    current_date = pd.Timestamp(signal_date).normalize()
    cooldown_map = {code: pd.Timestamp(value).normalize() for code, value in (cooldown_until or {}).items()}

    if progress is not None:
        progress("loading factor-switch research context")
    universe_panel = build_universe_panel(context=ctx, config=UniverseConfig(include_bj=False), progress=progress)
    factor_panel = build_factor_panel(context=ctx, universe_panel=universe_panel, progress=progress)
    labels = build_next_open_to_n_close_labels(
        horizons=(5,),
        context=ctx,
        universe_panel=universe_panel,
        progress=progress,
    )
    config = EvaluationConfig(label_name="next_open_to_n_close", horizon_days=5, quantiles=5, min_obs_per_date=20)
    evaluation = evaluate_factor_panel(
        factor_panel=factor_panel,
        labels=labels,
        factor_names=DEFAULT_FACTOR_NAMES,
        config=config,
    )

    core3_prefix = "factor_switch_core3"
    core3_score_col = f"{core3_prefix}_rank_mean_score"
    core3_multi = evaluate_multifactor_methods(
        merged=evaluation["merged"],
        direction_summary=evaluation["direction_summary"],
        factor_names=CORE3_SPEC["factor_names"],
        config=config,
        score_methods=("rank_mean",),
        score_prefix=core3_prefix,
    )
    core3_candidates = _select_ranked_candidates(
        scored_df=core3_multi["scored_frame"],
        signal_date=current_date,
        score_col=core3_score_col,
        top_n=baseline.TOP_N,
        risk_filter_name=CORE3_SPEC["risk_filter_name"],
        cooldown_until=cooldown_map,
    )

    core5_prefix = "factor_switch_core5"
    core5_score_col = f"{core5_prefix}_rank_mean_score"
    core5_multi = evaluate_multifactor_methods(
        merged=evaluation["merged"],
        direction_summary=evaluation["direction_summary"],
        factor_names=CORE5_SPEC["factor_names"],
        config=config,
        score_methods=("rank_mean",),
        score_prefix=core5_prefix,
    )
    core5_candidates = _select_ranked_candidates(
        scored_df=core5_multi["scored_frame"],
        signal_date=current_date,
        score_col=core5_score_col,
        top_n=baseline.TOP_N,
        risk_filter_name=CORE5_SPEC["risk_filter_name"],
        cooldown_until=cooldown_map,
    )

    regime = _build_regime_map(ctx.benchmark).get(
        current_date,
        {
            "trend_regime": "unknown",
            "vol_regime": "unknown",
            "combined_regime": "unknown__unknown",
        },
    )
    prefer_core5 = _prefer_core5(switch_policy, regime)
    primary_group = CORE5_NAME if prefer_core5 else CORE3_NAME
    fallback_group = CORE3_NAME if prefer_core5 else CORE5_NAME
    primary_candidates = core5_candidates if prefer_core5 else core3_candidates
    fallback_candidates = core3_candidates if prefer_core5 else core5_candidates
    used_fallback = not primary_candidates and bool(fallback_candidates)
    selected_group = fallback_group if used_fallback else primary_group
    selected_candidates = fallback_candidates if used_fallback else primary_candidates

    baseline_candidates = baseline._BASE.select_candidates(
        current_date,
        ctx.daily_data,
        ctx.instruments,
        ctx.st_status,
        cooldown_map,
    )
    baseline_rows = [{"code": code, "score": float(score)} for code, score in baseline_candidates]

    baseline_codes = [row["code"] for row in baseline_rows]
    selected_codes = [row["code"] for row in selected_candidates]
    return {
        "signal_date": str(current_date.date()),
        "candidate_source": DEFAULT_CANDIDATE_SOURCE,
        "candidate_version": candidate_version,
        "switch_policy": switch_policy,
        "market_regime": regime,
        "prefer_core5": prefer_core5,
        "primary_group": primary_group,
        "fallback_group": fallback_group,
        "used_fallback": used_fallback,
        "selected_factor_group": selected_group,
        "selected_top_n": len(selected_candidates),
        "selected_codes": selected_codes,
        "baseline_codes": baseline_codes,
        "overlap_codes": sorted(set(selected_codes) & set(baseline_codes)),
        "selected_candidates": selected_candidates,
        "baseline_candidates": baseline_rows,
        "candidate_groups": {
            CORE3_NAME: core3_candidates,
            CORE5_NAME: core5_candidates,
        },
    }
