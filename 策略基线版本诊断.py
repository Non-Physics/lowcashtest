from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent
CURRENT_PATH = PROJECT_ROOT / "股票策略回测.py"
BASELINE_PATH = PROJECT_ROOT / "股票策略回测_基线版.py"

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


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def digest_df(df: pd.DataFrame, cols: list[str]) -> str:
    if df.empty:
        return "empty"
    payload = df[[c for c in cols if c in df.columns]].astype(str).to_csv(index=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def summarize(result: dict) -> dict:
    trades = result["trades_df"]
    sells = result["sells_df"]
    return {
        "metrics": {k: result["metrics"][k] for k in ["return_pct", "annual_pct", "max_drawdown_pct", "sharpe", "sell_trade_count", "win_rate_pct", "final_value"]},
        "trade_digest": digest_df(trades, ["datetime", "code", "action", "price", "shares", "reason"]),
        "sell_digest": digest_df(sells, ["datetime", "code", "price", "shares", "reason", "profit"]),
        "first_10_buy_codes": trades[trades["action"] == "买入"]["code"].head(10).tolist() if not trades.empty else [],
        "first_10_sell_codes": sells["code"].head(10).tolist() if not sells.empty else [],
        "first_10_sell_reasons": sells["reason"].head(10).tolist() if not sells.empty else [],
    }


def run_case(module, params: dict) -> dict:
    for key, value in params.items():
        setattr(module, key, value)
    module.BACKTEST_START = "2025-01-01"
    module.BACKTEST_END = "2026-03-27"
    result = module.run_backtest(verbose=False, export_outputs=False, preloaded_context=module.get_backtest_context())
    return summarize(result)


def main() -> None:
    versions = {
        "current": load_module(CURRENT_PATH, "baseline_diag_current"),
        "baseline": load_module(BASELINE_PATH, "baseline_diag_baseline"),
    }
    summary = {}
    for version_name, module in versions.items():
        summary[version_name] = {}
        for case_name, params in PARAM_SETS.items():
            summary[version_name][case_name] = run_case(module, params)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
