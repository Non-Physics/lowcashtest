from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent
CURRENT_FILE = PROJECT_ROOT / "股票策略回测.py"

PARAM_SETS = {
    "loose": dict(
        TOP_N=2,
        MAX_POSITION_PCT=0.20,
        MIN_RET20=0.035,
        MIN_MA20_SLOPE_PCT=0.006,
        MIN_INTRADAY_CLOSE_FROM_LOW_PCT=0.002,
        EARLY_WEAK_EXIT_CHECK_DAY=4,
        EARLY_WEAK_EXIT_MIN_CLOSE_RET=-0.01,
        EARLY_WEAK_EXIT_MIN_HIGH_RET=0.02,
        TRAILING_STOP_ATR_MULTIPLIER=2.2,
        MAX_HOLDING_DAYS=18,
    ),
    "strict": dict(
        TOP_N=3,
        MAX_POSITION_PCT=0.24,
        MIN_RET20=0.06,
        MIN_MA20_SLOPE_PCT=0.012,
        MIN_INTRADAY_CLOSE_FROM_LOW_PCT=0.01,
        EARLY_WEAK_EXIT_CHECK_DAY=5,
        EARLY_WEAK_EXIT_MIN_CLOSE_RET=-0.01,
        EARLY_WEAK_EXIT_MIN_HIGH_RET=0.02,
        TRAILING_STOP_ATR_MULTIPLIER=2.2,
        MAX_HOLDING_DAYS=20,
    ),
}


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def digest_df(df: pd.DataFrame) -> str:
    if df.empty:
        return "empty"
    cols = [c for c in ["datetime", "code", "action", "price", "shares", "reason", "profit"] if c in df.columns]
    payload = df[cols].astype(str).to_csv(index=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def summarize_result(result: dict) -> dict:
    trades = result["trades_df"]
    sells = result["sells_df"]
    reason_summary = []
    if not sells.empty:
        grouped = sells.groupby("reason")["profit"].agg(["count", "sum"]).reset_index()
        reason_summary = grouped.to_dict(orient="records")
    return {
        "metrics": result["metrics"],
        "trade_count": int(len(trades)),
        "sell_count": int(len(sells)),
        "trade_digest": digest_df(trades),
        "sell_digest": digest_df(sells),
        "first_12_buy_codes": trades[trades["action"] == "买入"]["code"].head(12).tolist() if not trades.empty else [],
        "first_12_sell_codes": sells["code"].head(12).tolist() if not sells.empty else [],
        "first_12_sell_reasons": sells["reason"].head(12).tolist() if not sells.empty else [],
        "reason_summary": reason_summary,
    }


def run_case(module, context: dict, intraday_enabled: bool, params: dict) -> dict:
    for key, value in params.items():
        setattr(module, key, value)
    module.ENABLE_INTRADAY_EXIT = intraday_enabled
    module.BACKTEST_START = "2025-01-01"
    module.BACKTEST_END = "2026-03-27"
    result = module.run_backtest(verbose=False, export_outputs=False, preloaded_context=context)
    return summarize_result(result)


def main() -> None:
    module = load_module(CURRENT_FILE, "intraday_exit_diag")
    context = module.get_backtest_context()
    reports = []
    for mode in [True, False]:
        for name, params in PARAM_SETS.items():
            reports.append(
                {
                    "intraday_exit_enabled": mode,
                    "param_set": name,
                    "summary": run_case(module, context, mode, params),
                }
            )
    print(json.dumps(reports, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
