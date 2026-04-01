from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
CURRENT_FILE = PROJECT_ROOT / "股票策略回测.py"
LEGACY_FILE = PROJECT_ROOT / "股票策略回测_旧版参考.py"

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
        TOP_N=2,
        MAX_POSITION_PCT=0.20,
        MIN_RET20=0.06,
        MIN_MA20_SLOPE_PCT=0.012,
        MIN_INTRADAY_CLOSE_FROM_LOW_PCT=0.008,
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


def summarize(module, label: str, params: dict) -> dict:
    for k, v in params.items():
        setattr(module, k, v)
    module.BACKTEST_START = "2025-01-01"
    module.BACKTEST_END = "2026-03-27"
    ctx = module.get_backtest_context() if hasattr(module, "get_backtest_context") else None
    result = module.run_backtest(verbose=False, export_outputs=False, preloaded_context=ctx) if ctx is not None else module.run_backtest(verbose=False, export_outputs=False)
    sells = result["sells_df"]
    trades = result["trades_df"]
    return {
        "label": label,
        "params": params,
        "metrics": result["metrics"],
        "trade_count": int(len(trades)),
        "sell_count": int(len(sells)),
        "first_10_buy_codes": trades[trades["action"] == "买入"]["code"].head(10).tolist() if not trades.empty else [],
        "first_10_sell_codes": sells["code"].head(10).tolist() if not sells.empty else [],
        "first_10_sell_reasons": sells["reason"].head(10).tolist() if not sells.empty else [],
    }


def main() -> None:
    legacy = load_module(LEGACY_FILE, "legacy_backtest")
    current = load_module(CURRENT_FILE, "current_backtest")
    reports = []
    for name, params in PARAM_SETS.items():
        reports.append(summarize(legacy, f"legacy_{name}", params))
        reports.append(summarize(current, f"current_{name}", params))
    print(json.dumps(reports, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
