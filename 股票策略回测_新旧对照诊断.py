from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent
CURRENT_PATH = PROJECT_ROOT / '股票策略回测.py'
LEGACY_PATH = PROJECT_ROOT / '股票策略回测_旧版反演.py'

PARAM_SETS = [
    {
        'name': 'baseline_loose',
        'TOP_N': 2,
        'MAX_POSITION_PCT': 0.20,
        'MIN_RET20': 0.035,
        'MIN_MA20_SLOPE_PCT': 0.006,
        'MIN_INTRADAY_CLOSE_FROM_LOW_PCT': 0.002,
        'EARLY_WEAK_EXIT_CHECK_DAY': 4,
        'EARLY_WEAK_EXIT_MIN_CLOSE_RET': -0.01,
        'EARLY_WEAK_EXIT_MIN_HIGH_RET': 0.02,
        'TRAILING_STOP_ATR_MULTIPLIER': 2.2,
        'MAX_HOLDING_DAYS': 18,
    },
    {
        'name': 'entry_mid',
        'TOP_N': 2,
        'MAX_POSITION_PCT': 0.20,
        'MIN_RET20': 0.045,
        'MIN_MA20_SLOPE_PCT': 0.01,
        'MIN_INTRADAY_CLOSE_FROM_LOW_PCT': 0.006,
        'EARLY_WEAK_EXIT_CHECK_DAY': 4,
        'EARLY_WEAK_EXIT_MIN_CLOSE_RET': -0.01,
        'EARLY_WEAK_EXIT_MIN_HIGH_RET': 0.02,
        'TRAILING_STOP_ATR_MULTIPLIER': 2.2,
        'MAX_HOLDING_DAYS': 18,
    },
    {
        'name': 'entry_strict',
        'TOP_N': 2,
        'MAX_POSITION_PCT': 0.20,
        'MIN_RET20': 0.06,
        'MIN_MA20_SLOPE_PCT': 0.012,
        'MIN_INTRADAY_CLOSE_FROM_LOW_PCT': 0.01,
        'EARLY_WEAK_EXIT_CHECK_DAY': 5,
        'EARLY_WEAK_EXIT_MIN_CLOSE_RET': -0.01,
        'EARLY_WEAK_EXIT_MIN_HIGH_RET': 0.02,
        'TRAILING_STOP_ATR_MULTIPLIER': 2.2,
        'MAX_HOLDING_DAYS': 20,
    },
    {
        'name': 'top3_variant',
        'TOP_N': 3,
        'MAX_POSITION_PCT': 0.24,
        'MIN_RET20': 0.045,
        'MIN_MA20_SLOPE_PCT': 0.01,
        'MIN_INTRADAY_CLOSE_FROM_LOW_PCT': 0.006,
        'EARLY_WEAK_EXIT_CHECK_DAY': 5,
        'EARLY_WEAK_EXIT_MIN_CLOSE_RET': -0.01,
        'EARLY_WEAK_EXIT_MIN_HIGH_RET': 0.02,
        'TRAILING_STOP_ATR_MULTIPLIER': 2.2,
        'MAX_HOLDING_DAYS': 20,
    },
]


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'failed to load {path}')
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def trade_digest(df: pd.DataFrame) -> str:
    if df.empty:
        return 'empty'
    cols = [c for c in ['datetime', 'code', 'action', 'price', 'shares', 'reason', 'profit'] if c in df.columns]
    payload = df[cols].astype(str).to_csv(index=False)
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()[:16]


def summarize_result(result: dict) -> dict:
    trades = result['trades_df']
    sells = result['sells_df']
    summary = {
        'metrics': result['metrics'],
        'trade_count': int(len(trades)),
        'sell_count': int(len(sells)),
        'trade_digest': trade_digest(trades),
        'sell_digest': trade_digest(sells),
        'first_buy_codes': trades[trades['action'] == '买入']['code'].head(15).tolist() if not trades.empty else [],
        'first_sell_codes': sells['code'].head(15).tolist() if not sells.empty else [],
        'first_sell_reasons': sells['reason'].head(15).tolist() if not sells.empty else [],
    }
    if not sells.empty:
        grouped = sells.groupby('reason')['profit'].agg(['count', 'sum']).reset_index()
        summary['reason_summary'] = grouped.to_dict(orient='records')
    else:
        summary['reason_summary'] = []
    return summary


def run_with_params(module, context: dict, params: dict) -> dict:
    for key, value in params.items():
        setattr(module, key, value)
    module.BACKTEST_START = '2025-01-01'
    module.BACKTEST_END = '2026-03-27'
    return module.run_backtest(verbose=False, export_outputs=False, preloaded_context=context)


def main() -> None:
    current = load_module(CURRENT_PATH, 'compare_current')
    legacy = load_module(LEGACY_PATH, 'compare_legacy')
    current_ctx = current.get_backtest_context()
    legacy_ctx = legacy.get_backtest_context()

    rows = []
    for params in PARAM_SETS:
        current_result = summarize_result(run_with_params(current, current_ctx, params))
        legacy_result = summarize_result(run_with_params(legacy, legacy_ctx, params))
        rows.append({
            'params': params,
            'current': current_result,
            'legacy': legacy_result,
        })

    print(json.dumps(rows, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
