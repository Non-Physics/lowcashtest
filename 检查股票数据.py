"""
检查下载后的股票数据完整性。
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent
ROOT = Path(os.getenv("THS_STOCK_DATA_DIR", str(PROJECT_ROOT / "data" / "stock_data")))


def sample_files(path: Path, suffix: str = ".csv", limit: int = 5) -> list[Path]:
    return sorted([p for p in path.glob(f"*{suffix}")])[:limit]


def check_sorted_and_nulls(df: pd.DataFrame, time_col: str) -> dict:
    return {
        "rows": len(df),
        "is_sorted": bool(df[time_col].is_monotonic_increasing),
        "null_counts": {col: int(df[col].isna().sum()) for col in df.columns},
    }


def inspect_csvs(path: Path, time_col: str) -> pd.DataFrame:
    rows = []
    for file in sample_files(path, limit=10):
        df = pd.read_csv(file)
        if time_col in df.columns:
            df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
        info = check_sorted_and_nulls(df, time_col)
        rows.append(
            {
                "file": file.name,
                "rows": info["rows"],
                "is_sorted": info["is_sorted"],
                "time_nulls": info["null_counts"].get(time_col, -1),
                "close_nulls": info["null_counts"].get("close", -1),
                "amount_nulls": info["null_counts"].get("amount", -1),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    daily_stock = ROOT / "daily" / "stock"
    intraday_stock = ROOT / "intraday_30m" / "stock"
    metadata = ROOT / "metadata"

    print(f"检查目录: {ROOT}")
    print(f"日线文件数: {len(list(daily_stock.glob('*.csv')))}")
    print(f"30分钟文件数: {len(list(intraday_stock.glob('*.csv')))}")
    print(f"元数据文件数: {len(list(metadata.glob('*.csv')))}")

    print("\n日线样本检查:")
    print(inspect_csvs(daily_stock, "date").to_string(index=False))

    print("\n30分钟样本检查:")
    print(inspect_csvs(intraday_stock, "datetime").to_string(index=False))

    for name in ["instruments.csv", "trading_calendar.csv", "adjustment_factors.csv", "st_status.csv"]:
        path = metadata / name
        print(f"\n元数据文件 {name}: {'存在' if path.exists() else '缺失'}")
        if path.exists():
            df = pd.read_csv(path)
            print(f"  行数: {len(df)}")
            print(f"  列: {list(df.columns)}")


if __name__ == "__main__":
    main()
