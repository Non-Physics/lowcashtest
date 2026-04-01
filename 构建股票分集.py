"""
将当前股票数据按股票代码维度拆分为 train / validation / test 三个分集。

默认输入目录：
    <项目目录>/data/stock_data

默认输出目录：
    <项目目录>/data/datasets/stock_splits

输出结构示例：
    data/datasets/stock_splits/
      split_summary.json
      train/
        daily/stock/*.csv
        daily/index/*.csv
        intraday_30m/stock/*.csv
        metadata/*.csv
        universe.csv
      validation/
        ...
      test/
        ...

用途：
1. 保持同一套策略代码不变。
2. 通过切换 THS_STOCK_DATA_DIR 到不同分集目录进行回测。
3. 先在 train 上调策略，再在 validation / test 上验证泛化能力。
"""

from __future__ import annotations

import json
import os
import random
import shutil
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent
SOURCE_ROOT = Path(os.getenv("THS_STOCK_DATA_DIR", str(PROJECT_ROOT / "data" / "stock_data")))
OUTPUT_ROOT = Path(os.getenv("THS_SPLIT_OUTPUT_DIR", str(PROJECT_ROOT / "data" / "datasets" / "stock_splits")))

SPLIT_SEED = int(os.getenv("THS_SPLIT_SEED", "20260330"))
TRAIN_RATIO = float(os.getenv("THS_TRAIN_RATIO", "0.6"))
VALIDATION_RATIO = float(os.getenv("THS_VALIDATION_RATIO", "0.2"))
TEST_RATIO = float(os.getenv("THS_TEST_RATIO", "0.2"))


@dataclass(frozen=True)
class SplitPaths:
    root: Path
    daily_stock: Path
    daily_index: Path
    intraday_stock: Path
    intraday_index: Path
    metadata: Path
    universe_file: Path


def is_tradeable_a_share(code: str) -> bool:
    symbol = code.split(".")[0]
    if symbol.startswith(("300", "301", "688", "8", "4")):
        return False
    return True


def build_split_paths(base_dir: Path, split_name: str) -> SplitPaths:
    root = base_dir / split_name
    return SplitPaths(
        root=root,
        daily_stock=root / "daily" / "stock",
        daily_index=root / "daily" / "index",
        intraday_stock=root / "intraday_30m" / "stock",
        intraday_index=root / "intraday_30m" / "index",
        metadata=root / "metadata",
        universe_file=root / "universe.csv",
    )


def ensure_clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def validate_ratios() -> None:
    total = TRAIN_RATIO + VALIDATION_RATIO + TEST_RATIO
    if abs(total - 1.0) > 1e-9:
        raise ValueError(f"分集比例之和必须为 1，当前为 {total:.6f}")


def collect_available_codes(source_root: Path) -> list[str]:
    daily_dir = source_root / "daily" / "stock"
    intraday_dir = source_root / "intraday_30m" / "stock"
    daily_codes = {p.stem for p in daily_dir.glob("*.csv")}
    intraday_codes = {p.stem for p in intraday_dir.glob("*.csv")}
    codes = sorted(code for code in daily_codes & intraday_codes if is_tradeable_a_share(code))
    if not codes:
        raise FileNotFoundError("未找到可用于拆分的股票数据，请先检查 daily/stock 和 intraday_30m/stock。")
    return codes


def split_codes(codes: list[str]) -> dict[str, list[str]]:
    shuffled = list(codes)
    random.Random(SPLIT_SEED).shuffle(shuffled)

    train_end = int(len(shuffled) * TRAIN_RATIO)
    validation_end = train_end + int(len(shuffled) * VALIDATION_RATIO)

    train_codes = sorted(shuffled[:train_end])
    validation_codes = sorted(shuffled[train_end:validation_end])
    test_codes = sorted(shuffled[validation_end:])

    if not train_codes or not validation_codes or not test_codes:
        raise ValueError("拆分后某个分集为空，请调整比例。")

    return {
        "train": train_codes,
        "validation": validation_codes,
        "test": test_codes,
    }


def copy_tree_files(src_dir: Path, dst_dir: Path) -> None:
    dst_dir.mkdir(parents=True, exist_ok=True)
    for file in src_dir.glob("*.csv"):
        shutil.copy2(file, dst_dir / file.name)


def materialize_split(source_root: Path, output_root: Path, split_name: str, codes: list[str]) -> None:
    paths = build_split_paths(output_root, split_name)
    ensure_clean_dir(paths.root)
    paths.daily_stock.mkdir(parents=True, exist_ok=True)
    paths.daily_index.mkdir(parents=True, exist_ok=True)
    paths.intraday_stock.mkdir(parents=True, exist_ok=True)
    paths.intraday_index.mkdir(parents=True, exist_ok=True)
    paths.metadata.mkdir(parents=True, exist_ok=True)

    for code in codes:
        shutil.copy2(source_root / "daily" / "stock" / f"{code}.csv", paths.daily_stock / f"{code}.csv")
        shutil.copy2(source_root / "intraday_30m" / "stock" / f"{code}.csv", paths.intraday_stock / f"{code}.csv")

    copy_tree_files(source_root / "daily" / "index", paths.daily_index)
    copy_tree_files(source_root / "intraday_30m" / "index", paths.intraday_index)
    copy_tree_files(source_root / "metadata", paths.metadata)

    universe_df = pd.DataFrame({"code": codes, "split": split_name})
    universe_df.to_csv(paths.universe_file, index=False, encoding="utf-8-sig")


def write_summary(output_root: Path, split_map: dict[str, list[str]]) -> Path:
    summary = {
        "source_root": str(SOURCE_ROOT),
        "output_root": str(output_root),
        "seed": SPLIT_SEED,
        "train_ratio": TRAIN_RATIO,
        "validation_ratio": VALIDATION_RATIO,
        "test_ratio": TEST_RATIO,
        "split_sizes": {name: len(codes) for name, codes in split_map.items()},
        "split_codes": split_map,
    }
    summary_path = output_root / "split_summary.json"
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    return summary_path


def main() -> None:
    validate_ratios()
    codes = collect_available_codes(SOURCE_ROOT)
    split_map = split_codes(codes)

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    for split_name, split_codes_list in split_map.items():
        materialize_split(SOURCE_ROOT, OUTPUT_ROOT, split_name, split_codes_list)

    summary_path = write_summary(OUTPUT_ROOT, split_map)

    print(f"源数据目录: {SOURCE_ROOT}")
    print(f"输出目录: {OUTPUT_ROOT}")
    print(f"随机种子: {SPLIT_SEED}")
    for split_name, split_codes_list in split_map.items():
        print(f"{split_name} 股票数: {len(split_codes_list)}")
        print(f"  数据目录: {OUTPUT_ROOT / split_name}")
    print(f"摘要文件: {summary_path}")


if __name__ == "__main__":
    main()
