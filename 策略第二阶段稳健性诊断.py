from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent
OPTIMIZATION_DIR = PROJECT_ROOT / "outputs" / "股票策略参数优化_基线退出层第二阶段"
LEADERBOARD_PATH = OPTIMIZATION_DIR / "参数排行榜.csv"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "策略第二阶段稳健性诊断"
OUTPUT_PATH = OUTPUT_DIR / "latest.json"

TOP_N = 12


def summarize_top_cluster(df: pd.DataFrame) -> dict:
    top_df = df.head(TOP_N).copy()
    return {
        "top_count": int(len(top_df)),
        "balanced_score_unique": int(top_df["objective_score_balanced"].nunique(dropna=False)),
        "return_score_unique": int(top_df["objective_score_return"].nunique(dropna=False)),
        "train_sell_digest_unique": int(top_df["train_sell_digest"].nunique(dropna=False)),
        "validation_sell_digest_unique": int(top_df["validation_sell_digest"].nunique(dropna=False)),
        "test_sell_digest_unique": int(top_df["test_sell_digest"].nunique(dropna=False)),
        "top_rows": top_df[
            [
                "param_slug",
                "objective_score_balanced",
                "objective_score_return",
                "train_return_pct",
                "validation_return_pct",
                "test_return_pct",
                "train_max_drawdown_pct",
                "validation_max_drawdown_pct",
                "test_max_drawdown_pct",
                "train_sharpe",
                "validation_sharpe",
                "test_sharpe",
                "train_sell_digest",
                "validation_sell_digest",
                "test_sell_digest",
            ]
        ].to_dict(orient="records"),
    }


def summarize_score_tracks(df: pd.DataFrame) -> dict:
    balanced_top = df.sort_values(
        by=["objective_score_balanced", "objective_score_return", "test_return_pct"],
        ascending=False,
    ).iloc[0]
    return_top = df.sort_values(
        by=["objective_score_return", "objective_score_balanced", "test_return_pct"],
        ascending=False,
    ).iloc[0]
    return {
        "balanced_top": balanced_top.to_dict(),
        "return_top": return_top.to_dict(),
        "same_param_slug": bool(balanced_top["param_slug"] == return_top["param_slug"]),
        "same_train_sell_digest": bool(balanced_top["train_sell_digest"] == return_top["train_sell_digest"]),
        "same_validation_sell_digest": bool(balanced_top["validation_sell_digest"] == return_top["validation_sell_digest"]),
        "same_test_sell_digest": bool(balanced_top["test_sell_digest"] == return_top["test_sell_digest"]),
    }


def main() -> None:
    if not LEADERBOARD_PATH.exists():
        raise FileNotFoundError(f"未找到排行榜: {LEADERBOARD_PATH}")

    df = pd.read_csv(LEADERBOARD_PATH).sort_values(
        by=["objective_score_balanced", "objective_score_return", "test_return_pct"],
        ascending=False,
    ).reset_index(drop=True)

    report = {
        "leaderboard_path": str(LEADERBOARD_PATH),
        "row_count": int(len(df)),
        "top_cluster": summarize_top_cluster(df),
        "score_tracks": summarize_score_tracks(df),
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"结果已写入: {OUTPUT_PATH}")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
