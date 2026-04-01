"""
下载并清洗股票日线、30分钟、元数据。

默认输出目录：
    <项目目录>/data/stock_data

如果需要改目录，设置环境变量：
    THS_STOCK_DATA_DIR=/your/path

默认行为：
1. 仅保留可交易 A 股，排除创业板(300/301)、科创板(688)和北交所(8/4)。
2. 默认随机抽样 600 只股票，可通过环境变量调整：
   THS_SAMPLE_SIZE=600
   THS_SAMPLE_METHOD=random
   THS_SAMPLE_SEED=20260330

运行前要求：
1. 环境变量 TUSHARE_TOKEN 已配置
2. 已安装 tushare / pandas
3. 必须使用 Tushare 独立 API 地址初始化
"""

from __future__ import annotations

import os
import random
import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import tushare as ts
import tushare.pro.client as client

client.DataApi._DataApi__http_url = "http://tushare.xyz"

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DATA_ROOT = PROJECT_ROOT / "data" / "stock_data"
DEFAULT_TOKEN = "b1eff5f19497236f848d520d5f27ca4e4624b20889e51eaea7f629e1"

START_DATE = "20230101"
END_DATE = "20260331"
INTRADAY_FREQ = "30min"
INTRADAY_START = "2024-01-01 09:30:00"
INTRADAY_END = "2026-03-31 15:00:00"
SLEEP_SECONDS = 0.12
BATCH_SIZE = 50
DEFAULT_SCOPE = "sample"
DEFAULT_SAMPLE_SIZE = 600
DEFAULT_SAMPLE_METHOD = "random"
DEFAULT_SAMPLE_SEED = 20260330

INDEX_CODES = {
    "000001.SH": "上证指数",
    "399001.SZ": "深证成指",
    "000300.SH": "沪深300",
    "000905.SH": "中证500",
    "399006.SZ": "创业板指",
}


@dataclass
class Paths:
    root: Path
    daily_stock: Path
    daily_index: Path
    intraday_stock: Path
    intraday_index: Path
    metadata: Path


def get_token() -> str:
    token = os.getenv("TUSHARE_TOKEN", DEFAULT_TOKEN).strip()
    if not token:
        raise RuntimeError("未检测到环境变量 TUSHARE_TOKEN。")
    return token


def build_paths() -> Paths:
    root = Path(os.getenv("THS_STOCK_DATA_DIR", str(DEFAULT_DATA_ROOT)))
    return Paths(
        root=root,
        daily_stock=root / "daily" / "stock",
        daily_index=root / "daily" / "index",
        intraday_stock=root / "intraday_30m" / "stock",
        intraday_index=root / "intraday_30m" / "index",
        metadata=root / "metadata",
    )


def get_scope() -> str:
    return os.getenv("THS_DOWNLOAD_SCOPE", DEFAULT_SCOPE).strip().lower()


def get_sample_size() -> int:
    try:
        return int(os.getenv("THS_SAMPLE_SIZE", str(DEFAULT_SAMPLE_SIZE)))
    except ValueError:
        return DEFAULT_SAMPLE_SIZE


def get_sample_method() -> str:
    return os.getenv("THS_SAMPLE_METHOD", DEFAULT_SAMPLE_METHOD).strip().lower()


def get_sample_seed() -> int:
    try:
        return int(os.getenv("THS_SAMPLE_SEED", str(DEFAULT_SAMPLE_SEED)))
    except ValueError:
        return DEFAULT_SAMPLE_SEED


def ensure_dirs(paths: Paths) -> None:
    for path in [
        paths.root,
        paths.daily_stock,
        paths.daily_index,
        paths.intraday_stock,
        paths.intraday_index,
        paths.metadata,
    ]:
        path.mkdir(parents=True, exist_ok=True)


def init_pro():
    token = get_token()
    return ts.pro_api(token)


def normalize_daily(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "trade_date": "date",
        "vol": "volume",
    }
    keep = [col for col in ["trade_date", "ts_code", "open", "high", "low", "close", "vol", "amount", "pre_close"] if col in df.columns]
    out = df[keep].copy()
    out = out.rename(columns=rename_map)
    out["code"] = df["ts_code"]
    out["date"] = pd.to_datetime(out["date"])
    out = out.sort_values("date").reset_index(drop=True)
    return out


def normalize_30m(df: pd.DataFrame, code: str) -> pd.DataFrame:
    rename_map = {
        "trade_time": "datetime",
        "vol": "volume",
    }
    keep = [col for col in ["trade_time", "open", "high", "low", "close", "vol", "amount"] if col in df.columns]
    out = df[keep].copy()
    out = out.rename(columns=rename_map)
    out["code"] = code
    out["datetime"] = pd.to_datetime(out["datetime"])
    out = out.sort_values("datetime").reset_index(drop=True)
    return out


def save_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False, encoding="utf-8-sig")


def is_tradeable_a_share(code: str) -> bool:
    symbol = code.split(".")[0]
    if symbol.startswith(("300", "301", "688", "8", "4")):
        return False
    return True


def fetch_stock_basic(pro) -> pd.DataFrame:
    frames = []
    for status in ["L", "D", "P"]:
        df = pro.stock_basic(
            exchange="",
            list_status=status,
            fields="ts_code,symbol,name,area,industry,market,list_date,delist_date,list_status"
        )
        if df is not None and not df.empty:
            frames.append(df)
        time.sleep(SLEEP_SECONDS)
    out = pd.concat(frames, ignore_index=True).drop_duplicates("ts_code")
    out = out.rename(columns={"ts_code": "code", "list_status": "status", "market": "board"})
    return out.sort_values("code").reset_index(drop=True)


def fetch_trade_calendar(pro) -> pd.DataFrame:
    df = pro.trade_cal(
        exchange="SSE",
        start_date=START_DATE,
        end_date=END_DATE,
        fields="exchange,cal_date,is_open,pretrade_date"
    )
    df = df.rename(columns={"cal_date": "date"})
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


def fetch_adj_factor(pro, codes: list[str]) -> pd.DataFrame:
    frames = []
    for idx, code in enumerate(codes, start=1):
        df = pro.adj_factor(ts_code=code, start_date=START_DATE, end_date=END_DATE)
        if df is not None and not df.empty:
            frames.append(df)
        if idx % 200 == 0:
            print(f"  已完成复权因子: {idx}/{len(codes)}")
        time.sleep(SLEEP_SECONDS)
    out = pd.concat(frames, ignore_index=True)
    out = out.rename(columns={"ts_code": "code", "trade_date": "date"})
    out["date"] = pd.to_datetime(out["date"])
    return out.sort_values(["code", "date"]).reset_index(drop=True)


def fetch_name_change(pro, codes: list[str]) -> pd.DataFrame:
    frames = []
    for i in range(0, len(codes), BATCH_SIZE):
        chunk = codes[i:i + BATCH_SIZE]
        df = pro.namechange(ts_code=",".join(chunk), fields="ts_code,name,start_date,end_date,change_reason")
        if df is not None and not df.empty:
            frames.append(df)
        time.sleep(SLEEP_SECONDS)
    if not frames:
        return pd.DataFrame(columns=["code", "name", "start_date", "end_date", "change_reason"])
    out = pd.concat(frames, ignore_index=True)
    out = out.rename(columns={"ts_code": "code"})
    return out.sort_values(["code", "start_date"]).reset_index(drop=True)


def fetch_daily_one(pro, code: str) -> pd.DataFrame:
    df = pro.daily(ts_code=code, start_date=START_DATE, end_date=END_DATE)
    if df is None or df.empty:
        return pd.DataFrame()
    return normalize_daily(df)


def fetch_stk_mins_one(pro, code: str) -> pd.DataFrame:
    df = pro.stk_mins(
        ts_code=code,
        start_time=INTRADAY_START,
        end_time=INTRADAY_END,
        freq=INTRADAY_FREQ,
    )
    if df is None or df.empty:
        return pd.DataFrame()
    return normalize_30m(df, code)


def fetch_index_daily_one(pro, code: str) -> pd.DataFrame:
    df = pro.index_daily(ts_code=code, start_date=START_DATE, end_date=END_DATE)
    if df is None or df.empty:
        return pd.DataFrame()
    return normalize_daily(df.rename(columns={"ts_code": "ts_code"}))


def build_st_status(name_change_df: pd.DataFrame, calendar_df: pd.DataFrame) -> pd.DataFrame:
    if name_change_df.empty:
        return pd.DataFrame(columns=["date", "code", "is_st"])

    open_dates = calendar_df[calendar_df["is_open"] == 1]["date"].sort_values()
    rows = []
    for _, row in name_change_df.iterrows():
        name = str(row.get("name", ""))
        if "ST" not in name.upper():
            continue
        start = pd.to_datetime(row["start_date"])
        end = pd.to_datetime(row["end_date"]) if pd.notna(row["end_date"]) else open_dates.max()
        mask = (open_dates >= start) & (open_dates <= end)
        rows.extend({"date": d, "code": row["code"], "is_st": 1} for d in open_dates[mask])
    if not rows:
        return pd.DataFrame(columns=["date", "code", "is_st"])
    out = pd.DataFrame(rows).drop_duplicates(["date", "code"])
    return out.sort_values(["code", "date"]).reset_index(drop=True)


def clean_and_filter_stock_basic(stock_basic: pd.DataFrame) -> pd.DataFrame:
    out = stock_basic.copy()
    out = out[out["code"].map(is_tradeable_a_share)]
    out = out[~out["name"].fillna("").str.contains("退", na=False)]
    return out.reset_index(drop=True)


def select_codes(stock_basic: pd.DataFrame) -> list[str]:
    scope = get_scope()
    explicit_codes = os.getenv("THS_CODE_LIST", "").strip()
    if explicit_codes:
        selected = [code.strip() for code in explicit_codes.split(",") if code.strip()]
        return [code for code in selected if code in set(stock_basic["code"])]

    if scope == "all":
        return stock_basic["code"].tolist()

    sample_size = get_sample_size()
    out = stock_basic.copy()
    out = out[out["status"] == "L"].copy()
    out["list_date"] = pd.to_datetime(out["list_date"], errors="coerce")
    out = out.dropna(subset=["list_date"]).copy()

    sample_method = get_sample_method()
    if sample_method == "random":
        rng = random.Random(get_sample_seed())
        codes = out["code"].tolist()
        if sample_size >= len(codes):
            rng.shuffle(codes)
            return sorted(codes)
        return sorted(rng.sample(codes, sample_size))

    if sample_method == "sorted":
        out = out.sort_values(["board", "list_date", "code"], ascending=[True, True, True])
        return out["code"].head(sample_size).tolist()

    raise ValueError(f"不支持的抽样方式: {sample_method}")


def run() -> None:
    paths = build_paths()
    ensure_dirs(paths)
    pro = init_pro()

    print(f"数据目录: {paths.root}")
    print("下载股票基础信息...")
    stock_basic = clean_and_filter_stock_basic(fetch_stock_basic(pro))
    save_csv(stock_basic, paths.metadata / "instruments.csv")

    print("下载交易日历...")
    trade_cal = fetch_trade_calendar(pro)
    save_csv(trade_cal, paths.metadata / "trading_calendar.csv")

    codes = select_codes(stock_basic)
    print(f"下载范围: {get_scope()}")
    print(f"抽样方式: {get_sample_method()}")
    print(f"抽样种子: {get_sample_seed()}")
    print(f"股票数量: {len(codes)}")
    save_csv(pd.DataFrame({"code": codes}), paths.metadata / "selected_universe.csv")
    save_csv(
        pd.DataFrame(
            [
                {
                    "scope": get_scope(),
                    "sample_method": get_sample_method(),
                    "sample_seed": get_sample_seed(),
                    "sample_size": len(codes),
                }
            ]
        ),
        paths.metadata / "download_config.csv",
    )

    print("下载复权因子...")
    adj_factor = fetch_adj_factor(pro, codes)
    save_csv(adj_factor, paths.metadata / "adjustment_factors.csv")

    print("生成 ST 状态...")
    name_change = fetch_name_change(pro, codes)
    st_status = build_st_status(name_change, trade_cal)
    save_csv(st_status, paths.metadata / "st_status.csv")

    print("下载股票日线...")
    for idx, code in enumerate(codes, start=1):
        df = fetch_daily_one(pro, code)
        if not df.empty:
            save_csv(df, paths.daily_stock / f"{code}.csv")
        if idx % 100 == 0:
            print(f"  已完成日线: {idx}/{len(codes)}")
        time.sleep(SLEEP_SECONDS)

    print("下载指数日线...")
    for code in INDEX_CODES:
        df = fetch_index_daily_one(pro, code)
        if not df.empty:
            save_csv(df, paths.daily_index / f"{code}.csv")
        time.sleep(SLEEP_SECONDS)

    print("下载股票30分钟...")
    for idx, code in enumerate(codes, start=1):
        df = fetch_stk_mins_one(pro, code)
        if not df.empty:
            save_csv(df, paths.intraday_stock / f"{code}.csv")
        if idx % 20 == 0:
            print(f"  已完成30分钟: {idx}/{len(codes)}")
        time.sleep(SLEEP_SECONDS)

    print("下载完成。")
    print(f"输出目录: {paths.root}")


if __name__ == "__main__":
    run()
