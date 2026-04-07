"""
下载并清洗股票日线、30分钟、元数据。

默认输出目录：
    <项目目录>/data/stock_data

如果需要改目录，设置环境变量：
    THS_STOCK_DATA_DIR=/your/path

当前默认行为：
1. 默认下载全 A 可交易股票，排除创业板(300/301)、科创板(688)和北交所(8/4)。
2. 首次运行做全量初始化；后续运行自动基于本地文件做增量续更。
3. 自动识别最新已完成交易日，默认在次日开盘前同步上一交易日数据。
4. 每次运行结束后写出下载状态和提醒文件，提示在每个交易日开盘前执行一次。

运行前要求：
1. 环境变量 TUSHARE_TOKEN 已配置
2. 已安装 tushare / pandas
3. 必须使用 Tushare 独立 API 地址初始化
"""

from __future__ import annotations

import json
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
HISTORY_START_DATE = "20220101"
INTRADAY_FREQ = "30min"
INTRADAY_HISTORY_START = "2022-01-01 09:30:00"
DEFAULT_CALENDAR_LOOKAHEAD_DAYS = 14
DEFAULT_SLEEP_SECONDS = 0.35
DEFAULT_BATCH_SIZE = 50
DEFAULT_SCOPE = "all"
DEFAULT_SAMPLE_SIZE = 600
DEFAULT_SAMPLE_METHOD = "random"
DEFAULT_SAMPLE_SEED = 20260330
DEFAULT_ADJ_FACTOR_REFRESH_LOOKBACK_DAYS = 30
DEFAULT_REMINDER_TEXT = "建议在每个交易日开盘前运行一次本脚本做增量更新，此时上一交易日数据通常已经完成晚间刷新。"

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


@dataclass
class ProgressTracker:
    label: str
    total: int
    every: int
    start_ts: float


def create_progress(label: str, total: int, every: int) -> ProgressTracker:
    safe_total = max(total, 1)
    safe_every = max(every, 1)
    print(f"[{label}] 0/{safe_total} (0.0%)")
    return ProgressTracker(label=label, total=safe_total, every=safe_every, start_ts=time.time())


def update_progress(progress: ProgressTracker, current: int) -> None:
    if current != 1 and current != progress.total and current % progress.every != 0:
        return
    elapsed = max(time.time() - progress.start_ts, 1e-6)
    pct = current / progress.total * 100
    speed = current / elapsed
    remaining = max(progress.total - current, 0)
    eta_seconds = int(remaining / speed) if speed > 0 else 0
    eta_min, eta_sec = divmod(eta_seconds, 60)
    print(
        f"[{progress.label}] {current}/{progress.total} "
        f"({pct:.1f}%) | elapsed={elapsed:.1f}s | eta={eta_min:02d}:{eta_sec:02d}"
    )


def get_env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def get_env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def get_history_start_date() -> str:
    return os.getenv("THS_HISTORY_START_DATE", HISTORY_START_DATE).strip() or HISTORY_START_DATE


def get_intraday_history_start() -> str:
    value = os.getenv("THS_INTRADAY_HISTORY_START", INTRADAY_HISTORY_START).strip()
    return value or INTRADAY_HISTORY_START


def get_sleep_seconds() -> float:
    return max(get_env_float("THS_SLEEP_SECONDS", DEFAULT_SLEEP_SECONDS), 0.0)


def get_batch_size() -> int:
    return max(get_env_int("THS_BATCH_SIZE", DEFAULT_BATCH_SIZE), 1)


def get_calendar_lookahead_days() -> int:
    return max(get_env_int("THS_CALENDAR_LOOKAHEAD_DAYS", DEFAULT_CALENDAR_LOOKAHEAD_DAYS), 1)


def get_adj_factor_refresh_lookback_days() -> int:
    return max(
        get_env_int(
            "THS_ADJ_FACTOR_REFRESH_LOOKBACK_DAYS",
            DEFAULT_ADJ_FACTOR_REFRESH_LOOKBACK_DAYS,
        ),
        1,
    )


def get_token() -> str:
    token = os.getenv("TUSHARE_TOKEN", "").strip()
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
    return get_env_int("THS_SAMPLE_SIZE", DEFAULT_SAMPLE_SIZE)


def get_sample_method() -> str:
    return os.getenv("THS_SAMPLE_METHOD", DEFAULT_SAMPLE_METHOD).strip().lower()


def get_sample_seed() -> int:
    return get_env_int("THS_SAMPLE_SEED", DEFAULT_SAMPLE_SEED)


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
    keep = [
        col
        for col in ["trade_date", "ts_code", "open", "high", "low", "close", "vol", "amount", "pre_close"]
        if col in df.columns
    ]
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


def read_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def merge_and_save_csv(
    new_df: pd.DataFrame,
    path: Path,
    sort_col: str,
    dedupe_cols: list[str],
) -> None:
    if new_df.empty:
        return
    existing = read_csv_if_exists(path)
    merged = pd.concat([existing, new_df], ignore_index=True)
    merged[sort_col] = pd.to_datetime(merged[sort_col], errors="coerce")
    merged = merged.dropna(subset=[sort_col])
    merged = merged.drop_duplicates(subset=dedupe_cols, keep="last")
    merged = merged.sort_values(sort_col).reset_index(drop=True)
    save_csv(merged, path)


def get_last_timestamp(path: Path, column: str) -> pd.Timestamp | None:
    if not path.exists():
        return None
    df = pd.read_csv(path, usecols=[column])
    if df.empty:
        return None
    series = pd.to_datetime(df[column], errors="coerce").dropna()
    if series.empty:
        return None
    return series.max()


def is_tradeable_a_share(code: str) -> bool:
    symbol = code.split(".")[0]
    if symbol.startswith(("300", "301", "688", "8", "4")):
        return False
    return True


def fetch_stock_basic(pro) -> pd.DataFrame:
    frames = []
    sleep_seconds = get_sleep_seconds()
    for status in ["L", "D", "P"]:
        df = pro.stock_basic(
            exchange="",
            list_status=status,
            fields="ts_code,symbol,name,area,industry,market,list_date,delist_date,list_status",
        )
        if df is not None and not df.empty:
            frames.append(df)
        time.sleep(sleep_seconds)
    out = pd.concat(frames, ignore_index=True).drop_duplicates("ts_code")
    out = out.rename(columns={"ts_code": "code", "list_status": "status", "market": "board"})
    return out.sort_values("code").reset_index(drop=True)


def fetch_trade_calendar(pro) -> pd.DataFrame:
    start_date = get_history_start_date()
    end_date = (pd.Timestamp.today().normalize() + pd.Timedelta(days=get_calendar_lookahead_days())).strftime("%Y%m%d")
    df = pro.trade_cal(
        exchange="SSE",
        start_date=start_date,
        end_date=end_date,
        fields="exchange,cal_date,is_open,pretrade_date",
    )
    df = df.rename(columns={"cal_date": "date"})
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


def fetch_adj_factor_refresh(
    pro,
    codes: list[str],
    existing_path: Path,
    target_trade_date: pd.Timestamp,
) -> pd.DataFrame:
    existing = read_csv_if_exists(existing_path)
    history_start = pd.Timestamp(get_history_start_date())
    refresh_start = history_start
    if not existing.empty and "date" in existing.columns:
        existing["date"] = pd.to_datetime(existing["date"], errors="coerce")
        latest_local = existing["date"].dropna().max()
        if pd.notna(latest_local):
            refresh_start = max(
                history_start,
                latest_local.normalize() - pd.Timedelta(days=get_adj_factor_refresh_lookback_days()),
            )

    frames = []
    sleep_seconds = get_sleep_seconds()
    refresh_start_str = refresh_start.strftime("%Y%m%d")
    target_trade_date_str = target_trade_date.strftime("%Y%m%d")
    for idx, code in enumerate(codes, start=1):
        df = pro.adj_factor(ts_code=code, start_date=refresh_start_str, end_date=target_trade_date_str)
        if df is not None and not df.empty:
            frames.append(df)
        if idx % 200 == 0:
            print(f"  已完成复权因子: {idx}/{len(codes)}")
        time.sleep(sleep_seconds)

    if not frames:
        if existing.empty:
            return pd.DataFrame(columns=["code", "date", "adj_factor"])
        return existing.sort_values(["code", "date"]).reset_index(drop=True)

    refreshed = pd.concat(frames, ignore_index=True)
    refreshed = refreshed.rename(columns={"ts_code": "code", "trade_date": "date"})
    refreshed["date"] = pd.to_datetime(refreshed["date"])

    if existing.empty:
        out = refreshed
    else:
        preserved = existing[existing["date"] < refresh_start].copy()
        out = pd.concat([preserved, refreshed], ignore_index=True)

    out = out.drop_duplicates(subset=["code", "date"], keep="last")
    return out.sort_values(["code", "date"]).reset_index(drop=True)


def fetch_name_change(pro, codes: list[str]) -> pd.DataFrame:
    frames = []
    batch_size = get_batch_size()
    sleep_seconds = get_sleep_seconds()
    for i in range(0, len(codes), batch_size):
        chunk = codes[i:i + batch_size]
        df = pro.namechange(ts_code=",".join(chunk), fields="ts_code,name,start_date,end_date,change_reason")
        if df is not None and not df.empty:
            frames.append(df)
        time.sleep(sleep_seconds)
    if not frames:
        return pd.DataFrame(columns=["code", "name", "start_date", "end_date", "change_reason"])
    out = pd.concat(frames, ignore_index=True)
    out = out.rename(columns={"ts_code": "code"})
    return out.sort_values(["code", "start_date"]).reset_index(drop=True)


def fetch_daily_one(pro, code: str, start_date: str, end_date: str) -> pd.DataFrame:
    df = pro.daily(ts_code=code, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        return pd.DataFrame()
    return normalize_daily(df)


def fetch_stk_mins_one(pro, code: str, start_time: str, end_time: str) -> pd.DataFrame:
    df = pro.stk_mins(
        ts_code=code,
        start_time=start_time,
        end_time=end_time,
        freq=INTRADAY_FREQ,
    )
    if df is None or df.empty:
        return pd.DataFrame()
    return normalize_30m(df, code)


def fetch_index_daily_one(pro, code: str, start_date: str, end_date: str) -> pd.DataFrame:
    df = pro.index_daily(ts_code=code, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        return pd.DataFrame()
    return normalize_daily(df)


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


def get_latest_completed_trade_date(calendar_df: pd.DataFrame) -> pd.Timestamp:
    open_days = calendar_df[calendar_df["is_open"] == 1]["date"].sort_values()
    if open_days.empty:
        raise RuntimeError("交易日历中未找到开市日期。")
    today = pd.Timestamp.today().normalize()
    open_day_set = set(open_days.tolist())
    if today in open_day_set:
        eligible = open_days[open_days < today]
    else:
        eligible = open_days[open_days <= today]
    if eligible.empty:
        raise RuntimeError("当前交易日历中没有已完成的交易日。")
    return eligible.max().normalize()


def resolve_daily_start_date(path: Path, list_date: str | float | None, target_trade_date: pd.Timestamp) -> str | None:
    history_start = pd.Timestamp(get_history_start_date())
    if pd.notna(list_date):
        list_date_ts = pd.to_datetime(list_date, errors="coerce")
        if pd.notna(list_date_ts):
            history_start = max(history_start, list_date_ts.normalize())

    last_date = get_last_timestamp(path, "date")
    if last_date is not None:
        start_date = last_date.normalize() + pd.Timedelta(days=1)
    else:
        start_date = history_start

    if start_date > target_trade_date:
        return None
    return start_date.strftime("%Y%m%d")


def resolve_intraday_window(
    path: Path,
    list_date: str | float | None,
    target_trade_date: pd.Timestamp,
) -> tuple[str, str] | None:
    history_start = pd.Timestamp(get_intraday_history_start())
    if pd.notna(list_date):
        list_date_ts = pd.to_datetime(list_date, errors="coerce")
        if pd.notna(list_date_ts):
            history_start = max(history_start, list_date_ts.normalize() + pd.Timedelta(hours=9, minutes=30))

    last_dt = get_last_timestamp(path, "datetime")
    if last_dt is not None:
        start_dt = last_dt + pd.Timedelta(minutes=30)
    else:
        start_dt = history_start

    end_dt = target_trade_date.normalize() + pd.Timedelta(hours=15)
    if start_dt > end_dt:
        return None
    return (
        start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        end_dt.strftime("%Y-%m-%d %H:%M:%S"),
    )


def resolve_index_start_date(path: Path, target_trade_date: pd.Timestamp) -> str | None:
    last_date = get_last_timestamp(path, "date")
    if last_date is not None:
        start_date = last_date.normalize() + pd.Timedelta(days=1)
    else:
        start_date = pd.Timestamp(get_history_start_date())

    if start_date > target_trade_date:
        return None
    return start_date.strftime("%Y%m%d")


def build_download_config(codes: list[str], target_trade_date: pd.Timestamp) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "scope": get_scope(),
                "sample_method": get_sample_method(),
                "sample_seed": get_sample_seed(),
                "sample_size": len(codes),
                "history_start_date": get_history_start_date(),
                "intraday_history_start": get_intraday_history_start(),
                "target_trade_date": target_trade_date.strftime("%Y-%m-%d"),
                "sleep_seconds": get_sleep_seconds(),
                "batch_size": get_batch_size(),
            }
        ]
    )


def write_status_files(paths: Paths, payload: dict) -> None:
    status_path = paths.metadata / "download_status.json"
    reminder_path = paths.metadata / "daily_update_reminder.txt"
    status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    reminder_path.write_text(
        "\n".join(
            [
                DEFAULT_REMINDER_TEXT,
                f"目标交易日: {payload['target_trade_date']}",
                f"最近运行时间: {payload['run_at']}",
                f"推荐命令: python 下载股票数据.py",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


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
    latest_open_trade_date = get_latest_completed_trade_date(trade_cal)
    print(f"最新已完成交易日: {latest_open_trade_date.strftime('%Y-%m-%d')}")

    codes = select_codes(stock_basic)
    code_to_list_date = stock_basic.set_index("code")["list_date"].to_dict()
    print(f"下载范围: {get_scope()}")
    print(f"抽样方式: {get_sample_method()}")
    print(f"抽样种子: {get_sample_seed()}")
    print(f"股票数量: {len(codes)}")
    save_csv(pd.DataFrame({"code": codes}), paths.metadata / "selected_universe.csv")
    save_csv(build_download_config(codes, latest_open_trade_date), paths.metadata / "download_config.csv")

    print("下载复权因子...")
    adj_factor = fetch_adj_factor_refresh(
        pro,
        codes,
        paths.metadata / "adjustment_factors.csv",
        latest_open_trade_date,
    )
    save_csv(adj_factor, paths.metadata / "adjustment_factors.csv")

    print("生成 ST 状态...")
    name_change = fetch_name_change(pro, codes)
    st_status = build_st_status(name_change, trade_cal)
    save_csv(st_status, paths.metadata / "st_status.csv")

    print("下载股票日线...")
    daily_created = 0
    daily_updated = 0
    daily_skipped = 0
    sleep_seconds = get_sleep_seconds()
    target_trade_date_str = latest_open_trade_date.strftime("%Y%m%d")
    daily_progress = create_progress("日线", len(codes), every=100)
    for idx, code in enumerate(codes, start=1):
        file_path = paths.daily_stock / f"{code}.csv"
        existed_before = file_path.exists()
        start_date = resolve_daily_start_date(file_path, code_to_list_date.get(code), latest_open_trade_date)
        if start_date is None:
            daily_skipped += 1
        else:
            df = fetch_daily_one(pro, code, start_date, target_trade_date_str)
            if not df.empty:
                merge_and_save_csv(df, file_path, sort_col="date", dedupe_cols=["date"])
                if existed_before:
                    daily_updated += 1
                else:
                    daily_created += 1
            else:
                daily_skipped += 1
        update_progress(daily_progress, idx)
        time.sleep(sleep_seconds)

    print("下载指数日线...")
    index_updated = 0
    index_skipped = 0
    index_codes = list(INDEX_CODES)
    index_progress = create_progress("指数日线", len(index_codes), every=1)
    for idx, code in enumerate(index_codes, start=1):
        file_path = paths.daily_index / f"{code}.csv"
        start_date = resolve_index_start_date(file_path, latest_open_trade_date)
        if start_date is None:
            index_skipped += 1
            update_progress(index_progress, idx)
            continue
        df = fetch_index_daily_one(pro, code, start_date, target_trade_date_str)
        if not df.empty:
            merge_and_save_csv(df, file_path, sort_col="date", dedupe_cols=["date"])
            index_updated += 1
        else:
            index_skipped += 1
        update_progress(index_progress, idx)
        time.sleep(sleep_seconds)

    print("下载股票30分钟...")
    intraday_created = 0
    intraday_updated = 0
    intraday_skipped = 0
    intraday_progress = create_progress("30分钟", len(codes), every=20)
    for idx, code in enumerate(codes, start=1):
        file_path = paths.intraday_stock / f"{code}.csv"
        existed_before = file_path.exists()
        window = resolve_intraday_window(file_path, code_to_list_date.get(code), latest_open_trade_date)
        if window is None:
            intraday_skipped += 1
        else:
            start_time, end_time = window
            df = fetch_stk_mins_one(pro, code, start_time, end_time)
            if not df.empty:
                merge_and_save_csv(df, file_path, sort_col="datetime", dedupe_cols=["datetime"])
                if existed_before:
                    intraday_updated += 1
                else:
                    intraday_created += 1
            else:
                intraday_skipped += 1
        update_progress(intraday_progress, idx)
        time.sleep(sleep_seconds)

    status_payload = {
        "run_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data_root": str(paths.root),
        "scope": get_scope(),
        "code_count": len(codes),
        "target_trade_date": latest_open_trade_date.strftime("%Y-%m-%d"),
        "history_start_date": get_history_start_date(),
        "intraday_history_start": get_intraday_history_start(),
        "sleep_seconds": get_sleep_seconds(),
        "batch_size": get_batch_size(),
        "daily_summary": {
            "created": daily_created,
            "updated": daily_updated,
            "skipped": daily_skipped,
        },
        "index_daily_summary": {
            "updated": index_updated,
            "skipped": index_skipped,
        },
        "intraday_summary": {
            "created": intraday_created,
            "updated": intraday_updated,
            "skipped": intraday_skipped,
        },
        "reminder": DEFAULT_REMINDER_TEXT,
        "recommended_command": "python 下载股票数据.py",
    }
    write_status_files(paths, status_payload)

    print("下载完成。")
    print(f"输出目录: {paths.root}")
    print(f"日线: 新建 {daily_created} | 增量更新 {daily_updated} | 跳过 {daily_skipped}")
    print(f"30分钟: 新建 {intraday_created} | 增量更新 {intraday_updated} | 跳过 {intraday_skipped}")
    print(DEFAULT_REMINDER_TEXT)
    print(f"状态文件: {paths.metadata / 'download_status.json'}")


if __name__ == "__main__":
    run()
