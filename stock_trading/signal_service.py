from __future__ import annotations

from dataclasses import dataclass
import importlib.util
from pathlib import Path
import sys
from typing import Any

import pandas as pd

from .common import (
    PlannedOrder,
    StrategyState,
    as_day_str,
    as_dt_str,
    generate_order_id,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _load_module(module_name: str, filename: str):
    if module_name in sys.modules:
        return sys.modules[module_name]
    path = PROJECT_ROOT / filename
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载模块: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@dataclass
class SignalRunResult:
    signal_date: str
    execute_date: str | None
    orders: list[PlannedOrder]
    report: dict[str, Any]
    updated_state: StrategyState


class StrategySignalService:
    def __init__(self) -> None:
        self.baseline = _load_module("stock_strategy_baseline_live", "股票策略回测_基线版.py")
        self.base = self.baseline._BASE
        self._calendar_cache: pd.DataFrame | None = None

    def get_context(self) -> dict[str, Any]:
        return self.baseline.get_backtest_context()

    def _open_calendar(self) -> pd.DataFrame:
        if self._calendar_cache is None:
            calendar_path = self.baseline.DATA_ROOT / "metadata" / "trading_calendar.csv"
            if calendar_path.exists():
                df = pd.read_csv(calendar_path)
                df["date"] = pd.to_datetime(df["date"])
                self._calendar_cache = df.sort_values("date").reset_index(drop=True)
            else:
                context = self.get_context()
                self._calendar_cache = pd.DataFrame({"date": sorted(context["all_dates"]), "is_open": 1})
        return self._calendar_cache

    def next_trade_date(self, current_date: pd.Timestamp) -> pd.Timestamp | None:
        calendar_df = self._open_calendar()
        open_days = calendar_df[calendar_df["is_open"] == 1]["date"]
        future = open_days[open_days > current_date.normalize()]
        if future.empty:
            return None
        return pd.Timestamp(future.iloc[0]).normalize()

    def _resolve_next_session(
        self,
        code: str,
        current_date: pd.Timestamp,
        intraday_data: dict[str, pd.DataFrame],
    ) -> tuple[pd.Timestamp | None, pd.Timestamp | None, float | None]:
        intraday_df = intraday_data.get(code)
        if intraday_df is not None:
            next_session = self.base.get_next_session_open(intraday_df, current_date)
            if next_session is not None:
                exec_dt, next_open = next_session
                return pd.Timestamp(exec_dt).normalize(), pd.Timestamp(exec_dt), float(next_open)
        trade_date = self.next_trade_date(current_date)
        if trade_date is None:
            return None, None, None
        scheduled_dt = pd.Timestamp(f"{trade_date.date()} 09:30:00")
        return trade_date, scheduled_dt, None

    def _build_price_map(
        self,
        signal_date: pd.Timestamp,
        state: StrategyState,
        daily_data: dict[str, pd.DataFrame],
    ) -> dict[str, float]:
        price_map: dict[str, float] = {}
        for code, pos in state.positions.items():
            row = daily_data.get(code)
            if row is None:
                price_map[code] = pos.latest_price
                continue
            matched = row[row["date"] == signal_date]
            if matched.empty:
                price_map[code] = pos.latest_price
                continue
            price_map[code] = float(matched.iloc[0]["close"])
        return price_map

    def generate_for_date(
        self,
        signal_date: str | pd.Timestamp,
        state: StrategyState,
        strategy_version: str | None = None,
        parameter_version: str | None = None,
        preloaded_context: dict[str, Any] | None = None,
    ) -> SignalRunResult:
        current_date = pd.Timestamp(signal_date).normalize()
        context = preloaded_context if preloaded_context is not None else self.get_context()
        instruments = context["instruments"]
        st_status = context["st_status"]
        daily_data = context["daily_data"]
        intraday_data = context["intraday_data"]
        benchmark = context["benchmark"]

        updated_state = StrategyState.from_dict(state.to_dict())
        updated_state.strategy_version = strategy_version or state.strategy_version
        updated_state.parameter_version = parameter_version or state.parameter_version

        risk_on = self.base.benchmark_risk_on(benchmark, current_date)
        risk_off = self.base.benchmark_risk_off(benchmark, current_date)
        price_map = self._build_price_map(current_date, updated_state, daily_data)
        estimated_equity = updated_state.total_equity(price_map)

        orders: list[PlannedOrder] = []
        sell_codes: set[str] = set()
        report: dict[str, Any] = {
            "signal_date": as_day_str(current_date),
            "risk_on": bool(risk_on),
            "risk_off": bool(risk_off),
            "estimated_equity": estimated_equity,
            "sell_reasons": [],
            "buy_candidates": [],
        }

        for code, pos in list(updated_state.positions.items()):
            daily_df = daily_data.get(code)
            if daily_df is None:
                continue
            row = daily_df[daily_df["date"] == current_date]
            if row.empty:
                continue
            daily_row = row.iloc[0]
            pos.latest_price = float(daily_row["close"])
            pos.highest_price = max(pos.highest_price, pos.latest_price)
            holding_days = (current_date - pd.Timestamp(pos.entry_dt).normalize()).days
            idx_list = daily_df.index[daily_df["date"] == current_date].tolist()
            prev_daily_row = daily_df.iloc[idx_list[0] - 1] if idx_list and idx_list[0] > 0 else None
            pos_dict = pos.to_dict()
            stop_price, stop_reason = self.base.calc_stop_price(pos_dict, daily_row)
            exit_reason: str | None = None

            if self.baseline.ENABLE_INTRADAY_EXIT and code in intraday_data:
                intraday_exit = self.base.get_day_intraday_exit(intraday_data[code], current_date, pos_dict)
                if intraday_exit is not None:
                    exit_reason = intraday_exit[1]

            if exit_reason is None and daily_row["close"] <= stop_price:
                exit_reason = stop_reason
            if exit_reason is None and self.base.should_exit_early_weakness(pos_dict, holding_days):
                exit_reason = "早期弱势淘汰"
            if exit_reason is None and holding_days >= self.baseline.MAX_HOLDING_DAYS:
                exit_reason = "达到最大持有天数"

            trend_break = bool(
                holding_days >= self.baseline.MIN_HOLDING_DAYS_BEFORE_TREND_EXIT
                and (
                    daily_row["close"] < daily_row["ma20"] * 0.992
                    or (
                        prev_daily_row is not None
                        and prev_daily_row["close"] < prev_daily_row["ma20"]
                        and daily_row["close"] < daily_row["ma20"]
                    )
                )
            )
            market_exit = bool(
                risk_off
                and holding_days >= self.baseline.MIN_HOLDING_DAYS_BEFORE_TREND_EXIT
                and daily_row["close"] < daily_row["ma20"]
            )

            if exit_reason is None and (trend_break or market_exit):
                exit_reason = "跌破20日线" if trend_break else "市场转弱"

            if exit_reason is None:
                continue

            execute_date, scheduled_dt, next_open = self._resolve_next_session(code, current_date, intraday_data)
            if execute_date is None:
                continue
            order = PlannedOrder(
                order_id=generate_order_id(current_date, "SELL", code),
                signal_date=as_day_str(current_date) or "",
                execute_date=as_day_str(execute_date) or "",
                execute_session="open",
                action="SELL",
                code=code,
                name=pos.name,
                market=pos.market,
                reason=exit_reason,
                target_value=0.0,
                requested_shares=pos.shares,
                estimated_price=float(next_open or pos.latest_price),
                estimated_shares=pos.shares,
                atr_at_signal=pos.atr_at_entry,
                price_reference="next_open" if next_open is not None else "close_estimate",
                risk_tag="risk_off" if market_exit else "position_exit",
                strategy_version=updated_state.strategy_version,
                parameter_version=updated_state.parameter_version,
                scheduled_datetime=as_dt_str(scheduled_dt),
            )
            orders.append(order)
            sell_codes.add(code)
            report["sell_reasons"].append({"code": code, "reason": exit_reason})

        if (not self.baseline.STRICT_RISK_ON_FOR_ENTRY) or risk_on:
            candidates = self.base.select_candidates(
                current_date,
                daily_data,
                instruments,
                st_status,
                {code: pd.Timestamp(value) for code, value in updated_state.cooldown_until.items()},
            )
            holding_codes = set(updated_state.positions.keys())
            for code, score in candidates:
                report["buy_candidates"].append({"code": code, "score": float(score)})
                if code in holding_codes or code in sell_codes:
                    continue
                if code not in intraday_data:
                    continue
                entry_signal_dt = self.base.find_intraday_entry(intraday_data[code], current_date)
                if entry_signal_dt is None:
                    continue
                execute_date, scheduled_dt, next_open = self._resolve_next_session(code, current_date, intraday_data)
                if execute_date is None:
                    continue
                daily_row = daily_data[code][daily_data[code]["date"] == current_date]
                if daily_row.empty:
                    continue
                row = daily_row.iloc[0]
                estimated_price = float(next_open or row["close"])
                target_value = estimated_equity * self.baseline.MAX_POSITION_PCT
                estimated_shares = int(target_value / estimated_price / 100) * 100 if estimated_price > 0 else 0
                order = PlannedOrder(
                    order_id=generate_order_id(current_date, "BUY", code),
                    signal_date=as_day_str(current_date) or "",
                    execute_date=as_day_str(execute_date) or "",
                    execute_session="open",
                    action="BUY",
                    code=code,
                    name=instruments.loc[instruments["code"] == code, "name"].iloc[0],
                    market=self.base.market_of(code),
                    reason="日线趋势+30分钟回踩突破",
                    target_value=float(target_value),
                    requested_shares=0,
                    estimated_price=estimated_price,
                    estimated_shares=estimated_shares,
                    atr_at_signal=float(row["atr14"]),
                    price_reference="next_open" if next_open is not None else "close_estimate",
                    risk_tag="risk_on" if risk_on else "risk_neutral",
                    strategy_version=updated_state.strategy_version,
                    parameter_version=updated_state.parameter_version,
                    scheduled_datetime=as_dt_str(scheduled_dt),
                )
                orders.append(order)

        execute_dates = sorted({order.execute_date for order in orders})
        report["order_count"] = len(orders)
        report["buy_count"] = sum(order.action == "BUY" for order in orders)
        report["sell_count"] = sum(order.action == "SELL" for order in orders)
        report["execute_dates"] = execute_dates
        updated_state.last_signal_date = as_day_str(current_date)
        return SignalRunResult(
            signal_date=as_day_str(current_date) or "",
            execute_date=execute_dates[0] if execute_dates else None,
            orders=orders,
            report=report,
            updated_state=updated_state,
        )
