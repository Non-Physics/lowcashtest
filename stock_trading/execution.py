from __future__ import annotations

from dataclasses import dataclass
import importlib.util
from pathlib import Path
import sys
from typing import Any

import pandas as pd

from .common import (
    PlannedOrder,
    RuntimePaths,
    StrategyPosition,
    StrategyState,
    account_snapshot_path,
    append_jsonl,
    as_day_str,
    as_dt_str,
    dump_json,
    execution_journal_path,
    load_due_orders,
    reconcile_report_path,
    save_state,
    update_orders_status,
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
class ExecutionBatchResult:
    trade_date: str
    records: list[dict[str, Any]]
    report: dict[str, Any]
    updated_state: StrategyState
    journal_path: Path


class PaperExecutionAdapter:
    def __init__(self, runtime_paths: RuntimePaths) -> None:
        self.runtime_paths = runtime_paths
        self.baseline = _load_module("stock_strategy_baseline_exec", "股票策略回测_基线版.py")
        self.base = self.baseline._BASE

    def get_context(self) -> dict[str, Any]:
        return self.baseline.get_backtest_context()

    def _resolve_open_price(
        self,
        code: str,
        trade_date: pd.Timestamp,
        context: dict[str, Any],
    ) -> tuple[pd.Timestamp | None, float | None]:
        intraday_df = context["intraday_data"].get(code)
        if intraday_df is not None:
            day_rows = intraday_df[intraday_df["day"] == trade_date]
            if not day_rows.empty:
                row = day_rows.iloc[0]
                return pd.Timestamp(row["datetime"]), float(row["open"])
        daily_df = context["daily_data"].get(code)
        if daily_df is not None:
            day_rows = daily_df[daily_df["date"] == trade_date]
            if not day_rows.empty:
                row = day_rows.iloc[0]
                return pd.Timestamp(f"{trade_date.date()} 09:30:00"), float(row["open"])
        return None, None

    def _mark_positions_close(
        self,
        state: StrategyState,
        trade_date: pd.Timestamp,
        context: dict[str, Any],
    ) -> None:
        for code, pos in state.positions.items():
            daily_df = context["daily_data"].get(code)
            if daily_df is None:
                continue
            row = daily_df[daily_df["date"] == trade_date]
            if row.empty:
                continue
            pos.latest_price = float(row.iloc[0]["close"])
            pos.highest_price = max(pos.highest_price, pos.latest_price)

    def execute_due_orders(
        self,
        trade_date: str | pd.Timestamp,
        state: StrategyState,
        orders: list[PlannedOrder] | None = None,
        preloaded_context: dict[str, Any] | None = None,
    ) -> ExecutionBatchResult:
        exec_date = pd.Timestamp(trade_date).normalize()
        context = preloaded_context if preloaded_context is not None else self.get_context()
        due_orders = orders if orders is not None else load_due_orders(self.runtime_paths, exec_date)
        working_state = StrategyState.from_dict(state.to_dict())
        records: list[dict[str, Any]] = []
        status_updates: dict[str, str] = {}

        sell_orders = [order for order in due_orders if order.action == "SELL"]
        buy_orders = [order for order in due_orders if order.action == "BUY"]

        for order in sell_orders:
            record = {
                "timestamp": as_dt_str(pd.Timestamp.now()),
                "trade_date": as_day_str(exec_date),
                "order_id": order.order_id,
                "signal_date": order.signal_date,
                "action": order.action,
                "code": order.code,
                "reason": order.reason,
            }
            if order.code not in working_state.positions:
                record.update({"status": "skipped", "message": "本地状态无对应持仓"})
                records.append(record)
                status_updates[order.order_id] = "skipped"
                continue
            pos = working_state.positions[order.code]
            exec_dt, open_price = self._resolve_open_price(order.code, exec_date, context)
            if open_price is None or exec_dt is None:
                record.update({"status": "failed", "message": "无法解析开盘价"})
                records.append(record)
                status_updates[order.order_id] = "failed"
                continue
            shares = pos.shares if order.requested_shares <= 0 else min(pos.shares, order.requested_shares)
            amount = shares * open_price
            net_proceeds, fee = self.base.calc_sell_net(amount, pos.market)
            cost_basis = pos.cost_basis * (shares / pos.shares)
            profit = net_proceeds - cost_basis
            working_state.cash += net_proceeds
            if shares == pos.shares:
                working_state.positions.pop(order.code)
                cooldown_date = exec_date + pd.Timedelta(days=self.baseline.REENTRY_COOLDOWN_DAYS)
                working_state.cooldown_until[order.code] = as_day_str(cooldown_date) or ""
            else:
                pos.shares -= shares
                pos.cost_basis -= cost_basis
            record.update(
                {
                    "status": "filled",
                    "exec_datetime": as_dt_str(exec_dt),
                    "price": round(open_price, 4),
                    "shares": shares,
                    "fee": round(fee, 4),
                    "cash_after": round(working_state.cash, 4),
                    "profit": round(profit, 4),
                }
            )
            records.append(record)
            status_updates[order.order_id] = "filled"

        for order in buy_orders:
            record = {
                "timestamp": as_dt_str(pd.Timestamp.now()),
                "trade_date": as_day_str(exec_date),
                "order_id": order.order_id,
                "signal_date": order.signal_date,
                "action": order.action,
                "code": order.code,
                "reason": order.reason,
            }
            if order.code in working_state.positions:
                record.update({"status": "skipped", "message": "本地状态已持仓"})
                records.append(record)
                status_updates[order.order_id] = "skipped"
                continue
            exec_dt, open_price = self._resolve_open_price(order.code, exec_date, context)
            if open_price is None or exec_dt is None:
                record.update({"status": "failed", "message": "无法解析开盘价"})
                records.append(record)
                status_updates[order.order_id] = "failed"
                continue

            shares = int(order.target_value / open_price / 100) * 100 if open_price > 0 else 0
            while shares > 0:
                amount = shares * open_price
                total_cost, fee = self.base.calc_buy_cost(amount, order.market)
                if total_cost <= working_state.cash:
                    break
                shares -= 100
            if shares <= 0:
                record.update({"status": "rejected", "message": "现金不足或目标仓位不足一手"})
                records.append(record)
                status_updates[order.order_id] = "rejected"
                continue

            amount = shares * open_price
            total_cost, fee = self.base.calc_buy_cost(amount, order.market)
            working_state.cash -= total_cost
            working_state.positions[order.code] = StrategyPosition(
                code=order.code,
                name=order.name,
                market=order.market,
                shares=shares,
                entry_dt=as_dt_str(exec_dt) or "",
                entry_price=float(open_price),
                cost_basis=float(total_cost),
                atr_at_entry=float(order.atr_at_signal),
                highest_price=float(open_price),
                latest_price=float(open_price),
            )
            record.update(
                {
                    "status": "filled",
                    "exec_datetime": as_dt_str(exec_dt),
                    "price": round(open_price, 4),
                    "shares": shares,
                    "fee": round(fee, 4),
                    "cash_after": round(working_state.cash, 4),
                }
            )
            records.append(record)
            status_updates[order.order_id] = "filled"

        self._mark_positions_close(working_state, exec_date, context)
        working_state.last_execution_date = as_day_str(exec_date)
        save_state(working_state, self.runtime_paths)
        journal_path = execution_journal_path(self.runtime_paths, exec_date)
        append_jsonl(records, journal_path)
        grouped_updates: dict[str, dict[str, str]] = {}
        for order in due_orders:
            grouped_updates.setdefault(order.signal_date, {})
            grouped_updates[order.signal_date][order.order_id] = status_updates.get(order.order_id, order.status)
        for signal_date, updates in grouped_updates.items():
            update_orders_status(self.runtime_paths, signal_date, updates)

        report = self.reconcile_with_state(exec_date, working_state, due_orders, broker_name="paper")
        dump_json(report, reconcile_report_path(self.runtime_paths, exec_date, "paper"))
        snapshot = {
            "trade_date": as_day_str(exec_date),
            "cash": working_state.cash,
            "positions": {code: pos.to_dict() for code, pos in working_state.positions.items()},
        }
        dump_json(snapshot, account_snapshot_path(self.runtime_paths, exec_date, "paper"))

        return ExecutionBatchResult(
            trade_date=as_day_str(exec_date) or "",
            records=records,
            report=report,
            updated_state=working_state,
            journal_path=journal_path,
        )

    def reconcile_with_state(
        self,
        trade_date: str | pd.Timestamp,
        state: StrategyState,
        orders: list[PlannedOrder],
        broker_name: str,
        broker_snapshot: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        snapshot_positions = {}
        snapshot_cash = state.cash
        if broker_snapshot is not None:
            snapshot_cash = float(broker_snapshot.get("cash", snapshot_cash))
            snapshot_positions = {item["code"]: int(item.get("shares", 0)) for item in broker_snapshot.get("positions", [])}
        else:
            snapshot_positions = {code: pos.shares for code, pos in state.positions.items()}

        local_positions = {code: pos.shares for code, pos in state.positions.items()}
        all_codes = sorted(set(local_positions) | set(snapshot_positions))
        position_diff = []
        for code in all_codes:
            local_shares = int(local_positions.get(code, 0))
            broker_shares = int(snapshot_positions.get(code, 0))
            if local_shares != broker_shares:
                position_diff.append(
                    {"code": code, "local_shares": local_shares, "broker_shares": broker_shares}
                )

        return {
            "trade_date": as_day_str(trade_date),
            "broker": broker_name,
            "due_order_count": len(orders),
            "local_cash": round(state.cash, 4),
            "broker_cash": round(snapshot_cash, 4),
            "position_diff_count": len(position_diff),
            "position_diffs": position_diff,
            "local_positions": local_positions,
            "broker_positions": snapshot_positions,
        }
