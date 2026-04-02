from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
import os
from pathlib import Path
from typing import Any
import uuid

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RUNTIME_ROOT = Path(
    os.getenv("THS_TRADING_RUNTIME_DIR", str(PROJECT_ROOT / "outputs" / "股票策略交易执行"))
)


def _iso_dt(value: pd.Timestamp | str | None) -> str | None:
    if value is None:
        return None
    return str(pd.Timestamp(value).isoformat())


def _iso_day(value: pd.Timestamp | str | None) -> str | None:
    if value is None:
        return None
    return str(pd.Timestamp(value).normalize().date())


@dataclass
class RuntimePaths:
    root: Path
    state_dir: Path
    reports_dir: Path
    logs_dir: Path


@dataclass
class StrategyPosition:
    code: str
    name: str
    market: str
    shares: int
    entry_dt: str
    entry_price: float
    cost_basis: float
    atr_at_entry: float
    highest_price: float
    latest_price: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StrategyPosition":
        return cls(
            code=data["code"],
            name=data["name"],
            market=data["market"],
            shares=int(data["shares"]),
            entry_dt=str(data["entry_dt"]),
            entry_price=float(data["entry_price"]),
            cost_basis=float(data["cost_basis"]),
            atr_at_entry=float(data.get("atr_at_entry", 0.0)),
            highest_price=float(data.get("highest_price", data["entry_price"])),
            latest_price=float(data.get("latest_price", data["entry_price"])),
        )


@dataclass
class PlannedOrder:
    order_id: str
    signal_date: str
    execute_date: str
    execute_session: str
    action: str
    code: str
    name: str
    market: str
    reason: str
    target_value: float
    requested_shares: int
    estimated_price: float
    estimated_shares: int
    atr_at_signal: float
    price_reference: str
    risk_tag: str
    strategy_version: str
    parameter_version: str
    scheduled_datetime: str | None = None
    status: str = "planned"
    created_at: str = field(default_factory=lambda: str(pd.Timestamp.now().isoformat()))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PlannedOrder":
        return cls(
            order_id=data["order_id"],
            signal_date=str(data["signal_date"]),
            execute_date=str(data["execute_date"]),
            execute_session=str(data.get("execute_session", "open")),
            action=str(data["action"]),
            code=str(data["code"]),
            name=str(data.get("name", data["code"])),
            market=str(data.get("market", "")),
            reason=str(data.get("reason", "")),
            target_value=float(data.get("target_value", 0.0)),
            requested_shares=int(data.get("requested_shares", 0)),
            estimated_price=float(data.get("estimated_price", 0.0)),
            estimated_shares=int(data.get("estimated_shares", 0)),
            atr_at_signal=float(data.get("atr_at_signal", 0.0)),
            price_reference=str(data.get("price_reference", "close_estimate")),
            risk_tag=str(data.get("risk_tag", "")),
            strategy_version=str(data.get("strategy_version", "")),
            parameter_version=str(data.get("parameter_version", "")),
            scheduled_datetime=data.get("scheduled_datetime"),
            status=str(data.get("status", "planned")),
            created_at=str(data.get("created_at", pd.Timestamp.now().isoformat())),
        )


@dataclass
class StrategyState:
    cash: float
    positions: dict[str, StrategyPosition] = field(default_factory=dict)
    cooldown_until: dict[str, str] = field(default_factory=dict)
    last_signal_date: str | None = None
    last_execution_date: str | None = None
    strategy_version: str = "baseline_live_v1"
    parameter_version: str = "baseline_default"
    notes: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "cash": self.cash,
            "positions": {code: pos.to_dict() for code, pos in self.positions.items()},
            "cooldown_until": dict(self.cooldown_until),
            "last_signal_date": self.last_signal_date,
            "last_execution_date": self.last_execution_date,
            "strategy_version": self.strategy_version,
            "parameter_version": self.parameter_version,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StrategyState":
        return cls(
            cash=float(data.get("cash", 0.0)),
            positions={
                code: StrategyPosition.from_dict(pos_data)
                for code, pos_data in data.get("positions", {}).items()
            },
            cooldown_until={k: str(v) for k, v in data.get("cooldown_until", {}).items()},
            last_signal_date=data.get("last_signal_date"),
            last_execution_date=data.get("last_execution_date"),
            strategy_version=str(data.get("strategy_version", "baseline_live_v1")),
            parameter_version=str(data.get("parameter_version", "baseline_default")),
            notes=dict(data.get("notes", {})),
        )

    def total_equity(self, price_map: dict[str, float]) -> float:
        return float(
            self.cash
            + sum(
                pos.shares * float(price_map.get(code, pos.latest_price))
                for code, pos in self.positions.items()
            )
        )


def build_runtime_paths(root: Path | None = None) -> RuntimePaths:
    runtime_root = Path(root or DEFAULT_RUNTIME_ROOT)
    return RuntimePaths(
        root=runtime_root,
        state_dir=runtime_root / "state",
        reports_dir=runtime_root / "reports",
        logs_dir=runtime_root / "logs",
    )


def ensure_runtime_dirs(paths: RuntimePaths) -> None:
    for path in [paths.root, paths.state_dir, paths.reports_dir, paths.logs_dir]:
        path.mkdir(parents=True, exist_ok=True)


def state_path(paths: RuntimePaths) -> Path:
    return paths.state_dir / "strategy_state.json"


def planned_orders_path(paths: RuntimePaths, signal_date: str | pd.Timestamp) -> Path:
    date_tag = pd.Timestamp(signal_date).strftime("%Y%m%d")
    return paths.state_dir / f"planned_orders_{date_tag}.json"


def signal_report_path(paths: RuntimePaths, signal_date: str | pd.Timestamp) -> Path:
    date_tag = pd.Timestamp(signal_date).strftime("%Y%m%d")
    return paths.reports_dir / f"signal_report_{date_tag}.json"


def execution_journal_path(paths: RuntimePaths, trade_date: str | pd.Timestamp) -> Path:
    date_tag = pd.Timestamp(trade_date).strftime("%Y%m%d")
    return paths.state_dir / f"execution_journal_{date_tag}.jsonl"


def reconcile_report_path(paths: RuntimePaths, trade_date: str | pd.Timestamp, broker: str) -> Path:
    date_tag = pd.Timestamp(trade_date).strftime("%Y%m%d")
    return paths.reports_dir / f"reconcile_{broker}_{date_tag}.json"


def account_snapshot_path(paths: RuntimePaths, trade_date: str | pd.Timestamp, broker: str) -> Path:
    date_tag = pd.Timestamp(trade_date).strftime("%Y%m%d")
    return paths.state_dir / f"account_snapshot_{broker}_{date_tag}.json"


def generate_order_id(signal_date: str | pd.Timestamp, action: str, code: str) -> str:
    date_tag = pd.Timestamp(signal_date).strftime("%Y%m%d")
    return f"{date_tag}-{action.upper()}-{code}-{uuid.uuid4().hex[:8]}"


def dump_json(data: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def append_jsonl(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def save_state(state: StrategyState, paths: RuntimePaths) -> Path:
    ensure_runtime_dirs(paths)
    path = state_path(paths)
    dump_json(state.to_dict(), path)
    return path


def load_state(
    paths: RuntimePaths,
    initial_capital: float,
    strategy_version: str = "baseline_live_v1",
    parameter_version: str = "baseline_default",
) -> StrategyState:
    ensure_runtime_dirs(paths)
    path = state_path(paths)
    if not path.exists():
        state = StrategyState(
            cash=float(initial_capital),
            strategy_version=strategy_version,
            parameter_version=parameter_version,
        )
        save_state(state, paths)
        return state
    return StrategyState.from_dict(load_json(path, default={}))


def save_planned_orders(
    orders: list[PlannedOrder],
    paths: RuntimePaths,
    signal_date: str | pd.Timestamp,
) -> Path:
    payload = {
        "signal_date": _iso_day(signal_date),
        "orders": [order.to_dict() for order in orders],
    }
    path = planned_orders_path(paths, signal_date)
    dump_json(payload, path)
    return path


def load_planned_orders(paths: RuntimePaths, signal_date: str | pd.Timestamp) -> list[PlannedOrder]:
    payload = load_json(planned_orders_path(paths, signal_date), default={"orders": []})
    return [PlannedOrder.from_dict(item) for item in payload.get("orders", [])]


def load_due_orders(paths: RuntimePaths, execute_date: str | pd.Timestamp) -> list[PlannedOrder]:
    due_date = _iso_day(execute_date)
    orders: list[PlannedOrder] = []
    for path in sorted(paths.state_dir.glob("planned_orders_*.json")):
        payload = load_json(path, default={"orders": []})
        for item in payload.get("orders", []):
            order = PlannedOrder.from_dict(item)
            if order.execute_date == due_date and order.status == "planned":
                orders.append(order)
    return orders


def write_signal_report(report: dict[str, Any], paths: RuntimePaths, signal_date: str | pd.Timestamp) -> Path:
    path = signal_report_path(paths, signal_date)
    dump_json(report, path)
    return path


def update_orders_status(
    paths: RuntimePaths,
    signal_date: str | pd.Timestamp,
    updates: dict[str, str],
) -> None:
    path = planned_orders_path(paths, signal_date)
    payload = load_json(path, default={"orders": []})
    for item in payload.get("orders", []):
        order_id = item.get("order_id")
        if order_id in updates:
            item["status"] = updates[order_id]
    dump_json(payload, path)


def order_rows(orders: list[PlannedOrder]) -> list[dict[str, Any]]:
    return [order.to_dict() for order in orders]


def normalize_date(value: str | pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(value).normalize()


def as_day_str(value: str | pd.Timestamp | None) -> str | None:
    return _iso_day(value)


def as_dt_str(value: str | pd.Timestamp | None) -> str | None:
    return _iso_dt(value)
