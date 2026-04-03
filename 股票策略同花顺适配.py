from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from stock_trading.common import PlannedOrder, as_dt_str


@dataclass
class TongHuaShunClientConfig:
    client_type: str = "ths"
    exe_path: str = ""
    account_json: str = ""
    auto_confirm: bool = False


class TongHuaShunGuiAdapter:
    def __init__(self, config: TongHuaShunClientConfig | None = None) -> None:
        self.config = config or TongHuaShunClientConfig()
        self._client = None

    def _ensure_easytrader(self):
        try:
            import easytrader
        except ImportError as exc:
            raise RuntimeError(
                "未安装 easytrader。请先在本地环境安装 easytrader，再启用同花顺 GUI 下单。"
            ) from exc
        return easytrader

    def connect(self):
        easytrader = self._ensure_easytrader()
        user = easytrader.use(self.config.client_type)
        if self.config.account_json:
            user.prepare(self.config.account_json)
        elif self.config.exe_path:
            connect = getattr(user, "connect", None)
            if connect is None:
                raise RuntimeError("当前 easytrader 客户端不支持 connect(exe_path) 方式。")
            connect(self.config.exe_path)
        self._client = user
        return user

    @property
    def client(self):
        if self._client is None:
            return self.connect()
        return self._client

    def _read_raw(self, attr_name: str) -> tuple[Any, str | None]:
        try:
            value = getattr(self.client, attr_name, None)
            if callable(value):
                value = value()
            return value, None
        except Exception as exc:  # noqa: BLE001
            return None, f"{type(exc).__name__}: {exc}"

    def preview_orders(self, orders: list[PlannedOrder]) -> list[dict[str, Any]]:
        preview = []
        for order in orders:
            preview.append(
                {
                    "order_id": order.order_id,
                    "action": order.action,
                    "code": order.code,
                    "name": order.name,
                    "price": order.estimated_price,
                    "shares": order.requested_shares or order.estimated_shares,
                    "reason": order.reason,
                    "execute_date": order.execute_date,
                }
            )
        return preview

    def get_cash(self) -> float:
        balance, _ = self._read_raw("balance")
        if isinstance(balance, list) and balance:
            row = balance[0]
        elif isinstance(balance, dict):
            row = balance
        else:
            return 0.0
        for key in ["可用金额", "资金余额", "可用", "可用资金"]:
            if key in row:
                return float(row[key])
        return 0.0

    def get_positions(self) -> list[dict[str, Any]]:
        positions, _ = self._read_raw("position")
        if positions is None:
            return []
        rows: list[dict[str, Any]] = []
        for row in positions:
            code = row.get("证券代码") or row.get("code") or row.get("股票代码")
            shares = row.get("股票余额") or row.get("当前持仓") or row.get("股份余额") or 0
            available = row.get("可用余额") or row.get("可用股份") or shares
            rows.append(
                {
                    "code": str(code),
                    "shares": int(float(shares)),
                    "available_shares": int(float(available)),
                    "raw": row,
                }
            )
        return rows

    def get_today_entrusts(self) -> list[dict[str, Any]]:
        entrusts, _ = self._read_raw("today_entrusts")
        if entrusts is None:
            return []
        return list(entrusts)

    def get_today_trades(self) -> list[dict[str, Any]]:
        trades, _ = self._read_raw("today_trades")
        if trades is None:
            return []
        return list(trades)

    def build_account_snapshot(self) -> dict[str, Any]:
        raw_balance, balance_error = self._read_raw("balance")
        raw_positions, positions_error = self._read_raw("position")
        raw_entrusts, entrusts_error = self._read_raw("today_entrusts")
        raw_trades, trades_error = self._read_raw("today_trades")

        normalized_positions = []
        if raw_positions is not None:
            for row in list(raw_positions):
                code = row.get("证券代码") or row.get("code") or row.get("股票代码")
                shares = row.get("股票余额") or row.get("当前持仓") or row.get("股份余额") or 0
                available = row.get("可用余额") or row.get("可用股份") or shares
                normalized_positions.append(
                    {
                        "code": str(code),
                        "shares": int(float(shares)),
                        "available_shares": int(float(available)),
                        "raw": row,
                    }
                )

        normalized_cash = 0.0
        if isinstance(raw_balance, list) and raw_balance:
            balance_row = raw_balance[0]
        elif isinstance(raw_balance, dict):
            balance_row = raw_balance
        else:
            balance_row = {}
        for key in ["可用金额", "资金余额", "可用", "可用资金"]:
            if key in balance_row:
                normalized_cash = float(balance_row[key])
                break

        return {
            "timestamp": as_dt_str(pd.Timestamp.now()),
            "cash": normalized_cash,
            "positions": normalized_positions,
            "today_entrusts": list(raw_entrusts or []),
            "today_trades": list(raw_trades or []),
            "raw_balance": raw_balance,
            "raw_positions": raw_positions,
            "raw_today_entrusts": raw_entrusts,
            "raw_today_trades": raw_trades,
            "errors": {
                "balance": balance_error,
                "position": positions_error,
                "today_entrusts": entrusts_error,
                "today_trades": trades_error,
            },
        }

    def submit_orders(
        self,
        orders: list[PlannedOrder],
        auto_confirm: bool | None = None,
    ) -> list[dict[str, Any]]:
        effective_auto_confirm = self.config.auto_confirm if auto_confirm is None else auto_confirm
        preview = self.preview_orders(orders)
        if not preview:
            return []

        if not effective_auto_confirm:
            print("以下订单将提交到同花顺客户端：")
            for item in preview:
                print(
                    f"  {item['action']} {item['code']} {item['shares']}股 "
                    f"@{item['price']:.3f} | {item['reason']}"
                )
            confirmed = input("确认继续提交? [y/N]: ").strip().lower()
            if confirmed not in {"y", "yes"}:
                return [
                    {
                        "order_id": item["order_id"],
                        "status": "cancelled",
                        "message": "用户取消提交",
                    }
                    for item in preview
                ]

        results: list[dict[str, Any]] = []
        for order in orders:
            price = float(order.estimated_price)
            amount = int(order.requested_shares or order.estimated_shares)
            if amount <= 0:
                results.append(
                    {
                        "order_id": order.order_id,
                        "status": "skipped",
                        "message": "订单股数为 0",
                    }
                )
                continue
            if order.action == "BUY":
                response = self.client.buy(order.code, price=price, amount=amount)
            elif order.action == "SELL":
                response = self.client.sell(order.code, price=price, amount=amount)
            else:
                raise RuntimeError(f"未知订单动作: {order.action}")
            results.append(
                {
                    "order_id": order.order_id,
                    "status": "submitted",
                    "action": order.action,
                    "code": order.code,
                    "price": price,
                    "shares": amount,
                    "response": response,
                }
            )
        return results
