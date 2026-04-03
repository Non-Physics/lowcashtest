from __future__ import annotations

from dataclasses import dataclass
import re
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

    def _normalize_code(self, code: Any) -> str:
        text = str(code or "").strip().upper()
        if not text:
            return ""
        if "." in text:
            return text
        digits = re.sub(r"\D", "", text)
        if len(digits) != 6:
            return text
        if digits.startswith(("600", "601", "603", "605", "688", "689", "510", "511", "512", "513", "515", "518", "588")):
            suffix = ".SH"
        elif digits.startswith(("430", "831", "832", "833", "834", "835", "836", "837", "838", "839", "870", "871", "872", "873", "874", "875", "876", "877", "878", "879")):
            suffix = ".BJ"
        else:
            suffix = ".SZ"
        return f"{digits}{suffix}"

    def _normalize_position_row(self, row: dict[str, Any]) -> dict[str, Any]:
        code = self._normalize_code(row.get("证券代码") or row.get("code") or row.get("股票代码"))
        shares = row.get("股票余额") or row.get("当前持仓") or row.get("股份余额") or 0
        available = row.get("可用余额") or row.get("可用股份") or shares
        return {
            "code": code,
            "shares": int(float(shares)),
            "available_shares": int(float(available)),
            "raw": row,
        }

    def _normalize_trade_like_row(self, row: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(row)
        code_key = None
        for key in ("证券代码", "股票代码", "code"):
            if key in normalized:
                code_key = key
                break
        if code_key is not None:
            normalized[code_key] = self._normalize_code(normalized.get(code_key))
        normalized["normalized_code"] = self._normalize_code(
            row.get("证券代码") or row.get("股票代码") or row.get("code")
        )
        return normalized

    def _parse_position_lines_from_ocr(self, lines: list[str]) -> list[dict[str, Any]]:
        parsed: list[dict[str, Any]] = []
        code_pattern = re.compile(r"\b(\d{6})\b")
        float_pattern = re.compile(r"-?\d+(?:\.\d+)?")
        int_pattern = re.compile(r"\b\d+\b")
        for line in lines:
            code_match = code_pattern.search(line)
            if not code_match:
                continue
            code = code_match.group(1)
            suffix = ".SH" if code.startswith("6") else ".SZ"
            tail = line[code_match.end() :].strip()
            numbers = [float(x) for x in float_pattern.findall(tail)]
            int_numbers = [int(x) for x in int_pattern.findall(tail)]
            shares = None
            available_shares = None
            price = None
            market_value = None
            if int_numbers:
                share_candidates = [
                    x
                    for x in int_numbers
                    if 1 <= x <= 1_000_000 and x % 100 == 0
                ]
                if share_candidates:
                    shares = share_candidates[0]
                    available_shares = share_candidates[1] if len(share_candidates) >= 2 else shares
                else:
                    fallback_ints = [x for x in int_numbers if 1 <= x <= 1_000_000]
                    if fallback_ints:
                        shares = fallback_ints[0]
                        available_shares = shares

            float_like = [x for x in numbers if abs(x - round(x)) > 1e-6]
            if float_like:
                # first decimal is usually a price-like field
                price = float_like[0]
                # the largest positive decimal is usually market value / amount-like
                positives = [x for x in float_like if x > 0]
                if positives:
                    market_value = max(positives)

            parsed.append(
                {
                    "code": f"{code}{suffix}",
                    "shares": int(shares or 0),
                    "available_shares": int(available_shares or shares or 0),
                    "last_price_guess": price,
                    "market_value_guess": market_value,
                    "source": "ocr_fallback",
                    "raw_line": line,
                }
            )
        return parsed

    def _ocr_grid_fallback(self) -> dict[str, Any]:
        try:
            import pytesseract
            from PIL import ImageOps
        except ImportError as exc:  # noqa: BLE001
            return {"error": f"{type(exc).__name__}: {exc}", "ocr_lines": [], "positions": []}

        result: dict[str, Any] = {
            "error": None,
            "ocr_lines": [],
            "positions": [],
            "selected_rectangle": "",
        }
        try:
            window = self.client._main
            candidates = []
            for ctrl in window.descendants():
                info = getattr(ctrl, "element_info", None)
                cls = getattr(info, "class_name", "") or ""
                cid = getattr(info, "control_id", None)
                if cid != 1047 or cls != "CVirtualGridCtrl":
                    continue
                rect = ctrl.rectangle()
                area = max(rect.right - rect.left, 0) * max(rect.bottom - rect.top, 0)
                candidates.append((ctrl, area, rect.top))
            candidates.sort(key=lambda item: (-item[1], item[2]))
            if not candidates:
                result["error"] = "未找到 1047/CVirtualGridCtrl 候选表格"
                return result
            wrapper = candidates[0][0]
            rect = wrapper.rectangle()
            result["selected_rectangle"] = f"{rect.left},{rect.top},{rect.right},{rect.bottom}"
            image = wrapper.capture_as_image()
            gray = ImageOps.grayscale(image)
            enlarged = gray.resize((gray.width * 2, gray.height * 2))
            text = pytesseract.image_to_string(enlarged, lang="eng", config="--psm 6")
            lines = [line for line in text.splitlines() if line.strip()]
            result["ocr_lines"] = lines[:80]
            result["positions"] = self._parse_position_lines_from_ocr(lines)
            return result
        except Exception as exc:  # noqa: BLE001
            result["error"] = f"{type(exc).__name__}: {exc}"
            return result

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
            rows.append(self._normalize_position_row(row))
        return rows

    def get_today_entrusts(self) -> list[dict[str, Any]]:
        entrusts, _ = self._read_raw("today_entrusts")
        if entrusts is None:
            return []
        return [self._normalize_trade_like_row(row) for row in list(entrusts)]

    def get_today_trades(self) -> list[dict[str, Any]]:
        trades, _ = self._read_raw("today_trades")
        if trades is None:
            return []
        return [self._normalize_trade_like_row(row) for row in list(trades)]

    def build_account_snapshot(self) -> dict[str, Any]:
        raw_balance, balance_error = self._read_raw("balance")
        raw_positions, positions_error = self._read_raw("position")
        raw_entrusts, entrusts_error = self._read_raw("today_entrusts")
        raw_trades, trades_error = self._read_raw("today_trades")

        normalized_positions = []
        if raw_positions is not None:
            for row in list(raw_positions):
                normalized_positions.append(self._normalize_position_row(row))

        normalized_entrusts = []
        if raw_entrusts is not None:
            normalized_entrusts = [self._normalize_trade_like_row(row) for row in list(raw_entrusts)]

        normalized_trades = []
        if raw_trades is not None:
            normalized_trades = [self._normalize_trade_like_row(row) for row in list(raw_trades)]

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

        ocr_fallback = None
        has_stock_value = False
        if isinstance(balance_row, dict):
            for key in ["股票市值", "A股市值", "证券市值"]:
                if key in balance_row and float(balance_row[key]) > 0:
                    has_stock_value = True
                    break
        if (raw_positions == [] or raw_positions is None) and has_stock_value:
            ocr_fallback = self._ocr_grid_fallback()
            if ocr_fallback and ocr_fallback.get("positions"):
                normalized_positions = ocr_fallback["positions"]

        return {
            "timestamp": as_dt_str(pd.Timestamp.now()),
            "cash": normalized_cash,
            "positions": normalized_positions,
            "today_entrusts": normalized_entrusts,
            "today_trades": normalized_trades,
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
            "ocr_fallback": ocr_fallback,
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
