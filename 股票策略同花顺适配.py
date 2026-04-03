from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
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
    grid_strategy_order: tuple[str, ...] = ("wmcopy", "copy")
    pdf_root_dir: str = ""


class TongHuaShunGuiAdapter:
    def __init__(self, config: TongHuaShunClientConfig | None = None) -> None:
        self.config = config or TongHuaShunClientConfig()
        self._client = None
        self._last_read_diagnostics: dict[str, Any] = {}

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

    def _pdf_root_dir(self) -> Path:
        if self.config.pdf_root_dir:
            path = Path(self.config.pdf_root_dir)
        else:
            path = Path(__file__).resolve().parent / "outputs" / "股票策略交易执行" / "state" / "pdf_exports"
        path.mkdir(parents=True, exist_ok=True)
        for name in ("position", "today_trades", "today_entrusts"):
            (path / name).mkdir(parents=True, exist_ok=True)
        return path

    def _build_grid_strategy_instance(self, strategy_name: str):
        easytrader = self._ensure_easytrader()
        grid_strategies = getattr(easytrader, "grid_strategies", None)
        if grid_strategies is None:
            import easytrader.grid_strategies as grid_strategies  # type: ignore

        normalized = strategy_name.strip().lower()
        if normalized == "wmcopy":
            strategy_cls = getattr(grid_strategies, "WMCopy", None)
            if strategy_cls is None:
                raise RuntimeError("easytrader.grid_strategies.WMCopy 不存在")
            return strategy_cls()
        if normalized == "copy":
            strategy_cls = getattr(grid_strategies, "Copy", None)
            if strategy_cls is None:
                raise RuntimeError("easytrader.grid_strategies.Copy 不存在")
            return strategy_cls()
        raise RuntimeError(f"未知 grid strategy: {strategy_name}")

    def _apply_grid_strategy(self, strategy_name: str) -> None:
        client = self.client
        client.grid_strategy = self._build_grid_strategy_instance(strategy_name)
        if hasattr(client, "_grid_strategy_instance"):
            client._grid_strategy_instance = None

    def _row_count(self, value: Any) -> int:
        if isinstance(value, list):
            return len(value)
        if isinstance(value, dict):
            return 1
        return 0

    def _is_grid_payload_usable(self, attr_name: str, value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, list):
            if not value:
                return False
            if attr_name == "position":
                return any(
                    isinstance(row, dict)
                    and (row.get("证券代码") or row.get("股票代码") or row.get("code"))
                    and (row.get("股票余额") or row.get("当前持仓") or row.get("股份余额") or 0)
                    for row in value
                )
            if attr_name in {"today_trades", "today_entrusts"}:
                return any(
                    isinstance(row, dict)
                    and (row.get("证券代码") or row.get("股票代码") or row.get("code"))
                    for row in value
                )
            return True
        if isinstance(value, dict):
            return bool(value)
        return False

    def _read_grid_raw(self, attr_name: str) -> tuple[Any, str | None, list[dict[str, Any]], str | None]:
        attempts: list[dict[str, Any]] = []
        selected_strategy = None
        for strategy_name in self.config.grid_strategy_order:
            attempt: dict[str, Any] = {
                "strategy": strategy_name,
                "error": None,
                "row_count": 0,
                "usable": False,
            }
            try:
                self._apply_grid_strategy(strategy_name)
                value = getattr(self.client, attr_name, None)
                if callable(value):
                    value = value()
                attempt["row_count"] = self._row_count(value)
                attempt["usable"] = self._is_grid_payload_usable(attr_name, value)
                attempts.append(attempt)
                if attempt["usable"]:
                    selected_strategy = strategy_name
                    return value, None, attempts, selected_strategy
            except Exception as exc:  # noqa: BLE001
                attempt["error"] = f"{type(exc).__name__}: {exc}"
                attempts.append(attempt)
        error = " | ".join(
            f"{item['strategy']}={item['error'] or 'empty'}"
            for item in attempts
        ) if attempts else "未执行 grid strategy"
        return [], error, attempts, selected_strategy

    def _extract_pdf_text(self, pdf_path: Path) -> str:
        try:
            from pypdf import PdfReader
        except ImportError as exc:
            raise RuntimeError("未安装 pypdf，请先在对应环境中安装。") from exc
        reader = PdfReader(str(pdf_path))
        return "\n".join((page.extract_text() or "") for page in reader.pages)

    def _latest_pdf_for_attr(self, attr_name: str) -> Path | None:
        pdf_dir = self._pdf_root_dir() / attr_name
        pdf_files = sorted(pdf_dir.glob("*.pdf"), key=lambda p: p.stat().st_mtime, reverse=True)
        return pdf_files[0] if pdf_files else None

    def _parse_positions_from_pdf_text(self, text: str) -> list[dict[str, Any]]:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        parsed: list[dict[str, Any]] = []
        header_seen = any("证券代码" in line and "证券名称" in line and "股票余额" in line for line in lines)
        if not header_seen:
            return []
        for line in lines:
            if not re.match(r"^\d{6}\s+\S+", line):
                continue
            parts = line.split()
            if len(parts) < 10:
                continue
            code = self._normalize_code(parts[0])
            name = parts[1]
            try:
                shares = int(float(parts[2]))
                available = int(float(parts[3]))
            except ValueError:
                continue
            row = {
                "证券代码": parts[0],
                "证券名称": name,
                "股票余额": shares,
                "可用余额": available,
            }
            if len(parts) >= 6:
                row["成本价"] = parts[5] if parts[4].isdigit() else parts[5]
            if len(parts) >= 7:
                row["市价"] = parts[6]
            if len(parts) >= 10:
                row["市值"] = parts[9]
            parsed.append(
                {
                    "code": code,
                    "shares": shares,
                    "available_shares": available,
                    "raw": row,
                    "source": "pdf_fallback",
                    "raw_line": line,
                }
            )
        return parsed

    def _parse_trade_like_from_pdf_text(self, text: str) -> list[dict[str, Any]]:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        parsed: list[dict[str, Any]] = []
        for line in lines:
            if not re.match(r"^\d{8}\s+\d{1,2}:\d{2}:\d{2}\s+\d{6}\s+", line):
                continue
            if not ("买入" in line or "卖出" in line):
                continue
            parts = line.split()
            if len(parts) < 8:
                continue
            trade_date, trade_time, code, name, action, qty_text, price_text, amount_text = parts[:8]
            parsed.append(
                {
                    "证券代码": self._normalize_code(code),
                    "证券名称": name,
                    "成交日期": trade_date,
                    "成交时间": trade_time,
                    "操作": action,
                    "成交数量": int(float(qty_text)),
                    "成交均价": float(price_text),
                    "成交金额": float(amount_text),
                    "raw_line": line,
                    "source": "pdf_fallback",
                }
            )
        return parsed

    def _read_pdf_fallback(self, attr_name: str) -> tuple[Any, str | None, dict[str, Any]]:
        pdf_path = self._latest_pdf_for_attr(attr_name)
        meta = {"pdf_path": str(pdf_path) if pdf_path else "", "row_count": 0}
        if pdf_path is None:
            return [], "未找到 PDF 文件", meta
        try:
            text = self._extract_pdf_text(pdf_path)
            if attr_name == "position":
                parsed = self._parse_positions_from_pdf_text(text)
            else:
                parsed = self._parse_trade_like_from_pdf_text(text)
            meta["row_count"] = self._row_count(parsed)
            return parsed, None if parsed else "PDF 解析结果为空", meta
        except Exception as exc:  # noqa: BLE001
            return [], f"{type(exc).__name__}: {exc}", meta

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
        if {"code", "shares", "available_shares"}.issubset(row.keys()):
            normalized = {
                "code": self._normalize_code(row.get("code")),
                "shares": int(float(row.get("shares", 0) or 0)),
                "available_shares": int(float(row.get("available_shares", row.get("shares", 0)) or 0)),
            }
            if "raw" in row:
                normalized["raw"] = row.get("raw")
            if "source" in row:
                normalized["source"] = row.get("source")
            if "raw_line" in row:
                normalized["raw_line"] = row.get("raw_line")
            return normalized
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
        positions, _, attempts, selected_strategy = self._read_grid_raw("position")
        self._last_read_diagnostics["position"] = {
            "strategy_attempts": attempts,
            "strategy_selected": selected_strategy,
        }
        if positions is None:
            return []
        rows: list[dict[str, Any]] = []
        for row in positions:
            rows.append(self._normalize_position_row(row))
        return rows

    def get_today_entrusts(self) -> list[dict[str, Any]]:
        entrusts, _, attempts, selected_strategy = self._read_grid_raw("today_entrusts")
        self._last_read_diagnostics["today_entrusts"] = {
            "strategy_attempts": attempts,
            "strategy_selected": selected_strategy,
        }
        if entrusts is None:
            return []
        return [self._normalize_trade_like_row(row) for row in list(entrusts)]

    def get_today_trades(self) -> list[dict[str, Any]]:
        trades, _, attempts, selected_strategy = self._read_grid_raw("today_trades")
        self._last_read_diagnostics["today_trades"] = {
            "strategy_attempts": attempts,
            "strategy_selected": selected_strategy,
        }
        if trades is None:
            return []
        return [self._normalize_trade_like_row(row) for row in list(trades)]

    def build_account_snapshot(self) -> dict[str, Any]:
        raw_balance, balance_error = self._read_raw("balance")
        raw_positions, positions_error, position_attempts, position_selected = self._read_grid_raw("position")
        raw_entrusts, entrusts_error, entrust_attempts, entrust_selected = self._read_grid_raw("today_entrusts")
        raw_trades, trades_error, trades_attempts, trades_selected = self._read_grid_raw("today_trades")

        pdf_position_meta = None
        if raw_positions == [] or raw_positions is None:
            pdf_positions, pdf_error, pdf_position_meta = self._read_pdf_fallback("position")
            position_attempts.append(
                {
                    "strategy": "pdf",
                    "error": pdf_error,
                    "row_count": self._row_count(pdf_positions),
                    "usable": bool(pdf_positions),
                    "pdf_path": (pdf_position_meta or {}).get("pdf_path", ""),
                }
            )
            if pdf_positions:
                raw_positions = pdf_positions
                positions_error = None
                position_selected = "pdf"

        pdf_entrust_meta = None
        if raw_entrusts == [] or raw_entrusts is None:
            pdf_entrusts, pdf_error, pdf_entrust_meta = self._read_pdf_fallback("today_entrusts")
            entrust_attempts.append(
                {
                    "strategy": "pdf",
                    "error": pdf_error,
                    "row_count": self._row_count(pdf_entrusts),
                    "usable": bool(pdf_entrusts),
                    "pdf_path": (pdf_entrust_meta or {}).get("pdf_path", ""),
                }
            )
            if pdf_entrusts:
                raw_entrusts = pdf_entrusts
                entrusts_error = None
                entrust_selected = "pdf"

        pdf_trade_meta = None
        if raw_trades == [] or raw_trades is None:
            pdf_trades, pdf_error, pdf_trade_meta = self._read_pdf_fallback("today_trades")
            trades_attempts.append(
                {
                    "strategy": "pdf",
                    "error": pdf_error,
                    "row_count": self._row_count(pdf_trades),
                    "usable": bool(pdf_trades),
                    "pdf_path": (pdf_trade_meta or {}).get("pdf_path", ""),
                }
            )
            if pdf_trades:
                raw_trades = pdf_trades
                trades_error = None
                trades_selected = "pdf"

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
            "read_diagnostics": {
                "position": {
                    "strategy_attempts": position_attempts,
                    "strategy_selected": "ocr_fallback" if ocr_fallback and ocr_fallback.get("positions") else position_selected,
                    "raw_row_count": self._row_count(raw_positions),
                    "parse_success": bool(normalized_positions),
                },
                "today_entrusts": {
                    "strategy_attempts": entrust_attempts,
                    "strategy_selected": entrust_selected,
                    "raw_row_count": self._row_count(raw_entrusts),
                    "parse_success": bool(normalized_entrusts),
                },
                "today_trades": {
                    "strategy_attempts": trades_attempts,
                    "strategy_selected": trades_selected,
                    "raw_row_count": self._row_count(raw_trades),
                    "parse_success": bool(normalized_trades),
                },
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
