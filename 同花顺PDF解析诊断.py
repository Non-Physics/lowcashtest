from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "股票策略交易执行" / "reports"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="同花顺 PDF 解析诊断")
    parser.add_argument("--pdf-path", required=True, help="待解析 PDF 路径")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="输出目录")
    parser.add_argument("--mode", default="position", choices=["position", "today_trades", "today_entrusts"], help="PDF 类型")
    return parser.parse_args()


def extract_pdf_text(pdf_path: Path) -> tuple[str, str]:
    try:
        from pypdf import PdfReader
    except ImportError as exc:
        raise RuntimeError("未安装 pypdf，请先在对应环境中安装。") from exc

    reader = PdfReader(str(pdf_path))
    pages: list[str] = []
    for page in reader.pages:
        pages.append(page.extract_text() or "")
    return "\n".join(pages), "pypdf"


def parse_position_candidates(text: str) -> list[dict[str, Any]]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    parsed: list[dict[str, Any]] = []
    code_pattern = re.compile(r"\b(\d{6})\b")
    for line in lines:
        match = code_pattern.search(line)
        if not match:
            continue
        code = match.group(1)
        suffix = ".SH" if code.startswith("6") else ".SZ"
        numbers = [float(x) for x in re.findall(r"-?\d+(?:\.\d+)?", line)]
        ints = [int(x) for x in re.findall(r"\b\d+\b", line)]
        shares = next((x for x in ints if 1 <= x <= 1_000_000 and x % 100 == 0), 0)
        decimals = [x for x in numbers if abs(x - round(x)) > 1e-6]
        price_guess = decimals[0] if decimals else None
        market_value_guess = None
        if shares > 0:
            amount_candidates = [x for x in decimals if x >= shares]
            if amount_candidates:
                market_value_guess = max(amount_candidates)
        parsed.append(
            {
                "code": f"{code}{suffix}",
                "shares": int(shares),
                "price_guess": price_guess,
                "market_value_guess": market_value_guess,
                "raw_line": line,
            }
        )
    return parsed


def parse_trade_candidates(text: str) -> list[dict[str, Any]]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    parsed: list[dict[str, Any]] = []
    header_seen = any("证券代码" in line and ("成交" in line or "操作" in line) for line in lines)
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
                "trade_date": trade_date,
                "trade_time": trade_time,
                "code": f"{code}{'.SH' if code.startswith('6') else '.SZ'}",
                "name": name,
                "action": action,
                "qty_guess": int(float(qty_text)),
                "price_guess": float(price_text),
                "amount_guess": float(amount_text),
                "raw_line": line,
            }
        )
    if header_seen:
        return parsed
    return parsed


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    pdf_path = Path(args.pdf_path)
    if not pdf_path.exists():
        raise FileNotFoundError(f"找不到 PDF: {pdf_path}")

    text, engine = extract_pdf_text(pdf_path)
    if args.mode == "position":
        parsed = parse_position_candidates(text)
        payload_key = "parsed_positions"
    else:
        parsed = parse_trade_candidates(text)
        payload_key = "parsed_rows"
    output_dir = Path(args.output_dir)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / f"ths_pdf_parse_{stamp}.json"
    payload = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "pdf_path": str(pdf_path),
        "mode": args.mode,
        "engine": engine,
        "text_length": len(text),
        "preview_lines": [line for line in text.splitlines() if line.strip()][:80],
        payload_key: parsed,
    }
    write_json(out_path, payload)

    print(f"PDF: {pdf_path}")
    print(f"engine: {engine}")
    print(f"text_length: {len(text)}")
    print(f"parsed_rows: {len(parsed)}")
    print(f"report: {out_path}")
    if payload["preview_lines"]:
        print("preview:")
        for line in payload["preview_lines"][:10]:
            print(f"  {line}")


if __name__ == "__main__":
    main()
