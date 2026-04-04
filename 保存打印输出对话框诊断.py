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
    parser = argparse.ArgumentParser(description="保存打印输出对话框诊断")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="输出目录")
    parser.add_argument("--title-re", default=r".*(保存打印输出|另存打印输出|打印输出另存为|将打印输出另存为|Save Print Output As|Save As).*", help="保存对话框标题正则")
    parser.add_argument("--timeout", type=float, default=20.0, help="等待秒数")
    return parser.parse_args()


def ensure_pywinauto():
    try:
        from pywinauto import Desktop
    except ImportError as exc:
        raise RuntimeError("未安装 pywinauto，请在 Windows 交易环境中安装后再运行。") from exc
    return Desktop


def safe_text(wrapper: Any) -> str:
    try:
        return str(wrapper.window_text())
    except Exception:  # noqa: BLE001
        return ""


def safe_class(wrapper: Any) -> str:
    try:
        info = getattr(wrapper, "element_info", None)
        return str(getattr(info, "class_name", "") or "")
    except Exception:  # noqa: BLE001
        return ""


def safe_rect(wrapper: Any) -> str:
    try:
        rect = wrapper.rectangle()
        return f"{rect.left},{rect.top},{rect.right},{rect.bottom}"
    except Exception:  # noqa: BLE001
        return ""


def dump_controls(window: Any, max_controls: int = 300) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        descendants = window.descendants()
    except Exception as exc:  # noqa: BLE001
        return [{"error": f"{type(exc).__name__}: {exc}"}]
    for idx, ctrl in enumerate(descendants[:max_controls], start=1):
        rows.append(
            {
                "index": idx,
                "text": safe_text(ctrl),
                "class_name": safe_class(ctrl),
                "rectangle": safe_rect(ctrl),
            }
        )
    return rows


def main() -> None:
    args = parse_args()
    Desktop = ensure_pywinauto()
    deadline = time.time() + args.timeout
    target = None
    backend_used = ""
    seen: list[str] = []

    while time.time() < deadline and target is None:
        seen = []
        for backend in ("uia", "win32"):
            try:
                windows = Desktop(backend=backend).windows()
            except Exception:  # noqa: BLE001
                continue
            for win in windows:
                title = safe_text(win)
                class_name = safe_class(win)
                seen.append(f"{backend}:{class_name}:{title or '[no-title]'}")
                if re.search(args.title_re, title, re.IGNORECASE):
                    target = win
                    backend_used = backend
                    break
                if class_name in {"#32770", "NUIDialog"}:
                    try:
                        descendants = win.descendants()
                    except Exception:  # noqa: BLE001
                        descendants = []
                    child_classes = {safe_class(child) for child in descendants}
                    child_texts = {safe_text(child).strip() for child in descendants if safe_text(child).strip()}
                    if "Edit" in child_classes and any(text in {"保存", "Save", "&Save"} for text in child_texts):
                        target = win
                        backend_used = backend
                        break
            if target is not None:
                break
        if target is None:
            time.sleep(0.3)

    payload = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "backend": backend_used,
        "found": target is not None,
        "title": safe_text(target) if target is not None else "",
        "class_name": safe_class(target) if target is not None else "",
        "rectangle": safe_rect(target) if target is not None else "",
        "visible_windows": seen[:50],
        "controls": dump_controls(target) if target is not None else [],
    }

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"ths_save_dialog_diag_{stamp}.json"
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"找到对话框: {payload['found']}")
    print(f"backend: {payload['backend'] or 'N/A'}")
    print(f"标题: {payload['title'] or '[no-title]'}")
    print(f"类名: {payload['class_name'] or 'N/A'}")
    print(f"输出文件: {output_path}")


if __name__ == "__main__":
    main()
