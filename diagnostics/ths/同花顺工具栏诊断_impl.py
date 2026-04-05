from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "股票策略交易执行" / "reports"
DEFAULT_EXE_PATH = r"D:\量化\同花顺\同花顺\xiadan.exe"
DEFAULT_TITLE_RE = r".*(网上股票交易系统|股票交易系统|同花顺).*"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="同花顺 ToolbarWindow32 诊断")
    parser.add_argument("--exe-path", default=DEFAULT_EXE_PATH, help="同花顺下单端 exe 路径")
    parser.add_argument("--title-re", default=DEFAULT_TITLE_RE, help="主窗口标题正则")
    parser.add_argument("--backend", default="win32", choices=["win32", "uia"], help="pywinauto backend")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="输出目录")
    parser.add_argument("--control-id", type=int, default=59392, help="工具栏控件 ID")
    parser.add_argument("--menu-path", default="查询[F4],资金股票", help="诊断前切换菜单路径")
    return parser.parse_args()


def ensure_pywinauto():
    try:
        from pywinauto import Application
        from pywinauto.findwindows import ElementNotFoundError
    except ImportError as exc:
        raise RuntimeError("未安装 pywinauto，请在 Windows 交易环境中安装后再运行。") from exc
    return Application, ElementNotFoundError


def safe_text(wrapper: Any) -> str:
    try:
        return str(wrapper.window_text())
    except Exception:  # noqa: BLE001
        return ""


def safe_rect(wrapper: Any) -> str:
    try:
        rect = wrapper.rectangle()
        return f"{rect.left},{rect.top},{rect.right},{rect.bottom}"
    except Exception:  # noqa: BLE001
        return ""


def connect_window(args: argparse.Namespace):
    Application, ElementNotFoundError = ensure_pywinauto()
    app = Application(backend=args.backend)
    try:
        app.connect(path=args.exe_path)
    except Exception:
        app.connect(title_re=args.title_re)
    try:
        window = app.window(title_re=args.title_re)
        window.wait("ready", timeout=8)
    except ElementNotFoundError:
        windows = app.windows()
        if not windows:
            raise RuntimeError("未找到同花顺主窗口。")
        window = windows[0]
    return window


def switch_menu(window: Any, menu_path: str) -> str | None:
    targets = [item.strip() for item in menu_path.split(",") if item.strip()]
    if not targets:
        return None
    try:
        tree = window.child_window(control_id=129, class_name="SysTreeView32")
        tree.wait("ready", timeout=5)
        tree.wrapper_object().get_item(targets).select()
        time.sleep(1.0)
        return None
    except Exception as exc:  # noqa: BLE001
        return f"{type(exc).__name__}: {exc}"


def dump_toolbar(toolbar: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "toolbar_text": safe_text(toolbar),
        "toolbar_rect": safe_rect(toolbar),
        "buttons": [],
        "errors": [],
    }
    wrapper = toolbar.wrapper_object()
    for attr_name in ["button_count", "buttons", "texts"]:
        if hasattr(wrapper, attr_name):
            payload.setdefault("available_methods", []).append(attr_name)

    try:
        count = wrapper.button_count()
        payload["button_count"] = count
    except Exception as exc:  # noqa: BLE001
        payload["errors"].append(f"button_count: {type(exc).__name__}: {exc}")
        count = 0

    if count:
        for idx in range(count):
            row: dict[str, Any] = {"index": idx}
            try:
                button = wrapper.button(idx)
                row["text"] = safe_text(button)
                row["rect"] = safe_rect(button)
                row["class_name"] = str(getattr(getattr(button, "element_info", None), "class_name", "") or "")
            except Exception as exc:  # noqa: BLE001
                row["error"] = f"{type(exc).__name__}: {exc}"
            payload["buttons"].append(row)

    if not payload["buttons"]:
        try:
            for idx, child in enumerate(wrapper.children(), start=1):
                payload["buttons"].append(
                    {
                        "index": idx,
                        "text": safe_text(child),
                        "rect": safe_rect(child),
                        "class_name": str(getattr(getattr(child, "element_info", None), "class_name", "") or ""),
                    }
                )
        except Exception as exc:  # noqa: BLE001
            payload["errors"].append(f"children: {type(exc).__name__}: {exc}")

    return payload


def main() -> None:
    args = parse_args()
    window = connect_window(args)
    menu_error = switch_menu(window, args.menu_path)
    toolbar = window.child_window(control_id=args.control_id, class_name="ToolbarWindow32")
    toolbar.wait("exists ready", timeout=5)
    payload = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "backend": args.backend,
        "exe_path": args.exe_path,
        "window_text": safe_text(window),
        "menu_path": args.menu_path,
        "menu_error": menu_error,
        "control_id": args.control_id,
        "toolbar": dump_toolbar(toolbar),
    }

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"ths_toolbar_diag_{args.backend}_{stamp}.json"
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"窗口标题: {payload['window_text']}")
    print(f"菜单切换错误: {menu_error or 'None'}")
    print(f"工具栏区域: {payload['toolbar'].get('toolbar_rect','')}")
    print(f"按钮数: {payload['toolbar'].get('button_count', 0)}")
    print(f"输出文件: {output_path}")
    if payload["toolbar"]["buttons"]:
        print("前十个按钮:")
        for item in payload["toolbar"]["buttons"][:10]:
            print(f"  - idx={item.get('index')} text={item.get('text','')} rect={item.get('rect','')} error={item.get('error','')}")
    if payload["toolbar"]["errors"]:
        print("错误:")
        for err in payload["toolbar"]["errors"]:
            print(f"  - {err}")


if __name__ == "__main__":
    main()
