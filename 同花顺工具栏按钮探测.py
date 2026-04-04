from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "股票策略交易执行" / "reports"
DEFAULT_EXE_PATH = r"D:\量化\同花顺\同花顺\xiadan.exe"
DEFAULT_TITLE_RE = r".*(网上股票交易系统|股票交易系统|同花顺).*"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="同花顺工具栏按钮探测")
    parser.add_argument("--exe-path", default=DEFAULT_EXE_PATH, help="同花顺下单端 exe 路径")
    parser.add_argument("--title-re", default=DEFAULT_TITLE_RE, help="主窗口标题正则")
    parser.add_argument("--backend", default="uia", choices=["win32", "uia"], help="pywinauto backend")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="输出目录")
    parser.add_argument("--control-id", type=int, default=59392, help="工具栏控件 ID")
    parser.add_argument("--menu-path", default="查询[F4],资金股票", help="探测前切换菜单路径")
    parser.add_argument("--button-index", type=int, required=True, help="要点击的工具栏按钮索引")
    parser.add_argument("--print-dialog-title-re", default=r".*(打印|Print).*", help="打印对话框标题正则")
    parser.add_argument("--timeout", type=float, default=8.0, help="点击后等待打印对话框秒数")
    return parser.parse_args()


def ensure_pywinauto():
    try:
        from pywinauto import Application, Desktop
        from pywinauto.findwindows import ElementNotFoundError
    except ImportError as exc:
        raise RuntimeError("未安装 pywinauto，请在 Windows 交易环境中安装后再运行。") from exc
    return Application, Desktop, ElementNotFoundError


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
    Application, _, ElementNotFoundError = ensure_pywinauto()
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


def get_toolbar_button(window: Any, control_id: int, button_index: int):
    toolbar = window.child_window(control_id=control_id, class_name="ToolbarWindow32")
    toolbar.wait("exists ready", timeout=5)
    wrapper = toolbar.wrapper_object()
    button = wrapper.button(button_index)
    return toolbar, button


def wait_print_dialog(backend: str, title_re: str, timeout: float) -> tuple[bool, str, list[str]]:
    _, Desktop, _ = ensure_pywinauto()
    deadline = time.time() + timeout
    last_titles: list[str] = []
    while time.time() < deadline:
        windows = Desktop(backend=backend).windows()
        titles: list[str] = []
        for win in windows:
            title = safe_text(win)
            if title:
                titles.append(title)
            if re.search(title_re, title, re.IGNORECASE):
                return True, title, titles
        last_titles = titles
        time.sleep(0.3)
    return False, "", last_titles


def main() -> None:
    args = parse_args()
    window = connect_window(args)
    menu_error = switch_menu(window, args.menu_path)
    toolbar, button = get_toolbar_button(window, args.control_id, args.button_index)

    payload = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "backend": args.backend,
        "exe_path": args.exe_path,
        "window_text": safe_text(window),
        "menu_path": args.menu_path,
        "menu_error": menu_error,
        "control_id": args.control_id,
        "button_index": args.button_index,
        "toolbar_rect": safe_rect(toolbar),
        "button_text": safe_text(button),
        "button_rect": safe_rect(button),
        "clicked": False,
        "print_dialog_detected": False,
        "print_dialog_title": "",
        "visible_titles": [],
        "error": "",
    }

    try:
        window.set_focus()
        time.sleep(0.3)
        button.click_input()
        payload["clicked"] = True
        detected, title, titles = wait_print_dialog(args.backend, args.print_dialog_title_re, args.timeout)
        payload["print_dialog_detected"] = detected
        payload["print_dialog_title"] = title
        payload["visible_titles"] = titles[:30]
    except Exception as exc:  # noqa: BLE001
        payload["error"] = f"{type(exc).__name__}: {exc}"

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"ths_toolbar_probe_{args.backend}_idx{args.button_index}_{stamp}.json"
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"窗口标题: {payload['window_text']}")
    print(f"菜单切换错误: {menu_error or 'None'}")
    print(f"按钮索引: {args.button_index}")
    print(f"按钮文本: {payload['button_text']}")
    print(f"按钮区域: {payload['button_rect']}")
    print(f"已点击: {payload['clicked']}")
    print(f"检测到打印框: {payload['print_dialog_detected']}")
    if payload["print_dialog_title"]:
        print(f"打印框标题: {payload['print_dialog_title']}")
    if payload["error"]:
        print(f"错误: {payload['error']}")
    print(f"输出文件: {output_path}")


if __name__ == "__main__":
    main()
