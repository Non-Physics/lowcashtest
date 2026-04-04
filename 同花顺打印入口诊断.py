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

PAGE_CONFIG = {
    "position": "查询[F4],资金股票",
    "today_trades": "查询[F4],当日成交",
    "today_entrusts": "查询[F4],当日委托",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="同花顺打印入口专项诊断")
    parser.add_argument("--exe-path", default=DEFAULT_EXE_PATH, help="同花顺下单端 exe 路径")
    parser.add_argument("--title-re", default=r".*(网上股票交易系统|股票交易系统|同花顺).*", help="主窗口标题正则")
    parser.add_argument("--backend", default="win32", choices=["win32", "uia"], help="pywinauto backend")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="输出目录")
    parser.add_argument("--page", default="position", choices=sorted(PAGE_CONFIG), help="目标页面")
    parser.add_argument("--menu-path", default="", help="自定义菜单路径，留空则按 page 自动映射")
    parser.add_argument("--max-controls", type=int, default=1500, help="最多扫描控件数")
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


def safe_class(wrapper: Any) -> str:
    try:
        info = getattr(wrapper, "element_info", None)
        return str(getattr(info, "class_name", "") or "")
    except Exception:  # noqa: BLE001
        return ""


def safe_control_id(wrapper: Any):
    try:
        info = getattr(wrapper, "element_info", None)
        return getattr(info, "control_id", None)
    except Exception:  # noqa: BLE001
        return None


def safe_rect(wrapper: Any) -> str:
    try:
        rect = wrapper.rectangle()
        return f"{rect.left},{rect.top},{rect.right},{rect.bottom}"
    except Exception:  # noqa: BLE001
        return ""


def safe_friendly(wrapper: Any) -> str:
    try:
        return str(wrapper.friendly_class_name())
    except Exception:  # noqa: BLE001
        return ""


def try_connect(args: argparse.Namespace):
    Application, ElementNotFoundError = ensure_pywinauto()
    app = Application(backend=args.backend)
    try:
        if args.exe_path.strip():
            app.connect(path=args.exe_path)
        else:
            app.connect(title_re=args.title_re)
    except Exception:
        app = Application(backend=args.backend).connect(title_re=args.title_re)
    try:
        window = app.window(title_re=args.title_re)
        window.wait("ready", timeout=8)
    except ElementNotFoundError:
        windows = app.windows()
        if not windows:
            raise RuntimeError("未找到同花顺主窗口。")
        window = windows[0]
    return app, window


def try_switch_menu(window: Any, menu_path: str) -> str | None:
    if not menu_path.strip():
        return None
    targets = [item.strip() for item in menu_path.split(",") if item.strip()]
    try:
        tree = window.child_window(control_id=129, class_name="SysTreeView32")
        tree.wait("ready", timeout=5)
        tree.wrapper_object().get_item(targets).select()
        time.sleep(1.0)
        return None
    except Exception as exc:  # noqa: BLE001
        return f"{type(exc).__name__}: {exc}"


def collect_candidates(window: Any, max_controls: int) -> list[dict[str, Any]]:
    patterns = [
        re.compile(r"(打印|Print)", re.IGNORECASE),
        re.compile(r"(导出|输出)", re.IGNORECASE),
        re.compile(r"(^文件$|^File$)", re.IGNORECASE),
        re.compile(r"(工具栏|菜单)", re.IGNORECASE),
    ]
    rows: list[dict[str, Any]] = []
    try:
        descendants = window.descendants()
    except Exception as exc:  # noqa: BLE001
        return [{"error": f"{type(exc).__name__}: {exc}"}]

    for idx, ctrl in enumerate(descendants[:max_controls], start=1):
        text = safe_text(ctrl).strip()
        class_name = safe_class(ctrl)
        friendly = safe_friendly(ctrl)
        if text and any(pattern.search(text) for pattern in patterns):
            rows.append(
                {
                    "index": idx,
                    "text": text,
                    "class_name": class_name,
                    "friendly_class_name": friendly,
                    "control_id": safe_control_id(ctrl),
                    "rectangle": safe_rect(ctrl),
                }
            )
            continue
        if class_name in {"ToolbarWindow32", "SysMenuBar32"} or friendly.lower() in {"toolbar", "menubar"}:
            rows.append(
                {
                    "index": idx,
                    "text": text,
                    "class_name": class_name,
                    "friendly_class_name": friendly,
                    "control_id": safe_control_id(ctrl),
                    "rectangle": safe_rect(ctrl),
                }
            )
    return rows


def main() -> None:
    args = parse_args()
    menu_path = args.menu_path or PAGE_CONFIG[args.page]
    _, window = try_connect(args)
    menu_error = try_switch_menu(window, menu_path)
    candidates = collect_candidates(window, args.max_controls)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"ths_print_entry_diag_{args.page}_{args.backend}_{stamp}.json"
    payload = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "backend": args.backend,
        "exe_path": args.exe_path,
        "title_re": args.title_re,
        "page": args.page,
        "menu_path": menu_path,
        "menu_error": menu_error,
        "window_text": safe_text(window),
        "window_class": safe_class(window),
        "candidate_count": len(candidates),
        "candidates": candidates,
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"窗口标题: {payload['window_text']}")
    print(f"窗口类名: {payload['window_class']}")
    print(f"菜单切换错误: {menu_error or 'None'}")
    print(f"候选控件数: {len(candidates)}")
    print(f"输出文件: {output_path}")
    if candidates:
        print("前十个候选控件:")
        for item in candidates[:10]:
            print(f"  - {item.get('class_name','')} | {item.get('friendly_class_name','')} | {item.get('text','')}")
    else:
        print("未发现明显打印入口候选控件。")


if __name__ == "__main__":
    main()
