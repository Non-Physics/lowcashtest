from __future__ import annotations

import argparse
import json
from pathlib import Path
import time
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "股票策略交易执行" / "reports"
DEFAULT_EXE_PATH = r"D:\量化\同花顺\同花顺\xiadan.exe"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="同花顺控件诊断工具")
    parser.add_argument("--exe-path", default=DEFAULT_EXE_PATH, help="同花顺下单端 exe 路径")
    parser.add_argument("--title-re", default=".*同花顺.*", help="窗口标题正则")
    parser.add_argument("--backend", default="win32", choices=["win32", "uia"], help="pywinauto backend")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="诊断输出目录")
    parser.add_argument("--depth", type=int, default=4, help="树形输出最大深度")
    parser.add_argument("--max-controls", type=int, default=500, help="最多导出控件数")
    parser.add_argument("--menu-path", default="", help="可选，诊断前先切换菜单，例如 查询[F4],资金股票")
    return parser.parse_args()


def ensure_pywinauto():
    try:
        from pywinauto import Application
        from pywinauto.findwindows import ElementNotFoundError
    except ImportError as exc:
        raise RuntimeError("未安装 pywinauto，请在 Windows 交易环境中安装后再运行。") from exc
    return Application, ElementNotFoundError


def try_connect(args: argparse.Namespace):
    Application, ElementNotFoundError = ensure_pywinauto()
    app = Application(backend=args.backend)
    exe_path = args.exe_path.strip()
    try:
        if exe_path:
            app.connect(path=exe_path)
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
            raise RuntimeError("未找到同花顺相关窗口。")
        window = windows[0]
    return app, window


def safe_text(wrapper) -> str:
    try:
        return str(wrapper.window_text())
    except Exception:  # noqa: BLE001
        return ""


def safe_class(wrapper) -> str:
    try:
        info = getattr(wrapper, "element_info", None)
        return str(getattr(info, "class_name", "") or "")
    except Exception:  # noqa: BLE001
        return ""


def safe_control_id(wrapper) -> Any:
    try:
        info = getattr(wrapper, "element_info", None)
        return getattr(info, "control_id", None)
    except Exception:  # noqa: BLE001
        return None


def safe_rect(wrapper) -> str:
    try:
        rect = wrapper.rectangle()
        return f"{rect.left},{rect.top},{rect.right},{rect.bottom}"
    except Exception:  # noqa: BLE001
        return ""


def dump_controls(window, max_controls: int) -> list[dict[str, Any]]:
    controls = []
    try:
        descendants = window.descendants()
    except Exception as exc:  # noqa: BLE001
        return [{"error": f"{type(exc).__name__}: {exc}"}]
    for idx, ctrl in enumerate(descendants[:max_controls], start=1):
        controls.append(
            {
                "index": idx,
                "text": safe_text(ctrl),
                "class_name": safe_class(ctrl),
                "control_id": safe_control_id(ctrl),
                "rectangle": safe_rect(ctrl),
                "friendly_class_name": getattr(ctrl, "friendly_class_name", lambda: "")(),
            }
        )
    return controls


def dump_tree_lines(wrapper, depth: int, level: int = 0, lines: list[str] | None = None) -> list[str]:
    if lines is None:
        lines = []
    prefix = "  " * level
    label = safe_text(wrapper)
    class_name = safe_class(wrapper)
    control_id = safe_control_id(wrapper)
    rect = safe_rect(wrapper)
    lines.append(f"{prefix}- text={label!r} class={class_name!r} id={control_id!r} rect={rect}")
    if level >= depth:
        return lines
    try:
        children = wrapper.children()
    except Exception:  # noqa: BLE001
        return lines
    for child in children:
        dump_tree_lines(child, depth=depth, level=level + 1, lines=lines)
    return lines


def try_switch_menu(window, menu_path: str) -> str | None:
    if not menu_path.strip():
        return None
    targets = [x.strip() for x in menu_path.split(",") if x.strip()]
    if not targets:
        return None

    try:
        tree = window.child_window(control_id=129, class_name="SysTreeView32")
        tree.wait("ready", timeout=5)
        current = tree.wrapper_object()
        current.get_item(targets).select()
        time.sleep(1.0)
        return None
    except Exception as exc:  # noqa: BLE001
        return f"{type(exc).__name__}: {exc}"


def build_summary(window, args: argparse.Namespace, menu_error: str | None) -> dict[str, Any]:
    return {
        "timestamp": pd_timestamp(),
        "backend": args.backend,
        "exe_path": args.exe_path,
        "title_re": args.title_re,
        "menu_path": args.menu_path,
        "menu_error": menu_error,
        "window_text": safe_text(window),
        "window_class": safe_class(window),
        "window_control_id": safe_control_id(window),
        "window_rectangle": safe_rect(window),
    }


def pd_timestamp() -> str:
    try:
        import pandas as pd
    except ImportError:
        return time.strftime("%Y-%m-%dT%H:%M:%S")
    return str(pd.Timestamp.now().isoformat())


def write_outputs(output_dir: Path, stem: str, summary: dict[str, Any], controls: list[dict[str, Any]], tree_lines: list[str]) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{stem}.json"
    tree_path = output_dir / f"{stem}.tree.txt"
    summary_path = output_dir / f"{stem}.summary.txt"

    json_path.write_text(
        json.dumps({"summary": summary, "controls": controls}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tree_path.write_text("\n".join(tree_lines), encoding="utf-8")
    summary_path.write_text(
        "\n".join(f"{key}: {value}" for key, value in summary.items()),
        encoding="utf-8",
    )
    return json_path, tree_path, summary_path


def main() -> None:
    args = parse_args()
    app, window = try_connect(args)
    menu_error = try_switch_menu(window, args.menu_path)
    summary = build_summary(window, args, menu_error)
    controls = dump_controls(window, max_controls=args.max_controls)
    tree_lines = dump_tree_lines(window, depth=args.depth)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    stem = f"ths_control_dump_{stamp}"
    json_path, tree_path, summary_path = write_outputs(Path(args.output_dir), stem, summary, controls, tree_lines)

    print(f"窗口标题: {summary['window_text']}")
    print(f"窗口类名: {summary['window_class']}")
    print(f"控件导出: {json_path}")
    print(f"树形导出: {tree_path}")
    print(f"摘要导出: {summary_path}")
    if menu_error:
        print(f"菜单切换失败: {menu_error}")
    else:
        print("菜单切换: success" if args.menu_path else "菜单切换: skipped")


if __name__ == "__main__":
    main()
