from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "股票策略交易执行" / "reports"
DEFAULT_EXE_PATH = r"D:\量化\同花顺\同花顺\xiadan.exe"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="同花顺表格抓取诊断")
    parser.add_argument("--exe-path", default=DEFAULT_EXE_PATH, help="同花顺下单端 exe 路径")
    parser.add_argument("--title-re", default=".*网上股票交易系统.*", help="窗口标题正则")
    parser.add_argument("--backend", default="win32", choices=["win32", "uia"], help="pywinauto backend")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="输出目录")
    parser.add_argument("--menu-path", default="查询[F4],资金股票", help="诊断前先切换菜单")
    parser.add_argument("--grid-control-id", type=int, default=1047, help="目标表格控件 ID")
    parser.add_argument("--grid-class-name", default="CVirtualGridCtrl", help="目标表格控件类名")
    parser.add_argument("--candidate-index", type=int, default=1, help="匹配到多个候选表格时，按排序后的第几个抓取，默认 1")
    return parser.parse_args()


def ensure_pywinauto():
    try:
        from pywinauto import Application
        from pywinauto import clipboard
        from pywinauto.keyboard import send_keys
    except ImportError as exc:
        raise RuntimeError("未安装 pywinauto，请在 Windows 交易环境中安装后再运行。") from exc
    return Application, clipboard, send_keys


def connect_window(args: argparse.Namespace):
    Application, clipboard, send_keys = ensure_pywinauto()
    app = Application(backend=args.backend)
    try:
        if args.exe_path:
            app.connect(path=args.exe_path)
        else:
            app.connect(title_re=args.title_re)
    except Exception:
        app = Application(backend=args.backend).connect(title_re=args.title_re)
    window = app.window(title_re=args.title_re)
    return app, window, clipboard, send_keys


def switch_menu(window, menu_path: str) -> str | None:
    targets = [x.strip() for x in menu_path.split(",") if x.strip()]
    if not targets:
        return None
    try:
        tree = window.child_window(control_id=129, class_name="SysTreeView32")
        tree.wait("ready", timeout=5)
        tree.wrapper_object().get_item(targets).select()
        time.sleep(1.5)
        return None
    except Exception as exc:  # noqa: BLE001
        return f"{type(exc).__name__}: {exc}"


def dump_candidates(window) -> list[dict[str, Any]]:
    candidates = []
    for ctrl in window.descendants():
        cls = getattr(getattr(ctrl, "element_info", None), "class_name", "") or ""
        cid = getattr(getattr(ctrl, "element_info", None), "control_id", None)
        if "Grid" not in cls and "List" not in cls:
            continue
        try:
            rect = ctrl.rectangle()
            rect_text = f"{rect.left},{rect.top},{rect.right},{rect.bottom}"
            area = max(rect.right - rect.left, 0) * max(rect.bottom - rect.top, 0)
            top = rect.top
        except Exception:  # noqa: BLE001
            rect_text = ""
            area = 0
            top = 10**9
        candidates.append(
            {
                "class_name": cls,
                "control_id": cid,
                "text": ctrl.window_text(),
                "rectangle": rect_text,
                "area": area,
                "top": top,
            }
        )
    return candidates


def resolve_grid_candidates(window, args: argparse.Namespace) -> list[Any]:
    matched = []
    for ctrl in window.descendants():
        info = getattr(ctrl, "element_info", None)
        cls = getattr(info, "class_name", "") or ""
        cid = getattr(info, "control_id", None)
        if cid != args.grid_control_id or cls != args.grid_class_name:
            continue
        try:
            rect = ctrl.rectangle()
            area = max(rect.right - rect.left, 0) * max(rect.bottom - rect.top, 0)
            top = rect.top
        except Exception:  # noqa: BLE001
            area = 0
            top = 10**9
        matched.append((ctrl, area, top))
    matched.sort(key=lambda item: (-item[1], item[2]))
    return [item[0] for item in matched]


def try_copy_grid(window, args: argparse.Namespace, clipboard, send_keys) -> dict[str, Any]:
    result: dict[str, Any] = {
        "grid_control_id": args.grid_control_id,
        "grid_class_name": args.grid_class_name,
        "candidate_index": args.candidate_index,
        "clipboard_text": "",
        "error": None,
    }
    try:
        candidates = resolve_grid_candidates(window, args)
        result["matched_candidate_count"] = len(candidates)
        if not candidates:
            result["error"] = "未找到匹配的表格控件"
            return result
        selected_idx = max(args.candidate_index - 1, 0)
        if selected_idx >= len(candidates):
            result["error"] = f"candidate-index 越界，可用候选数为 {len(candidates)}"
            return result
        wrapper = candidates[selected_idx]
        try:
            rect = wrapper.rectangle()
            result["selected_rectangle"] = f"{rect.left},{rect.top},{rect.right},{rect.bottom}"
        except Exception:  # noqa: BLE001
            result["selected_rectangle"] = ""
        wrapper.set_focus()
        try:
            wrapper.click_input()
        except Exception:
            pass
        time.sleep(0.3)
        try:
            clipboard.EmptyClipboard()
        except Exception:
            pass
        send_keys("^a")
        time.sleep(0.2)
        send_keys("^c")
        time.sleep(0.6)
        text = ""
        try:
            text = clipboard.GetData()
        except Exception:
            text = ""
        result["clipboard_text"] = text or ""
        if text:
            lines = [line for line in str(text).splitlines() if line.strip()]
            result["line_count"] = len(lines)
            result["preview_lines"] = lines[:20]
        else:
            result["line_count"] = 0
            result["preview_lines"] = []
        return result
    except Exception as exc:  # noqa: BLE001
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result


def write_output(output_dir: Path, stem: str, payload: dict[str, Any]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{stem}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def timestamp_tag() -> str:
    return time.strftime("%Y%m%d_%H%M%S")


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def main() -> None:
    args = parse_args()
    _, window, clipboard, send_keys = connect_window(args)
    menu_error = switch_menu(window, args.menu_path)
    candidates = dump_candidates(window)
    capture = try_copy_grid(window, args, clipboard, send_keys)

    payload = {
        "timestamp": now_iso(),
        "exe_path": args.exe_path,
        "title_re": args.title_re,
        "menu_path": args.menu_path,
        "menu_error": menu_error,
        "window_text": window.window_text(),
        "grid_candidates": candidates,
        "capture": capture,
    }
    out_path = write_output(Path(args.output_dir), f"ths_grid_capture_{timestamp_tag()}", payload)

    print(f"窗口标题: {payload['window_text']}")
    print(f"菜单切换错误: {menu_error or 'None'}")
    print(f"候选表格数: {len(candidates)}")
    print(f"抓取结果文件: {out_path}")
    print(f"剪贴板行数: {capture.get('line_count', 0)}")
    print(f"命中候选数: {capture.get('matched_candidate_count', 0)}")
    if capture.get("selected_rectangle"):
        print(f"选中表格区域: {capture['selected_rectangle']}")
    if capture.get("error"):
        print(f"抓取错误: {capture['error']}")
    elif capture.get("preview_lines"):
        print("剪贴板预览:")
        for line in capture["preview_lines"][:10]:
            print(f"  {line}")
    else:
        print("剪贴板为空。")


if __name__ == "__main__":
    main()
