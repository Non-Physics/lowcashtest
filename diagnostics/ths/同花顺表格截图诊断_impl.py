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
    parser = argparse.ArgumentParser(description="同花顺表格截图 + OCR 诊断")
    parser.add_argument("--exe-path", default=DEFAULT_EXE_PATH, help="同花顺下单端 exe 路径")
    parser.add_argument("--title-re", default=".*网上股票交易系统.*", help="窗口标题正则")
    parser.add_argument("--backend", default="win32", choices=["win32", "uia"], help="pywinauto backend")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="输出目录")
    parser.add_argument("--menu-path", default="查询[F4],资金股票", help="诊断前先切换菜单")
    parser.add_argument("--grid-control-id", type=int, default=1047, help="目标表格控件 ID")
    parser.add_argument("--grid-class-name", default="CVirtualGridCtrl", help="目标表格控件类名")
    parser.add_argument("--candidate-index", type=int, default=1, help="按面积排序后的候选序号，默认 1")
    parser.add_argument("--ocr-lang", default="eng", help="tesseract 语言，默认 eng")
    return parser.parse_args()


def ensure_windows_tools():
    try:
        from pywinauto import Application
    except ImportError as exc:
        raise RuntimeError("未安装 pywinauto。") from exc
    try:
        import pytesseract
    except ImportError as exc:
        raise RuntimeError("未安装 pytesseract。") from exc
    return Application, pytesseract


def connect_window(args: argparse.Namespace):
    Application, pytesseract = ensure_windows_tools()
    app = Application(backend=args.backend)
    try:
        if args.exe_path:
            app.connect(path=args.exe_path)
        else:
            app.connect(title_re=args.title_re)
    except Exception:
        app = Application(backend=args.backend).connect(title_re=args.title_re)
    window = app.window(title_re=args.title_re)
    return window, pytesseract


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


def resolve_candidates(window, args: argparse.Namespace) -> list[Any]:
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


def ocr_image(image_path: Path, pytesseract, lang: str) -> dict[str, Any]:
    from PIL import Image, ImageOps

    image = Image.open(image_path)
    gray = ImageOps.grayscale(image)
    enlarged = gray.resize((gray.width * 2, gray.height * 2))
    text = pytesseract.image_to_string(enlarged, lang=lang, config="--psm 6")
    return {
        "ocr_text": text,
        "ocr_lines": [line for line in text.splitlines() if line.strip()],
    }


def write_payload(output_dir: Path, stem: str, payload: dict[str, Any]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{stem}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def main() -> None:
    args = parse_args()
    window, pytesseract = connect_window(args)
    menu_error = switch_menu(window, args.menu_path)
    candidates = resolve_candidates(window, args)
    if not candidates:
        raise RuntimeError("未找到匹配的 CVirtualGridCtrl。")

    index = max(args.candidate_index - 1, 0)
    if index >= len(candidates):
        raise RuntimeError(f"candidate-index 越界，当前候选数为 {len(candidates)}。")

    target = candidates[index]
    target.set_focus()
    try:
        target.click_input()
    except Exception:
        pass
    time.sleep(0.5)

    output_dir = Path(args.output_dir)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    stem = f"ths_grid_ocr_{stamp}"
    screenshot_path = output_dir / f"{stem}.png"
    try:
        target.capture_as_image().save(screenshot_path)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"截图失败: {type(exc).__name__}: {exc}") from exc

    ocr_result = ocr_image(screenshot_path, pytesseract, args.ocr_lang)
    rect = target.rectangle()
    payload = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "exe_path": args.exe_path,
        "title_re": args.title_re,
        "menu_path": args.menu_path,
        "menu_error": menu_error,
        "candidate_index": args.candidate_index,
        "matched_candidate_count": len(candidates),
        "selected_rectangle": f"{rect.left},{rect.top},{rect.right},{rect.bottom}",
        "screenshot_path": str(screenshot_path),
        "ocr_lang": args.ocr_lang,
        "ocr_text": ocr_result["ocr_text"],
        "ocr_lines": ocr_result["ocr_lines"][:80],
    }
    json_path = write_payload(output_dir, stem, payload)

    print(f"窗口标题: {window.window_text()}")
    print(f"菜单切换错误: {menu_error or 'None'}")
    print(f"命中候选数: {len(candidates)}")
    print(f"选中表格区域: {payload['selected_rectangle']}")
    print(f"截图文件: {screenshot_path}")
    print(f"OCR 文件: {json_path}")
    print(f"OCR 行数: {len(payload['ocr_lines'])}")
    if payload["ocr_lines"]:
        print("OCR 预览:")
        for line in payload["ocr_lines"][:10]:
            print(f"  {line}")
    else:
        print("OCR 无有效文本。")


if __name__ == "__main__":
    main()
