from __future__ import annotations

import argparse
import json
import re
import shutil
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_REPORT_DIR = PROJECT_ROOT / "outputs" / "股票策略交易执行" / "reports"
DEFAULT_PDF_ROOT = PROJECT_ROOT / "outputs" / "股票策略交易执行" / "state" / "pdf_exports"
DEFAULT_EXE_PATH = r"D:\量化\同花顺\同花顺\xiadan.exe"

PAGE_CONFIG = {
    "position": {
        "menu_path": "查询[F4],资金股票",
        "filename_prefix": "position",
    },
    "today_trades": {
        "menu_path": "查询[F4],当日成交",
        "filename_prefix": "today_trades",
    },
    "today_entrusts": {
        "menu_path": "查询[F4],当日委托",
        "filename_prefix": "today_entrusts",
    },
}


@dataclass
class PdfExportConfig:
    exe_path: str = DEFAULT_EXE_PATH
    page: str = "position"
    trade_date: str = ""
    printer: str = "Microsoft Print to PDF"
    title_re: str = r".*(网上股票交易系统|股票交易系统|同花顺).*"
    backend: str = "win32"
    pdf_root_dir: str = str(DEFAULT_PDF_ROOT)
    incoming_dir: str = ""
    output_dir: str = str(DEFAULT_REPORT_DIR)
    print_dialog_title_re: str = r".*(打印|Print).*"
    save_dialog_title_re: str = r".*(保存打印输出|另存打印输出|打印输出另存为|将打印输出另存为|Save Print Output As|Save As).*"
    print_dialog_timeout: float = 10.0
    export_timeout: float = 30.0
    stable_wait_seconds: float = 1.0
    poll_interval: float = 0.5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="同花顺页面自动导出 PDF")
    parser.add_argument("--exe-path", default=DEFAULT_EXE_PATH, help="同花顺下单端 exe 路径")
    parser.add_argument("--page", required=True, choices=sorted(PAGE_CONFIG), help="导出的页面类型")
    parser.add_argument("--trade-date", required=True, help="交易日，例如 2025-03-07")
    parser.add_argument("--printer", default="Microsoft Print to PDF", help="目标 PDF 打印机名称")
    parser.add_argument("--title-re", default=r".*(网上股票交易系统|股票交易系统|同花顺).*", help="同花顺主窗口标题正则")
    parser.add_argument("--backend", default="win32", choices=["win32", "uia"], help="pywinauto backend")
    parser.add_argument("--pdf-root-dir", default=str(DEFAULT_PDF_ROOT), help="PDF 根目录")
    parser.add_argument("--incoming-dir", default="", help="PDFCreator 自动保存的接收目录，默认为 pdf_root_dir/_incoming")
    parser.add_argument("--output-dir", default=str(DEFAULT_REPORT_DIR), help="报告输出目录")
    parser.add_argument("--print-dialog-title-re", default=r".*(打印|Print).*", help="打印对话框标题正则")
    parser.add_argument("--save-dialog-title-re", default=r".*(保存打印输出|另存打印输出|打印输出另存为|将打印输出另存为|Save Print Output As|Save As).*", help="保存对话框标题正则")
    parser.add_argument("--print-dialog-timeout", type=float, default=10.0, help="等待打印对话框秒数")
    parser.add_argument("--export-timeout", type=float, default=30.0, help="等待 PDF 落盘秒数")
    parser.add_argument("--stable-wait-seconds", type=float, default=1.0, help="文件大小稳定等待秒数")
    parser.add_argument("--poll-interval", type=float, default=0.5, help="轮询间隔秒数")
    return parser.parse_args()


def ensure_pywinauto():
    try:
        from pywinauto import Application, Desktop, keyboard
        from pywinauto.findwindows import ElementNotFoundError
    except ImportError as exc:
        raise RuntimeError("未安装 pywinauto，请在 Windows 32 位交易环境中安装后再运行。") from exc
    return Application, Desktop, ElementNotFoundError, keyboard


def _safe_text(wrapper: Any) -> str:
    try:
        return str(wrapper.window_text())
    except Exception:  # noqa: BLE001
        return ""


def _safe_class(wrapper: Any) -> str:
    try:
        info = getattr(wrapper, "element_info", None)
        return str(getattr(info, "class_name", "") or "")
    except Exception:  # noqa: BLE001
        return ""


def _connect_main_window(config: PdfExportConfig):
    Application, _, ElementNotFoundError, _ = ensure_pywinauto()
    app = Application(backend=config.backend)
    try:
        if config.exe_path.strip():
            app.connect(path=config.exe_path)
        else:
            app.connect(title_re=config.title_re)
    except Exception:
        app = Application(backend=config.backend).connect(title_re=config.title_re)
    try:
        window = app.window(title_re=config.title_re)
        window.wait("ready", timeout=8)
    except ElementNotFoundError:
        windows = app.windows()
        if not windows:
            raise RuntimeError("未找到同花顺主窗口。")
        window = windows[0]
    return app, window


def _switch_menu(window: Any, menu_path: str) -> None:
    targets = [item.strip() for item in menu_path.split(",") if item.strip()]
    if not targets:
        return
    tree = window.child_window(control_id=129, class_name="SysTreeView32")
    tree.wait("ready", timeout=5)
    tree.wrapper_object().get_item(targets).select()
    time.sleep(1.0)


def _iter_candidate_buttons(window: Any) -> list[Any]:
    candidates: list[Any] = []
    try:
        descendants = window.descendants()
    except Exception:  # noqa: BLE001
        return candidates
    for child in descendants:
        if _safe_class(child) != "Button":
            continue
        text = _safe_text(child).strip()
        if not text:
            continue
        candidates.append(child)
    return candidates


def _rect_area(rect: Any) -> int:
    try:
        return max(0, rect.width()) * max(0, rect.height())
    except Exception:  # noqa: BLE001
        return 0


def _focus_primary_content(window: Any) -> str:
    preferred = {"CVirtualGridCtrl", "EXCEL7", "AfxWnd", "AfxControlBar", "Static"}
    candidates: list[tuple[int, Any, str]] = []
    try:
        descendants = window.descendants()
    except Exception:  # noqa: BLE001
        descendants = []

    for child in descendants:
        class_name = _safe_class(child)
        if class_name not in preferred:
            continue
        try:
            rect = child.rectangle()
        except Exception:  # noqa: BLE001
            continue
        area = _rect_area(rect)
        if area <= 0:
            continue
        candidates.append((area, child, class_name))

    candidates.sort(key=lambda item: item[0], reverse=True)
    for _, child, class_name in candidates:
        try:
            child.click_input()
            return f"content_click:{class_name}:{_safe_text(child) or '[no-text]'}"
        except Exception:  # noqa: BLE001
            continue

    try:
        rect = window.rectangle()
        x = rect.left + max(200, int((rect.right - rect.left) * 0.3))
        y = rect.top + max(180, int((rect.bottom - rect.top) * 0.3))
        window.click_input(coords=(x - rect.left, y - rect.top))
        return "content_click:window_fallback"
    except Exception:  # noqa: BLE001
        return "content_click:failed"


def _collect_print_candidates(window: Any) -> list[dict[str, str]]:
    patterns = [
        re.compile(r"(打印|Print)", re.IGNORECASE),
        re.compile(r"(导出|输出)", re.IGNORECASE),
        re.compile(r"(文件|File)", re.IGNORECASE),
    ]
    rows: list[dict[str, str]] = []
    try:
        descendants = window.descendants()
    except Exception:  # noqa: BLE001
        return rows
    for child in descendants:
        text = _safe_text(child).strip()
        class_name = _safe_class(child).strip()
        if not text:
            continue
        if not any(pattern.search(text) for pattern in patterns):
            continue
        rows.append(
            {
                "text": text,
                "class_name": class_name,
                "rectangle": getattr(getattr(child, "rectangle", lambda: None)(), "__str__", lambda: "")(),
            }
        )
    return rows


def _trigger_print(window: Any) -> str:
    button_patterns = [
        re.compile(r"(打印|Print)", re.IGNORECASE),
        re.compile(r"(输出|导出)", re.IGNORECASE),
    ]
    try:
        descendants = window.descendants()
    except Exception:  # noqa: BLE001
        descendants = []
    for child in descendants:
        text = _safe_text(child).strip()
        if not text or not any(pattern.search(text) for pattern in button_patterns):
            continue
        try:
            child.click_input()
            return f"control:{_safe_class(child)}:{text}"
        except Exception:  # noqa: BLE001
            continue

    for button in _iter_candidate_buttons(window):
        text = _safe_text(button).strip()
        if any(pattern.search(text) for pattern in button_patterns):
            try:
                button.click_input()
                return f"button:{text}"
            except Exception:  # noqa: BLE001
                continue

    _, _, _, keyboard = ensure_pywinauto()
    focus_result = _focus_primary_content(window)
    try:
        window.type_keys("^p", set_foreground=True)
        return f"{focus_result}|window_type_keys:Ctrl+P"
    except Exception:  # noqa: BLE001
        keyboard.send_keys("^p")
    return f"{focus_result}|hotkey:Ctrl+P"


def _click_dialog_confirm(dialog: Any) -> str:
    exact_texts = {"打印", "确定", "print", "ok", "确认"}
    partial_patterns = [
        re.compile(r"^(打印|Print)\b", re.IGNORECASE),
        re.compile(r"^(确定|OK|确认)\b", re.IGNORECASE),
    ]
    for child in dialog.descendants():
        if _safe_class(child) != "Button":
            continue
        text = _safe_text(child).strip()
        if not text:
            continue
        normalized = text.lower()
        if normalized in exact_texts or any(pattern.search(text) for pattern in partial_patterns):
            child.click_input()
            return text
    raise RuntimeError("未找到可点击的打印确认按钮。")


def _collect_dialog_buttons(dialog: Any) -> list[str]:
    rows: list[str] = []
    try:
        descendants = dialog.descendants()
    except Exception:  # noqa: BLE001
        return rows
    for child in descendants:
        if _safe_class(child) != "Button":
            continue
        text = _safe_text(child).strip()
        if text:
            rows.append(text)
    return rows


def _wait_print_dialog(config: PdfExportConfig):
    _, Desktop, _, _ = ensure_pywinauto()
    deadline = time.time() + config.print_dialog_timeout
    last_titles: list[str] = []
    while time.time() < deadline:
        windows = Desktop(backend=config.backend).windows()
        candidates = []
        last_titles = []
        for win in windows:
            title = _safe_text(win)
            if title:
                last_titles.append(title)
            if re.search(config.print_dialog_title_re, title, re.IGNORECASE):
                candidates.append(win)
        if candidates:
            dialog = candidates[0]
            try:
                dialog.wait("ready", timeout=2)
            except Exception:  # noqa: BLE001
                pass
            return dialog, last_titles
        time.sleep(config.poll_interval)
    raise RuntimeError(
        "等待打印对话框超时，最近可见窗口标题: "
        + " | ".join(last_titles[:20])
    )


def _wait_named_dialog(title_re: str, timeout: float, backends: tuple[str, ...] = ("uia", "win32")):
    _, Desktop, _, _ = ensure_pywinauto()
    deadline = time.time() + timeout
    last_titles: list[str] = []
    while time.time() < deadline:
        last_titles = []
        for backend in backends:
            try:
                windows = Desktop(backend=backend).windows()
            except Exception:  # noqa: BLE001
                continue
            for win in windows:
                title = _safe_text(win)
                class_name = _safe_class(win)
                if title:
                    last_titles.append(f"{backend}:{class_name}:{title}")
                elif class_name:
                    last_titles.append(f"{backend}:{class_name}:[no-title]")
                if re.search(title_re, title, re.IGNORECASE):
                    try:
                        win.wait("ready", timeout=2)
                    except Exception:  # noqa: BLE001
                        pass
                    return win, backend, last_titles
                if class_name in {"#32770", "NUIDialog"}:
                    try:
                        descendants = win.descendants()
                    except Exception:  # noqa: BLE001
                        descendants = []
                    child_classes = {_safe_class(child) for child in descendants}
                    child_texts = {_safe_text(child).strip() for child in descendants if _safe_text(child).strip()}
                    has_edit = "Edit" in child_classes or "ComboBoxEx32" in child_classes or "ComboBox" in child_classes
                    has_save = any(
                        text in {"保存", "Save", "&Save", "另存为(&S)", "另存为", "保存(&S)"}
                        for text in child_texts
                    )
                    has_file_hint = any("打印输出" in text or "另存为" in text or "保存" in text for text in child_texts)
                    if has_edit and (has_save or has_file_hint):
                        try:
                            win.wait("ready", timeout=2)
                        except Exception:  # noqa: BLE001
                            pass
                        return win, backend, last_titles
        time.sleep(0.3)
    raise RuntimeError("等待保存对话框超时，最近可见窗口标题: " + " | ".join(last_titles[:20]))


def _snapshot_pdf_files(paths: list[Path]) -> dict[str, float]:
    snapshot: dict[str, float] = {}
    for root in paths:
        if not root.exists():
            continue
        for file_path in root.glob("*.pdf"):
            try:
                snapshot[str(file_path.resolve())] = file_path.stat().st_mtime
            except FileNotFoundError:
                continue
    return snapshot


def _wait_for_stable_file(path: Path, stable_wait_seconds: float, poll_interval: float) -> None:
    previous_size = -1
    stable_rounds = 0
    deadline = time.time() + max(stable_wait_seconds * 6, 5.0)
    while time.time() < deadline:
        if not path.exists():
            time.sleep(poll_interval)
            continue
        current_size = path.stat().st_size
        if current_size > 0 and current_size == previous_size:
            stable_rounds += 1
            if stable_rounds >= 2:
                return
        else:
            stable_rounds = 0
            previous_size = current_size
        time.sleep(stable_wait_seconds)
    raise RuntimeError(f"文件未在预期时间内稳定: {path}")


def _wait_for_exported_pdf(
    config: PdfExportConfig,
    watch_dirs: list[Path],
    before_snapshot: dict[str, float],
    started_at: float,
) -> Path:
    deadline = time.time() + config.export_timeout
    newest_candidate: Path | None = None
    newest_mtime = started_at
    while time.time() < deadline:
        for root in watch_dirs:
            if not root.exists():
                continue
            for file_path in root.glob("*.pdf"):
                try:
                    resolved = str(file_path.resolve())
                    stat = file_path.stat()
                except FileNotFoundError:
                    continue
                if resolved in before_snapshot:
                    continue
                if stat.st_mtime < started_at - 1.0:
                    continue
                if stat.st_mtime >= newest_mtime:
                    newest_candidate = file_path
                    newest_mtime = stat.st_mtime
        if newest_candidate is not None:
            _wait_for_stable_file(newest_candidate, config.stable_wait_seconds, config.poll_interval)
            return newest_candidate
        time.sleep(config.poll_interval)
    raise RuntimeError("等待 PDF 落盘超时。")


def _normalize_output_path(config: PdfExportConfig, source_path: Path) -> Path:
    page_dir = Path(config.pdf_root_dir) / config.page
    page_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    filename_prefix = PAGE_CONFIG[config.page]["filename_prefix"]
    target_path = page_dir / f"{filename_prefix}_{config.trade_date}_{stamp}.pdf"
    suffix = 1
    while target_path.exists():
        target_path = page_dir / f"{filename_prefix}_{config.trade_date}_{stamp}_{suffix}.pdf"
        suffix += 1
    if source_path.resolve() != target_path.resolve():
        shutil.move(str(source_path), str(target_path))
    return target_path


def _build_target_pdf_path(config: PdfExportConfig) -> Path:
    page_dir = Path(config.pdf_root_dir) / config.page
    page_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    filename_prefix = PAGE_CONFIG[config.page]["filename_prefix"]
    target_path = page_dir / f"{filename_prefix}_{config.trade_date}_{stamp}.pdf"
    suffix = 1
    while target_path.exists():
        target_path = page_dir / f"{filename_prefix}_{config.trade_date}_{stamp}_{suffix}.pdf"
        suffix += 1
    return target_path


def _find_best_edit(dialog: Any) -> Any | None:
    candidates: list[tuple[int, Any]] = []
    try:
        descendants = dialog.descendants()
    except Exception:  # noqa: BLE001
        return None
    for child in descendants:
        if _safe_class(child) != "Edit":
            continue
        try:
            rect = child.rectangle()
        except Exception:  # noqa: BLE001
            continue
        candidates.append((_rect_area(rect), child))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def _set_edit_text(edit: Any, value: str) -> bool:
    methods = ["set_edit_text", "set_text"]
    for method_name in methods:
        method = getattr(edit, method_name, None)
        if method is None:
            continue
        try:
            method(value)
            return True
        except Exception:  # noqa: BLE001
            continue
    return False


def _click_button_by_text(dialog: Any, texts: tuple[str, ...]) -> str:
    wanted = {text.lower() for text in texts}
    try:
        descendants = dialog.descendants()
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"无法遍历对话框按钮: {exc}") from exc
    for child in descendants:
        if _safe_class(child) != "Button":
            continue
        text = _safe_text(child).strip()
        if not text:
            continue
        normalized = text.lower()
        normalized_base = re.sub(r"\(&.\)", "", normalized).strip()
        if normalized in wanted or normalized_base in wanted:
            child.click_input()
            return text
    raise RuntimeError(f"未找到按钮: {texts}")


def _save_via_dialog(config: PdfExportConfig, target_path: Path) -> tuple[str, str]:
    _, _, _, keyboard = ensure_pywinauto()
    dialog, backend_used, visible_titles = _wait_named_dialog(config.save_dialog_title_re, config.export_timeout)
    dialog_title = _safe_text(dialog)
    edit = _find_best_edit(dialog)
    if edit is None:
        dialog.set_focus()
        keyboard.send_keys("^l")
        time.sleep(0.2)
        keyboard.send_keys(str(target_path), with_spaces=True)
    else:
        try:
            edit.click_input()
        except Exception:  # noqa: BLE001
            pass
        if not _set_edit_text(edit, str(target_path)):
            try:
                edit.select()
            except Exception:  # noqa: BLE001
                pass
            keyboard.send_keys("^a")
            keyboard.send_keys(str(target_path), with_spaces=True)

    clicked = _click_button_by_text(dialog, ("保存", "save", "另存为", "另存为(&s)", "&save"))
    overwrite_note = ""
    try:
        overwrite_dialog, _, _ = _wait_named_dialog(r".*(确认另存为|Confirm Save As|替换|确认|另存为).*", 3.0)
        overwrite_clicked = _click_button_by_text(overwrite_dialog, ("是", "yes", "确定", "保存"))
        overwrite_note = f"|overwrite:{overwrite_clicked}"
    except Exception:  # noqa: BLE001
        pass

    _wait_for_stable_file(target_path, config.stable_wait_seconds, config.poll_interval)
    return f"{backend_used}:{clicked}{overwrite_note}", f"{dialog_title}|{' | '.join(visible_titles[:20])}"


def export_pdf(config: PdfExportConfig) -> dict[str, Any]:
    if config.page not in PAGE_CONFIG:
        raise RuntimeError(f"未知页面类型: {config.page}")

    pdf_root = Path(config.pdf_root_dir)
    incoming_dir = Path(config.incoming_dir) if config.incoming_dir else pdf_root / "_incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)
    (pdf_root / config.page).mkdir(parents=True, exist_ok=True)

    started_at_iso = time.strftime("%Y-%m-%dT%H:%M:%S")
    started_at_epoch = time.time()
    watch_dirs = [incoming_dir, pdf_root / config.page]
    before_snapshot = _snapshot_pdf_files(watch_dirs)

    app, window = _connect_main_window(config)
    menu_path = PAGE_CONFIG[config.page]["menu_path"]

    result: dict[str, Any] = {
        "success": False,
        "page": config.page,
        "trade_date": config.trade_date,
        "printer": config.printer,
        "menu_path": menu_path,
        "window_title": _safe_text(window),
        "window_class": _safe_class(window),
        "pdf_root_dir": str(pdf_root),
        "incoming_dir": str(incoming_dir),
        "target_pdf": "",
        "target_meta": "",
        "source_pdf": "",
        "started_at": started_at_iso,
        "finished_at": "",
        "print_dialog_title": "",
        "save_dialog_title": "",
        "trigger_method": "",
        "print_confirm_button": "",
        "save_action": "",
        "visible_windows_after_confirm": [],
        "warnings": [],
        "error": "",
        "progress": [],
        "print_candidates": [],
    }

    try:
        result["progress"].append("connect_main_window")
        result["print_candidates"] = _collect_print_candidates(window)
        _switch_menu(window, menu_path)
        result["progress"].append("switch_menu")
        window.set_focus()
        time.sleep(0.5)
        result["trigger_method"] = _trigger_print(window)
        result["progress"].append(f"trigger_print:{result['trigger_method']}")
        dialog, visible_titles = _wait_print_dialog(config)
        result["print_dialog_title"] = _safe_text(dialog)
        result["progress"].append("print_dialog_visible")
        dialog_buttons = _collect_dialog_buttons(dialog)
        if dialog_buttons:
            result["warnings"].append("打印框按钮: " + " | ".join(dialog_buttons[:20]))

        dialog_texts = " ".join(_safe_text(item) for item in dialog.descendants())
        if config.printer.lower() not in dialog_texts.lower():
            result["warnings"].append(f"打印对话框中未明确看到打印机名: {config.printer}")
            result["warnings"].append("可见窗口标题: " + " | ".join(visible_titles[:20]))

        result["print_confirm_button"] = _click_dialog_confirm(dialog)
        result["progress"].append(f"confirm_print:{result['print_confirm_button']}")
        target_pdf = _build_target_pdf_path(config)
        result["target_pdf"] = str(target_pdf)
        if "pdfcreator" in config.printer.lower():
            exported_pdf = _wait_for_exported_pdf(config, watch_dirs, before_snapshot, started_at_epoch)
            result["source_pdf"] = str(exported_pdf)
            result["progress"].append("pdf_exported")
            normalized_pdf = _normalize_output_path(config, exported_pdf)
            result["target_pdf"] = str(normalized_pdf)
            result["progress"].append("pdf_moved")
        else:
            save_action, visible_titles_note = _save_via_dialog(config, target_pdf)
            result["save_action"] = save_action
            result["save_dialog_title"] = visible_titles_note.split("|", 1)[0] if visible_titles_note else ""
            result["progress"].append(f"save_dialog:{save_action}")
            if visible_titles_note:
                result["visible_windows_after_confirm"] = visible_titles_note.split(" | ")
                result["warnings"].append("保存框可见标题: " + visible_titles_note)
        result["success"] = True
    except Exception as exc:  # noqa: BLE001
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        result["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")

    return result


def write_report(config: PdfExportConfig, result: dict[str, Any]) -> Path:
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    report_path = output_dir / f"ths_pdf_export_{config.page}_{config.trade_date.replace('-', '')}_{stamp}.json"
    target_pdf = result.get("target_pdf")
    if target_pdf:
        sidecar_path = Path(target_pdf).with_suffix(".json")
        result["target_meta"] = str(sidecar_path)
    payload = {"config": asdict(config), "result": result}
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if target_pdf:
        sidecar_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return report_path


def main() -> None:
    args = parse_args()
    config = PdfExportConfig(
        exe_path=args.exe_path,
        page=args.page,
        trade_date=args.trade_date,
        printer=args.printer,
        title_re=args.title_re,
        backend=args.backend,
        pdf_root_dir=args.pdf_root_dir,
        incoming_dir=args.incoming_dir,
        output_dir=args.output_dir,
        print_dialog_title_re=args.print_dialog_title_re,
        save_dialog_title_re=args.save_dialog_title_re,
        print_dialog_timeout=args.print_dialog_timeout,
        export_timeout=args.export_timeout,
        stable_wait_seconds=args.stable_wait_seconds,
        poll_interval=args.poll_interval,
    )
    result = export_pdf(config)
    report_path = write_report(config, result)
    print(f"页面: {result['page']}")
    print(f"成功: {result['success']}")
    if result["trigger_method"]:
        print(f"打印触发: {result['trigger_method']}")
    if result["progress"]:
        print("进度:")
        for step in result["progress"]:
            print(f"  - {step}")
    if result["target_pdf"]:
        print(f"目标文件: {result['target_pdf']}")
    if result["target_meta"]:
        print(f"元数据文件: {result['target_meta']}")
    if result["error"]:
        print(f"错误: {result['error']}")
    if result["warnings"]:
        print("警告:")
        for warning in result["warnings"]:
            print(f"  - {warning}")
    print(f"报告文件: {report_path}")
    if not result["success"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
