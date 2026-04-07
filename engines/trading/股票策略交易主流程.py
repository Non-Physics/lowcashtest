from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
import sys
from typing import Any

import pandas as pd

from stock_trading.common import (
    build_runtime_paths,
    dump_json,
    ensure_runtime_dirs,
    load_due_orders,
    load_state,
    save_planned_orders,
    save_state,
    write_signal_report,
)
from stock_trading.execution import PaperExecutionAdapter
from stock_trading.factor_switch import build_factor_switch_decision
from stock_trading.signal_service import StrategySignalService

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_project_module(module_name: str, filename: str):
    if module_name in sys.modules:
        return sys.modules[module_name]
    path = PROJECT_ROOT / filename
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载模块: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="股票策略交易主流程")
    parser.add_argument("--runtime-root", default="", help="交易执行运行目录，默认 outputs/股票策略交易执行")
    subparsers = parser.add_subparsers(dest="command", required=True)

    refresh = subparsers.add_parser("refresh-data", help="刷新本地数据")
    refresh.add_argument("--check", action="store_true", help="刷新完成后顺带跑数据检查")

    subparsers.add_parser("check-data", help="检查本地数据完整性")

    gen = subparsers.add_parser("generate-signals", help="按基线策略生成次日订单")
    gen.add_argument("--date", required=True, help="信号日期，例如 2026-04-02")
    gen.add_argument("--strategy-version", default="baseline_live_v1")
    gen.add_argument("--parameter-version", default="baseline_default")
    gen.add_argument("--enable-factor-switch-gray", action="store_true", help="并行生成条件切换候选灰度报告与假想订单")
    gen.add_argument("--factor-switch-candidate-version", default="factor_switch_v1")
    gen.add_argument("--factor-switch-policy", default="pullback_or_bull_highvol_to_core5")

    exec_paper = subparsers.add_parser("paper-execute", help="执行到期纸面订单")
    exec_paper.add_argument("--date", required=True, help="执行日期，例如 2026-04-03")

    cycle = subparsers.add_parser("paper-cycle", help="串行执行 生成信号 -> 次日纸面成交")
    cycle.add_argument("--signal-date", required=True)
    cycle.add_argument("--execute-date", required=True)
    cycle.add_argument("--strategy-version", default="baseline_live_v1")
    cycle.add_argument("--parameter-version", default="baseline_default")

    preview = subparsers.add_parser("preview-orders", help="预览某日将执行的待处理订单")
    preview.add_argument("--date", required=True)

    ths_preview = subparsers.add_parser("ths-preview", help="预览同花顺待提交订单")
    ths_preview.add_argument("--date", required=True)
    ths_preview.add_argument("--client-type", default="ths")
    ths_preview.add_argument("--exe-path", default="")
    ths_preview.add_argument("--account-json", default="")
    ths_preview.add_argument("--pdf-root-dir", default="")

    ths_submit = subparsers.add_parser("ths-submit", help="通过同花顺客户端提交订单")
    ths_submit.add_argument("--date", required=True)
    ths_submit.add_argument("--client-type", default="ths")
    ths_submit.add_argument("--exe-path", default="")
    ths_submit.add_argument("--account-json", default="")
    ths_submit.add_argument("--pdf-root-dir", default="")
    ths_submit.add_argument("--yes", action="store_true", help="跳过命令行二次确认")

    ths_reconcile = subparsers.add_parser("ths-reconcile", help="抓取同花顺账户快照并与本地状态对账")
    ths_reconcile.add_argument("--date", required=True)
    ths_reconcile.add_argument("--client-type", default="ths")
    ths_reconcile.add_argument("--exe-path", default="")
    ths_reconcile.add_argument("--account-json", default="")
    ths_reconcile.add_argument("--pdf-root-dir", default="")

    ths_export_pdf = subparsers.add_parser("ths-export-pdf", help="从同花顺查询页打印并自动保存 PDF")
    ths_export_pdf.add_argument("--date", required=True)
    ths_export_pdf.add_argument("--page", required=True, choices=["position", "today_trades", "today_entrusts"])
    ths_export_pdf.add_argument("--exe-path", default="")
    ths_export_pdf.add_argument("--pdf-root-dir", default="")
    ths_export_pdf.add_argument("--printer", default="Microsoft Print to PDF")
    ths_export_pdf.add_argument("--incoming-dir", default="")
    ths_export_pdf.add_argument("--title-re", default=r".*(网上股票交易系统|股票交易系统|同花顺).*")
    ths_export_pdf.add_argument("--backend", default="win32", choices=["win32", "uia"])
    ths_export_pdf.add_argument("--print-dialog-title-re", default=r".*(打印|Print).*")
    ths_export_pdf.add_argument("--print-dialog-timeout", type=float, default=10.0)
    ths_export_pdf.add_argument("--export-timeout", type=float, default=30.0)

    return parser.parse_args()


def get_runtime_paths(args: argparse.Namespace):
    runtime_root = Path(args.runtime_root) if args.runtime_root else None
    paths = build_runtime_paths(runtime_root)
    ensure_runtime_dirs(paths)
    return paths


def handle_refresh_data(with_check: bool) -> None:
    downloader = _load_project_module("ths_downloader_live", "下载股票数据.py")
    downloader.run()
    if with_check:
        checker = _load_project_module("ths_checker_live", "检查股票数据.py")
        checker.main()


def handle_check_data() -> None:
    checker = _load_project_module("ths_checker_live", "检查股票数据.py")
    checker.main()


def handle_generate_signals(args: argparse.Namespace) -> None:
    paths = get_runtime_paths(args)
    signal_service = StrategySignalService()
    state = load_state(
        paths,
        initial_capital=signal_service.baseline.INITIAL_CAPITAL,
        strategy_version=args.strategy_version,
        parameter_version=args.parameter_version,
    )
    run_result = signal_service.generate_for_date(
        args.date,
        state,
        strategy_version=args.strategy_version,
        parameter_version=args.parameter_version,
    )
    orders_path = save_planned_orders(run_result.orders, paths, args.date)
    report_path = write_signal_report(run_result.report, paths, args.date)
    save_state(run_result.updated_state, paths)

    gray_report_path = None
    gray_orders_path = None
    compare_report_path = None
    if args.enable_factor_switch_gray:
        cooldown_map = {code: pd.Timestamp(value) for code, value in state.cooldown_until.items()}
        gray_decision = build_factor_switch_decision(
            signal_date=args.date,
            cooldown_until=cooldown_map,
            switch_policy=args.factor_switch_policy,
            candidate_version=args.factor_switch_candidate_version,
            progress=lambda msg: print(f"[factor-switch-gray] {msg}"),
        )
        original_select_candidates = signal_service.base.select_candidates
        try:
            selected_rows = gray_decision.get("selected_candidates", [])

            def _gray_selector(current_date, daily_data, instruments, st_status, cooldown_until):
                current_ts = pd.Timestamp(current_date).normalize()
                if current_ts != pd.Timestamp(args.date).normalize():
                    return original_select_candidates(current_date, daily_data, instruments, st_status, cooldown_until)
                return [(str(row["code"]), float(row["score"])) for row in selected_rows]

            signal_service.base.select_candidates = _gray_selector
            gray_result = signal_service.generate_for_date(
                args.date,
                state,
                strategy_version=f"{args.strategy_version}__factor_switch_gray",
                parameter_version=args.parameter_version,
            )
        finally:
            signal_service.base.select_candidates = original_select_candidates

        date_tag = args.date.replace("-", "")
        gray_orders_path = paths.state_dir / f"factor_switch_planned_orders_{date_tag}.json"
        gray_report_path = paths.reports_dir / f"factor_switch_signal_report_{date_tag}.json"
        compare_report_path = paths.reports_dir / f"factor_switch_gray_{date_tag}.json"
        dump_json(
            {
                "signal_date": args.date,
                "candidate_source": gray_decision["candidate_source"],
                "candidate_version": gray_decision["candidate_version"],
                "switch_policy": gray_decision["switch_policy"],
                "selected_factor_group": gray_decision["selected_factor_group"],
                "orders": [order.to_dict() for order in gray_result.orders],
            },
            gray_orders_path,
        )
        dump_json(gray_result.report, gray_report_path)
        dump_json(
            {
                "signal_date": args.date,
                "baseline": {
                    "strategy_version": args.strategy_version,
                    "order_count": len(run_result.orders),
                    "buy_codes": [order.code for order in run_result.orders if order.action == "BUY"],
                    "sell_codes": [order.code for order in run_result.orders if order.action == "SELL"],
                    "buy_candidates": run_result.report.get("buy_candidates", []),
                },
                "factor_switch_gray": {
                    "candidate_source": gray_decision["candidate_source"],
                    "candidate_version": gray_decision["candidate_version"],
                    "switch_policy": gray_decision["switch_policy"],
                    "market_regime": gray_decision["market_regime"],
                    "selected_factor_group": gray_decision["selected_factor_group"],
                    "used_fallback": gray_decision["used_fallback"],
                    "selected_top_n": gray_decision["selected_top_n"],
                    "selected_codes": gray_decision["selected_codes"],
                    "baseline_codes": gray_decision["baseline_codes"],
                    "overlap_codes": gray_decision["overlap_codes"],
                    "order_count": len(gray_result.orders),
                    "buy_codes": [order.code for order in gray_result.orders if order.action == "BUY"],
                    "sell_codes": [order.code for order in gray_result.orders if order.action == "SELL"],
                    "candidate_groups": gray_decision["candidate_groups"],
                },
                "output_files": {
                    "gray_orders_path": str(gray_orders_path),
                    "gray_signal_report_path": str(gray_report_path),
                },
            },
            compare_report_path,
        )

    print(f"信号日期: {run_result.signal_date}")
    print(f"预计执行日: {run_result.execute_date}")
    print(f"订单数量: {len(run_result.orders)}")
    print(f"订单文件: {orders_path}")
    print(f"报告文件: {report_path}")
    if gray_report_path is not None and gray_orders_path is not None and compare_report_path is not None:
        print(f"灰度假想订单: {gray_orders_path}")
        print(f"灰度比较报告: {compare_report_path}")


def handle_paper_execute(args: argparse.Namespace) -> None:
    paths = get_runtime_paths(args)
    signal_service = StrategySignalService()
    state = load_state(
        paths,
        initial_capital=signal_service.baseline.INITIAL_CAPITAL,
    )
    adapter = PaperExecutionAdapter(paths)
    batch = adapter.execute_due_orders(args.date, state)
    print(f"执行日期: {batch.trade_date}")
    print(f"成交记录数: {len(batch.records)}")
    print(f"日志文件: {batch.journal_path}")


def handle_paper_cycle(args: argparse.Namespace) -> None:
    paths = get_runtime_paths(args)
    signal_service = StrategySignalService()
    state = load_state(
        paths,
        initial_capital=signal_service.baseline.INITIAL_CAPITAL,
        strategy_version=args.strategy_version,
        parameter_version=args.parameter_version,
    )
    run_result = signal_service.generate_for_date(
        args.signal_date,
        state,
        strategy_version=args.strategy_version,
        parameter_version=args.parameter_version,
    )
    orders_path = save_planned_orders(run_result.orders, paths, args.signal_date)
    report_path = write_signal_report(run_result.report, paths, args.signal_date)
    save_state(run_result.updated_state, paths)

    adapter = PaperExecutionAdapter(paths)
    batch = adapter.execute_due_orders(args.execute_date, run_result.updated_state)

    print(f"信号文件: {orders_path}")
    print(f"信号报告: {report_path}")
    print(f"执行日期: {batch.trade_date}")
    print(f"成交记录数: {len(batch.records)}")
    print(f"日志文件: {batch.journal_path}")


def handle_preview_orders(args: argparse.Namespace) -> None:
    paths = get_runtime_paths(args)
    due_orders = load_due_orders(paths, args.date)
    preview_path = paths.reports_dir / f"preview_orders_{args.date.replace('-', '')}.json"
    dump_json({"trade_date": args.date, "orders": [order.to_dict() for order in due_orders]}, preview_path)
    print(f"到期订单数: {len(due_orders)}")
    print(f"预览文件: {preview_path}")


def build_ths_adapter(args: argparse.Namespace):
    adapter_module = _load_project_module("ths_gui_adapter_live", "股票策略同花顺适配.py")
    config = adapter_module.TongHuaShunClientConfig(
        client_type=args.client_type,
        exe_path=args.exe_path,
        account_json=args.account_json,
        auto_confirm=getattr(args, "yes", False),
        pdf_root_dir=getattr(args, "pdf_root_dir", ""),
    )
    return adapter_module.TongHuaShunGuiAdapter(config)


def handle_ths_preview(args: argparse.Namespace) -> None:
    paths = get_runtime_paths(args)
    due_orders = load_due_orders(paths, args.date)
    adapter = build_ths_adapter(args)
    preview = adapter.preview_orders(due_orders)
    preview_path = paths.reports_dir / f"ths_preview_{args.date.replace('-', '')}.json"
    dump_json({"trade_date": args.date, "orders": preview}, preview_path)
    print(f"待提交订单数: {len(preview)}")
    print(f"预览文件: {preview_path}")


def handle_ths_submit(args: argparse.Namespace) -> None:
    paths = get_runtime_paths(args)
    due_orders = load_due_orders(paths, args.date)
    adapter = build_ths_adapter(args)
    submit_results = adapter.submit_orders(due_orders, auto_confirm=args.yes)
    output_path = paths.reports_dir / f"ths_submit_{args.date.replace('-', '')}.json"
    dump_json({"trade_date": args.date, "results": submit_results}, output_path)
    print(f"提交结果文件: {output_path}")


def handle_ths_reconcile(args: argparse.Namespace) -> None:
    paths = get_runtime_paths(args)
    signal_service = StrategySignalService()
    state = load_state(paths, initial_capital=signal_service.baseline.INITIAL_CAPITAL)
    due_orders = load_due_orders(paths, args.date)
    adapter = build_ths_adapter(args)
    snapshot = adapter.build_account_snapshot()
    snapshot_path = paths.state_dir / f"account_snapshot_ths_{args.date.replace('-', '')}.json"
    dump_json(snapshot, snapshot_path)

    paper_adapter = PaperExecutionAdapter(paths)
    report = paper_adapter.reconcile_with_state(args.date, state, due_orders, broker_name="ths", broker_snapshot=snapshot)
    report_path = paths.reports_dir / f"reconcile_ths_{args.date.replace('-', '')}.json"
    dump_json(report, report_path)
    print(f"账户快照: {snapshot_path}")
    print(f"对账报告: {report_path}")


def handle_ths_export_pdf(args: argparse.Namespace) -> None:
    paths = get_runtime_paths(args)
    exporter_module = _load_project_module("ths_pdf_exporter_live", "同花顺PDF自动导出.py")
    config = exporter_module.PdfExportConfig(
        exe_path=args.exe_path,
        page=args.page,
        trade_date=args.date,
        printer=args.printer,
        title_re=args.title_re,
        backend=args.backend,
        pdf_root_dir=args.pdf_root_dir or str(paths.state_dir / "pdf_exports"),
        incoming_dir=args.incoming_dir,
        output_dir=str(paths.reports_dir),
        print_dialog_title_re=args.print_dialog_title_re,
        print_dialog_timeout=args.print_dialog_timeout,
        export_timeout=args.export_timeout,
    )
    result = exporter_module.export_pdf(config)
    report_path = exporter_module.write_report(config, result)
    print(f"页面: {result['page']}")
    print(f"成功: {result['success']}")
    if result.get("trigger_method"):
        print(f"打印触发: {result['trigger_method']}")
    if result.get("progress"):
        print("进度:")
        for step in result["progress"]:
            print(f"  - {step}")
    if result.get("target_pdf"):
        print(f"目标文件: {result['target_pdf']}")
    if result.get("error"):
        print(f"错误: {result['error']}")
    if result.get("print_candidates"):
        print("疑似打印入口:")
        for item in result["print_candidates"][:10]:
            print(f"  - {item.get('class_name','')} | {item.get('text','')}")
    print(f"报告文件: {report_path}")
    if result.get("warnings"):
        print("警告:")
        for warning in result["warnings"]:
            print(f"  - {warning}")
    if not result["success"]:
        raise RuntimeError(result.get("error") or "PDF 导出失败")


def main() -> None:
    args = parse_args()
    command_handlers: dict[str, Any] = {
        "refresh-data": lambda: handle_refresh_data(args.check),
        "check-data": handle_check_data,
        "generate-signals": lambda: handle_generate_signals(args),
        "paper-execute": lambda: handle_paper_execute(args),
        "paper-cycle": lambda: handle_paper_cycle(args),
        "preview-orders": lambda: handle_preview_orders(args),
        "ths-preview": lambda: handle_ths_preview(args),
        "ths-submit": lambda: handle_ths_submit(args),
        "ths-reconcile": lambda: handle_ths_reconcile(args),
        "ths-export-pdf": lambda: handle_ths_export_pdf(args),
    }
    command_handlers[args.command]()


if __name__ == "__main__":
    main()
