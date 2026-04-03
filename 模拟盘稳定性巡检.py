from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_RUNTIME_ROOT = PROJECT_ROOT / "outputs" / "股票策略交易执行"
DEFAULT_EXE_PATH = r"D:\量化\同花顺\同花顺\xiadan.exe"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="模拟盘稳定性巡检")
    parser.add_argument("--date", required=True, help="巡检日期，例如 2025-03-07")
    parser.add_argument("--rounds", type=int, default=3, help="重复轮数，默认 3")
    parser.add_argument("--runtime-root", default=str(DEFAULT_RUNTIME_ROOT), help="交易运行目录")
    parser.add_argument("--exe-path", default=DEFAULT_EXE_PATH, help="同花顺下单端 exe 路径")
    parser.add_argument("--client-type", default="ths", help="easytrader client type")
    parser.add_argument("--python-exe", default=sys.executable, help="调用主流程脚本的 Python 解释器")
    parser.add_argument("--sleep-seconds", type=float, default=2.0, help="每步之间等待秒数")
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any] | list[Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def build_command(
    python_exe: str,
    runtime_root: str,
    action: str,
    trade_date: str,
    client_type: str,
    exe_path: str,
) -> list[str]:
    return [
        python_exe,
        str(PROJECT_ROOT / "股票策略交易主流程.py"),
        "--runtime-root",
        runtime_root,
        action,
        "--date",
        trade_date,
        "--client-type",
        client_type,
        "--exe-path",
        exe_path,
    ]


def run_step(
    python_exe: str,
    runtime_root: str,
    action: str,
    trade_date: str,
    client_type: str,
    exe_path: str,
) -> dict[str, Any]:
    cmd = build_command(python_exe, runtime_root, action, trade_date, client_type, exe_path)
    started_at = time.strftime("%Y-%m-%d %H:%M:%S")
    completed = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    finished_at = time.strftime("%Y-%m-%d %H:%M:%S")
    return {
        "action": action,
        "started_at": started_at,
        "finished_at": finished_at,
        "command": cmd,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def summarize_files(runtime_root: Path, trade_date: str) -> dict[str, Any]:
    date_tag = trade_date.replace("-", "")
    snapshot_path = runtime_root / "state" / f"account_snapshot_ths_{date_tag}.json"
    reconcile_path = runtime_root / "reports" / f"reconcile_ths_{date_tag}.json"
    preview_path = runtime_root / "reports" / f"ths_preview_{date_tag}.json"

    snapshot = load_json(snapshot_path)
    reconcile = load_json(reconcile_path)
    preview = load_json(preview_path)

    summary: dict[str, Any] = {
        "snapshot_path": str(snapshot_path),
        "reconcile_path": str(reconcile_path),
        "preview_path": str(preview_path),
    }
    if isinstance(snapshot, dict):
        summary["snapshot"] = {
            "cash": snapshot.get("cash"),
            "position_count": len(snapshot.get("positions", [])),
            "entrust_count": len(snapshot.get("today_entrusts", [])),
            "trade_count": len(snapshot.get("today_trades", [])),
        }
    if isinstance(reconcile, dict):
        summary["reconcile"] = {
            "broker_cash": reconcile.get("broker_cash"),
            "local_cash": reconcile.get("local_cash"),
            "position_diff_count": reconcile.get("position_diff_count"),
            "due_order_count": reconcile.get("due_order_count"),
        }
    if isinstance(preview, dict):
        summary["preview"] = {
            "order_count": len(preview.get("orders", [])),
        }
    return summary


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def print_step_result(step: dict[str, Any], summary: dict[str, Any]) -> None:
    print(f"[{step['action']}] exit={step['returncode']}")
    stdout = step["stdout"].strip()
    stderr = step["stderr"].strip()
    if stdout:
        print(stdout)
    if stderr:
        print(stderr)
    if summary.get("snapshot"):
        snap = summary["snapshot"]
        print(
            f"  snapshot cash={snap['cash']} positions={snap['position_count']} "
            f"entrusts={snap['entrust_count']} trades={snap['trade_count']}"
        )
    if summary.get("preview"):
        print(f"  preview orders={summary['preview']['order_count']}")
    if summary.get("reconcile"):
        rec = summary["reconcile"]
        print(
            f"  reconcile broker_cash={rec['broker_cash']} local_cash={rec['local_cash']} "
            f"diffs={rec['position_diff_count']} due_orders={rec['due_order_count']}"
        )


def main() -> None:
    args = parse_args()
    runtime_root = Path(args.runtime_root)
    runtime_root.mkdir(parents=True, exist_ok=True)
    (runtime_root / "logs").mkdir(parents=True, exist_ok=True)
    log_path = runtime_root / "logs" / f"sim_check_{args.date.replace('-', '')}.jsonl"

    flow = ["ths-reconcile", "ths-preview", "ths-reconcile"]
    print("=" * 72)
    print("模拟盘稳定性巡检")
    print(f"日期: {args.date}")
    print(f"轮数: {args.rounds}")
    print(f"运行目录: {runtime_root}")
    print(f"客户端路径: {args.exe_path}")
    print(f"日志文件: {log_path}")
    print("=" * 72)

    all_ok = True
    for round_idx in range(1, args.rounds + 1):
        print(f"\n[Round {round_idx}/{args.rounds}]")
        for step_idx, action in enumerate(flow, start=1):
            print(f"  [{step_idx}/{len(flow)}] {action}")
            step = run_step(
                python_exe=args.python_exe,
                runtime_root=str(runtime_root),
                action=action,
                trade_date=args.date,
                client_type=args.client_type,
                exe_path=args.exe_path,
            )
            summary = summarize_files(runtime_root, args.date)
            record = {
                "round": round_idx,
                "step_index": step_idx,
                "step": action,
                "result": step,
                "summary": summary,
            }
            append_jsonl(log_path, record)
            print_step_result(step, summary)
            if step["returncode"] != 0:
                all_ok = False
            if step_idx < len(flow):
                time.sleep(args.sleep_seconds)

    print("\n" + "=" * 72)
    print("巡检完成")
    print(f"整体状态: {'SUCCESS' if all_ok else 'FAILED'}")
    print(f"日志文件: {log_path}")
    print("=" * 72)


if __name__ == "__main__":
    main()
