# -*- coding: utf-8 -*-
"""Run MetroPT analysis jobs with per-step logs."""
# 阅读提示：本文件是 analysis/00-03 的轻量 runner，用于统一日志、步骤裁剪和失败即停。
# P9/P10 后续脚本仍建议按交付记录单独运行或由新的 runner 显式纳入。
# 学习导读：
# - 链路位置：基础分析 runner，只串联 analysis/00 到 analysis/03。
# - 主要输入：start-at/stop-after、执行器选择、是否 require-full。
# - 主要输出：analysis/logs/<run_id>/ 下的 step log 和 analysis_run_summary.tsv。
# - 核心概念：runner 负责复现和排错证据，不负责新增分析结论。
# - 边界提醒：P9/P10 脚本没有自动纳入这个 runner，不能误以为跑完它就完成全部建模链路。
import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from analysis_common import LOG_DIR, elapsed_text, ensure_analysis_dirs, load_config, now_run_id, uses_distributed_paths  # noqa: E402


ANALYSIS_DIR = Path(__file__).resolve().parent
DEFAULT_STEPS = [
    # 默认只覆盖基础 analysis 链路；P9/P10 属于后续建模与 warehouse parity 扩展。
    "00_validate_analysis_inputs.py",
    "01_data_quality_analysis.py",
    "02_multidim_analysis.py",
    "03_model_baseline.py",
]


def _resolve_steps(start_at: str, stop_after: str) -> List[str]:
    # start-at/stop-after 只在默认步骤内裁剪，避免误把未登记脚本静默加入运行链路。
    steps = list(DEFAULT_STEPS)
    if start_at:
        if start_at not in steps:
            raise ValueError(f"Unknown start-at step: {start_at}")
        steps = steps[steps.index(start_at) :]
    if stop_after:
        if stop_after not in steps:
            raise ValueError(f"Unknown stop-after step: {stop_after}")
        steps = steps[: steps.index(stop_after) + 1]
    return steps


def _auto_step_executor() -> str:
    """Use spark-submit for analysis when the active config points at HDFS-like paths."""
    try:
        config = load_config()
    except Exception:
        return "python"
    return "spark-submit" if uses_distributed_paths(config) else "python"


def _command_for(step: str, python_exec: str, spark_submit: str, step_executor: str, require_full: bool) -> List[str]:
    command = [spark_submit if step_executor == "spark-submit" else python_exec, str(ANALYSIS_DIR / step)]
    if step == "00_validate_analysis_inputs.py" and require_full:
        command.append("--require-full")
    return command


def main() -> None:
    # 主流程为每个子步骤写独立 log 和 summary TSV，便于失败时从具体 step 继续复现。
    parser = argparse.ArgumentParser()
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--spark-submit", default=os.environ.get("SPARK_SUBMIT", "spark-submit"))
    parser.add_argument("--log-dir", default=str(LOG_DIR))
    parser.add_argument("--start-at", default="")
    parser.add_argument("--stop-after", default="")
    parser.add_argument(
        "--step-executor",
        choices=["auto", "python", "spark-submit"],
        default="auto",
        help="auto uses spark-submit for HDFS configs and python for local configs.",
    )
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="Do not force ODS/DWD/DWS readiness during the validation step.",
    )
    args = parser.parse_args()

    ensure_analysis_dirs()
    steps = _resolve_steps(args.start_at, args.stop_after)
    step_executor = args.step_executor
    if step_executor == "auto":
        # 有 HDFS-like 路径时自动走 spark-submit；纯本地分析可以用当前 Python 进程启动子脚本。
        step_executor = _auto_step_executor()
    run_id = now_run_id()
    log_dir = Path(args.log_dir) / run_id
    log_dir.mkdir(parents=True, exist_ok=True)
    require_full = not args.allow_partial

    print("MetroPT analysis runner")
    print("run_id:", run_id)
    print("config:", os.environ.get("METROPT_CONFIG", "default local config"))
    print("log_dir:", log_dir)
    print("require_full:", require_full)
    print("step_executor:", step_executor)
    print("steps:", ", ".join(steps))

    summary_rows = []
    child_env = os.environ.copy()
    # runner 不依赖 .pyc 缓存，禁用字节码写入可减少说明阶段目录噪音。
    child_env["PYTHONDONTWRITEBYTECODE"] = "1"
    for step in steps:
        # 每个分析步骤独立 log，失败后可以按 start-at 从该步骤继续，而不是重跑全部。
        step_start = datetime.now()
        log_path = log_dir / f"{Path(step).stem}.log"
        cmd = _command_for(step, args.python, args.spark_submit, step_executor, require_full=require_full)
        print(f"[START] {step} -> {log_path}")
        with open(log_path, "w", encoding="utf-8") as log_file:
            log_file.write(f"step={step}\n")
            log_file.write(f"start={step_start.isoformat(timespec='seconds')}\n")
            log_file.write(f"command={' '.join(cmd)}\n\n")
            log_file.flush()
            proc = subprocess.run(
                cmd,
                cwd=str(ANALYSIS_DIR.parents[0]),
                env=child_env,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
            )
            step_end = datetime.now()
            log_file.write("\n")
            log_file.write(f"end={step_end.isoformat(timespec='seconds')}\n")
            log_file.write(f"elapsed={elapsed_text(step_start, step_end)}\n")
            log_file.write(f"return_code={proc.returncode}\n")

        row = {
            "step": step,
            "start": step_start.isoformat(timespec="seconds"),
            "end": step_end.isoformat(timespec="seconds"),
            "elapsed": elapsed_text(step_start, step_end),
            "return_code": proc.returncode,
            "log": str(log_path),
        }
        summary_rows.append(row)
        print(f"[DONE] {step} rc={proc.returncode} elapsed={row['elapsed']}")
        if proc.returncode != 0:
            # 失败即停能保护后续报告：上游分析不完整时，不继续制造半成品结论。
            print(f"[STOP] {step} failed. Downstream analysis steps were not run.")
            break

    summary_path = log_dir / "analysis_run_summary.tsv"
    with open(summary_path, "w", encoding="utf-8", newline="\n") as f:
        # summary TSV 是 analysis runner 的第一排错入口，和离线 runner 的 offline_run_summary.tsv 对齐。
        f.write("step\tstart\tend\telapsed\treturn_code\tlog\n")
        for row in summary_rows:
            f.write(
                "\t".join(
                    [
                        row["step"],
                        row["start"],
                        row["end"],
                        row["elapsed"],
                        str(row["return_code"]),
                        row["log"],
                    ]
                )
                + "\n"
            )
    print("summary:", summary_path)
    if any(row["return_code"] != 0 for row in summary_rows):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
