# -*- coding: utf-8 -*-
"""Run the MetroPT-3 offline pipeline with per-step logs."""
# 阅读提示：本文件是离线链路统一 runner。
# 它不实现业务转换，而是串联 00->06 脚本、记录每步日志和 return_code，方便复现与排错。
# 学习导读：
# - 链路位置：离线链路总入口，负责调度 src/00 到 src/06。
# - 主要输入：start-at/stop-after 参数、Python/spark-submit 执行器、METROPT_CONFIG。
# - 主要输出：data/metropt_quality/logs/<run_id>/ 下的每步 log 和 offline_run_summary.tsv。
# - 核心概念：runner 只管“按顺序运行和记录证据”，不改变任何业务转换规则。
# - 边界提醒：某一步失败时先看 summary 和对应 log，不要直接跳过失败步骤继续下游。
import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List


ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from metropt_utils import is_distributed_path, load_metropt_config  # noqa: E402

DEFAULT_STEPS = [
    # 默认顺序对应离线数据层级：preflight -> profile -> ODS -> DWD -> DWS -> Hive/Iceberg -> BI views。
    # start-at / stop-after 只裁剪这个列表，不改变单个脚本的行为。
    "00_metropt_preflight.py",
    "01_metropt_profile.py",
    "02_metropt_csv_to_parquet.py",
    "03_metropt_dwd_sensor_long.py",
    "04_metropt_kpi_calc.py",
    "05_metropt_to_hive_iceberg.py",
    "06_metropt_hive_views.py",
]


def _now_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _elapsed(start: datetime, end: datetime) -> str:
    seconds = int((end - start).total_seconds())
    return f"{seconds // 3600:02d}:{seconds % 3600 // 60:02d}:{seconds % 60:02d}"


def _resolve_steps(start_at: str, stop_after: str) -> List[str]:
    # 允许从失败步骤继续或只跑到指定层级；
    # 这对集群排错很重要，因为不需要每次都重跑完整链路。
    steps = list(DEFAULT_STEPS)
    if start_at:
        if start_at not in steps:
            raise ValueError(f"未知 start-at: {start_at}")
        steps = steps[steps.index(start_at) :]
    if stop_after:
        if stop_after not in steps:
            raise ValueError(f"未知 stop-after: {stop_after}")
        steps = steps[: steps.index(stop_after) + 1]
    return steps


def _auto_preflight_with_spark() -> bool:
    """Use spark-submit for preflight when the active config points at YARN/HDFS."""
    try:
        config = load_metropt_config()
    except Exception:
        return False
    spark_mode = str(config.get("spark", {}).get("mode", "local")).lower()
    input_csv = str(config.get("paths", {}).get("input_csv", "") or "")
    return spark_mode == "yarn" or is_distributed_path(input_csv)


def _command_for(step: str, spark_submit: str, python_exec: str, preflight_executor: str) -> List[str]:
    script = str(SRC_DIR / step)
    if step.startswith("00_"):
        if preflight_executor == "spark-submit":
            return [spark_submit, script]
        return [python_exec, script]
    return [spark_submit, script]


def main() -> None:
    # 主流程为每个 step 创建独立 log，并在 offline_run_summary.tsv 中写入耗时和返回码；
    # 读者复现失败时应先看 summary，再打开失败步骤对应日志。
    parser = argparse.ArgumentParser()
    parser.add_argument("--spark-submit", default=os.environ.get("SPARK_SUBMIT", "spark-submit"))
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--log-dir", default=str(ROOT_DIR / "data" / "metropt_quality" / "logs"))
    parser.add_argument("--start-at", default="")
    parser.add_argument("--stop-after", default="")
    parser.add_argument(
        "--preflight-executor",
        choices=["auto", "python", "spark-submit"],
        default="auto",
        help="auto uses spark-submit for YARN/HDFS configs and python for local configs.",
    )
    args = parser.parse_args()

    steps = _resolve_steps(args.start_at, args.stop_after)
    preflight_executor = args.preflight_executor
    if preflight_executor == "auto":
        # preflight 在本地配置下可以直接 python 跑；遇到 HDFS/YARN 路径时要走 spark-submit 才能读分布式路径。
        preflight_executor = "spark-submit" if _auto_preflight_with_spark() else "python"
    run_id = _now_id()
    log_dir = Path(args.log_dir) / run_id
    log_dir.mkdir(parents=True, exist_ok=True)

    print("MetroPT offline runner")
    print("run_id:", run_id)
    print("config:", os.environ.get("METROPT_CONFIG", "default local config"))
    print("log_dir:", log_dir)
    print("preflight_executor:", preflight_executor)
    print("steps:", ", ".join(steps))

    summary_rows = []
    for step in steps:
        # 每一步单独记录 log，保证失败时可以把问题缩小到一个脚本，而不是翻整段终端输出。
        step_start = datetime.now()
        log_path = log_dir / f"{Path(step).stem}.log"
        cmd = _command_for(step, args.spark_submit, args.python, preflight_executor)
        print(f"[START] {step} -> {log_path}")
        with open(log_path, "w", encoding="utf-8") as log_file:
            log_file.write(f"step={step}\n")
            log_file.write(f"start={step_start.isoformat(timespec='seconds')}\n")
            log_file.write(f"command={' '.join(cmd)}\n\n")
            log_file.flush()
            proc = subprocess.run(cmd, cwd=str(ROOT_DIR), stdout=log_file, stderr=subprocess.STDOUT, text=True)
            step_end = datetime.now()
            log_file.write("\n")
            log_file.write(f"end={step_end.isoformat(timespec='seconds')}\n")
            log_file.write(f"elapsed={_elapsed(step_start, step_end)}\n")
            log_file.write(f"return_code={proc.returncode}\n")
        row = {
            "step": step,
            "start": step_start.isoformat(timespec="seconds"),
            "end": step_end.isoformat(timespec="seconds"),
            "elapsed": _elapsed(step_start, step_end),
            "return_code": proc.returncode,
            "log": str(log_path),
        }
        summary_rows.append(row)
        print(f"[DONE] {step} rc={proc.returncode} elapsed={row['elapsed']}")
        if proc.returncode != 0:
            # 失败即停是为了保护数据层级：上游产物不可信时，下游继续运行只会制造更难解释的错误。
            print(f"[STOP] {step} failed. Downstream steps were not run.")
            break

    summary_path = log_dir / "offline_run_summary.tsv"
    with open(summary_path, "w", encoding="utf-8") as f:
        # summary 是排错入口：先看 return_code 和 log 路径，再打开具体 step log。
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
