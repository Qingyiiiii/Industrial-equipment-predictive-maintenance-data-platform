# -*- coding: utf-8 -*-
"""Validate MetroPT analysis inputs and report missing upstream datasets."""
# 阅读提示：本文件是 analysis 链路的入口检查器。
# 它不生成特征或模型，只判断 Raw/ODS/DWD/DWS 等上游产物是否足够支撑后续分析。
# - 链路位置：analysis/00，是所有分析、P9/P10 特征和模型脚本之前的输入门禁。
# - 主要输入：配置中的 Raw/ODS/DWD/DWS 路径，以及本地或 HDFS 的实际文件状态。
# - 主要输出：analysis_input_validation*.json 和控制台 PASS/WARN/FAIL 信息。
# - 边界提醒：输入检查通过不代表分析结论正确，只代表后续脚本具备读取条件。
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from analysis_common import (  # noqa: E402
    REPORT_DIR,
    build_missing_inputs_message,
    collect_input_status,
    create_metropt_spark,
    ensure_analysis_dirs,
    load_config,
    missing_full_inputs,
    uses_distributed_paths,
    write_json,
)


def main() -> None:
    # 主流程根据路径类型决定是否需要 Spark/HDFS 检查；--require-full 用于 runner 场景强制阻断缺失输入。
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--require-full",
        action="store_true",
        help="Exit non-zero when ODS/DWD/DWS analysis inputs are incomplete.",
    )
    args = parser.parse_args()

    ensure_analysis_dirs()
    config = load_config()
    spark = None
    try:
        if uses_distributed_paths(config):
            # 只有配置里出现 HDFS-like 路径时才启动 Spark，纯本地检查不需要集群 session。
            spark = create_metropt_spark("MetroPT_Analysis_00_Validate_Inputs", config=config)
            spark.sparkContext.setLogLevel("WARN")
        statuses = collect_input_status(config, spark=spark)
    finally:
        if spark is not None:
            spark.stop()

    missing = missing_full_inputs(statuses)
    ready = not missing
    # full_analysis_ready 只代表分析输入齐全，不代表后续 EDA/模型已执行成功。
    payload = {
        "config": os.environ.get("METROPT_CONFIG", "default local config"),
        "full_analysis_ready": ready,
        "checks": statuses,
        "missing_full_inputs": missing,
    }
    report_path = write_json(REPORT_DIR / "analysis_input_validation.json", payload)

    print("MetroPT analysis input validation")
    print("config:", payload["config"])
    print("report:", report_path)
    for item in statuses:
        marker = "OK" if item["exists"] else "MISSING"
        error = f" | {item['check_error']}" if item.get("check_error") else ""
        print(f"[{marker}] {item['label']}: {item['path']}{error}")

    if missing:
        message = build_missing_inputs_message(missing)
        print("")
        print(message)
        if args.require_full:
            # runner 场景用非零退出阻断后续步骤，交互学习时可不加 --require-full 只看诊断。
            raise SystemExit(1)
    else:
        print("Full MetroPT analysis inputs are ready.")


if __name__ == "__main__":
    main()
