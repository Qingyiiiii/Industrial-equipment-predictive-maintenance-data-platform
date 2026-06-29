# -*- coding: utf-8 -*-
"""Preflight checks for the MetroPT-3 offline pipeline."""
# 阅读提示：本文件是离线链路的第一道门禁，只做“能否继续跑”的判断。
# 它不生成业务数据，而是提前暴露配置、CSV 字段、failure window 和 Spark/HDFS 可读性问题。
# 学习导读：
# - 链路位置：src/00，是所有离线任务之前的 preflight gate。
# - 主要输入：METROPT_CONFIG 指向的配置、Raw CSV 路径、failure_windows 配置和 Spark/HDFS 环境。
# - 主要输出：控制台检查结论；失败时直接退出，避免下游脚本在更深的位置报难读的错。
# - 核心概念：这里校验的是“运行前条件”，不是数据质量报告，也不是业务验收报告。
# - 边界提醒：preflight 通过只说明可以继续跑，不代表 ODS/DWD/DWS/Hive 已经成功产出。
import csv
import os
import sys
from datetime import datetime
from typing import Iterable, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from metropt_utils import (  # noqa: E402
    ANALOG_SENSORS,
    DIGITAL_SENSORS,
    RAW_TO_STANDARD,
    assert_path_exists,
    create_metropt_spark,
    is_distributed_path,
    load_metropt_config,
    parse_failure_windows,
    to_local_path,
)


EXPECTED_RAW_COLUMNS = ["timestamp", "DV_eletric"]


def _failures(messages: Iterable[str]) -> None:
    issues = list(messages)
    if not issues:
        return
    print("MetroPT preflight failed:")
    for item in issues:
        print(f"  - {item}")
    raise SystemExit(1)


def _parse_ts(value: str, label: str) -> None:
    try:
        datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        raise ValueError(f"{label} 时间格式错误: {value}") from exc


def _is_raw_index_header(col_name: str) -> bool:
    """Accept raw CSV empty-index headers after local csv or Spark parsing."""
    stripped = str(col_name or "").strip()
    return stripped == "" or stripped.startswith("_c") or stripped.lower().startswith("unnamed")


def _check_failure_windows(config: dict) -> List[str]:
    # failure window 是后续弱标签、DWS KPI 和模型分析的共同口径；
    # 这里先校验格式和时间可解析性，避免下游在 Spark 作业中才失败。
    issues: List[str] = []
    try:
        windows = parse_failure_windows(config)
    except Exception as exc:
        return [f"failure_windows 无法解析: {exc}"]
    if not windows:
        issues.append("metropt.failure_windows 为空，故障窗口标签将全部为 normal")
    for start, end, failure_type in windows:
        try:
            _parse_ts(start, "start")
            _parse_ts(end, "end")
        except ValueError as exc:
            issues.append(str(exc))
        if not failure_type:
            issues.append(f"failure_type 为空: {start}|{end}|{failure_type}")
    return issues


def _read_local_header(input_csv: str) -> List[str]:
    with open(to_local_path(input_csv), "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        return next(reader)


def _check_local_csv(input_csv: str) -> List[str]:
    issues: List[str] = []
    if not os.path.exists(to_local_path(input_csv)):
        return [f"本地 CSV 不存在: {input_csv}"]
    try:
        header = _read_local_header(input_csv)
    except Exception as exc:
        return [f"CSV header 读取失败: {exc}"]

    if not header:
        issues.append("CSV header 为空")
        return issues
    if not _is_raw_index_header(header[0]):
        issues.append(f"CSV 第一列应为空索引列，实际为: {header[0]}")
    for col_name in EXPECTED_RAW_COLUMNS:
        if col_name not in header:
            issues.append(f"CSV header 缺少原始字段: {col_name}")

    known_raw = set(RAW_TO_STANDARD)
    sensor_raw_missing = []
    for sensor_name in ANALOG_SENSORS + DIGITAL_SENSORS:
        # 这里用 standard sensor name 反查 raw header，确保原始 CSV 能映射到项目统一字段。
        if sensor_name == "dv_electric":
            if "DV_eletric" not in header and "DV_electric" not in header:
                sensor_raw_missing.append("DV_eletric/DV_electric")
            continue
        raw_names = [raw for raw, standard in RAW_TO_STANDARD.items() if standard == sensor_name]
        if not any(raw in header for raw in raw_names):
            sensor_raw_missing.append(sensor_name)
    if sensor_raw_missing:
        issues.append(f"CSV header 缺少传感器字段: {sensor_raw_missing}")

    unexpected = [c for c in header[1:] if c not in known_raw]
    if unexpected:
        issues.append(f"CSV header 存在未映射字段: {unexpected}")
    return issues


def _check_config(config: dict) -> List[str]:
    # 配置检查聚焦“项目能否按统一口径运行”：输入路径、输出层级、Hive/Iceberg
    # 命名空间和传感器字段都必须先对齐，后续脚本才可以复用同一份 config。
    issues: List[str] = []
    paths = config.get("paths", {})
    spark_cfg = config.get("spark", {})
    hive_cfg = config.get("hive", {})
    iceberg_cfg = config.get("iceberg", {})

    required_paths = [
        "input_csv",
        "profile_dir",
        "ods_readings_parquet",
        "dwd_sensor_long",
        "dws_overall_kpi",
        "dws_window_kpi",
        "dws_sensor_kpi",
    ]
    for key in required_paths:
        if not paths.get(key):
            issues.append(f"paths.{key} 为空")

    mode = str(spark_cfg.get("mode", "local")).lower()
    if mode not in {"local", "yarn"}:
        issues.append(f"spark.mode 只建议使用 local/yarn，实际为: {mode}")
    if int(spark_cfg.get("shuffle_partitions", 0) or 0) <= 0:
        issues.append("spark.shuffle_partitions 必须大于 0")

    if spark_cfg.get("enable_hive_support", False):
        if not hive_cfg.get("database"):
            issues.append("Hive 开启时 hive.database 不能为空")
        if not hive_cfg.get("metastore_uris"):
            issues.append("Hive 开启时 hive.metastore_uris 不能为空")
    if bool(iceberg_cfg.get("enable", False)):
        if not iceberg_cfg.get("catalog"):
            issues.append("Iceberg 开启时 iceberg.catalog 不能为空")
        if not iceberg_cfg.get("database"):
            issues.append("Iceberg 开启时 iceberg.database 不能为空")
        if not hive_cfg.get("metastore_uris"):
            issues.append("Iceberg hive catalog 需要 hive.metastore_uris")
    return issues


def main() -> None:
    # 主流程按本地配置 -> failure window -> 本地 CSV -> Spark/HDFS 的顺序检查。
    # 这个顺序便于先发现低成本问题，再进入可能需要集群资源的 Spark 读取。
    config_path = os.environ.get("METROPT_CONFIG")
    config = load_metropt_config()
    paths = config.get("paths", {})
    input_csv = str(paths.get("input_csv", ""))
    spark_cfg = config.get("spark", {})
    issues = []

    print("MetroPT preflight")
    print("config:", config_path or "default local config")
    print("spark.mode:", spark_cfg.get("mode", "local"))
    print("input_csv:", input_csv)

    issues.extend(_check_config(config))
    issues.extend(_check_failure_windows(config))

    spark = None
    try:
        if input_csv and is_distributed_path(input_csv):
            # 分布式路径检查必须通过 Spark/Hadoop FileSystem；本地 os.path 无法判断 hdfs:// 是否存在。
            spark = create_metropt_spark("MetroPT_00_Preflight", config=config)
            spark.sparkContext.setLogLevel("WARN")
            assert_path_exists(spark, input_csv, "MetroPT-3 CSV")
            raw = spark.read.option("header", True).csv(input_csv).limit(1)
            cols = raw.columns
            if not cols:
                issues.append("HDFS CSV header 为空或文件不可读")
            if cols and not _is_raw_index_header(cols[0]):
                issues.append(f"HDFS CSV 第一列应为空索引列，实际为: {cols[0]}")
            for col_name in EXPECTED_RAW_COLUMNS:
                if col_name not in cols:
                    issues.append(f"HDFS CSV header 缺少原始字段: {col_name}")
        elif input_csv:
            issues.extend(_check_local_csv(input_csv))
        else:
            issues.append("paths.input_csv 为空")
    except Exception as exc:
        issues.append(f"输入数据检查失败: {exc}")
    finally:
        if spark is not None:
            spark.stop()

    _failures(issues)
    print("MetroPT preflight passed.")
    print("analog_sensors:", ",".join(ANALOG_SENSORS))
    print("digital_sensors:", ",".join(DIGITAL_SENSORS))


if __name__ == "__main__":
    main()
