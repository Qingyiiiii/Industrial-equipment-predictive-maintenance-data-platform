# -*- coding: utf-8 -*-
"""Build P10 warehouse-derived P9 minute features from accepted ODS/DWD/DWS Parquet."""
# 阅读提示：本文件把 P9 CSV-derived 特征升级为 warehouse-derived 特征。
# 输入来自已验收的 ODS/DWD/DWS Parquet，输出单独保存，不覆盖原始 P9 CSV-derived feature table。
# 学习导读：
# - 链路位置：P10 起点，把 P9 特征工程迁移到已验收湖仓数据层。
# - 主要输入：ODS/DWD/DWS Parquet，以及已有 P9 CSV-derived feature table。
# - 主要输出：warehouse-derived minute features、sample、summary 和 parity report。
# - 核心概念：warehouse-derived features 用来证明建模输入可以从正式数据层复现，而不是只依赖本地 CSV。
# - 边界提醒：parity 允许有可解释差异；目标是对齐口径和边界，不是机械追求每个值完全相同。
import argparse
import copy
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from analysis_common import (  # noqa: E402
    ANALOG_SENSORS,
    DIGITAL_SENSORS,
    MODEL_DIR,
    REPORT_DIR,
    ensure_analysis_dirs,
    load_config,
    require_paths_ready,
    write_json,
    write_markdown,
)
from p9_common import build_minute_feature_table, relative_path  # noqa: E402

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from metropt_utils import create_metropt_spark  # noqa: E402


WAREHOUSE_FEATURE_PATH = MODEL_DIR / "p9_window_features_1min_warehouse.parquet"
WAREHOUSE_SAMPLE_PATH = MODEL_DIR / "p9_window_features_1min_warehouse_sample.tsv"
WAREHOUSE_SUMMARY_PATH = MODEL_DIR / "p10_warehouse_feature_summary.json"
PARITY_SUMMARY_PATH = MODEL_DIR / "p10_feature_parity_summary.json"
PARITY_REPORT_PATH = REPORT_DIR / "p9_feature_parity_report.md"
CSV_FEATURE_PATH = MODEL_DIR / "p9_window_features_1min.parquet"

REQUIRED_PATH_KEYS = [
    "ods_readings_parquet",
    "dwd_sensor_long",
    "dws_window_kpi",
    "dws_sensor_kpi",
]

LABEL_COLUMNS = [
    # 这些列保留给 parity 和离线评估使用；后续模型必须显式排除，避免 leakage。
    "failure_window",
    "pre_failure_1h",
    "pre_failure_6h",
    "pre_failure_24h",
    "post_maintenance",
    "normal_candidate",
]

KEY_COMPARE_COLUMNS = [
    "sample_count",
    "failure_window",
    "pre_failure_24h",
    "post_maintenance",
    "normal_candidate",
    "mean_tp2",
    "mean_tp3",
    "mean_reservoirs",
    "mean_oil_temperature",
    "mean_motor_current",
    "mean_delta_tp3_reservoirs",
    "mean_delta_tp2_tp3",
    "state_transition_count",
]


def _spark_config_without_metastore(config: Dict[str, Any]) -> Dict[str, Any]:
    """Use HDFS Parquet directly; P10 does not require Hive metastore for this job."""
    cfg = copy.deepcopy(config)
    # P10 feature builder 直接读 Parquet，不需要 Hive metastore；关闭 Hive/Iceberg 可减少集群依赖面。
    cfg.setdefault("spark", {})["enable_hive_support"] = False
    cfg.setdefault("iceberg", {})["enable"] = False
    return cfg


def _count_rows(df) -> int:
    return int(df.count())


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _spark_dataset_summaries(spark, config: Dict[str, Any]) -> Dict[str, Any]:
    # 源数据摘要用来证明 ODS/DWD/DWS 层级之间的行数、分钟数、sensor 数可以对齐。
    from pyspark.sql import functions as F

    paths = config["paths"]
    ods = spark.read.parquet(paths["ods_readings_parquet"])
    dwd = spark.read.parquet(paths["dwd_sensor_long"])
    dws_window = spark.read.parquet(paths["dws_window_kpi"])
    dws_sensor = spark.read.parquet(paths["dws_sensor_kpi"])

    ods_row = ods.agg(
        F.count("*").alias("row_count"),
        F.countDistinct("event_minute").alias("distinct_event_minutes"),
        F.date_format(F.min("event_time"), "yyyy-MM-dd HH:mm:ss").alias("min_event_time"),
        F.date_format(F.max("event_time"), "yyyy-MM-dd HH:mm:ss").alias("max_event_time"),
        F.sum(F.col("is_failure_window").cast("long")).alias("failure_window_rows"),
    ).collect()[0]
    dws_window_row = dws_window.agg(
        F.count("*").alias("row_count"),
        F.countDistinct("event_minute").alias("distinct_event_minutes"),
        F.sum(F.col("sample_count").cast("long")).alias("sample_count_sum"),
        F.sum(F.col("failure_sample_count").cast("long")).alias("failure_sample_count_sum"),
        F.date_format(F.min("event_minute"), "yyyy-MM-dd HH:mm:ss").alias("min_event_minute"),
        F.date_format(F.max("event_minute"), "yyyy-MM-dd HH:mm:ss").alias("max_event_minute"),
    ).collect()[0]

    return {
        "ods_readings": {
            "path": paths["ods_readings_parquet"],
            "row_count": int(ods_row["row_count"]),
            "distinct_event_minutes": int(ods_row["distinct_event_minutes"]),
            "min_event_time": ods_row["min_event_time"],
            "max_event_time": ods_row["max_event_time"],
            "failure_window_rows": int(ods_row["failure_window_rows"] or 0),
        },
        "dwd_sensor_long": {
            "path": paths["dwd_sensor_long"],
            "row_count": _count_rows(dwd),
            "distinct_sensor_count": int(dwd.select("sensor_name").distinct().count()),
        },
        "dws_window_kpi": {
            "path": paths["dws_window_kpi"],
            "row_count": int(dws_window_row["row_count"]),
            "distinct_event_minutes": int(dws_window_row["distinct_event_minutes"]),
            "sample_count_sum": int(dws_window_row["sample_count_sum"] or 0),
            "failure_sample_count_sum": int(dws_window_row["failure_sample_count_sum"] or 0),
            "min_event_minute": dws_window_row["min_event_minute"],
            "max_event_minute": dws_window_row["max_event_minute"],
        },
        "dws_sensor_kpi": {
            "path": paths["dws_sensor_kpi"],
            "row_count": _count_rows(dws_sensor),
            "distinct_sensor_count": int(dws_sensor.select("sensor_name").distinct().count()),
        },
    }


def _read_ods_to_pandas(spark, config: Dict[str, Any]) -> pd.DataFrame:
    # P10 特征仍复用 P9 的 pandas feature builder，但数据入口改为 accepted warehouse ODS Parquet。
    columns = ["event_time", *ANALOG_SENSORS, *DIGITAL_SENSORS]
    pdf = (
        spark.read.parquet(config["paths"]["ods_readings_parquet"])
        .select(*columns)
        .orderBy("event_time")
        .toPandas()
    )
    pdf["event_time"] = pd.to_datetime(pdf["event_time"])
    return pdf


def _feature_profile(df: pd.DataFrame, source: str) -> Dict[str, Any]:
    # profile 只抽取对 parity 有解释价值的结构和关键统计，避免把大表内容写进报告。
    profile: Dict[str, Any] = {
        "source": source,
        "exists": True,
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "min_event_minute": str(pd.to_datetime(df["event_minute"]).min()) if "event_minute" in df else None,
        "max_event_minute": str(pd.to_datetime(df["event_minute"]).max()) if "event_minute" in df else None,
        "columns": list(df.columns),
        "label_distribution": {},
        "key_statistics": {},
    }
    for label in LABEL_COLUMNS:
        if label in df.columns:
            positives = int(pd.to_numeric(df[label], errors="coerce").fillna(0).sum())
            profile["label_distribution"][label] = {
                "positive_rows": positives,
                "positive_rate": float(positives / len(df)) if len(df) else 0.0,
            }
    for col in KEY_COMPARE_COLUMNS:
        if col in df.columns:
            series = pd.to_numeric(df[col], errors="coerce")
            profile["key_statistics"][col] = {
                "mean": _safe_float(series.mean()),
                "std": _safe_float(series.std()),
                "min": _safe_float(series.min()),
                "max": _safe_float(series.max()),
                "null_rows": int(series.isna().sum()),
            }
    return profile


def _missing_profile(source: str, reason: str) -> Dict[str, Any]:
    return {
        "source": source,
        "exists": False,
        "reason": reason,
        "row_count": 0,
        "column_count": 0,
        "min_event_minute": None,
        "max_event_minute": None,
        "columns": [],
        "label_distribution": {},
        "key_statistics": {},
    }


def _compare_profiles(csv_profile: Dict[str, Any], warehouse_profile: Dict[str, Any], source_summaries: Dict[str, Any]) -> Dict[str, Any]:
    # parity 比较的是两条特征来源的结构、标签分布和关键统计是否可解释地接近。
    row_delta = warehouse_profile["row_count"] - csv_profile.get("row_count", 0)
    column_delta = warehouse_profile["column_count"] - csv_profile.get("column_count", 0)
    csv_cols = set(csv_profile.get("columns", []))
    wh_cols = set(warehouse_profile.get("columns", []))
    labels = {}
    for label in LABEL_COLUMNS:
        csv_label = csv_profile.get("label_distribution", {}).get(label, {})
        wh_label = warehouse_profile.get("label_distribution", {}).get(label, {})
        labels[label] = {
            "csv_positive_rows": csv_label.get("positive_rows"),
            "warehouse_positive_rows": wh_label.get("positive_rows"),
            "positive_row_delta": None
            if csv_label.get("positive_rows") is None or wh_label.get("positive_rows") is None
            else int(wh_label["positive_rows"] - csv_label["positive_rows"]),
            "csv_positive_rate": csv_label.get("positive_rate"),
            "warehouse_positive_rate": wh_label.get("positive_rate"),
        }

    stats = {}
    for col in sorted(set(csv_profile.get("key_statistics", {})) | set(warehouse_profile.get("key_statistics", {}))):
        left = csv_profile.get("key_statistics", {}).get(col, {})
        right = warehouse_profile.get("key_statistics", {}).get(col, {})
        stats[col] = {
            "csv_mean": left.get("mean"),
            "warehouse_mean": right.get("mean"),
            "mean_delta": None
            if left.get("mean") is None or right.get("mean") is None
            else float(right["mean"] - left["mean"]),
            "csv_std": left.get("std"),
            "warehouse_std": right.get("std"),
            "std_delta": None
            if left.get("std") is None or right.get("std") is None
            else float(right["std"] - left["std"]),
        }

    verdicts: List[Dict[str, str]] = []
    if not csv_profile.get("exists"):
        verdicts.append({"status": "WARN", "item": "csv_feature_reference", "detail": csv_profile.get("reason", "CSV-derived feature table is missing.")})
    elif row_delta == 0 and column_delta == 0:
        verdicts.append({"status": "PASS", "item": "shape_parity", "detail": "Warehouse-derived and CSV-derived feature tables have identical row and column counts."})
    else:
        verdicts.append({"status": "WARN", "item": "shape_parity", "detail": f"Shape differs: row_delta={row_delta}, column_delta={column_delta}."})

    if csv_profile.get("exists") and csv_profile.get("min_event_minute") == warehouse_profile.get("min_event_minute") and csv_profile.get("max_event_minute") == warehouse_profile.get("max_event_minute"):
        verdicts.append({"status": "PASS", "item": "time_range_parity", "detail": "Time ranges match exactly."})
    else:
        verdicts.append({"status": "WARN", "item": "time_range_parity", "detail": "Time range differs or CSV reference is unavailable."})

    label_deltas = [item.get("positive_row_delta") for item in labels.values() if item.get("positive_row_delta") is not None]
    if label_deltas and all(delta == 0 for delta in label_deltas):
        verdicts.append({"status": "PASS", "item": "label_distribution_parity", "detail": "P9 label positive-row counts match exactly."})
    else:
        verdicts.append({"status": "WARN", "item": "label_distribution_parity", "detail": "Label distribution differs or CSV reference is unavailable."})

    ods_minutes = source_summaries.get("ods_readings", {}).get("distinct_event_minutes")
    if ods_minutes == warehouse_profile["row_count"]:
        verdicts.append({"status": "PASS", "item": "ods_minute_alignment", "detail": "Warehouse feature row count matches ODS distinct event_minute count."})
    else:
        verdicts.append({"status": "FAIL", "item": "ods_minute_alignment", "detail": f"Warehouse feature rows={warehouse_profile['row_count']} but ODS distinct minutes={ods_minutes}."})

    dws_sample_count = source_summaries.get("dws_window_kpi", {}).get("sample_count_sum")
    ods_rows = source_summaries.get("ods_readings", {}).get("row_count")
    if dws_sample_count == ods_rows:
        verdicts.append({"status": "PASS", "item": "dws_sample_count_alignment", "detail": "DWS window sample_count sum matches ODS row count."})
    else:
        verdicts.append({"status": "WARN", "item": "dws_sample_count_alignment", "detail": f"DWS sample_count_sum={dws_sample_count}; ODS rows={ods_rows}."})

    return {
        "row_delta": row_delta,
        "column_delta": column_delta,
        "missing_in_warehouse": sorted(csv_cols - wh_cols),
        "extra_in_warehouse": sorted(wh_cols - csv_cols),
        "label_distribution": labels,
        "key_statistics": stats,
        "verdicts": verdicts,
    }


def _overall_status(verdicts: List[Dict[str, str]]) -> str:
    if any(row["status"] == "FAIL" for row in verdicts):
        return "FAIL"
    if any(row["status"] == "WARN" for row in verdicts):
        return "PASS_WITH_WARNINGS"
    return "PASS"


def _markdown(payload: Dict[str, Any]) -> str:
    verdict_lines = "\n".join(f"| {row['item']} | {row['status']} | {row['detail']} |" for row in payload["parity"]["verdicts"])
    source_lines = "\n".join(
        f"| {name} | `{item['path']}` | {item.get('row_count')} | {item.get('distinct_event_minutes', item.get('distinct_sensor_count', ''))} |"
        for name, item in payload["warehouse_sources"].items()
    )
    label_lines = "\n".join(
        f"| {label} | {item.get('csv_positive_rows')} | {item.get('warehouse_positive_rows')} | {item.get('positive_row_delta')} |"
        for label, item in payload["parity"]["label_distribution"].items()
    )
    stat_lines = "\n".join(
        f"| {col} | {item.get('csv_mean')} | {item.get('warehouse_mean')} | {item.get('mean_delta')} |"
        for col, item in payload["parity"]["key_statistics"].items()
    )
    return f"""# P9 Feature Parity Report

## Scope

- Run id: `{payload['run_id']}`.
- Overall status: `{payload['overall_status']}`.
- Warehouse feature table: `{payload['warehouse_feature']['path']}`.
- Warehouse feature sample: `{payload['warehouse_feature']['sample_path']}`.
- CSV-derived reference: `{payload['csv_feature']['path']}`.

This report upgrades P9 feature generation from CSV-derived analysis to warehouse-derived analysis by reading accepted HDFS ODS/DWD/DWS Parquet paths. It does not overwrite the original P9 CSV-derived feature table.

## Warehouse Sources

| Source | Path | Rows | Distinct count |
| --- | --- | ---: | ---: |
{source_lines}

## Feature Shape

| Source | Rows | Columns | Min event minute | Max event minute |
| --- | ---: | ---: | --- | --- |
| CSV-derived | {payload['csv_feature']['row_count']} | {payload['csv_feature']['column_count']} | {payload['csv_feature']['min_event_minute']} | {payload['csv_feature']['max_event_minute']} |
| warehouse-derived | {payload['warehouse_feature']['row_count']} | {payload['warehouse_feature']['column_count']} | {payload['warehouse_feature']['min_event_minute']} | {payload['warehouse_feature']['max_event_minute']} |

## Parity Checks

| Check | Status | Detail |
| --- | --- | --- |
{verdict_lines}

## Label Distribution

| Label | CSV positive rows | Warehouse positive rows | Delta |
| --- | ---: | ---: | ---: |
{label_lines}

## Key Statistic Mean Comparison

| Column | CSV mean | Warehouse mean | Delta |
| --- | ---: | ---: | ---: |
{stat_lines}

## Decision Boundary

- Warehouse-derived P9 feature table is stored separately from the P9 CSV-derived feature table.
- Label columns are retained for evaluation and parity checks only.
- P10 model reruns must explicitly exclude `failure_window`, `pre_failure_*`, `post_maintenance`, `normal_candidate`, and `rul_seconds` from model features.
"""


def main() -> None:
    # 主流程先检查 warehouse 路径，再生成独立特征表和 parity 报告，便于和 CSV-derived 结果并排复核。
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--reuse-existing",
        action="store_true",
        help="Skip warehouse feature rebuild when the feature table and parity summary already exist.",
    )
    args = parser.parse_args()
    if args.reuse_existing and WAREHOUSE_FEATURE_PATH.exists() and PARITY_SUMMARY_PATH.exists() and PARITY_REPORT_PATH.exists():
        print("P10 warehouse feature builder reuse-existing enabled.")
        print("warehouse_feature_table:", WAREHOUSE_FEATURE_PATH)
        print("parity_json:", PARITY_SUMMARY_PATH)
        print("parity_report:", PARITY_REPORT_PATH)
        return

    started = datetime.now()
    ensure_analysis_dirs()
    config = load_config()
    spark_config = _spark_config_without_metastore(config)
    spark = create_metropt_spark("MetroPT_P10_Warehouse_P9_Features", config=spark_config, enable_hive_support=False)
    spark.sparkContext.setLogLevel("WARN")
    try:
        require_paths_ready(spark, spark_config, REQUIRED_PATH_KEYS)
        source_summaries = _spark_dataset_summaries(spark, spark_config)
        ods_pdf = _read_ods_to_pandas(spark, spark_config)
    finally:
        spark.stop()

    # 关键设计：warehouse-derived 仍复用 P9 feature builder，保证特征定义不因数据源迁移而漂移。
    minute_features = build_minute_feature_table(ods_pdf)
    minute_features.to_parquet(WAREHOUSE_FEATURE_PATH, index=False)
    minute_features.head(500).to_csv(WAREHOUSE_SAMPLE_PATH, sep="\t", index=False)

    warehouse_profile = _feature_profile(minute_features, "warehouse_ods_hdfs")
    if CSV_FEATURE_PATH.exists():
        csv_profile = _feature_profile(pd.read_parquet(CSV_FEATURE_PATH), "csv_derived")
        csv_profile["path"] = relative_path(CSV_FEATURE_PATH)
    else:
        csv_profile = _missing_profile("csv_derived", f"Missing reference table: {relative_path(CSV_FEATURE_PATH)}")
        csv_profile["path"] = relative_path(CSV_FEATURE_PATH)

    warehouse_profile["path"] = relative_path(WAREHOUSE_FEATURE_PATH)
    warehouse_profile["sample_path"] = relative_path(WAREHOUSE_SAMPLE_PATH)
    parity = _compare_profiles(csv_profile, warehouse_profile, source_summaries)
    overall = _overall_status(parity["verdicts"])

    finished = datetime.now()
    # payload 把 source summaries 和 parity verdicts 放在一起，便于从 P14 报告追溯到 P10 证据。
    payload: Dict[str, Any] = {
        "run_id": started.strftime("%Y%m%d_%H%M%S"),
        "started": started.isoformat(timespec="seconds"),
        "finished": finished.isoformat(timespec="seconds"),
        "elapsed_seconds": int((finished - started).total_seconds()),
        "overall_status": overall,
        "config": os.environ.get("METROPT_CONFIG", "default config"),
        "warehouse_sources": source_summaries,
        "csv_feature": csv_profile,
        "warehouse_feature": warehouse_profile,
        "parity": parity,
        "artifacts": {
            "warehouse_feature_table": relative_path(WAREHOUSE_FEATURE_PATH),
            "warehouse_feature_sample": relative_path(WAREHOUSE_SAMPLE_PATH),
            "warehouse_summary_json": relative_path(WAREHOUSE_SUMMARY_PATH),
            "parity_summary_json": relative_path(PARITY_SUMMARY_PATH),
            "parity_report": relative_path(PARITY_REPORT_PATH),
        },
    }
    write_json(WAREHOUSE_SUMMARY_PATH, {"run_id": payload["run_id"], "warehouse_feature": warehouse_profile, "warehouse_sources": source_summaries})
    write_json(PARITY_SUMMARY_PATH, payload)
    write_markdown(PARITY_REPORT_PATH, _markdown(payload))

    print("P10 warehouse feature builder completed.")
    print("overall_status:", overall)
    print("warehouse_feature_table:", WAREHOUSE_FEATURE_PATH)
    print("warehouse_feature_sample:", WAREHOUSE_SAMPLE_PATH)
    print("parity_json:", PARITY_SUMMARY_PATH)
    print("parity_report:", PARITY_REPORT_PATH)


if __name__ == "__main__":
    main()
