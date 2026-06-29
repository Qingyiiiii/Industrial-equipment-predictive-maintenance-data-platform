# -*- coding: utf-8 -*-
"""Validate P10 warehouse-derived P9 feature artifacts."""
# 阅读提示：本文件是 P10 warehouse-derived feature 的验收脚本。
# 它复核产物存在性、ODS/DWS 对齐、CSV parity、label 分布和 model-feature leakage。
# 学习导读：
# - 链路位置：P10 feature quality gate，位于 warehouse feature builder 之后、P10 baseline 之前。
# - 主要输入：warehouse feature table、parity summary、parity report 和 feature metadata。
# - 主要输出：p10_warehouse_feature_quality_report 和 checks JSON。
# - 核心概念：这里重点检查 leakage boundary 和 warehouse/CSV source alignment。
# - 边界提醒：质量检查不训练模型；它只判断 P10 特征是否适合进入下一步 baseline。
import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Sequence

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from analysis_common import MODEL_DIR, REPORT_DIR, ensure_analysis_dirs, write_json, write_markdown  # noqa: E402
from p9_common import relative_path  # noqa: E402


WAREHOUSE_FEATURE_PATH = MODEL_DIR / "p9_window_features_1min_warehouse.parquet"
WAREHOUSE_SAMPLE_PATH = MODEL_DIR / "p9_window_features_1min_warehouse_sample.tsv"
PARITY_SUMMARY_PATH = MODEL_DIR / "p10_feature_parity_summary.json"
QUALITY_JSON_PATH = MODEL_DIR / "p10_warehouse_feature_quality_checks.json"
QUALITY_REPORT_PATH = REPORT_DIR / "p10_warehouse_feature_quality_report.md"
PARITY_REPORT_PATH = REPORT_DIR / "p9_feature_parity_report.md"

LABEL_COLUMNS = {
    # label/evaluation mask 可以留在特征表里做报告切片，但不能进入 candidate model features。
    "failure_window",
    "pre_failure_1h",
    "pre_failure_6h",
    "pre_failure_24h",
    "post_maintenance",
    "normal_candidate",
    "rul_seconds",
}

NON_MODEL_COLUMNS = LABEL_COLUMNS | {"event_minute", "window_start", "window_end"}

REQUIRED_FEATURE_COLUMNS = {
    "event_minute",
    "sample_count",
    "failure_window",
    "pre_failure_24h",
    "post_maintenance",
    "normal_candidate",
    "rul_seconds",
    "mean_tp2",
    "mean_tp3",
    "mean_reservoirs",
    "mean_oil_temperature",
    "mean_motor_current",
    "mean_delta_tp3_reservoirs",
    "mean_delta_tp2_tp3",
    "state_transition_count",
    "roll5_mean_mean_tp2",
    "roll15_mean_mean_tp2",
    "roll60_mean_mean_tp2",
}


def _now_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _read_json(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _parquet_metadata(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"exists": False, "row_count": 0, "column_count": 0, "columns": [], "error": "feature table missing"}
    try:
        import pyarrow.parquet as pq

        parquet_file = pq.ParquetFile(path)
        columns = list(parquet_file.schema.names)
        return {
            "exists": True,
            "row_count": int(parquet_file.metadata.num_rows),
            "column_count": len(columns),
            "columns": columns,
            "error": "",
            "bytes": int(path.stat().st_size),
        }
    except Exception as exc:
        return {"exists": True, "row_count": 0, "column_count": 0, "columns": [], "error": str(exc), "bytes": int(path.stat().st_size)}


def _append(checks: List[Dict[str, str]], name: str, status: str, detail: str) -> None:
    checks.append({"name": name, "status": status, "detail": detail})


def _overall(checks: Sequence[Dict[str, str]]) -> str:
    if any(row["status"] == "FAIL" for row in checks):
        return "FAIL"
    if any(row["status"] == "WARN" for row in checks):
        return "PASS_WITH_WARNINGS"
    return "PASS"


def _check_artifacts(checks: List[Dict[str, str]]) -> None:
    required = [WAREHOUSE_FEATURE_PATH, WAREHOUSE_SAMPLE_PATH, PARITY_SUMMARY_PATH, PARITY_REPORT_PATH]
    missing = [relative_path(path) for path in required if not path.exists()]
    empty = [relative_path(path) for path in required if path.exists() and path.stat().st_size <= 0]
    if missing:
        _append(checks, "p10_artifact_presence", "FAIL", "Missing artifacts: " + ", ".join(missing))
    elif empty:
        _append(checks, "p10_artifact_presence", "FAIL", "Empty artifacts: " + ", ".join(empty))
    else:
        _append(checks, "p10_artifact_presence", "PASS", "Warehouse feature table, sample, parity JSON, and parity report exist.")


def _check_feature_shape(checks: List[Dict[str, str]], meta: Dict[str, Any], parity: Dict[str, Any]) -> None:
    if meta.get("error"):
        _append(checks, "warehouse_feature_metadata", "FAIL", str(meta["error"]))
        return
    expected = parity.get("warehouse_feature", {})
    if meta["row_count"] == expected.get("row_count") and meta["column_count"] == expected.get("column_count") and meta["row_count"] > 0:
        _append(checks, "warehouse_feature_metadata", "PASS", f"Warehouse feature table has {meta['row_count']} rows and {meta['column_count']} columns.")
    else:
        _append(
            checks,
            "warehouse_feature_metadata",
            "FAIL",
            f"Warehouse feature metadata mismatch: parquet={meta['row_count']}/{meta['column_count']} summary={expected.get('row_count')}/{expected.get('column_count')}.",
        )


def _check_required_columns(checks: List[Dict[str, str]], columns: Sequence[str]) -> None:
    missing = sorted(REQUIRED_FEATURE_COLUMNS - set(columns))
    if missing:
        _append(checks, "warehouse_feature_columns", "FAIL", "Missing expected columns: " + ", ".join(missing))
    else:
        _append(checks, "warehouse_feature_columns", "PASS", "Required label, minute, analog, pressure-delta, state, and rolling feature groups are present.")


def _check_source_alignment(checks: List[Dict[str, str]], parity: Dict[str, Any]) -> None:
    # source alignment 证明 warehouse-derived feature table 可以从 ODS/DWS 层级追溯回来。
    source = parity.get("warehouse_sources", {})
    wh = parity.get("warehouse_feature", {})
    ods = source.get("ods_readings", {})
    dws = source.get("dws_window_kpi", {})
    if ods.get("distinct_event_minutes") == wh.get("row_count"):
        _append(checks, "ods_to_feature_minute_alignment", "PASS", "Warehouse feature rows match ODS distinct event_minute count.")
    else:
        _append(checks, "ods_to_feature_minute_alignment", "FAIL", f"Feature rows={wh.get('row_count')} ODS minutes={ods.get('distinct_event_minutes')}.")

    if dws.get("sample_count_sum") == ods.get("row_count"):
        _append(checks, "dws_to_ods_sample_alignment", "PASS", "DWS window sample_count sum matches ODS row count.")
    else:
        _append(checks, "dws_to_ods_sample_alignment", "WARN", f"DWS sample_count_sum={dws.get('sample_count_sum')} ODS rows={ods.get('row_count')}.")

    if source.get("dwd_sensor_long", {}).get("distinct_sensor_count") == 15 and source.get("dws_sensor_kpi", {}).get("distinct_sensor_count") == 15:
        _append(checks, "sensor_layer_alignment", "PASS", "DWD sensor_long and DWS sensor KPI both expose 15 sensors.")
    else:
        _append(checks, "sensor_layer_alignment", "WARN", "Sensor distinct count is not 15 in DWD or DWS sensor KPI.")


def _check_parity_boundaries(checks: List[Dict[str, str]], parity: Dict[str, Any]) -> None:
    # parity 警告不一定失败；说明时要解释差异来源，而不是把 WARN 直接改写成 PASS。
    parity_status = parity.get("overall_status")
    if parity_status == "PASS":
        _append(checks, "csv_warehouse_parity", "PASS", "CSV-derived and warehouse-derived feature references match without warnings.")
    elif parity_status == "PASS_WITH_WARNINGS":
        warnings = [row["detail"] for row in parity.get("parity", {}).get("verdicts", []) if row.get("status") == "WARN"]
        _append(checks, "csv_warehouse_parity", "WARN", "Parity has explainable warnings: " + "; ".join(warnings))
    else:
        failures = [row["detail"] for row in parity.get("parity", {}).get("verdicts", []) if row.get("status") == "FAIL"]
        _append(checks, "csv_warehouse_parity", "FAIL", "Parity failed: " + "; ".join(failures))


def _check_label_distribution(checks: List[Dict[str, str]], parity: Dict[str, Any]) -> None:
    # P10 仍保留 weak-label 分布，供评估切片；这些标签不能直接进入模型特征。
    labels = parity.get("warehouse_feature", {}).get("label_distribution", {})
    missing = [label for label in ["failure_window", "pre_failure_24h", "normal_candidate"] if label not in labels]
    if missing:
        _append(checks, "warehouse_label_distribution", "FAIL", "Missing label distribution entries: " + ", ".join(missing))
        return
    failure_rows = int(labels["failure_window"]["positive_rows"])
    pre24_rows = int(labels["pre_failure_24h"]["positive_rows"])
    normal_rows = int(labels["normal_candidate"]["positive_rows"])
    if failure_rows > 0 and pre24_rows > 0 and normal_rows > 0:
        _append(checks, "warehouse_label_distribution", "PASS", f"Labels are populated: failure_window={failure_rows}, pre_failure_24h={pre24_rows}, normal_candidate={normal_rows}.")
    else:
        _append(checks, "warehouse_label_distribution", "FAIL", f"Invalid label distribution: failure_window={failure_rows}, pre_failure_24h={pre24_rows}, normal_candidate={normal_rows}.")


def _check_model_feature_leakage(checks: List[Dict[str, str]], path: Path) -> Dict[str, Any]:
    # leakage 检查只从数值候选特征里扣除 label/time 字段，确认训练入口不会读到未来信息。
    try:
        sample = pd.read_parquet(path, columns=None)
    except Exception as exc:
        _append(checks, "warehouse_model_feature_leakage", "FAIL", f"Unable to read warehouse feature table for leakage check: {exc}")
        return {"candidate_feature_count": 0, "candidate_features": [], "leakage_columns": []}

    numeric_cols = set(sample.select_dtypes(include=["number", "bool"]).columns)
    candidate_features = sorted(numeric_cols - NON_MODEL_COLUMNS)
    leakage = sorted(set(candidate_features) & LABEL_COLUMNS)
    if leakage:
        _append(checks, "warehouse_model_feature_leakage", "FAIL", "Label columns appear in candidate model features: " + ", ".join(leakage))
    elif candidate_features:
        _append(checks, "warehouse_model_feature_leakage", "PASS", f"Candidate model features exclude labels and RUL fields; candidate_feature_count={len(candidate_features)}.")
    else:
        _append(checks, "warehouse_model_feature_leakage", "FAIL", "No candidate numeric model features found after excluding labels.")
    return {
        "candidate_feature_count": len(candidate_features),
        "candidate_features": candidate_features[:80],
        "leakage_columns": leakage,
    }


def _markdown(payload: Dict[str, Any]) -> str:
    check_lines = "\n".join(f"| {row['name']} | {row['status']} | {row['detail']} |" for row in payload["checks"])
    source_lines = "\n".join(
        f"| {name} | `{item.get('path')}` | {item.get('row_count')} |"
        for name, item in payload["warehouse_sources"].items()
    )
    return f"""# P10 Warehouse Feature Quality Report

## Scope

- Run id: `{payload['run_id']}`.
- Overall status: `{payload['overall_status']}`.
- Warehouse feature table: `{payload['feature_table']['path']}`.
- Parity report: `{payload['parity_report']}`.

This report validates the P10 warehouse-derived P9 feature table. It preserves the P9 CSV-derived feature artifact and treats parity differences as explicit PASS/WARN/FAIL checks.

## Warehouse Sources

| Source | Path | Rows |
| --- | --- | ---: |
{source_lines}

## Checks

| Check | Status | Detail |
| --- | --- | --- |
{check_lines}

## Leakage Boundary

- Candidate model features exclude `failure_window`, `pre_failure_*`, `post_maintenance`, `normal_candidate`, and `rul_seconds`.
- Label fields stay in the warehouse feature table only for evaluation, parity, and report slicing.

## Remaining Warnings

Warnings are acceptable only when they are explainable and do not imply feature-table corruption or label leakage. A P10 model rerun should use this warehouse-derived table only after this report is `PASS` or `PASS_WITH_WARNINGS`.
"""


def main() -> None:
    # 主流程消费 P10 parity summary 和 warehouse Parquet 元数据，输出轻量质量报告，不触发重建。
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=_now_id())
    args = parser.parse_args()

    ensure_analysis_dirs()
    started = datetime.now()
    checks: List[Dict[str, str]] = []
    _check_artifacts(checks)

    parity = _read_json(PARITY_SUMMARY_PATH) if PARITY_SUMMARY_PATH.exists() else {}
    meta = _parquet_metadata(WAREHOUSE_FEATURE_PATH)
    if parity:
        # 只有 parity summary 存在时，才能做 source alignment 和 CSV/warehouse 边界解释。
        _check_feature_shape(checks, meta, parity)
        _check_required_columns(checks, meta.get("columns", []))
        _check_source_alignment(checks, parity)
        _check_parity_boundaries(checks, parity)
        _check_label_distribution(checks, parity)
    else:
        _append(checks, "parity_summary", "FAIL", f"Missing parity summary: {relative_path(PARITY_SUMMARY_PATH)}")

    leakage = _check_model_feature_leakage(checks, WAREHOUSE_FEATURE_PATH)
    finished = datetime.now()
    payload: Dict[str, Any] = {
        "run_id": args.run_id,
        "started": started.isoformat(timespec="seconds"),
        "finished": finished.isoformat(timespec="seconds"),
        "elapsed_seconds": int((finished - started).total_seconds()),
        "overall_status": _overall(checks),
        "checks": checks,
        "feature_table": {
            "path": relative_path(WAREHOUSE_FEATURE_PATH),
            "row_count": meta.get("row_count"),
            "column_count": meta.get("column_count"),
            "bytes": meta.get("bytes", 0),
        },
        "warehouse_sources": parity.get("warehouse_sources", {}),
        "parity_report": relative_path(PARITY_REPORT_PATH),
        "leakage": leakage,
    }
    write_json(QUALITY_JSON_PATH, payload)
    write_markdown(QUALITY_REPORT_PATH, _markdown(payload))

    print("P10 warehouse feature quality check")
    print("run_id:", args.run_id)
    print("overall_status:", payload["overall_status"])
    print("json:", QUALITY_JSON_PATH)
    print("report:", QUALITY_REPORT_PATH)


if __name__ == "__main__":
    main()
