# -*- coding: utf-8 -*-
"""Validate P9 feature-engineering artifacts without rerunning heavy jobs."""
# 阅读提示：本文件是 P9 产物验收脚本，用于检查已经生成的 artifacts，而不是重跑特征或模型。
# 它把 artifact presence、feature table、leakage、time split、metrics 统一汇总成 PASS/WARN/FAIL。
# 学习导读：
# - 链路位置：P9 产物质量闸门，通常用于复盘或 P14 中确认 P9 artifacts 是否完整。
# - 主要输入：P9 reports、models、feature table、metrics 和 feature dictionary。
# - 主要输出：p9_feature_quality_report 和 checks JSON。
# - 核心概念：质量检查只复核已经存在的产物，不消耗时间重跑重任务。
# - 边界提醒：PASS 表示 P9 artifacts 自洽，不等于模型可生产部署。
import argparse
import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from analysis_common import (  # noqa: E402
    FIGURE_DIR,
    LOG_DIR,
    MODEL_DIR,
    REPORT_DIR,
    collect_input_status,
    ensure_analysis_dirs,
    load_config,
    missing_full_inputs,
    write_json,
    write_markdown,
)
from p9_common import ROOT_DIR, relative_path  # noqa: E402


LABEL_COLUMNS = {
    # 这些列是 label/evaluation mask，质量检查必须确认它们没有进入 model features。
    "failure_window",
    "pre_failure_1h",
    "pre_failure_6h",
    "pre_failure_24h",
    "post_maintenance",
    "normal_candidate",
    "rul_seconds",
}

EXPECTED_REPORTS = [
    REPORT_DIR / "p9_sensor_dictionary.md",
    REPORT_DIR / "p9_label_system.md",
    REPORT_DIR / "p9_eda_report.md",
    REPORT_DIR / "p9_feature_dictionary.md",
    REPORT_DIR / "p9_model_baseline_report.md",
]

EXPECTED_MODELS = [
    MODEL_DIR / "p9_label_summary.json",
    MODEL_DIR / "p9_label_summary.tsv",
    MODEL_DIR / "p9_feature_eda_summary.json",
    MODEL_DIR / "p9_feature_dictionary.tsv",
    MODEL_DIR / "p9_window_features_1min.parquet",
    MODEL_DIR / "p9_window_features_1min_sample.tsv",
    MODEL_DIR / "p9_model_metrics.json",
    MODEL_DIR / "p9_logistic_feature_weights.tsv",
    MODEL_DIR / "p9_model_prediction_sample.tsv",
]

EXPECTED_FIGURES = [
    FIGURE_DIR / "p9_daily_sample_failure_trend.png",
    FIGURE_DIR / "p9_pre_failure_sensor_delta.png",
    FIGURE_DIR / "p9_sensor_correlation_heatmap.png",
    FIGURE_DIR / "p9_pressure_current_oil_fault_timeline.png",
    FIGURE_DIR / "p9_state_transition_frequency.png",
    FIGURE_DIR / "p9_baseline_confusion_matrices.png",
    FIGURE_DIR / "p9_logistic_feature_weights.png",
    FIGURE_DIR / "p9_risk_score_timeline.png",
]


def _now_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _elapsed(start: datetime, end: datetime) -> str:
    seconds = int((end - start).total_seconds())
    return f"{seconds // 3600:02d}:{seconds % 3600 // 60:02d}:{seconds % 60:02d}"


def _display_path(path: Any) -> str:
    text = str(path)
    if text.startswith("<WORKER_PROJECT_ROOT>"):
        return text.replace("\\", "/")
    try:
        candidate = Path(text)
    except Exception:
        return text
    try:
        return "<WORKER_PROJECT_ROOT>/" + str(candidate.resolve().relative_to(ROOT_DIR)).replace("\\", "/")
    except Exception:
        return text.replace("\\", "/")


def _display_input_status(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    sanitized = []
    for row in rows:
        item = dict(row)
        item["path"] = _display_path(item.get("path", ""))
        sanitized.append(item)
    return sanitized


def _read_json(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _read_tsv(path: Path) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def _artifact_rows(paths: Sequence[Path], artifact_type: str) -> List[Dict[str, Any]]:
    rows = []
    for path in paths:
        rows.append(
            {
                "artifact": relative_path(path),
                "type": artifact_type,
                "exists": path.exists(),
                "bytes": path.stat().st_size if path.exists() else 0,
            }
        )
    return rows


def _parquet_metadata(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"exists": False, "row_count": 0, "column_count": 0, "columns": [], "error": "feature table does not exist"}
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
        }
    except Exception as exc:
        return {"exists": True, "row_count": 0, "column_count": 0, "columns": [], "error": str(exc)}


def _append_check(checks: List[Dict[str, Any]], name: str, status: str, detail: str) -> None:
    checks.append({"name": name, "status": status, "detail": detail})


def _check_artifacts(checks: List[Dict[str, Any]], artifacts: Sequence[Dict[str, Any]]) -> None:
    missing = [row["artifact"] for row in artifacts if not row["exists"]]
    empty = [row["artifact"] for row in artifacts if row["exists"] and row["bytes"] <= 0]
    if missing:
        _append_check(checks, "p9_artifact_presence", "FAIL", "Missing artifacts: " + ", ".join(missing))
    elif empty:
        _append_check(checks, "p9_artifact_presence", "FAIL", "Empty artifacts: " + ", ".join(empty))
    else:
        _append_check(checks, "p9_artifact_presence", "PASS", f"Checked {len(artifacts)} required P9 artifacts; all exist and are non-empty.")


def _check_feature_table(checks: List[Dict[str, Any]], feature_meta: Dict[str, Any], eda_summary: Dict[str, Any]) -> None:
    if feature_meta.get("error"):
        _append_check(checks, "feature_table_metadata", "FAIL", feature_meta["error"])
        return
    # Parquet 元数据要和 EDA summary 对齐，证明报告和特征表来自同一轮产物。
    expected_rows = int(eda_summary.get("minute_feature_rows", 0) or 0)
    expected_cols = len(eda_summary.get("feature_columns", []) or [])
    rows_match = feature_meta["row_count"] == expected_rows
    cols_match = feature_meta["column_count"] == expected_cols
    if rows_match and cols_match and feature_meta["row_count"] > 0:
        _append_check(
            checks,
            "feature_table_metadata",
            "PASS",
            f"Feature table has {feature_meta['row_count']} rows and {feature_meta['column_count']} columns, matching EDA summary.",
        )
    else:
        _append_check(
            checks,
            "feature_table_metadata",
            "FAIL",
            f"Feature table rows/columns mismatch: actual={feature_meta['row_count']}/{feature_meta['column_count']}, expected={expected_rows}/{expected_cols}.",
        )


def _check_feature_groups(checks: List[Dict[str, Any]], columns: Iterable[str]) -> None:
    cols = set(columns)
    required = {
        "event_minute",
        "sample_count",
        "mean_tp2",
        "mean_tp3",
        "mean_oil_temperature",
        "mean_motor_current",
        "mean_delta_tp3_reservoirs",
        "mean_delta_tp2_tp3",
        "active_count_dv_electric",
        "toggle_count_comp",
        "state_transition_count",
        "roll5_mean_mean_tp2",
        "roll15_mean_mean_tp2",
        "roll60_mean_mean_tp2",
    }
    missing = sorted(required - cols)
    if missing:
        _append_check(checks, "feature_group_coverage", "FAIL", "Missing expected feature columns: " + ", ".join(missing))
    else:
        _append_check(checks, "feature_group_coverage", "PASS", "Minute, rolling, pressure-delta, digital activity, and state-transition feature groups are present.")


def _check_leakage_controls(checks: List[Dict[str, Any]], metrics: Dict[str, Any], feature_dict_rows: Sequence[Dict[str, str]]) -> None:
    # 这里同时检查“模型实际使用列”和“字典里是否标注风险”，一个管执行，一个管说明文档。
    model_features = set(metrics.get("features", []) or [])
    leakage_features = sorted(model_features & LABEL_COLUMNS)
    if leakage_features:
        _append_check(checks, "model_feature_leakage", "FAIL", "Label columns are used as model features: " + ", ".join(leakage_features))
    else:
        _append_check(checks, "model_feature_leakage", "PASS", "Model feature list excludes P9 labels and RUL fields.")

    label_rows = [
        row
        for row in feature_dict_rows
        if "failure_window" in row.get("feature_pattern", "") and row.get("leakage_risk", "").lower() == "yes"
    ]
    if label_rows:
        _append_check(checks, "feature_dictionary_leakage_flag", "PASS", "Feature dictionary marks labels and RUL as leakage-risk target/grouping fields.")
    else:
        _append_check(checks, "feature_dictionary_leakage_flag", "FAIL", "Feature dictionary does not mark label fields as leakage-risk fields.")


def _check_time_split(checks: List[Dict[str, Any]], metrics: Dict[str, Any]) -> None:
    split = metrics.get("split", {})
    try:
        train_max = datetime.fromisoformat(split["train_max_event_minute"])
        validation_min = datetime.fromisoformat(split["validation_min_event_minute"])
        validation_max = datetime.fromisoformat(split["validation_max_event_minute"])
        test_min = datetime.fromisoformat(split["test_min_event_minute"])
    except Exception as exc:
        _append_check(checks, "chronological_split", "FAIL", f"Unable to parse split timestamps: {exc}")
        return
    if train_max < validation_min and validation_max < test_min:
        # 时间切分严格递增是 P9 baseline 可解释性的底线。
        _append_check(checks, "chronological_split", "PASS", "Train, validation, and test ranges are strictly chronological.")
    else:
        _append_check(checks, "chronological_split", "FAIL", f"Split ranges overlap or are out of order: {split}")


def _check_model_metrics(checks: List[Dict[str, Any]], metrics: Dict[str, Any]) -> None:
    required = {"precision", "recall", "f1", "pr_auc", "false_alarms_per_day", "confusion_matrix"}
    failures = []
    trained_models = []
    skipped_models = []
    for name, payload in (metrics.get("models", {}) or {}).items():
        if payload.get("status") != "trained":
            if payload.get("status") == "skipped":
                skipped_models.append(f"{name}: {payload.get('reason', 'no reason recorded')}")
            continue
        trained_models.append(name)
        test_metrics = payload.get("test_metrics", {})
        missing = sorted(required - set(test_metrics))
        if missing:
            failures.append(f"{name}: missing {missing}")
    if failures:
        _append_check(checks, "trained_model_metrics", "FAIL", "; ".join(failures))
    elif trained_models:
        detail = (
            "Trained fallback baseline models "
            f"({', '.join(trained_models)}) include precision, recall, F1, PR-AUC, false alarms/day, and confusion matrix."
        )
        if skipped_models:
            detail += " Skipped sklearn models: " + "; ".join(skipped_models) + "."
        _append_check(checks, "trained_model_metrics", "PASS", detail)
    else:
        _append_check(checks, "trained_model_metrics", "FAIL", "No trained P9 baseline model is recorded.")


def _overall_status(checks: Sequence[Dict[str, Any]]) -> str:
    if any(row["status"] == "FAIL" for row in checks):
        return "FAIL"
    if any(row["status"] == "WARN" for row in checks):
        return "PASS_WITH_WARNINGS"
    return "PASS"


def _markdown(payload: Dict[str, Any]) -> str:
    check_lines = "\n".join(f"| {row['name']} | {row['status']} | {row['detail']} |" for row in payload["checks"])
    input_lines = "\n".join(
        f"| `{row['key']}` | {'OK' if row['exists'] else 'MISSING'} | `{_display_path(row['path'])}` | {row.get('producer', '')} |"
        for row in payload["input_status"]
    )
    artifact_lines = "\n".join(
        f"| `{row['artifact']}` | {row['type']} | {'yes' if row['exists'] else 'no'} | {row['bytes']} |"
        for row in payload["artifacts"]
    )
    master_lines = "\n".join(f"- {item}" for item in payload["master_validation"])
    return f"""# P9 Feature Quality Report

## Scope

- Role: Worker node offline data engineer.
- Run id: `{payload['run_id']}`.
- Overall status: `{payload['overall_status']}`.
- Feature table: `{payload['feature_table']['path']}`.
- Feature rows/columns: `{payload['feature_table']['row_count']}` / `{payload['feature_table']['column_count']}`.
- Log path: `{payload['log_path']}`.

This report validates worker-local P9 feature artifacts. It does not prove master Spark/Hive/ODS/DWD/DWS parity; those items remain 待 master 验证.

## Input Status

| Config key | Status | Path | Upstream producer |
| --- | --- | --- | --- |
{input_lines}

## Checks

| Check | Status | Detail |
| --- | --- | --- |
{check_lines}

## Artifact Manifest View

| Artifact | Type | Exists | Bytes |
| --- | --- | ---: | ---: |
{artifact_lines}

## Offline Engineering Notes

- `src/00` to `src/06` were reviewed as P0-P8 main-chain scripts; no P9 change was made to the accepted main chain.
- P9 feature generation remains in `analysis/`, so it does not overwrite ODS/DWD/DWS/Hive/Iceberg outputs.
- Standard `python -m compileall src analysis` is blocked by existing `__pycache__` replace permissions on this worker copy. Syntax was validated with `compile(...)` without writing `.pyc`.
- Local full-analysis Parquet inputs are missing in this worker copy. Existing P9 features were generated directly from the full CSV and should be regenerated or compared on master.
- The large `p9_window_features_1min.parquet` file is reproducible and may be omitted from cloud sync if size is a concern.

## Master Validation Required

{master_lines}
"""


def main() -> None:
    # 主流程读取现有 JSON/TSV/Parquet 元数据并写入日志；任何 WARN 都要保留原始原因，不能当作完全通过。
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=_now_id(), help="Stable run id for the local quality-check log directory.")
    args = parser.parse_args()

    started = datetime.now()
    ensure_analysis_dirs()
    log_dir = LOG_DIR / args.run_id
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "07_p9_feature_quality_check.log"

    log_lines: List[str] = []

    def log(message: str) -> None:
        print(message)
        log_lines.append(message)

    try:
        log("P9 feature quality check")
        log(f"run_id: {args.run_id}")
        log(f"started: {started.isoformat(timespec='seconds')}")

        config = load_config()
        raw_input_status = collect_input_status(config, spark=None)
        missing_inputs = missing_full_inputs(raw_input_status)
        input_status = _display_input_status(raw_input_status)

        artifacts = (
            _artifact_rows(EXPECTED_REPORTS, "report")
            + _artifact_rows(EXPECTED_MODELS, "model_or_metadata")
            + _artifact_rows(EXPECTED_FIGURES, "figure")
        )

        eda_summary = _read_json(MODEL_DIR / "p9_feature_eda_summary.json")
        metrics = _read_json(MODEL_DIR / "p9_model_metrics.json")
        feature_dict_rows = _read_tsv(MODEL_DIR / "p9_feature_dictionary.tsv")
        feature_table_path = MODEL_DIR / "p9_window_features_1min.parquet"
        feature_meta = _parquet_metadata(feature_table_path)

        checks: List[Dict[str, Any]] = []
        # 检查顺序从 artifact presence 到 leakage/time split/metrics，先保证产物在，再解释模型。
        _check_artifacts(checks, artifacts)
        _check_feature_table(checks, feature_meta, eda_summary)
        _check_feature_groups(checks, feature_meta.get("columns", []))
        _check_leakage_controls(checks, metrics, feature_dict_rows)
        _check_time_split(checks, metrics)
        _check_model_metrics(checks, metrics)

        if missing_inputs:
            _append_check(
                checks,
                "local_full_analysis_inputs",
                "WARN",
                "ODS/DWD/DWS Parquet inputs are missing locally; P9 CSV-derived artifacts require master parity validation.",
            )
        else:
            _append_check(checks, "local_full_analysis_inputs", "PASS", "ODS/DWD/DWS Parquet inputs are present locally.")

        finished = datetime.now()
        payload: Dict[str, Any] = {
            "run_id": args.run_id,
            "started": started.isoformat(timespec="seconds"),
            "finished": finished.isoformat(timespec="seconds"),
            "elapsed": _elapsed(started, finished),
            "overall_status": _overall_status(checks),
            "config": os.environ.get("METROPT_CONFIG", "default local config"),
            "input_status": input_status,
            "artifacts": artifacts,
            "checks": checks,
            "feature_table": {
                "path": relative_path(feature_table_path),
                "row_count": feature_meta.get("row_count", 0),
                "column_count": feature_meta.get("column_count", 0),
                "bytes": feature_table_path.stat().st_size if feature_table_path.exists() else 0,
            },
            "model_target": metrics.get("target"),
            "model_feature_count": metrics.get("feature_count"),
            "master_validation": [
                "Run or compare `analysis/05_p9_feature_engineering.py` on master after ODS/DWD/DWS parity is confirmed.",
                "Check whether P9 features should remain CSV-derived analysis artifacts or be rebuilt from master ODS/DWS Parquet.",
        "If master has a working scikit-learn runtime, rerun `analysis/06_p9_model_experiments.py` to train Random Forest and Isolation Forest baselines; otherwise keep them explicitly skipped with the recorded reason.",
                "Keep all cluster/Hive/Spark conclusions as pending until master writes final validation evidence.",
            ],
            "log_path": relative_path(log_path),
        }

        json_path = write_json(MODEL_DIR / "p9_feature_quality_checks.json", payload)
        report_path = write_markdown(REPORT_DIR / "p9_feature_quality_report.md", _markdown(payload))
        log(f"overall_status: {payload['overall_status']}")
        log(f"report: {_display_path(report_path)}")
        log(f"json: {_display_path(json_path)}")
        log(f"finished: {finished.isoformat(timespec='seconds')}")
        log(f"elapsed: {payload['elapsed']}")
    except Exception as exc:
        finished = datetime.now()
        log(f"status: FAIL")
        log(f"error: {exc}")
        log(f"finished: {finished.isoformat(timespec='seconds')}")
        log(f"elapsed: {_elapsed(started, finished)}")
        raise
    finally:
        with open(log_path, "w", encoding="utf-8", newline="\n") as f:
            for line in log_lines:
                f.write(line.rstrip() + "\n")


if __name__ == "__main__":
    main()
