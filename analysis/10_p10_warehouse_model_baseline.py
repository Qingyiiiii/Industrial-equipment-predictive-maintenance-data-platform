# -*- coding: utf-8 -*-
"""Run P10 baseline models on warehouse-derived P9 features and compare with CSV-derived features."""
# 阅读提示：本文件在 warehouse-derived P9 features 上重跑 baseline，并和 CSV-derived baseline 做指标对比。
# 它仍然是 weak-label baseline，不是生产 ML 模型，也不会把模型接入在线 Flink 链路。
# 学习导读：
# - 链路位置：P10 模型验证脚本，把正式湖仓来源特征用于 baseline 并与 P9 CSV baseline 对比。
# - 主要输入：CSV-derived features、warehouse-derived features、P9 模型模块和 active config。
# - 主要输出：P10 metrics、CSV reference metrics、comparison TSV/JSON/Markdown 和官方报告。
# - 核心概念：比较的重点是特征来源迁移后的模型行为是否可解释、边界是否一致。
# - 边界提醒：P10 仍是 weak-label baseline，不会把模型部署到 P11 realtime risk job。
import argparse
import importlib.util
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib

matplotlib.use("Agg")
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from analysis_common import MODEL_DIR, REPORT_DIR, ensure_analysis_dirs, write_json, write_markdown  # noqa: E402
from p9_common import active_config, relative_path, write_tsv  # noqa: E402


ROOT_DIR = Path(__file__).resolve().parents[1]
P9_MODEL_SCRIPT = Path(__file__).resolve().parent / "06_p9_model_experiments.py"

CSV_FEATURE_PATH = MODEL_DIR / "p9_window_features_1min.parquet"
WAREHOUSE_FEATURE_PATH = MODEL_DIR / "p9_window_features_1min_warehouse.parquet"

OFFICIAL_METRICS_PATH = MODEL_DIR / "p9_model_metrics.json"
OFFICIAL_REPORT_PATH = REPORT_DIR / "p9_model_baseline_report.md"
WAREHOUSE_METRICS_PATH = MODEL_DIR / "p10_warehouse_model_metrics.json"
CSV_REFERENCE_METRICS_PATH = MODEL_DIR / "p10_csv_reference_model_metrics.json"
COMPARISON_TSV_PATH = MODEL_DIR / "p10_model_metric_comparison.tsv"
COMPARISON_JSON_PATH = MODEL_DIR / "p10_model_metric_comparison.json"
COMPARISON_REPORT_PATH = REPORT_DIR / "p10_model_baseline_comparison_report.md"

TARGET = "pre_failure_24h"

LABEL_COLUMNS = {
    "failure_window",
    "pre_failure_1h",
    "pre_failure_6h",
    "pre_failure_24h",
    "post_maintenance",
    "normal_candidate",
    "rul_seconds",
}


def _load_p9_model_module():
    # 复用 P9 的特征选择、清洗、baseline 训练和 metrics helper，保证 P10 只改变数据来源。
    spec = importlib.util.spec_from_file_location("p9_model_experiments", P9_MODEL_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load P9 model script: {P9_MODEL_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


P9 = _load_p9_model_module()


def _read_features(path: Path) -> pd.DataFrame:
    # 读取特征时优先裁剪到 event_time、label 和 P9 选择的候选特征，降低大 Parquet 的内存压力。
    if not path.exists():
        raise FileNotFoundError(f"Feature table missing: {relative_path(path)}")
    columns = None
    try:
        import pyarrow.parquet as pq

        available_columns = list(pq.ParquetFile(path).schema.names)
        selected_features = P9._selected_feature_columns(pd.DataFrame(columns=available_columns))
        required_columns = ["event_minute"] + sorted(LABEL_COLUMNS & set(available_columns)) + selected_features
        columns = list(dict.fromkeys([col for col in required_columns if col in available_columns]))
    except Exception:
        columns = None
    df = pd.read_parquet(path, columns=columns)
    if "event_minute" not in df.columns:
        raise ValueError(f"Feature table missing event_minute: {relative_path(path)}")
    df["event_minute"] = pd.to_datetime(df["event_minute"])
    return df.sort_values("event_minute").reset_index(drop=True)


def _split_is_chronological(result: Dict[str, Any]) -> Tuple[bool, str]:
    # P10 复用 P9 模型模块后，仍要单独复核 split 顺序，防止比较报告掩盖时间泄漏。
    split = result.get("split", {})
    try:
        train_max = pd.Timestamp(split["train_max_event_minute"])
        validation_min = pd.Timestamp(split["validation_min_event_minute"])
        validation_max = pd.Timestamp(split["validation_max_event_minute"])
        test_min = pd.Timestamp(split["test_min_event_minute"])
    except Exception as exc:
        return False, f"Unable to parse split timestamps: {exc}"
    if train_max < validation_min and validation_max < test_min:
        return True, "Train, validation, and test ranges are strictly chronological."
    return False, f"Split ranges overlap or are out of order: {split}"


def _model_metric_row(source_type: str, feature_source: str, model_name: str, payload: Dict[str, Any], lead_time: Dict[str, Any]) -> Dict[str, Any]:
    row = {
        "source_type": source_type,
        "feature_source": feature_source,
        "model_name": model_name,
        "status": payload.get("status", "unknown"),
        "reason": payload.get("reason", ""),
        "precision": "",
        "recall": "",
        "f1": "",
        "pr_auc": "",
        "false_alarms_per_day": "",
        "lead_time_model": "",
        "detected_windows": "",
        "mean_lead_time_hours": "",
    }
    if payload.get("status") == "trained":
        metrics = payload.get("test_metrics", {})
        for key in ["precision", "recall", "f1", "pr_auc", "false_alarms_per_day"]:
            row[key] = metrics.get(key)
        if model_name == "numpy_logistic_regression":
            row["lead_time_model"] = "numpy_logistic_regression"
            row["detected_windows"] = lead_time.get("detected_windows")
            row["mean_lead_time_hours"] = lead_time.get("mean_lead_time_hours")
    return row


def _run_baseline(feature_path: Path, source_type: str, build_official_artifacts: bool) -> Dict[str, Any]:
    # 每个数据源都使用同一 target、同一 chronological split 和同一 leakage check，保证指标可比。
    config = active_config()
    features = _read_features(feature_path)
    feature_source = relative_path(feature_path)

    model_df = features[(features["failure_window"] == 0) & (features["post_maintenance"] == 0)].copy()
    model_df[TARGET] = model_df[TARGET].astype(int)
    feature_cols = P9._selected_feature_columns(model_df)
    if not feature_cols:
        raise RuntimeError(f"No model features found for {feature_source}.")

    leakage_features = sorted(set(feature_cols) & LABEL_COLUMNS)
    # CSV 和 warehouse 两条路径必须使用相同切分边界，指标比较才有意义。
    train = model_df[model_df["event_minute"] < P9.TRAIN_END].copy()
    valid = model_df[(model_df["event_minute"] >= P9.TRAIN_END) & (model_df["event_minute"] < P9.VALID_END)].copy()
    test = model_df[model_df["event_minute"] >= P9.VALID_END].copy()
    if train.empty or valid.empty or test.empty:
        raise RuntimeError(f"Chronological split produced an empty train, validation, or test set for {feature_source}.")

    train_x, valid_x, test_x, _, _, _ = P9._clean_matrix(train, valid, test, feature_cols)
    train_y = train[TARGET].to_numpy(dtype=int)
    valid_y = valid[TARGET].to_numpy(dtype=int)
    test_y = test[TARGET].to_numpy(dtype=int)

    result: Dict[str, Any] = {
        "run_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "source_type": source_type,
        "feature_source": feature_source,
        "target": TARGET,
        "rows_used": int(len(model_df)),
        "feature_count": int(len(feature_cols)),
        "features": feature_cols,
        "leakage_check": {
            "label_columns_in_features": leakage_features,
            "status": "PASS" if not leakage_features else "FAIL",
        },
        "split": {
            "strategy": "chronological_fixed_cutoffs",
            "train_rows": int(len(train)),
            "validation_rows": int(len(valid)),
            "test_rows": int(len(test)),
            "train_positive_rows": int(train_y.sum()),
            "validation_positive_rows": int(valid_y.sum()),
            "test_positive_rows": int(test_y.sum()),
            "train_min_event_minute": str(train["event_minute"].min()),
            "train_max_event_minute": str(train["event_minute"].max()),
            "validation_min_event_minute": str(valid["event_minute"].min()),
            "validation_max_event_minute": str(valid["event_minute"].max()),
            "test_min_event_minute": str(test["event_minute"].min()),
            "test_max_event_minute": str(test["event_minute"].max()),
        },
        "models": {},
    }

    chrono_ok, chrono_detail = _split_is_chronological(result)
    result["chronological_split_check"] = {"status": "PASS" if chrono_ok else "FAIL", "detail": chrono_detail}

    coef = P9._fit_logistic_regression(train_x, train_y)
    valid_scores = P9._predict_logistic(valid_x, coef)
    test_scores = P9._predict_logistic(test_x, coef)
    threshold, validation_metrics = P9._choose_threshold(valid_y, valid_scores)
    test_metrics = P9._metrics(test_y, test_scores, threshold, test["event_minute"], "test")
    feature_weights = [
        {"feature": feature, "coefficient": float(value), "abs_coefficient": abs(float(value))}
        for feature, value in sorted(zip(feature_cols, coef[1:]), key=lambda item: abs(item[1]), reverse=True)
    ]
    result["models"]["numpy_logistic_regression"] = {
        "status": "trained",
        "training": {"iterations": 220, "learning_rate": 0.08, "l2": 0.001, "class_weight": "balanced_capped_30"},
        "validation_metrics": validation_metrics,
        "test_metrics": test_metrics,
        "top_feature_weights": feature_weights[:30],
    }

    train_normal_mask = train["normal_candidate"].to_numpy(dtype=int) == 1
    anomaly_valid_scores, anomaly_test_scores = P9._robust_anomaly_scores(train_x, valid_x, test_x, train_normal_mask)
    anomaly_threshold, anomaly_validation = P9._choose_threshold(valid_y, anomaly_valid_scores)
    anomaly_test = P9._metrics(test_y, anomaly_test_scores, anomaly_threshold, test["event_minute"], "test")
    result["models"]["robust_anomaly_score"] = {
        "status": "trained",
        "training": {"method": "median_mad_distance_from_train_normal_candidates"},
        "validation_metrics": anomaly_validation,
        "test_metrics": anomaly_test,
    }

    optional = P9._optional_sklearn_models(train_x, train_y, valid_x, valid_y, test_x, feature_cols)
    for model_name, payload in optional.items():
        if payload.get("status") == "trained":
            val_scores = np.asarray(payload.pop("validation_scores"), dtype=float)
            tst_scores = np.asarray(payload.pop("test_scores"), dtype=float)
            opt_threshold, opt_validation = P9._choose_threshold(valid_y, val_scores)
            payload["validation_metrics"] = opt_validation
            payload["test_metrics"] = P9._metrics(test_y, tst_scores, opt_threshold, test["event_minute"], "test")
        result["models"][model_name] = payload

    test_frame = test[["event_minute", TARGET, "failure_window"]].copy()
    test_frame["numpy_logistic_score"] = test_scores
    test_frame["prediction"] = (test_scores >= threshold).astype(int)
    result["lead_time"] = P9._lead_time_hours(test_frame, config)
    result["lead_time"]["model_name"] = "numpy_logistic_regression"
    result["lead_time"]["scope_note"] = "Lead time is computed only from numpy_logistic_regression predictions."

    if build_official_artifacts:
        feature_weight_path = MODEL_DIR / "p9_logistic_feature_weights.tsv"
        write_tsv(feature_weight_path, feature_weights, ["feature", "coefficient", "abs_coefficient"])
        prediction_sample_path = MODEL_DIR / "p9_model_prediction_sample.tsv"
        test_frame.head(2000).to_csv(prediction_sample_path, sep="\t", index=False)
        figure_paths = P9._build_figures(result, test_frame, feature_weights)
        result["figures"] = [relative_path(path) for path in figure_paths]
        result["artifacts"] = {
            "feature_weights": relative_path(feature_weight_path),
            "prediction_sample": relative_path(prediction_sample_path),
        }
    else:
        result["figures"] = []
        result["artifacts"] = {}
    return result


def _comparison_rows(csv_result: Dict[str, Any], warehouse_result: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for result in [csv_result, warehouse_result]:
        source_type = result["source_type"]
        feature_source = result["feature_source"]
        for model_name, payload in sorted(result.get("models", {}).items()):
            rows.append(_model_metric_row(source_type, feature_source, model_name, payload, result.get("lead_time", {})))

    indexed = {(row["source_type"], row["model_name"]): row for row in rows}
    for model_name in sorted({row["model_name"] for row in rows}):
        left = indexed.get(("csv_derived", model_name))
        right = indexed.get(("warehouse_derived", model_name))
        if not left or not right or left["status"] != "trained" or right["status"] != "trained":
            continue
        delta = {
            "source_type": "warehouse_minus_csv",
            "feature_source": "metric_delta",
            "model_name": model_name,
            "status": "delta",
            "reason": "",
            "lead_time_model": "numpy_logistic_regression" if model_name == "numpy_logistic_regression" else "",
        }
        for key in ["precision", "recall", "f1", "pr_auc", "false_alarms_per_day", "detected_windows", "mean_lead_time_hours"]:
            if left.get(key) == "" or right.get(key) == "" or left.get(key) is None or right.get(key) is None:
                delta[key] = ""
            else:
                delta[key] = float(right[key]) - float(left[key])
        rows.append(delta)
    return rows


def _format_metric(value: Any) -> str:
    if value == "" or value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.10g}"
    return str(value)


def _comparison_markdown(csv_result: Dict[str, Any], warehouse_result: Dict[str, Any], rows: List[Dict[str, Any]]) -> str:
    metric_lines = "\n".join(
        "| "
        + " | ".join(
            [
                str(row.get("source_type", "")),
                str(row.get("model_name", "")),
                str(row.get("status", "")),
                _format_metric(row.get("precision")),
                _format_metric(row.get("recall")),
                _format_metric(row.get("f1")),
                _format_metric(row.get("pr_auc")),
                _format_metric(row.get("false_alarms_per_day")),
                str(row.get("lead_time_model", "")),
                _format_metric(row.get("detected_windows")),
                _format_metric(row.get("mean_lead_time_hours")),
                str(row.get("reason", "")),
            ]
        )
        + " |"
        for row in rows
    )
    trained_notes = []
    for source_name, result in [("CSV-derived", csv_result), ("warehouse-derived", warehouse_result)]:
        for model_name, payload in result.get("models", {}).items():
            if payload.get("status") == "skipped":
                trained_notes.append(f"- {source_name} `{model_name}` skipped: {payload.get('reason')}")
    if not trained_notes:
        trained_notes.append("- Random Forest and Isolation Forest both produced trained metrics in this run.")
    return f"""# P10 Model Baseline Comparison Report

## Scope

- Target: `{TARGET}`.
- CSV-derived feature source: `{csv_result['feature_source']}`.
- Warehouse-derived feature source: `{warehouse_result['feature_source']}`.
- Official updated metrics: `{relative_path(OFFICIAL_METRICS_PATH)}`.
- Warehouse metrics copy: `{relative_path(WAREHOUSE_METRICS_PATH)}`.
- CSV reference metrics: `{relative_path(CSV_REFERENCE_METRICS_PATH)}`.
- Comparison TSV: `{relative_path(COMPARISON_TSV_PATH)}`.

The warehouse-derived baseline uses the same fixed chronological split as P9. No random split is used.

## Split And Leakage Checks

| Source | Split check | Leakage check | Feature count |
| --- | --- | --- | ---: |
| CSV-derived | {csv_result['chronological_split_check']['status']} | {csv_result['leakage_check']['status']} | {csv_result['feature_count']} |
| warehouse-derived | {warehouse_result['chronological_split_check']['status']} | {warehouse_result['leakage_check']['status']} | {warehouse_result['feature_count']} |

## Metric Comparison

| Source | Model | Status | Precision | Recall | F1 | PR-AUC | False alarms/day | Lead time model | Detected windows | Mean lead time hours | Reason |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | --- |
{metric_lines}

## RF / IF Availability

{chr(10).join(trained_notes)}

## Lead Time Boundary

Lead time is computed only from `numpy_logistic_regression` predictions in both CSV-derived and warehouse-derived runs. Random Forest, Isolation Forest, and robust anomaly score metrics are compared on precision/recall/F1/PR-AUC/false alarms per day, but their lead time is not reported in this run.

## Decision

The warehouse-derived model baseline is accepted only if the chronological split check and leakage check are `PASS`, and skipped RF/IF models have explicit reasons. If warehouse metrics diverge from CSV-derived metrics, the comparison rows should be reviewed before promoting the model baseline.
"""


def _official_model_report(result: Dict[str, Any], comparison_rows: List[Dict[str, Any]]) -> str:
    model_lines = []
    for name, item in result["models"].items():
        if item.get("status") != "trained":
            model_lines.append(f"- `{name}`: skipped, {item.get('reason')}")
            continue
        metrics = item["test_metrics"]
        model_lines.append(
            f"- `{name}`: precision `{metrics['precision']:.4f}`, recall `{metrics['recall']:.4f}`, "
            f"F1 `{metrics['f1']:.4f}`, PR-AUC `{metrics['pr_auc'] if metrics['pr_auc'] is not None else 'N/A'}`, "
            f"false alarms/day `{metrics['false_alarms_per_day']:.4f}`"
        )

    split = result["split"]
    lead = result["lead_time"]
    figure_lines = "\n".join(f"- `{path}`" for path in result.get("figures", []))
    if not figure_lines:
        figure_lines = "- No figures generated."
    return f"""# P9 Model Baseline Report

## Dataset and Target

- Feature source: `{result['feature_source']}`.
- Feature source type: `warehouse-derived`.
- Target: `{TARGET}`.
- Rows used after excluding failure and post-maintenance windows: `{result['rows_used']}`.
- Feature count: `{result['feature_count']}`.
- Train rows: `{split['train_rows']}`, positives `{split['train_positive_rows']}`.
- Validation rows: `{split['validation_rows']}`, positives `{split['validation_positive_rows']}`.
- Test rows: `{split['test_rows']}`, positives `{split['test_positive_rows']}`.

The target is a weak early-warning label derived from configured failure starts. It is not a manually verified row-level production alarm label.

## Time Split

- Train: before `2020-06-01 00:00:00`.
- Validation: `2020-06-01 00:00:00` to before `2020-07-01 00:00:00`.
- Test: from `2020-07-01 00:00:00`.
- Split strategy: chronological fixed cutoffs, no random split.
- Chronological split check: `{result['chronological_split_check']['status']}` - {result['chronological_split_check']['detail']}
- Leakage check: `{result['leakage_check']['status']}`; label columns in features: `{result['leakage_check']['label_columns_in_features']}`.

## Metrics

{chr(10).join(model_lines)}

## Lead Time

- Model: `numpy_logistic_regression`.
- Detected test failure windows: `{lead['detected_windows']}`.
- Mean lead time hours: `{lead['mean_lead_time_hours']}`.
- Lead time is not reported for Random Forest, Isolation Forest, or robust anomaly score in this run.

## Comparison Artifacts

- CSV reference metrics: `{relative_path(CSV_REFERENCE_METRICS_PATH)}`.
- Warehouse metrics copy: `{relative_path(WAREHOUSE_METRICS_PATH)}`.
- Metric comparison TSV: `{relative_path(COMPARISON_TSV_PATH)}`.
- Metric comparison report: `{relative_path(COMPARISON_REPORT_PATH)}`.

## Limitations

- This is a baseline only and must not be described as a production predictive-maintenance model.
- Labels come from failure windows and pre-failure windows, so they are weak labels.
- Warehouse-derived features are now validated against ODS/DWD/DWS parity, but online scoring is still not integrated into Flink.

## Figures

{figure_lines}
"""


def main() -> None:
    # 主流程先保留 CSV reference，再生成 warehouse official metrics 和 comparison report，便于回看 P9/P10 差异。
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--reuse-existing",
        action="store_true",
        help="Skip model rerun when official metrics, warehouse metrics, CSV reference metrics, and comparison report already exist.",
    )
    args = parser.parse_args()
    required_outputs = [
        OFFICIAL_METRICS_PATH,
        OFFICIAL_REPORT_PATH,
        WAREHOUSE_METRICS_PATH,
        CSV_REFERENCE_METRICS_PATH,
        COMPARISON_TSV_PATH,
        COMPARISON_JSON_PATH,
        COMPARISON_REPORT_PATH,
    ]
    if args.reuse_existing and all(path.exists() for path in required_outputs):
        print("P10 warehouse model baseline reuse-existing enabled.")
        print("official_metrics:", OFFICIAL_METRICS_PATH)
        print("official_report:", OFFICIAL_REPORT_PATH)
        print("comparison_report:", COMPARISON_REPORT_PATH)
        return

    ensure_analysis_dirs()
    started = datetime.now()
    csv_result = _run_baseline(CSV_FEATURE_PATH, "csv_derived", build_official_artifacts=False)
    warehouse_result = _run_baseline(WAREHOUSE_FEATURE_PATH, "warehouse_derived", build_official_artifacts=True)
    # comparison_rows 是作品集里解释 P9/P10 差异的主表：同一模型、同一 target，不同 feature source。
    comparison_rows = _comparison_rows(csv_result, warehouse_result)
    comparison_payload = {
        "run_id": started.strftime("%Y%m%d_%H%M%S"),
        "started": started.isoformat(timespec="seconds"),
        "finished": datetime.now().isoformat(timespec="seconds"),
        "target": TARGET,
        "csv_feature_source": csv_result["feature_source"],
        "warehouse_feature_source": warehouse_result["feature_source"],
        "comparison_rows": comparison_rows,
        "status": "PASS"
        if csv_result["chronological_split_check"]["status"] == "PASS"
        and warehouse_result["chronological_split_check"]["status"] == "PASS"
        and csv_result["leakage_check"]["status"] == "PASS"
        and warehouse_result["leakage_check"]["status"] == "PASS"
        else "FAIL",
    }

    write_json(CSV_REFERENCE_METRICS_PATH, csv_result)
    write_json(WAREHOUSE_METRICS_PATH, warehouse_result)
    write_json(OFFICIAL_METRICS_PATH, warehouse_result)
    write_json(COMPARISON_JSON_PATH, comparison_payload)
    # official metrics 指向 warehouse_result，是 P10 后的正式模型基线证据。
    write_tsv(
        COMPARISON_TSV_PATH,
        comparison_rows,
        [
            "source_type",
            "feature_source",
            "model_name",
            "status",
            "reason",
            "precision",
            "recall",
            "f1",
            "pr_auc",
            "false_alarms_per_day",
            "lead_time_model",
            "detected_windows",
            "mean_lead_time_hours",
        ],
    )
    write_markdown(COMPARISON_REPORT_PATH, _comparison_markdown(csv_result, warehouse_result, comparison_rows))
    write_markdown(OFFICIAL_REPORT_PATH, _official_model_report(warehouse_result, comparison_rows))

    print("P10 warehouse model baseline completed.")
    print("status:", comparison_payload["status"])
    print("official_metrics:", OFFICIAL_METRICS_PATH)
    print("official_report:", OFFICIAL_REPORT_PATH)
    print("warehouse_metrics:", WAREHOUSE_METRICS_PATH)
    print("csv_reference_metrics:", CSV_REFERENCE_METRICS_PATH)
    print("comparison_tsv:", COMPARISON_TSV_PATH)
    print("comparison_report:", COMPARISON_REPORT_PATH)


if __name__ == "__main__":
    main()
