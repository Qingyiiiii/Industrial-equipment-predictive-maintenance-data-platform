# -*- coding: utf-8 -*-
"""Baseline failure-window modeling from MetroPT DWS window KPI data."""
# 阅读提示：本文件是早期 baseline model，不是最终 P9/P10 模型证据。
# 它验证 DWS window KPI 是否具备可建模信号，并显式保留弱标签和时间切分限制。
# - 链路位置：analysis/03，是早期 DWS-window baseline，用来试探 KPI 层是否有预测信号。
# - 主要输入：DWS window KPI。
# - 主要输出：baseline metrics、confusion matrix、feature importance 和模型报告。
# - 边界提醒：这里的 failure-window 标签是 weak label，指标必须结合时间切分和数据泄漏边界解释。
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402

from analysis_common import (  # noqa: E402
    ANALOG_SENSORS,
    DIGITAL_SENSORS,
    FIGURE_DIR,
    MODEL_DIR,
    REPORT_DIR,
    available_columns,
    ensure_analysis_dirs,
    figure_relpath,
    load_config,
    prepare_spark_after_input_validation,
    read_parquet,
    save_csv,
    save_figure,
    write_json,
    write_markdown,
)


def _load_sklearn():
    # sklearn 是可选依赖；缺失时应给出清晰失败，而不是在模型阶段产生隐式错误。
    try:
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.impute import SimpleImputer
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import accuracy_score, confusion_matrix, f1_score, precision_score, recall_score
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "scikit-learn is required for baseline modeling. Install project dependencies with "
            "python -m pip install --user -r requirements.txt before running analysis/03_model_baseline.py."
        ) from exc

    return {
        "RandomForestClassifier": RandomForestClassifier,
        "SimpleImputer": SimpleImputer,
        "LogisticRegression": LogisticRegression,
        "accuracy_score": accuracy_score,
        "confusion_matrix": confusion_matrix,
        "f1_score": f1_score,
        "precision_score": precision_score,
        "recall_score": recall_score,
        "Pipeline": Pipeline,
        "StandardScaler": StandardScaler,
    }


def _feature_columns(df):
    # 特征列只来自 DWS KPI 的解释变量，排除标签和时间标识，避免 leakage。
    requested = ["sample_count"]
    for sensor_name in ANALOG_SENSORS:
        requested.extend(
            [
                f"avg_{sensor_name}",
                f"std_{sensor_name}",
                f"min_{sensor_name}",
                f"max_{sensor_name}",
            ]
        )
    requested.extend(f"active_count_{sensor_name}" for sensor_name in DIGITAL_SENSORS)
    return available_columns(df, requested)


def _metrics(y_true, y_pred, sklearn_api):
    return {
        "accuracy": float(sklearn_api["accuracy_score"](y_true, y_pred)),
        "precision": float(sklearn_api["precision_score"](y_true, y_pred, zero_division=0)),
        "recall": float(sklearn_api["recall_score"](y_true, y_pred, zero_division=0)),
        "f1": float(sklearn_api["f1_score"](y_true, y_pred, zero_division=0)),
        "confusion_matrix": sklearn_api["confusion_matrix"](y_true, y_pred, labels=[0, 1]).tolist(),
    }


def _importance_rows(model_name, pipeline, feature_names):
    estimator = pipeline.named_steps["model"]
    if model_name == "random_forest":
        values = estimator.feature_importances_
        value_name = "importance"
    else:
        values = estimator.coef_[0]
        value_name = "coefficient"
    rows = []
    for name, value in zip(feature_names, values):
        rows.append(
            {
                "model": model_name,
                "feature": name,
                value_name: float(value),
                "abs_value": abs(float(value)),
            }
        )
    return sorted(rows, key=lambda row: row["abs_value"], reverse=True)


def _markdown(result, figure_paths):
    metric_lines = []
    for model_name, metrics in result["models"].items():
        if metrics.get("status") == "skipped":
            metric_lines.append(f"- `{model_name}`: skipped, {metrics.get('reason')}")
            continue
        metric_lines.append(
            f"- `{model_name}`: accuracy `{metrics['accuracy']:.4f}`, precision `{metrics['precision']:.4f}`, "
            f"recall `{metrics['recall']:.4f}`, F1 `{metrics['f1']:.4f}`"
        )

    figure_lines = "\n".join(f"- `{figure_relpath(path)}`" for path in figure_paths)
    top_features = "\n".join(
        f"- `{row['model']}` / `{row['feature']}`: `{row.get('importance', row.get('coefficient')):.6f}`"
        for row in result["top_feature_signals"][:12]
    )

    return f"""# MetroPT Baseline Model Report

## Dataset

- Source: DWS window KPI.
- Rows: `{result['row_count']}`.
- Train rows: `{result['train_rows']}`.
- Test rows: `{result['test_rows']}`.
- Positive rows: `{result['positive_rows']}`.
- Negative rows: `{result['negative_rows']}`.
- Split strategy: time-ordered 70/30 split by `event_minute`.

The target is derived from `failure_sample_count > 0` or `failure_window_rate > 0`. This is an interval-derived failure-window label, not a manually verified row-level fault label.

## Metrics

{chr(10).join(metric_lines)}

## Top Feature Signals

{top_features}

## Limitations

- This is a baseline only; it should not be presented as a production predictive-maintenance model.
- Class imbalance can make accuracy misleading.
- Time leakage is controlled with a time-based split, but feature windows still need review before any future horizon-based prediction task.

## Figures

{figure_lines}
"""


def main() -> None:
    # 主流程按时间顺序切分训练/测试，输出 metrics、特征信号和 confusion matrix。
    ensure_analysis_dirs()
    config = load_config()
    spark = prepare_spark_after_input_validation(
        "MetroPT_Analysis_03_Model_Baseline",
        config,
        ["dws_window_kpi"],
    )[0]

    try:
        window_kpi = read_parquet(spark, config, "dws_window_kpi")
        numeric_features = _feature_columns(window_kpi)
        required_cols = ["event_minute", "operating_state", "failure_sample_count", "failure_window_rate", *numeric_features]
        pdf = window_kpi.select(*required_cols).orderBy("event_minute").toPandas()
    finally:
        spark.stop()

    if pdf.empty:
        result = {
            "status": "skipped",
            "reason": "DWS window KPI is empty",
            "row_count": 0,
            "models": {},
        }
        write_json(MODEL_DIR / "metropt_baseline_metrics.json", result)
        raise SystemExit("DWS window KPI is empty; baseline modeling skipped.")

    pdf["event_minute"] = pd.to_datetime(pdf["event_minute"])
    pdf = pdf.sort_values("event_minute").reset_index(drop=True)
    # 早期 baseline 的 label 来自 DWS failure KPI，只用于信号验证，不是 P9 的正式 pre_failure target。
    pdf["label"] = ((pdf["failure_sample_count"].fillna(0) > 0) | (pdf["failure_window_rate"].fillna(0.0) > 0)).astype(int)

    state_features = pd.get_dummies(pdf["operating_state"].fillna("unknown"), prefix="state")
    X = pd.concat([pdf[numeric_features].apply(pd.to_numeric, errors="coerce"), state_features], axis=1)
    y = pdf["label"].astype(int)
    feature_names = list(X.columns)

    split_idx = int(len(pdf) * 0.7)
    split_idx = max(1, min(split_idx, len(pdf) - 1))
    # 仍按时间顺序做 70/30 切分，避免用未来窗口随机混入训练集。
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    result = {
        "row_count": int(len(pdf)),
        "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "positive_rows": int(y.sum()),
        "negative_rows": int((1 - y).sum()),
        "train_positive_rows": int(y_train.sum()),
        "test_positive_rows": int(y_test.sum()),
        "feature_count": len(feature_names),
        "features": feature_names,
        "split": {
            "strategy": "time_ordered_70_30",
            "train_min_event_minute": pdf.iloc[:split_idx]["event_minute"].min(),
            "train_max_event_minute": pdf.iloc[:split_idx]["event_minute"].max(),
            "test_min_event_minute": pdf.iloc[split_idx:]["event_minute"].min(),
            "test_max_event_minute": pdf.iloc[split_idx:]["event_minute"].max(),
        },
        "target_definition": "failure_sample_count > 0 OR failure_window_rate > 0",
        "models": {},
    }

    if y_train.nunique() < 2:
        # 单类别训练集无法训练分类模型；记录 skipped 比强行训练一个无意义模型更可靠。
        result["models"]["logistic_regression"] = {
            "status": "skipped",
            "reason": "time-based training split contains only one class",
        }
        result["models"]["random_forest"] = {
            "status": "skipped",
            "reason": "time-based training split contains only one class",
        }
        write_json(MODEL_DIR / "metropt_baseline_metrics.json", result)
        write_markdown(REPORT_DIR / "metropt_baseline_model_report.md", _markdown({**result, "top_feature_signals": []}, []))
        print("Baseline modeling skipped because the training split contains one class.")
        return

    sklearn_api = _load_sklearn()
    Pipeline = sklearn_api["Pipeline"]
    SimpleImputer = sklearn_api["SimpleImputer"]
    StandardScaler = sklearn_api["StandardScaler"]
    LogisticRegression = sklearn_api["LogisticRegression"]
    RandomForestClassifier = sklearn_api["RandomForestClassifier"]

    models = {
        # 两个模型都只是 baseline：Logistic 便于解释，RandomForest 用于观察非线性信号。
        "logistic_regression": Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                ("model", LogisticRegression(max_iter=1000, class_weight="balanced", random_state=42)),
            ]
        ),
        "random_forest": Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "model",
                    RandomForestClassifier(
                        n_estimators=80,
                        max_depth=12,
                        min_samples_leaf=10,
                        class_weight="balanced",
                        random_state=42,
                        n_jobs=-1,
                    ),
                ),
            ]
        ),
    }

    importance_rows = []
    predictions = {}
    for model_name, pipeline in models.items():
        pipeline.fit(X_train, y_train)
        y_pred = pipeline.predict(X_test)
        predictions[model_name] = y_pred
        result["models"][model_name] = {
            "status": "trained",
            **_metrics(y_test, y_pred, sklearn_api),
        }
        importance_rows.extend(_importance_rows(model_name, pipeline, feature_names))

    fig_paths = []
    sns.set_theme(style="whitegrid")

    fig, axes = plt.subplots(1, len(models), figsize=(10, 4))
    if len(models) == 1:
        axes = [axes]
    for ax, (model_name, y_pred) in zip(axes, predictions.items()):
        matrix = sklearn_api["confusion_matrix"](y_test, y_pred, labels=[0, 1])
        sns.heatmap(matrix, annot=True, fmt="d", cmap="Blues", cbar=False, ax=ax)
        ax.set_title(model_name)
        ax.set_xlabel("Predicted")
        ax.set_ylabel("Actual")
        ax.set_xticklabels(["normal", "failure_window"])
        ax.set_yticklabels(["normal", "failure_window"], rotation=0)
    fig_paths.append(save_figure(fig, FIGURE_DIR / "baseline_confusion_matrices.png"))
    plt.close(fig)

    for model_name in models:
        rows = [row for row in importance_rows if row["model"] == model_name][:15]
        if not rows:
            continue
        plot_df = pd.DataFrame(rows)
        value_col = "importance" if "importance" in plot_df.columns and plot_df["importance"].notna().any() else "coefficient"
        fig, ax = plt.subplots(figsize=(8, 5))
        sns.barplot(data=plot_df, y="feature", x=value_col, ax=ax, color="#4c72b0")
        ax.set_title(f"{model_name} Top Feature Signals")
        ax.set_xlabel(value_col)
        ax.set_ylabel("Feature")
        fig_paths.append(save_figure(fig, FIGURE_DIR / f"{model_name}_feature_signals.png"))
        plt.close(fig)

    result["top_feature_signals"] = importance_rows[:30]
    result["figures"] = [str(path) for path in fig_paths]
    json_path = write_json(MODEL_DIR / "metropt_baseline_metrics.json", result)
    csv_path = save_csv(
        MODEL_DIR / "metropt_baseline_feature_signals.csv",
        importance_rows,
        ["model", "feature", "importance", "coefficient", "abs_value"],
    )
    md_path = write_markdown(REPORT_DIR / "metropt_baseline_model_report.md", _markdown(result, fig_paths))

    print("MetroPT baseline modeling completed.")
    print("metrics:", json_path)
    print("feature_signals:", csv_path)
    print("report:", md_path)
    for path in fig_paths:
        print("figure:", path)


if __name__ == "__main__":
    main()
