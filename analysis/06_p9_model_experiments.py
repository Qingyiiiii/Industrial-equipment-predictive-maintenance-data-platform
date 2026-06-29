# -*- coding: utf-8 -*-
"""Run P9 time-split baseline experiments on MetroPT minute features."""
# 阅读提示：本文件使用 P9 minute features 做 time-split baseline。
# 目标是验证弱标签下的可解释 baseline 信号，而不是声明生产级预测维护模型。
# 学习导读：
# - 链路位置：P9 模型实验脚本，读取 minute features 并做 chronological split。
# - 主要输入：p9_window_features_1min.parquet 或临时构建的 minute feature table。
# - 主要输出：metrics、prediction samples、feature weights、baseline model report 和图表。
# - 核心概念：时间切分比随机切分更接近预测场景，可降低未来信息泄漏风险。
# - 边界提醒：指标来自 weak-label baseline，不应被讲成真实故障诊断模型的生产准确率。
import math
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from analysis_common import FIGURE_DIR, MODEL_DIR, REPORT_DIR, ensure_analysis_dirs, save_figure, write_json, write_markdown  # noqa: E402
from p9_common import active_config, build_minute_feature_table, failure_window_records, read_metropt_csv, relative_path, write_tsv  # noqa: E402


TARGET = "pre_failure_24h"
# 当前 baseline 预测 pre_failure_24h，表示故障前 24 小时窗口；它是弱标签任务定义。
TRAIN_END = pd.Timestamp("2020-06-01 00:00:00")
VALID_END = pd.Timestamp("2020-07-01 00:00:00")


def _load_or_build_features() -> pd.DataFrame:
    # 优先复用已生成的 feature table；缺失时才触发构建，避免无意义重跑重任务。
    feature_path = MODEL_DIR / "p9_window_features_1min.parquet"
    if feature_path.exists():
        return pd.read_parquet(feature_path)
    config = active_config()
    df = read_metropt_csv(config)
    minute = build_minute_feature_table(df)
    feature_path.parent.mkdir(parents=True, exist_ok=True)
    minute.to_parquet(feature_path, index=False)
    return minute


def _selected_feature_columns(df: pd.DataFrame) -> List[str]:
    # 特征选择排除标签列、时间列和泄漏风险列，只保留可用于当前分钟闭合后的解释变量。
    excluded = {
        "event_minute",
        "window_start",
        "window_end",
        "failure_window",
        "pre_failure_1h",
        "pre_failure_6h",
        "pre_failure_24h",
        "post_maintenance",
        "normal_candidate",
        "rul_seconds",
    }
    sensors = ["tp2", "tp3", "h1", "dv_pressure", "reservoirs", "oil_temperature", "motor_current"]
    pressure_features = ["delta_tp3_reservoirs", "delta_tp2_tp3"]
    key_digital = ["comp", "dv_electric", "mpg", "lps", "pressure_switch", "oil_level"]

    requested = ["sample_count", "state_transition_count"]
    for sensor in sensors + pressure_features:
        requested.extend([f"mean_{sensor}", f"std_{sensor}", f"min_{sensor}", f"max_{sensor}", f"slope_{sensor}"])
        for window in [5, 15, 60]:
            requested.extend(
                [
                    f"roll{window}_mean_mean_{sensor}",
                    f"roll{window}_std_mean_{sensor}",
                    f"roll{window}_min_mean_{sensor}",
                    f"roll{window}_max_mean_{sensor}",
                ]
            )
    for sensor in key_digital:
        requested.extend([f"active_count_{sensor}", f"toggle_count_{sensor}"])
        for window in [5, 15, 60]:
            requested.extend(
                [
                    f"roll{window}_mean_active_count_{sensor}",
                    f"roll{window}_mean_toggle_count_{sensor}",
                    f"roll{window}_max_toggle_count_{sensor}",
                ]
            )
    cols = [col for col in requested if col in df.columns and col not in excluded]
    return list(dict.fromkeys(cols))


def _clean_matrix(train: pd.DataFrame, valid: pd.DataFrame, test: pd.DataFrame, feature_cols: List[str]):
    train_x = train[feature_cols].replace([np.inf, -np.inf], np.nan)
    valid_x = valid[feature_cols].replace([np.inf, -np.inf], np.nan)
    test_x = test[feature_cols].replace([np.inf, -np.inf], np.nan)
    # 缺失值填充和标准化参数只从 train 学习，不能用 valid/test 的统计量反向影响训练。
    medians = train_x.median(numeric_only=True).fillna(0.0)
    means = train_x.fillna(medians).mean()
    stds = train_x.fillna(medians).std().replace(0, 1.0).fillna(1.0)

    def transform(frame: pd.DataFrame) -> np.ndarray:
        out = frame.fillna(medians)
        out = (out - means) / stds
        return out.to_numpy(dtype=np.float64)

    return transform(train_x), transform(valid_x), transform(test_x), medians, means, stds


def _sigmoid(z: np.ndarray) -> np.ndarray:
    z = np.clip(z, -35, 35)
    return 1.0 / (1.0 + np.exp(-z))


def _fit_logistic_regression(X: np.ndarray, y: np.ndarray, iterations: int = 220, lr: float = 0.08, l2: float = 0.001) -> np.ndarray:
    Xb = np.column_stack([np.ones(len(X)), X])
    coef = np.zeros(Xb.shape[1], dtype=np.float64)
    positives = max(float(y.sum()), 1.0)
    negatives = max(float(len(y) - y.sum()), 1.0)
    # 弱标签通常不平衡，balanced weight 能避免模型只学会预测多数类。
    weights = np.where(y == 1, len(y) / (2.0 * positives), len(y) / (2.0 * negatives))
    weights = np.minimum(weights, 30.0)
    weight_sum = weights.sum()
    for _ in range(iterations):
        pred = _sigmoid(Xb @ coef)
        error = (pred - y) * weights
        grad = (Xb.T @ error) / weight_sum
        grad[1:] += l2 * coef[1:]
        coef -= lr * grad
    return coef


def _predict_logistic(X: np.ndarray, coef: np.ndarray) -> np.ndarray:
    return _sigmoid(np.column_stack([np.ones(len(X)), X]) @ coef)


def _average_precision(y_true: np.ndarray, scores: np.ndarray) -> Optional[float]:
    positives = int(y_true.sum())
    if positives == 0:
        return None
    order = np.argsort(-scores)
    y_sorted = y_true[order]
    tp = np.cumsum(y_sorted == 1)
    fp = np.cumsum(y_sorted == 0)
    precision = tp / np.maximum(tp + fp, 1)
    recall = tp / positives
    recall_prev = np.concatenate([[0.0], recall[:-1]])
    return float(np.sum((recall - recall_prev) * precision))


def _metrics(y_true: np.ndarray, scores: np.ndarray, threshold: float, timestamps: pd.Series, split_name: str) -> Dict:
    pred = (scores >= threshold).astype(int)
    tp = int(((pred == 1) & (y_true == 1)).sum())
    fp = int(((pred == 1) & (y_true == 0)).sum())
    tn = int(((pred == 0) & (y_true == 0)).sum())
    fn = int(((pred == 0) & (y_true == 1)).sum())
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    days = max(int(pd.to_datetime(timestamps).dt.date.nunique()), 1)
    return {
        "split": split_name,
        "rows": int(len(y_true)),
        "positive_rows": int(y_true.sum()),
        "threshold": float(threshold),
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
        "pr_auc": _average_precision(y_true, scores),
        "false_alarms_per_day": float(fp / days),
        "confusion_matrix": [[tn, fp], [fn, tp]],
    }


def _choose_threshold(y_true: np.ndarray, scores: np.ndarray) -> Tuple[float, Dict]:
    if len(np.unique(y_true)) < 2:
        threshold = float(np.quantile(scores, 0.95))
        return threshold, _metrics(y_true, scores, threshold, pd.Series(pd.date_range("2020-01-01", periods=len(y_true), freq="min")), "validation")
    # threshold 只在 validation split 上选择，test split 保持最终未见数据角色。
    candidates = np.unique(np.quantile(scores, np.linspace(0.50, 0.995, 80)))
    best_threshold = float(candidates[0])
    best_metrics = None
    best_key = (-1.0, -1.0)
    fake_times = pd.Series(pd.date_range("2020-01-01", periods=len(y_true), freq="min"))
    for threshold in candidates:
        metrics = _metrics(y_true, scores, float(threshold), fake_times, "validation")
        key = (metrics["f1"], metrics["recall"])
        if key > best_key:
            best_key = key
            best_threshold = float(threshold)
            best_metrics = metrics
    return best_threshold, best_metrics or {}


def _lead_time_hours(pred_frame: pd.DataFrame, config) -> Dict:
    windows = failure_window_records(config)
    rows = []
    for record in windows:
        start = record["start"]
        if start < pred_frame["event_minute"].min() or start > pred_frame["event_minute"].max():
            continue
        candidates = pred_frame[
            (pred_frame["event_minute"] >= start - pd.Timedelta(hours=24))
            & (pred_frame["event_minute"] < start)
            & (pred_frame["prediction"] == 1)
        ].sort_values("event_minute")
        if candidates.empty:
            rows.append({"failure_id": record["failure_id"], "failure_start": str(start), "detected": False, "lead_time_hours": None})
        else:
            first_warning = candidates.iloc[0]["event_minute"]
            rows.append(
                {
                    "failure_id": record["failure_id"],
                    "failure_start": str(start),
                    "detected": True,
                    "first_warning": str(first_warning),
                    "lead_time_hours": float((start - first_warning).total_seconds() / 3600.0),
                }
            )
    detected = [row["lead_time_hours"] for row in rows if row.get("lead_time_hours") is not None]
    return {
        "windows": rows,
        "detected_windows": len(detected),
        "mean_lead_time_hours": float(np.mean(detected)) if detected else None,
        "max_lead_time_hours": float(np.max(detected)) if detected else None,
    }


def _robust_anomaly_scores(train_x: np.ndarray, valid_x: np.ndarray, test_x: np.ndarray, train_normal_mask: np.ndarray):
    normal_x = train_x[train_normal_mask]
    if len(normal_x) < 100:
        normal_x = train_x
    center = np.median(normal_x, axis=0)
    mad = np.median(np.abs(normal_x - center), axis=0)
    mad = np.where(mad < 1e-6, 1.0, mad)

    def score(x: np.ndarray) -> np.ndarray:
        z = np.abs((x - center) / mad)
        return np.nanmean(np.clip(z, 0, 25), axis=1)

    return score(valid_x), score(test_x)


def _optional_sklearn_models(train_x, train_y, valid_x, valid_y, test_x, feature_cols):
    try:
        from sklearn.ensemble import IsolationForest, RandomForestClassifier
    except ModuleNotFoundError as exc:
        return {
            "random_forest": {"status": "skipped", "reason": f"scikit-learn not installed: {exc.name}"},
            "isolation_forest": {"status": "skipped", "reason": f"scikit-learn not installed: {exc.name}"},
        }

    models = {}
    sklearn_n_jobs = int(os.environ.get("P9_SKLEARN_N_JOBS", "-1"))
    rf = RandomForestClassifier(
        n_estimators=120,
        max_depth=10,
        min_samples_leaf=20,
        class_weight="balanced",
        random_state=42,
        n_jobs=sklearn_n_jobs,
    )
    try:
        rf.fit(train_x, train_y)
        models["random_forest"] = {
            "status": "trained",
            "validation_scores": rf.predict_proba(valid_x)[:, 1].tolist(),
            "test_scores": rf.predict_proba(test_x)[:, 1].tolist(),
            "feature_importance": [
                {"feature": feature, "importance": float(value)}
                for feature, value in sorted(zip(feature_cols, rf.feature_importances_), key=lambda item: abs(item[1]), reverse=True)
            ][:30],
        }
    except Exception as exc:
        models["random_forest"] = {
            "status": "skipped",
            "reason": f"scikit-learn training failed: {type(exc).__name__}: {exc}",
        }

    normal_train = train_x[train_y == 0]
    if len(normal_train) > 50000:
        rng = np.random.default_rng(42)
        normal_train = normal_train[rng.choice(len(normal_train), size=50000, replace=False)]
    iso = IsolationForest(n_estimators=120, contamination="auto", random_state=42, n_jobs=sklearn_n_jobs)
    try:
        iso.fit(normal_train)
        models["isolation_forest"] = {
            "status": "trained",
            "validation_scores": (-iso.decision_function(valid_x)).tolist(),
            "test_scores": (-iso.decision_function(test_x)).tolist(),
            "feature_importance": [],
        }
    except Exception as exc:
        models["isolation_forest"] = {
            "status": "skipped",
            "reason": f"scikit-learn training failed: {type(exc).__name__}: {exc}",
        }
    return models


def _build_figures(result: Dict, test_frame: pd.DataFrame, feature_weights: List[Dict]) -> List[Path]:
    sns.set_theme(style="whitegrid")
    fig_paths: List[Path] = []
    trained = {name: item for name, item in result["models"].items() if item.get("status") == "trained"}

    if trained:
        fig, axes = plt.subplots(1, len(trained), figsize=(5 * len(trained), 4))
        if len(trained) == 1:
            axes = [axes]
        for ax, (name, item) in zip(axes, trained.items()):
            matrix = np.array(item["test_metrics"]["confusion_matrix"])
            sns.heatmap(matrix, annot=True, fmt="d", cmap="Blues", cbar=False, ax=ax)
            ax.set_title(name)
            ax.set_xlabel("Predicted")
            ax.set_ylabel("Actual")
            ax.set_xticklabels(["normal", TARGET])
            ax.set_yticklabels(["normal", TARGET], rotation=0)
        fig_paths.append(save_figure(fig, FIGURE_DIR / "p9_baseline_confusion_matrices.png"))
        plt.close(fig)

    top_weights = pd.DataFrame(feature_weights[:20])
    if not top_weights.empty:
        fig, ax = plt.subplots(figsize=(9, 6))
        sns.barplot(data=top_weights, y="feature", x="coefficient", ax=ax, color="#4c72b0")
        ax.axvline(0, color="#222222", linewidth=0.8)
        ax.set_title("P9 Numpy Logistic Regression Top Coefficients")
        ax.set_xlabel("Coefficient")
        ax.set_ylabel("Feature")
        fig_paths.append(save_figure(fig, FIGURE_DIR / "p9_logistic_feature_weights.png"))
        plt.close(fig)

    if "numpy_logistic_regression" in trained:
        plot_df = test_frame[["event_minute", TARGET, "failure_window", "numpy_logistic_score"]].copy()
        if len(plot_df) > 5000:
            plot_df = plot_df.iloc[:: max(1, len(plot_df) // 5000)].copy()
        fig, ax = plt.subplots(figsize=(13, 4))
        ax.plot(plot_df["event_minute"], plot_df["numpy_logistic_score"], linewidth=0.9, color="#2f6f9f", label="risk score")
        ax.fill_between(
            plot_df["event_minute"],
            0,
            plot_df[TARGET],
            step="pre",
            color="#dd8452",
            alpha=0.25,
            label=TARGET,
        )
        ax.fill_between(
            plot_df["event_minute"],
            0,
            plot_df["failure_window"],
            step="pre",
            color="#c44e52",
            alpha=0.25,
            label="failure_window",
        )
        ax.set_ylim(-0.03, 1.03)
        ax.set_title("P9 Test Risk Score Timeline")
        ax.set_xlabel("Event Minute")
        ax.set_ylabel("Score / Label")
        ax.legend(loc="upper right")
        fig_paths.append(save_figure(fig, FIGURE_DIR / "p9_risk_score_timeline.png"))
        plt.close(fig)
    return fig_paths


def _markdown(result: Dict, figure_paths: List[Path], feature_weight_path: Path, prediction_sample_path: Path) -> str:
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
    lead_line = (
        f"- `numpy_logistic_regression` detected test failure windows: `{lead['detected_windows']}`; "
        f"mean lead time hours: `{lead['mean_lead_time_hours']}`."
        if lead["windows"]
        else "- No configured failure start falls inside the test range."
    )
    figure_lines = "\n".join(f"- `{relative_path(path)}`" for path in figure_paths)
    return f"""# P9 Model Baseline Report

## Dataset and Target

- Feature source: `{result['feature_source']}`.
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
- Split strategy: chronological, no random split.

## Metrics

{chr(10).join(model_lines)}

## Lead Time

{lead_line}

## Artifacts

- Metrics JSON: `{relative_path(MODEL_DIR / 'p9_model_metrics.json')}`.
- Logistic feature weights: `{relative_path(feature_weight_path)}`.
- Prediction sample: `{relative_path(prediction_sample_path)}`.

## Limitations

- This is a baseline only and must not be described as a production predictive-maintenance model.
- Labels come from failure windows and pre-failure windows, so they are weak labels.
- Random Forest and Isolation Forest require a working scikit-learn runtime; when scikit-learn is missing or training fails in the local environment, they are recorded as skipped with an explicit reason and should be rerun by master or a dependency-complete worker environment.
- Worker local execution uses CSV-derived features. Cluster feature/table alignment is 待 master 验证.

## Figures

{figure_lines}
"""


def main() -> None:
    # 主流程固定时间切分、训练 baseline、导出 metrics/weights/prediction sample 和报告。
    ensure_analysis_dirs()
    config = active_config()
    features = _load_or_build_features()
    features["event_minute"] = pd.to_datetime(features["event_minute"])
    feature_source = relative_path(MODEL_DIR / "p9_window_features_1min.parquet")

    model_df = features[(features["failure_window"] == 0) & (features["post_maintenance"] == 0)].copy()
    model_df[TARGET] = model_df[TARGET].astype(int)
    feature_cols = _selected_feature_columns(model_df)
    if not feature_cols:
        raise RuntimeError("No P9 model feature columns were found.")

    train = model_df[model_df["event_minute"] < TRAIN_END].copy()
    valid = model_df[(model_df["event_minute"] >= TRAIN_END) & (model_df["event_minute"] < VALID_END)].copy()
    test = model_df[model_df["event_minute"] >= VALID_END].copy()
    # chronological split 是本脚本最重要的边界：不能随机打乱时间序列，否则会高估预警能力。
    if train.empty or valid.empty or test.empty:
        raise RuntimeError("Chronological P9 split produced an empty train, validation, or test set.")

    train_x, valid_x, test_x, _, _, _ = _clean_matrix(train, valid, test, feature_cols)
    train_y = train[TARGET].to_numpy(dtype=int)
    valid_y = valid[TARGET].to_numpy(dtype=int)
    test_y = test[TARGET].to_numpy(dtype=int)

    result: Dict = {
        "feature_source": feature_source,
        "target": TARGET,
        "rows_used": int(len(model_df)),
        "feature_count": int(len(feature_cols)),
        "features": feature_cols,
        "split": {
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

    coef = _fit_logistic_regression(train_x, train_y)
    valid_scores = _predict_logistic(valid_x, coef)
    test_scores = _predict_logistic(test_x, coef)
    threshold, validation_metrics = _choose_threshold(valid_y, valid_scores)
    # test metrics 使用 validation 选出的 threshold，避免在测试集上调参。
    test_metrics = _metrics(test_y, test_scores, threshold, test["event_minute"], "test")
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
    anomaly_valid_scores, anomaly_test_scores = _robust_anomaly_scores(train_x, valid_x, test_x, train_normal_mask)
    anomaly_threshold, anomaly_validation = _choose_threshold(valid_y, anomaly_valid_scores)
    anomaly_test = _metrics(test_y, anomaly_test_scores, anomaly_threshold, test["event_minute"], "test")
    result["models"]["robust_anomaly_score"] = {
        "status": "trained",
        "training": {"method": "median_mad_distance_from_train_normal_candidates"},
        "validation_metrics": anomaly_validation,
        "test_metrics": anomaly_test,
    }

    optional = _optional_sklearn_models(train_x, train_y, valid_x, valid_y, test_x, feature_cols)
    for model_name, payload in optional.items():
        if payload.get("status") == "trained":
            val_scores = np.asarray(payload.pop("validation_scores"), dtype=float)
            tst_scores = np.asarray(payload.pop("test_scores"), dtype=float)
            opt_threshold, opt_validation = _choose_threshold(valid_y, val_scores)
            payload["validation_metrics"] = opt_validation
            payload["test_metrics"] = _metrics(test_y, tst_scores, opt_threshold, test["event_minute"], "test")
        result["models"][model_name] = payload

    test_frame = test[["event_minute", TARGET, "failure_window"]].copy()
    test_frame["numpy_logistic_score"] = test_scores
    test_frame["prediction"] = (test_scores >= threshold).astype(int)
    result["lead_time"] = _lead_time_hours(test_frame, config)

    feature_weight_path = MODEL_DIR / "p9_logistic_feature_weights.tsv"
    write_tsv(feature_weight_path, feature_weights, ["feature", "coefficient", "abs_coefficient"])
    prediction_sample_path = MODEL_DIR / "p9_model_prediction_sample.tsv"
    test_frame.head(2000).to_csv(prediction_sample_path, sep="\t", index=False)

    figure_paths = _build_figures(result, test_frame, feature_weights)
    result["figures"] = [relative_path(path) for path in figure_paths]
    metrics_path = write_json(MODEL_DIR / "p9_model_metrics.json", result)
    report_path = write_markdown(REPORT_DIR / "p9_model_baseline_report.md", _markdown(result, figure_paths, feature_weight_path, prediction_sample_path))

    print("P9 model experiments completed.")
    print("metrics:", metrics_path)
    print("report:", report_path)
    print("feature_weights:", feature_weight_path)
    print("prediction_sample:", prediction_sample_path)
    for path in figure_paths:
        print("figure:", path)


if __name__ == "__main__":
    main()
