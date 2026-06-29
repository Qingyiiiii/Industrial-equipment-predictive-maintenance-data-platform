# -*- coding: utf-8 -*-
"""Build P9 EDA artifacts and minute-grain baseline features from MetroPT CSV."""
# 阅读提示：本文件从本地 CSV 构建 P9 深度 EDA 与 minute-grain feature table。
# 重点是把 weak label、压力差、数字信号切换和右对齐 rolling window 固化为可复现产物。
# 学习导读：
# - 链路位置：P9 特征工程核心脚本，基于 CSV-derived 路径生成第一版 minute features。
# - 主要输入：Raw CSV、weak label 规则、传感器字段和交互特征定义。
# - 主要输出：p9_window_features_1min.parquet、feature dictionary、EDA 报告和图表。
# - 核心概念：minute-grain features 把采样级数据压成每分钟窗口，便于时间切分模型训练。
# - 边界提醒：P9 CSV-derived features 是建模基线来源，后续 P10 会再用仓库层数据重建并做 parity。
import os
import sys
from pathlib import Path
from typing import Dict, List

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from analysis_common import (  # noqa: E402
    ANALOG_SENSORS,
    DIGITAL_SENSORS,
    FIGURE_DIR,
    MODEL_DIR,
    REPORT_DIR,
    ensure_analysis_dirs,
    save_figure,
    write_json,
    write_markdown,
)
from p9_common import (  # noqa: E402
    active_config,
    add_interaction_columns,
    add_operating_state,
    add_p9_labels,
    build_minute_feature_table,
    failure_window_records,
    read_metropt_csv,
    relative_path,
    write_tsv,
)


def _exclusive_label_segment(df: pd.DataFrame) -> pd.Series:
    # 多个弱标签可能嵌套，展示和统计时需要互斥 segment，避免同一行被重复解释。
    conditions = [
        df["failure_window"].eq(1),
        df["pre_failure_1h"].eq(1),
        df["pre_failure_6h"].eq(1) & df["pre_failure_1h"].eq(0),
        df["pre_failure_24h"].eq(1) & df["pre_failure_6h"].eq(0),
        df["post_maintenance"].eq(1),
        df["normal_candidate"].eq(1),
    ]
    choices = [
        "failure_window",
        "pre_failure_1h",
        "pre_failure_6h_only",
        "pre_failure_24h_only",
        "post_maintenance",
        "normal_candidate",
    ]
    return pd.Series(np.select(conditions, choices, default="other"), index=df.index)


def _top_correlation_pairs(corr: pd.DataFrame, limit: int = 10) -> List[Dict[str, float]]:
    rows = []
    cols = list(corr.columns)
    for i, left in enumerate(cols):
        for right in cols[i + 1 :]:
            value = corr.loc[left, right]
            if pd.notna(value):
                rows.append({"left": left, "right": right, "correlation": float(value), "abs_correlation": abs(float(value))})
    return sorted(rows, key=lambda row: row["abs_correlation"], reverse=True)[:limit]


def _feature_dictionary_rows() -> List[Dict[str, str]]:
    # Feature dictionary 明确每类特征的输入窗口和 leakage 风险，供读者复现时检查边界。
    rows = []
    for sensor in ANALOG_SENSORS:
        rows.append(
            {
                "feature_pattern": f"mean/std/min/max/first/last/slope_{sensor}",
                "source": sensor,
                "window": "1 minute event bucket",
                "description": f"Within-minute descriptive statistics and slope for {sensor}.",
                "leakage_risk": "no",
                "leakage_note": "Uses only readings inside the current minute bucket; for online use, emit after minute close.",
            }
        )
        for window in [1, 5, 15, 60]:
            rows.append(
                {
                    "feature_pattern": f"roll{window}_mean/std/min/max/q25/q75_mean_{sensor}",
                    "source": sensor,
                    "window": f"right-aligned past {window} minute(s)",
                    "description": f"Past-window rolling statistics for the minute-level mean of {sensor}.",
                    "leakage_risk": "no",
                    "leakage_note": "Right-aligned rolling window; no centered or future rows are used.",
                }
            )
    for feature in ["delta_tp3_reservoirs", "delta_tp2_tp3"]:
        rows.append(
            {
                "feature_pattern": f"mean/std/min/max/slope_{feature}",
                "source": feature,
                "window": "1 minute event bucket",
                "description": "Pressure relationship feature derived from same-row pressure sensors.",
                "leakage_risk": "no",
                "leakage_note": "Uses current or past sensor readings only.",
            }
        )
        for window in [1, 5, 15, 60]:
            rows.append(
                {
                    "feature_pattern": f"roll{window}_mean/std/min/max/q25/q75_mean_{feature}",
                    "source": feature,
                    "window": f"right-aligned past {window} minute(s)",
                    "description": "Rolling pressure-balance statistic.",
                    "leakage_risk": "no",
                    "leakage_note": "Right-aligned rolling window.",
                }
            )
    for sensor in DIGITAL_SENSORS:
        rows.append(
            {
                "feature_pattern": f"active_count_{sensor}, toggle_count_{sensor}",
                "source": sensor,
                "window": "1 minute event bucket",
                "description": f"Activation count and state-toggle count for binary signal {sensor}.",
                "leakage_risk": "no",
                "leakage_note": "Counts observed events in the current closed minute.",
            }
        )
    rows.extend(
        [
            {
                "feature_pattern": "state_loaded_count/state_unloaded_count/state_stopped_count",
                "source": "operating_state",
                "window": "1 minute event bucket",
                "description": "Approximate state duration counts derived from motor_current thresholds.",
                "leakage_risk": "no",
                "leakage_note": "Derived only from current sensor values.",
            },
            {
                "feature_pattern": "state_transition_count",
                "source": "operating_state",
                "window": "1 minute event bucket and rolling windows",
                "description": "Number of operating-state transitions.",
                "leakage_risk": "no",
                "leakage_note": "Uses previous observed state, not future state.",
            },
            {
                "feature_pattern": "failure_window/pre_failure_*/post_maintenance/normal_candidate/rul_seconds",
                "source": "configured failure windows",
                "window": "label/evaluation mask",
                "description": "Weak labels and evaluation masks from configured failure intervals.",
                "leakage_risk": "yes",
                "leakage_note": "Target or grouping fields only; must never be used as feature columns.",
            },
        ]
    )
    return rows


def _eda_markdown(summary: Dict, figure_paths: List[Path]) -> str:
    gap_lines = "\n".join(
        f"- `{row['event_time']}` after `{row['previous_event_time']}`: `{row['interval_seconds']}` seconds"
        for row in summary["top_sampling_gaps"][:10]
    )
    if not gap_lines:
        gap_lines = "- No large sampling gaps found."

    volatility_lines = "\n".join(
        f"- `{row['sensor']}`: std `{row['std']:.6f}`, mean `{row['mean']:.6f}`"
        for row in summary["volatility_ranking"][:10]
    )
    corr_lines = "\n".join(
        f"- `{row['left']}` / `{row['right']}`: `{row['correlation']:.4f}`"
        for row in summary["top_correlation_pairs"][:10]
    )
    pre_lines = "\n".join(
        f"- `{row['segment']}` / `{row['sensor']}`: mean `{row['mean']:.6f}`, delta vs normal `{row['delta_vs_normal']:.6f}`"
        for row in summary["pre_failure_sensor_contrast"][:18]
    )
    insight_lines = "\n".join(f"- {item}" for item in summary["business_insights"])
    figure_lines = "\n".join(f"- `{relative_path(path)}`" for path in figure_paths)

    return f"""# P9 Deep EDA Report

## Data Scope

- Source: full local CSV through project config.
- Row count: `{summary['row_count']}`.
- Time range: `{summary['time_range']['min_event_time']}` to `{summary['time_range']['max_event_time']}`.
- Active days: `{summary['active_days']}`.
- Failure-window rows: `{summary['failure_window_rows']}`.
- Failure-window rate: `{summary['failure_window_rate']:.6f}`.
- Minute feature rows generated: `{summary['minute_feature_rows']}`.

The failure-window fields are interval-derived weak labels, not manually verified row-level fault labels. Worker local results are based on CSV analysis and remain 待 master 验证 for cluster Parquet alignment.

## Sampling and Breaks

- Sampling interval seconds: min `{summary['sampling_interval_seconds']['min']}`, median `{summary['sampling_interval_seconds']['median']}`, average `{summary['sampling_interval_seconds']['mean']:.4f}`, max `{summary['sampling_interval_seconds']['max']}`.
- Top sampling gaps:

{gap_lines}

## Failure-Window and Pre-Failure Sensor Contrast

{pre_lines}

## Sensor Volatility Ranking

{volatility_lines}

## Correlation Highlights

{corr_lines}

## Business-Useful Signals

{insight_lines}

## Figures

{figure_lines}
"""


def _feature_markdown(feature_path: Path, feature_sample_path: Path, feature_dict_tsv: Path, feature_rows: int, feature_cols: int) -> str:
    return f"""# P9 Feature Dictionary

## Generated Feature Table

- Full local feature table: `{relative_path(feature_path)}`.
- Feature sample: `{relative_path(feature_sample_path)}`.
- Feature dictionary TSV: `{relative_path(feature_dict_tsv)}`.
- Rows: `{feature_rows}` minute-grain records.
- Columns: `{feature_cols}`.

## Design Rules

- Windows are right-aligned at 1min, 5min, 15min, and 60min scales.
- Rolling features use current and past minute buckets only.
- Pressure difference features include `tp3 - reservoirs` and `tp2 - tp3`.
- Digital features include activation counts and toggle counts.
- State features include state duration counts and state transition counts.
- Label fields are retained for offline evaluation, but they are explicitly marked as leakage-risk fields and must not enter model features.

## Feature Groups

| Group | Examples | Leakage risk |
| --- | --- | --- |
| Raw minute analog statistics | `mean_tp2`, `std_oil_temperature`, `slope_motor_current` | no |
| Multi-scale rolling statistics | `roll15_mean_mean_tp2`, `roll60_std_mean_oil_temperature` | no |
| Pressure deltas | `mean_delta_tp3_reservoirs`, `roll15_mean_mean_delta_tp2_tp3` | no |
| Digital activity | `active_count_dv_electric`, `toggle_count_comp` | no |
| Operating state | `state_loaded_count`, `state_transition_count` | no |
| Labels and masks | `pre_failure_24h`, `rul_seconds` | yes, target/grouping only |

## Validation Boundary

This worker artifact is generated from local CSV. Master should regenerate or compare it against the cluster ODS/DWS outputs before using it as an accepted project feature table.
"""


def _build_figures(df: pd.DataFrame, minute_features: pd.DataFrame, corr: pd.DataFrame, segment_means: pd.DataFrame) -> List[Path]:
    # 图表只展示 EDA 方向：采样趋势、故障窗口对比、相关性和运行状态变化，不替代模型评估。
    sns.set_theme(style="whitegrid")
    fig_paths: List[Path] = []

    daily = (
        df.assign(dt=df["event_time"].dt.date)
        .groupby("dt", observed=True)
        .agg(sample_count=("event_time", "size"), failure_window_rows=("failure_window", "sum"))
        .reset_index()
    )
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(pd.to_datetime(daily["dt"]), daily["sample_count"], color="#2f6f9f", linewidth=1.2, label="samples")
    ax.bar(pd.to_datetime(daily["dt"]), daily["failure_window_rows"], color="#c44e52", alpha=0.35, label="failure-window rows")
    ax.set_title("P9 Daily Samples and Failure-Window Rows")
    ax.set_xlabel("Date")
    ax.set_ylabel("Rows")
    ax.legend(loc="upper right")
    fig_paths.append(save_figure(fig, FIGURE_DIR / "p9_daily_sample_failure_trend.png"))
    plt.close(fig)

    selected_segments = ["pre_failure_24h_only", "pre_failure_6h_only", "pre_failure_1h", "failure_window"]
    selected_sensors = ["tp2", "tp3", "h1", "dv_pressure", "reservoirs", "oil_temperature", "motor_current"]
    normal = segment_means.loc["normal_candidate", selected_sensors] if "normal_candidate" in segment_means.index else None
    if normal is not None:
        delta = segment_means.reindex(selected_segments)[selected_sensors].subtract(normal, axis=1)
        fig, ax = plt.subplots(figsize=(10, 4.8))
        sns.heatmap(delta, cmap="vlag", center=0, annot=True, fmt=".2f", ax=ax)
        ax.set_title("P9 Pre-Failure and Failure Sensor Mean Delta vs Normal Candidate")
        ax.set_xlabel("Sensor")
        ax.set_ylabel("Segment")
        fig_paths.append(save_figure(fig, FIGURE_DIR / "p9_pre_failure_sensor_delta.png"))
        plt.close(fig)

    fig, ax = plt.subplots(figsize=(8, 6))
    sns.heatmap(corr, vmin=-1, vmax=1, cmap="vlag", annot=True, fmt=".2f", square=True, ax=ax)
    ax.set_title("P9 Analog and Pressure-Delta Correlation")
    fig_paths.append(save_figure(fig, FIGURE_DIR / "p9_sensor_correlation_heatmap.png"))
    plt.close(fig)

    hourly = (
        df.set_index("event_time")[["tp2", "tp3", "reservoirs", "motor_current", "oil_temperature"]]
        .resample("1h")
        .mean()
        .dropna(how="all")
    )
    fig, axes = plt.subplots(3, 1, figsize=(13, 8), sharex=True)
    axes[0].plot(hourly.index, hourly["tp2"], linewidth=0.8, label="tp2")
    axes[0].plot(hourly.index, hourly["tp3"], linewidth=0.8, label="tp3")
    axes[0].plot(hourly.index, hourly["reservoirs"], linewidth=0.8, label="reservoirs")
    axes[0].set_title("P9 Hourly Pressure, Current, and Oil Temperature")
    axes[0].set_ylabel("bar")
    axes[0].legend(loc="upper right")
    axes[1].plot(hourly.index, hourly["motor_current"], linewidth=0.8, color="#dd8452")
    axes[1].set_ylabel("A")
    axes[2].plot(hourly.index, hourly["oil_temperature"], linewidth=0.8, color="#55a868")
    axes[2].set_ylabel("C")
    axes[2].set_xlabel("Event Time")
    for record in failure_window_records(active_config()):
        for ax in axes:
            ax.axvspan(record["start"], record["end"], color="#c44e52", alpha=0.14)
    fig_paths.append(save_figure(fig, FIGURE_DIR / "p9_pressure_current_oil_fault_timeline.png"))
    plt.close(fig)

    transitions = (
        minute_features.assign(dt=minute_features["event_minute"].dt.date)
        .groupby("dt", observed=True)["state_transition_count"]
        .sum()
        .reset_index()
    )
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(pd.to_datetime(transitions["dt"]), transitions["state_transition_count"], linewidth=1.1, color="#8172b3")
    ax.set_title("P9 Daily Operating-State Transition Frequency")
    ax.set_xlabel("Date")
    ax.set_ylabel("Transitions")
    fig_paths.append(save_figure(fig, FIGURE_DIR / "p9_state_transition_frequency.png"))
    plt.close(fig)
    return fig_paths


def main() -> None:
    # 主流程读取全量 CSV，添加 P9 标签和交互特征，再输出 minute features、字典、EDA 摘要和图表。
    ensure_analysis_dirs()
    config = active_config()
    df = read_metropt_csv(config)
    # 这里一次性附加状态、交互特征和 weak labels，保证后续 EDA/feature table 使用同一份事件级口径。
    df = add_p9_labels(add_interaction_columns(add_operating_state(df)), config)
    df["label_segment"] = _exclusive_label_segment(df)

    minute_features = build_minute_feature_table(df)
    feature_path = MODEL_DIR / "p9_window_features_1min.parquet"
    feature_sample_path = MODEL_DIR / "p9_window_features_1min_sample.tsv"
    minute_features.to_parquet(feature_path, index=False)
    minute_features.head(500).to_csv(feature_sample_path, sep="\t", index=False)

    # feature dictionary 是说明和验收的边界说明：哪些特征来自当前/历史窗口，哪些列不能进模型。
    feature_dict_rows = _feature_dictionary_rows()
    feature_dict_tsv = MODEL_DIR / "p9_feature_dictionary.tsv"
    write_tsv(
        feature_dict_tsv,
        feature_dict_rows,
        ["feature_pattern", "source", "window", "description", "leakage_risk", "leakage_note"],
    )

    intervals = df["event_time"].diff().dt.total_seconds().dropna()
    top_gaps = (
        pd.DataFrame(
            {
                "previous_event_time": df["event_time"].shift(1),
                "event_time": df["event_time"],
                "interval_seconds": df["event_time"].diff().dt.total_seconds(),
            }
        )
        .dropna()
        .sort_values("interval_seconds", ascending=False)
        .head(10)
    )

    segment_means = df.groupby("label_segment", observed=True)[ANALOG_SENSORS + ["delta_tp3_reservoirs", "delta_tp2_tp3"]].mean()
    contrast_rows = []
    normal = segment_means.loc["normal_candidate"] if "normal_candidate" in segment_means.index else None
    for segment, values in segment_means.iterrows():
        if segment == "normal_candidate":
            continue
        for sensor in ANALOG_SENSORS:
            delta = float(values[sensor] - normal[sensor]) if normal is not None else np.nan
            contrast_rows.append({"segment": segment, "sensor": sensor, "mean": float(values[sensor]), "delta_vs_normal": delta})
    contrast_rows = sorted(contrast_rows, key=lambda row: abs(row["delta_vs_normal"]) if pd.notna(row["delta_vs_normal"]) else 0, reverse=True)

    # 相关性和对比图用于发现候选信号，不用于证明因果；后续模型才会在时间切分下验证。
    corr_columns = ANALOG_SENSORS + ["delta_tp3_reservoirs", "delta_tp2_tp3"]
    sample_for_corr = df[corr_columns].sample(n=min(200000, len(df)), random_state=42) if len(df) > 200000 else df[corr_columns]
    corr = sample_for_corr.corr(numeric_only=True)
    volatility = [
        {"sensor": sensor, "mean": float(df[sensor].mean()), "std": float(df[sensor].std())}
        for sensor in ANALOG_SENSORS + ["delta_tp3_reservoirs", "delta_tp2_tp3"]
    ]
    volatility = sorted(volatility, key=lambda row: abs(row["std"]), reverse=True)

    fig_paths = _build_figures(df, minute_features, corr, segment_means)
    business_insights = [
        "`oil_temperature` should remain a first-tier feature because thermal drift is directly tied to compressor stress and it ranks high in failure/pre-failure contrasts.",
        "`tp3 - reservoirs` is a pressure-balance feature with clear physical meaning: TP3 and reservoir pressure should be close during stable operation, so widening gaps can indicate air delivery or leak behavior.",
        "`motor_current` plus `COMP`/`DV_eletric`/`MPG` activation and toggle counts capture the compressor state machine better than any single raw digital signal.",
    ]

    summary = {
        "row_count": int(len(df)),
        "time_range": {
            "min_event_time": str(df["event_time"].min()),
            "max_event_time": str(df["event_time"].max()),
        },
        "active_days": int(df["event_time"].dt.date.nunique()),
        "failure_window_rows": int(df["failure_window"].sum()),
        "failure_window_rate": float(df["failure_window"].mean()),
        "minute_feature_rows": int(len(minute_features)),
        "sampling_interval_seconds": {
            "min": float(intervals.min()),
            "median": float(intervals.median()),
            "mean": float(intervals.mean()),
            "max": float(intervals.max()),
        },
        "top_sampling_gaps": [
            {
                "previous_event_time": str(row["previous_event_time"]),
                "event_time": str(row["event_time"]),
                "interval_seconds": float(row["interval_seconds"]),
            }
            for _, row in top_gaps.iterrows()
        ],
        "pre_failure_sensor_contrast": contrast_rows[:30],
        "volatility_ranking": volatility,
        "top_correlation_pairs": _top_correlation_pairs(corr),
        "business_insights": business_insights,
        "feature_table": str(feature_path),
        "feature_columns": list(minute_features.columns),
        "figures": [str(path) for path in fig_paths],
    }

    summary_path = write_json(MODEL_DIR / "p9_feature_eda_summary.json", summary)
    # P9 同时输出机器可读 JSON 和人可读 Markdown，方便自动验收和作品集讲解复用。
    eda_path = write_markdown(REPORT_DIR / "p9_eda_report.md", _eda_markdown(summary, fig_paths))
    feature_md_path = write_markdown(
        REPORT_DIR / "p9_feature_dictionary.md",
        _feature_markdown(feature_path, feature_sample_path, feature_dict_tsv, len(minute_features), len(minute_features.columns)),
    )

    print("P9 feature engineering and EDA completed.")
    print("feature_table:", feature_path)
    print("feature_sample:", feature_sample_path)
    print("feature_dictionary_tsv:", feature_dict_tsv)
    print("feature_eda_summary:", summary_path)
    print("eda_report:", eda_path)
    print("feature_dictionary_report:", feature_md_path)
    for path in fig_paths:
        print("figure:", path)


if __name__ == "__main__":
    main()
