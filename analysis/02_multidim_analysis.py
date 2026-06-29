# -*- coding: utf-8 -*-
"""MetroPT multidimensional analysis and visualization."""
# 阅读提示：本文件把 ODS/DWD/DWS 的质量事实转成 EDA 视角。
# 重点是运行状态、传感器联动、故障窗口对比和后续建模优先级。
# - 链路位置：analysis/02，在质量分析之后，把离线数据转成业务可解释的 EDA 图表。
# - 主要输入：ODS/DWD/DWS Parquet，尤其是传感器长表和窗口 KPI。
# - 主要输出：多维分析报告、传感器相关性、故障窗口对比和运行状态图表。
# - 边界提醒：EDA 图上的相关性不是因果关系，也不能直接当作模型结论。
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from analysis_common import (  # noqa: E402
    ANALOG_SENSORS,
    DIGITAL_SENSORS,
    FIGURE_DIR,
    FULL_ANALYSIS_PATH_KEYS,
    REPORT_DIR,
    available_columns,
    ensure_analysis_dirs,
    figure_relpath,
    load_config,
    prepare_spark_after_input_validation,
    read_parquet,
    save_figure,
    weighted_avg_expr,
    write_json,
    write_markdown,
)


def _downsample_by_row_number(df, order_col: str, max_rows: int):
    count = df.count()
    if count <= max_rows:
        return df.orderBy(order_col), count, 1
    step = max(1, count // max_rows)
    sampled = (
        df.withColumn("_analysis_row_no", F.row_number().over(Window.orderBy(order_col)))
        .filter((F.col("_analysis_row_no") % F.lit(step)) == F.lit(0))
        .drop("_analysis_row_no")
        .orderBy(order_col)
    )
    return sampled, count, step


def _build_failure_delta(failure_compare_rows):
    # 故障窗口对比只描述已知故障时间段内外的传感器差异；它是解释线索，不是强标签证明。
    by_sensor = {}
    for row in failure_compare_rows:
        item = by_sensor.setdefault(
            row["sensor_name"],
            {
                "sensor_name": row["sensor_name"],
                "sensor_type": row["sensor_type"],
                "normal_avg": None,
                "failure_avg": None,
                "normal_count": 0,
                "failure_count": 0,
                "avg_delta_failure_minus_normal": None,
            },
        )
        if int(row["is_failure_window"]) == 1:
            item["failure_avg"] = row["avg_sensor_value"]
            item["failure_count"] = row["sample_count"]
        else:
            item["normal_avg"] = row["avg_sensor_value"]
            item["normal_count"] = row["sample_count"]
    out = []
    for item in by_sensor.values():
        if item["normal_avg"] is not None and item["failure_avg"] is not None:
            item["avg_delta_failure_minus_normal"] = item["failure_avg"] - item["normal_avg"]
        out.append(item)
    return sorted(
        out,
        key=lambda row: abs(row["avg_delta_failure_minus_normal"] or 0.0),
        reverse=True,
    )


def _choose_attention_sensors(failure_delta_rows, sensor_kpi_rows, limit=3):
    # 优先关注的传感器由故障窗口变化和 sensor KPI 共同决定，避免只凭单一统计量排序。
    selected = []
    for row in failure_delta_rows:
        if row["sensor_name"] not in selected:
            selected.append(row["sensor_name"])
        if len(selected) >= limit:
            return selected

    std_rank = sorted(
        sensor_kpi_rows,
        key=lambda row: abs(row.get("std_sensor_value") or 0.0),
        reverse=True,
    )
    for row in std_rank:
        if row["sensor_name"] not in selected:
            selected.append(row["sensor_name"])
        if len(selected) >= limit:
            break
    return selected


def _markdown(summary, figure_paths):
    attention_lines = "\n".join(
        f"- `{name}`: {summary['attention_sensor_reasons'].get(name, 'ranked by failure-window contrast or volatility')}"
        for name in summary["attention_sensors"]
    )
    state_lines = "\n".join(
        f"- `{row['operating_state']}`: samples `{row['sample_count']}`, failure rate `{row['failure_window_rate']:.6f}`, "
        f"avg current `{row.get('avg_motor_current', 0.0):.4f}`"
        for row in summary["operating_state_metrics"]
    )
    figure_lines = "\n".join(f"- `{figure_relpath(path)}`" for path in figure_paths)
    failure_lines = "\n".join(
        f"- `{row['sensor_name']}`: failure-normal avg delta `{(row['avg_delta_failure_minus_normal'] or 0.0):.6f}`"
        for row in summary["failure_contrast_top"]
    )

    return f"""# MetroPT Multidimensional Analysis Report

## Executive Summary

- Data time range: `{summary['time_range']['min_event_time']}` to `{summary['time_range']['max_event_time']}`.
- Active days: `{summary['active_day_count']}`.
- ODS samples: `{summary['ods_row_count']}`.
- Window KPI rows: `{summary['window_kpi_row_count']}`.
- Sensor KPI rows: `{summary['sensor_kpi_row_count']}`.
- Failure-window samples: `{summary['failure_sample_count']}`.
- Failure-window rate: `{summary['failure_window_rate']:.6f}`.

The failure label used here is derived from the official failure time intervals configured in the project. It is not a manually verified row-level fault label, so model and analysis results should be treated as baseline evidence rather than proof of causal failure behavior.

## Operating State

{state_lines}

## Sensors Worth Tracking First

{attention_lines}

## Failure Window Contrast

Top average shifts between failure-window and normal-window samples:

{failure_lines}

These sensors are candidates for monitoring and feature engineering. They should be validated again after the full VM run because current results depend on the generated ODS/DWD/DWS artifacts.

## Modeling Boundary

The first model version should remain a baseline. Class imbalance, time leakage, and interval-derived labels are the main risks. Any reported accuracy must be read together with recall, F1, the confusion matrix, and the time-based train/test split.

## Figures

{figure_lines}
"""


def main() -> None:
    # 主流程生成多维统计和图表，供作品集叙事和 P9 进一步特征设计使用。
    ensure_analysis_dirs()
    config = load_config()
    spark = prepare_spark_after_input_validation(
        "MetroPT_Analysis_02_Multidim",
        config,
        FULL_ANALYSIS_PATH_KEYS,
    )[0]

    try:
        ods = read_parquet(spark, config, "ods_readings_parquet")
        dwd = read_parquet(spark, config, "dwd_sensor_long")
        window_kpi = read_parquet(spark, config, "dws_window_kpi")
        sensor_kpi = read_parquet(spark, config, "dws_sensor_kpi")

        ods_row_count = ods.count()
        window_kpi_row_count = window_kpi.count()
        sensor_kpi_row_count = sensor_kpi.count()
        time_bounds = ods.agg(
            F.min("event_time").alias("min_event_time"),
            F.max("event_time").alias("max_event_time"),
        ).first()
        active_day_count = ods.select("dt").distinct().count()
        failure_sample_count = int(ods.agg(F.sum("is_failure_window").alias("cnt")).first()["cnt"] or 0)
        failure_window_rate = float(failure_sample_count / ods_row_count) if ods_row_count else 0.0

        # daily_rows 把故障窗口样本数按天展示，帮助读者理解故障窗口在时间轴上的分布。
        daily_rows = [
            {"dt": str(row["dt"]), "sample_count": int(row["sample_count"]), "failure_sample_count": int(row["failure_sample_count"] or 0)}
            for row in ods.groupBy("dt")
            .agg(
                F.count("*").alias("sample_count"),
                F.sum("is_failure_window").alias("failure_sample_count"),
            )
            .orderBy("dt")
            .collect()
        ]

        operating_state_rows = [
            row.asDict()
            for row in ods.groupBy("operating_state")
            .agg(
                F.count("*").alias("sample_count"),
                F.sum("is_failure_window").cast("long").alias("failure_sample_count"),
                F.avg("tp2").alias("avg_tp2"),
                F.avg("tp3").alias("avg_tp3"),
                F.avg("reservoirs").alias("avg_reservoirs"),
                F.avg("oil_temperature").alias("avg_oil_temperature"),
                F.avg("motor_current").alias("avg_motor_current"),
            )
            .withColumn(
                "failure_window_rate",
                F.when(F.col("sample_count") > 0, F.col("failure_sample_count") / F.col("sample_count")).otherwise(F.lit(0.0)),
            )
            .orderBy("operating_state")
            .collect()
        ]

        sensor_kpi_rows = [row.asDict() for row in sensor_kpi.orderBy("sensor_name").collect()]
        # failure_compare_rows 比较 failure vs normal 的传感器均值差异，是后续关注传感器排序的依据。
        failure_compare_rows = [
            row.asDict()
            for row in dwd.groupBy("sensor_name", "sensor_type", "is_failure_window")
            .agg(
                F.count("*").alias("sample_count"),
                F.avg("sensor_value").alias("avg_sensor_value"),
                F.stddev("sensor_value").alias("std_sensor_value"),
            )
            .orderBy("sensor_name", "is_failure_window")
            .collect()
        ]
        failure_delta_rows = _build_failure_delta(failure_compare_rows)
        attention_sensors = _choose_attention_sensors(failure_delta_rows, sensor_kpi_rows)
        attention_reasons = {}
        for sensor_name in attention_sensors:
            delta = next((row for row in failure_delta_rows if row["sensor_name"] == sensor_name), None)
            kpi = next((row for row in sensor_kpi_rows if row["sensor_name"] == sensor_name), None)
            pieces = []
            if delta and delta["avg_delta_failure_minus_normal"] is not None:
                pieces.append(f"failure-window mean shift {delta['avg_delta_failure_minus_normal']:.6f}")
            if kpi:
                pieces.append(f"std {float(kpi.get('std_sensor_value') or 0.0):.6f}")
            attention_reasons[sensor_name] = "; ".join(pieces) if pieces else "ranked by available KPI signal"

        avg_cols = available_columns(window_kpi, [f"avg_{sensor}" for sensor in ANALOG_SENSORS])
        time_series_aggs = [
            F.sum("sample_count").alias("sample_count"),
            F.sum("failure_sample_count").cast("long").alias("failure_sample_count"),
        ]
        time_series_aggs.extend(weighted_avg_expr(F, col_name).alias(col_name) for col_name in avg_cols)
        window_ts = window_kpi.groupBy("event_minute", "dt").agg(*time_series_aggs)
        # 图表只抽样最多 5000 个时间点，保证报告生成稳定；抽样不改变 JSON 中的原始行数记录。
        window_ts_sampled, window_ts_count, window_ts_step = _downsample_by_row_number(window_ts, "event_minute", 5000)
        ts_pdf = window_ts_sampled.toPandas()

        corr_cols = [col_name for col_name in avg_cols if col_name in window_ts.columns]
        corr_fraction = 1.0
        if window_ts_count > 50000:
            corr_fraction = 50000 / float(window_ts_count)
        corr_pdf = (
            window_ts.select(*corr_cols)
            .dropna()
            .sample(withReplacement=False, fraction=corr_fraction, seed=42)
            .limit(50000)
            .toPandas()
            if corr_cols
            else pd.DataFrame()
        )
        corr_matrix = corr_pdf.corr(numeric_only=True) if not corr_pdf.empty else pd.DataFrame()

        fig_paths = []
        sns.set_theme(style="whitegrid")

        daily_pdf = pd.DataFrame(daily_rows)
        if not daily_pdf.empty:
            fig, ax = plt.subplots(figsize=(12, 4))
            ax.plot(daily_pdf["dt"], daily_pdf["sample_count"], color="#2f6f9f", linewidth=1.3, label="samples")
            ax.bar(daily_pdf["dt"], daily_pdf["failure_sample_count"], color="#c44e52", alpha=0.35, label="failure-window samples")
            ax.set_title("Daily Samples and Failure-Window Samples")
            ax.set_xlabel("Date")
            ax.set_ylabel("Samples")
            ax.tick_params(axis="x", labelrotation=45)
            ax.legend()
            fig_paths.append(save_figure(fig, FIGURE_DIR / "multidim_daily_samples_failure_trend.png"))
            plt.close(fig)

        state_pdf = pd.DataFrame(operating_state_rows)
        if not state_pdf.empty:
            fig, ax = plt.subplots(figsize=(7, 4))
            sns.barplot(data=state_pdf, x="operating_state", y="sample_count", ax=ax, color="#4c72b0")
            ax.set_title("Operating State Sample Distribution")
            ax.set_xlabel("Operating State")
            ax.set_ylabel("Samples")
            fig_paths.append(save_figure(fig, FIGURE_DIR / "operating_state_distribution.png"))
            plt.close(fig)

        sensor_pdf = pd.DataFrame(sensor_kpi_rows)
        if not sensor_pdf.empty:
            sensor_rank = sensor_pdf.sort_values("std_sensor_value", ascending=False).head(15)
            fig, axes = plt.subplots(1, 2, figsize=(13, 5))
            sns.barplot(data=sensor_pdf.sort_values("avg_sensor_value", ascending=False), y="sensor_name", x="avg_sensor_value", ax=axes[0], color="#55a868")
            axes[0].set_title("Sensor Mean Ranking")
            axes[0].set_xlabel("Mean")
            axes[0].set_ylabel("Sensor")
            sns.barplot(data=sensor_rank, y="sensor_name", x="std_sensor_value", ax=axes[1], color="#8172b3")
            axes[1].set_title("Sensor Volatility Ranking")
            axes[1].set_xlabel("Stddev")
            axes[1].set_ylabel("")
            fig_paths.append(save_figure(fig, FIGURE_DIR / "sensor_mean_volatility_ranking.png"))
            plt.close(fig)

        failure_delta_pdf = pd.DataFrame(failure_delta_rows)
        if not failure_delta_pdf.empty:
            # 故障窗口对比图用于发现候选信号，不应被单独解释为模型因果结论。
            plot_delta = failure_delta_pdf.head(15).copy()
            plot_delta["abs_delta"] = plot_delta["avg_delta_failure_minus_normal"].abs()
            fig, ax = plt.subplots(figsize=(9, 5))
            sns.barplot(data=plot_delta, y="sensor_name", x="avg_delta_failure_minus_normal", ax=ax, color="#c44e52")
            ax.axvline(0, color="#222222", linewidth=0.8)
            ax.set_title("Failure Window vs Normal Average Sensor Shift")
            ax.set_xlabel("Failure Avg - Normal Avg")
            ax.set_ylabel("Sensor")
            fig_paths.append(save_figure(fig, FIGURE_DIR / "failure_window_sensor_contrast.png"))
            plt.close(fig)

        if not ts_pdf.empty:
            ts_pdf["event_minute"] = pd.to_datetime(ts_pdf["event_minute"])
            fig, axes = plt.subplots(3, 1, figsize=(13, 9), sharex=True)
            for col_name in ["avg_tp2", "avg_tp3", "avg_reservoirs"]:
                if col_name in ts_pdf.columns:
                    axes[0].plot(ts_pdf["event_minute"], ts_pdf[col_name], linewidth=0.8, label=col_name)
            axes[0].set_title("Pressure Signals")
            axes[0].set_ylabel("bar")
            axes[0].legend(loc="upper right")
            if "avg_motor_current" in ts_pdf.columns:
                axes[1].plot(ts_pdf["event_minute"], ts_pdf["avg_motor_current"], color="#dd8452", linewidth=0.8)
            axes[1].set_title("Motor Current")
            axes[1].set_ylabel("ampere")
            if "avg_oil_temperature" in ts_pdf.columns:
                axes[2].plot(ts_pdf["event_minute"], ts_pdf["avg_oil_temperature"], color="#55a868", linewidth=0.8)
            axes[2].set_title("Oil Temperature")
            axes[2].set_ylabel("celsius")
            axes[2].set_xlabel("Event Minute")
            fig_paths.append(save_figure(fig, FIGURE_DIR / "pressure_current_temperature_timeseries.png"))
            plt.close(fig)

        if not corr_matrix.empty:
            fig, ax = plt.subplots(figsize=(8, 6))
            sns.heatmap(corr_matrix, vmin=-1, vmax=1, cmap="vlag", annot=True, fmt=".2f", square=True, ax=ax)
            ax.set_title("Analog Sensor Correlation Heatmap")
            fig_paths.append(save_figure(fig, FIGURE_DIR / "sensor_correlation_heatmap.png"))
            plt.close(fig)

        summary = {
            # summary 保留图表背后的结构化统计，方便后续 P9 报告和讲解引用。
            "ods_row_count": ods_row_count,
            "window_kpi_row_count": window_kpi_row_count,
            "sensor_kpi_row_count": sensor_kpi_row_count,
            "time_range": {
                "min_event_time": time_bounds["min_event_time"],
                "max_event_time": time_bounds["max_event_time"],
            },
            "active_day_count": active_day_count,
            "failure_sample_count": failure_sample_count,
            "failure_window_rate": failure_window_rate,
            "operating_state_metrics": operating_state_rows,
            "daily_samples": daily_rows,
            "sensor_kpi": sensor_kpi_rows,
            "failure_contrast_top": failure_delta_rows[:10],
            "attention_sensors": attention_sensors,
            "attention_sensor_reasons": attention_reasons,
            "window_time_series_original_rows": window_ts_count,
            "window_time_series_downsample_step": window_ts_step,
            "correlation_columns": corr_cols,
            "figures": [str(path) for path in fig_paths],
        }
        json_path = write_json(REPORT_DIR / "metropt_multidim_analysis_summary.json", summary)
        md_path = write_markdown(REPORT_DIR / "metropt_multidim_analysis_report.md", _markdown(summary, fig_paths))
        print("MetroPT multidimensional analysis completed.")
        print("json_summary:", json_path)
        print("markdown_report:", md_path)
        for path in fig_paths:
            print("figure:", path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
