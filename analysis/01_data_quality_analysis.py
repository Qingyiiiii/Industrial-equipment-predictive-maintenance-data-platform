# -*- coding: utf-8 -*-
"""MetroPT data quality analysis based on normalized ODS Parquet."""
# 阅读提示：本文件关注 ODS 层数据质量，不讨论模型效果。
# 输出的 Markdown/JSON/figure 用来判断采样、时间范围、空值和离散字段是否可靠。
# - 链路位置：analysis/01，基于 ODS readings 做基础质量分析。
# - 主要输入：src/02 生成的 ODS Parquet。
# - 主要输出：质量报告、JSON 摘要和采样/空值/离散字段图表。
# - 边界提醒：质量报告不是验收 P14，也不能证明预测维护模型已经可用。
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from analysis_common import (  # noqa: E402
    ANALOG_SENSORS,
    DIGITAL_SENSORS,
    FIGURE_DIR,
    REPORT_DIR,
    ensure_analysis_dirs,
    figure_relpath,
    load_config,
    prepare_spark_after_input_validation,
    read_parquet,
    save_figure,
    write_json,
    write_markdown,
)


def _quality_markdown(summary, figure_paths):
    # 报告模板只汇总质量事实，不做业务预测结论；后续 EDA/模型脚本再解释这些质量结果。
    anomalous_days = summary["daily_sample_quality"]["anomalous_days"]
    top_anomalies = anomalous_days[:10]
    anomaly_lines = "\n".join(
        f"- {row['dt']}: {row['count']} samples" for row in top_anomalies
    ) or "- No obvious daily sample-count outliers by the median +/-20% rule."

    digital_lines = []
    for sensor_name, counts in summary["digital_value_counts"].items():
        digital_lines.append(f"- `{sensor_name}`: {counts}")

    return f"""# MetroPT Data Quality Report

## Summary

- Row count: `{summary['row_count']}`.
- Expected row count: `{summary['expected_rows']}`.
- Row count matches expected: `{summary['row_count_matches_expected']}`.
- Time range: `{summary['time_range']['min_event_time']}` to `{summary['time_range']['max_event_time']}`.
- Duplicate event_time count: `{summary['duplicate_event_time_count']}`.
- Sampling interval seconds: min `{summary['sampling_interval_seconds']['min']}`, avg `{summary['sampling_interval_seconds']['avg']:.4f}`, max `{summary['sampling_interval_seconds']['max']}`.
- Failure-window labels are derived from configured official failure intervals; they are not manually verified row-level labels.

## Daily Sample Quality

- Active days: `{summary['daily_sample_quality']['active_days']}`.
- Median daily samples: `{summary['daily_sample_quality']['median_daily_samples']}`.
- Anomalous days by median +/-20% rule: `{len(anomalous_days)}`.

{anomaly_lines}

## Null Counts

All configured analog and digital sensors are checked after ODS normalization.

```json
{summary['sensor_null_counts']}
```

## Digital Sensor Values

{chr(10).join(digital_lines)}

## Figures

{chr(10).join(f'- `{figure_relpath(path)}`' for path in figure_paths)}
"""


def main() -> None:
    # 主流程读取 ODS，计算行数、时间跨度、采样间隔、空值、数字传感器取值，并保存图表。
    ensure_analysis_dirs()
    config = load_config()
    expected_rows = int(config.get("metropt", {}).get("expected_rows", 1516948))
    spark = prepare_spark_after_input_validation(
        "MetroPT_Analysis_01_Data_Quality",
        config,
        ["ods_readings_parquet"],
    )[0]

    try:
        df = read_parquet(spark, config, "ods_readings_parquet")
        row_count = df.count()
        time_bounds = df.agg(
            F.min("event_time").alias("min_event_time"),
            F.max("event_time").alias("max_event_time"),
        ).first()

        daily_rows = [
            {"dt": str(row["dt"]), "count": int(row["count"])}
            for row in df.groupBy("dt").count().orderBy("dt").collect()
        ]
        daily_counts = [row["count"] for row in daily_rows]
        median_daily = int(statistics.median(daily_counts)) if daily_counts else 0
        lower = median_daily * 0.8
        upper = median_daily * 1.2
        # 日样本数异常用于发现采样中断或数据缺口，不直接说明设备故障。
        anomalous_days = [
            row for row in daily_rows if median_daily > 0 and (row["count"] < lower or row["count"] > upper)
        ]

        interval_df = df.select(
            "event_time",
            (
                F.col("event_time").cast("long")
                - F.lag("event_time").over(Window.orderBy("event_time")).cast("long")
            ).alias("interval_seconds"),
        ).filter(F.col("interval_seconds").isNotNull())
        interval_stats = interval_df.agg(
            F.min("interval_seconds").alias("min"),
            F.avg("interval_seconds").alias("avg"),
            F.max("interval_seconds").alias("max"),
        ).first()
        interval_counts = [
            {"interval_seconds": int(row["interval_seconds"]), "count": int(row["count"])}
            for row in interval_df.groupBy("interval_seconds").count().orderBy("interval_seconds").collect()
        ]

        duplicate_event_time_count = df.groupBy("event_time").count().filter(F.col("count") > 1).count()
        # null/digital value 检查聚焦 schema 和解析质量，避免把解析错误误认为业务信号。
        null_counts = df.agg(
            *[
                F.sum(F.when(F.col(col_name).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(col_name)
                for col_name in ANALOG_SENSORS + DIGITAL_SENSORS
            ]
        ).first()

        digital_value_counts = {}
        for col_name in DIGITAL_SENSORS:
            digital_value_counts[col_name] = {
                str(row[col_name]): int(row["count"])
                for row in df.groupBy(col_name).count().orderBy(col_name).collect()
            }

        analog_aggs = []
        for col_name in ANALOG_SENSORS:
            analog_aggs.extend(
                [
                    F.avg(col_name).alias(f"avg_{col_name}"),
                    F.stddev(col_name).alias(f"std_{col_name}"),
                    F.min(col_name).alias(f"min_{col_name}"),
                    F.max(col_name).alias(f"max_{col_name}"),
                ]
            )
        analog_row = df.agg(*analog_aggs).first().asDict()
        analog_summary = {
            col_name: {
                "avg": analog_row.get(f"avg_{col_name}"),
                "std": analog_row.get(f"std_{col_name}"),
                "min": analog_row.get(f"min_{col_name}"),
                "max": analog_row.get(f"max_{col_name}"),
            }
            for col_name in ANALOG_SENSORS
        }

        failure_counts = {
            str(row["failure_type"]): int(row["count"])
            for row in df.groupBy("failure_type").count().orderBy("failure_type").collect()
        }
        operating_state_counts = {
            str(row["operating_state"]): int(row["count"])
            for row in df.groupBy("operating_state").count().orderBy("operating_state").collect()
        }

        fig_paths = []
        if daily_rows:
            # 质量图只展示采样覆盖情况，用于判断后续 EDA 和模型是否有稳定输入。
            xs = [row["dt"] for row in daily_rows]
            ys = [row["count"] for row in daily_rows]
            fig, ax = plt.subplots(figsize=(12, 4))
            ax.plot(xs, ys, linewidth=1.4, color="#2f6f9f")
            ax.axhline(median_daily, color="#c44e52", linestyle="--", linewidth=1, label="median")
            ax.set_title("MetroPT Daily Sample Count")
            ax.set_xlabel("Date")
            ax.set_ylabel("Samples")
            ax.tick_params(axis="x", labelrotation=45)
            ax.legend()
            fig_paths.append(save_figure(fig, FIGURE_DIR / "daily_sample_count_trend.png"))
            plt.close(fig)

        summary = {
            "row_count": row_count,
            "expected_rows": expected_rows,
            "row_count_matches_expected": row_count == expected_rows,
            "time_range": {
                "min_event_time": time_bounds["min_event_time"],
                "max_event_time": time_bounds["max_event_time"],
            },
            "failure_counts": failure_counts,
            "operating_state_counts": operating_state_counts,
            "daily_sample_quality": {
                "active_days": len(daily_rows),
                "median_daily_samples": median_daily,
                "anomalous_days": anomalous_days,
            },
            "duplicate_event_time_count": int(duplicate_event_time_count),
            "sampling_interval_seconds": {
                "min": int(interval_stats["min"] or 0),
                "avg": float(interval_stats["avg"] or 0.0),
                "max": int(interval_stats["max"] or 0),
                "distribution": interval_counts,
            },
            "sensor_null_counts": {col_name: int(null_counts[col_name] or 0) for col_name in ANALOG_SENSORS + DIGITAL_SENSORS},
            "digital_value_counts": digital_value_counts,
            "analog_summary": analog_summary,
            "figures": [str(path) for path in fig_paths],
        }
        # JSON 给自动验收和后续脚本读取，Markdown 给讲解和作品集阅读。
        json_path = write_json(REPORT_DIR / "metropt_data_quality_report.json", summary)
        md_path = write_markdown(
            REPORT_DIR / "metropt_data_quality_report.md",
            _quality_markdown(summary, fig_paths),
        )
        print("MetroPT data quality analysis completed.")
        print("json_report:", json_path)
        print("markdown_report:", md_path)
        for path in fig_paths:
            print("figure:", path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
