# -*- coding: utf-8 -*-
"""Profile and validate the MetroPT-3 raw CSV before ingestion."""
# 阅读提示：本文件读取 Raw CSV，生成最早期的数据画像。
# 它回答“原始数据有多少行、时间范围是什么、字段是否齐全”，为 ODS 入仓提供证据。
# 学习导读：
# - 链路位置：src/01，处于 Raw CSV 读取之后、ODS 写入之前。
# - 主要输入：Raw CSV 和字段标准化规则。
# - 主要输出：profile JSON 报告，记录行数、时间范围、故障窗口分布、空值和采样间隔。
# - 核心概念：profile 是“认识原始数据”，不是修正数据，也不是建模。
# - 边界提醒：这里的统计用于解释数据现状，不能单独证明模型效果或实时链路成功。
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import Window
from pyspark.sql import functions as F

from metropt_utils import (
    ANALOG_SENSORS,
    DIGITAL_SENSORS,
    assert_path_exists,
    create_metropt_spark,
    load_metropt_config,
    normalize_readings,
    read_metropt_csv,
    write_json_report,
)


def main() -> None:
    # Profile 阶段仍保持原始粒度，不做业务聚合；
    # 这里只标准化必要字段并产出质量摘要，避免把建模口径提前混入 Raw 画像。
    config = load_metropt_config()
    paths = config["paths"]
    spark = create_metropt_spark("MetroPT_01_Profile", config=config)
    spark.sparkContext.setLogLevel("WARN")

    assert_path_exists(spark, paths["input_csv"], "MetroPT-3 CSV")
    raw = read_metropt_csv(spark, paths["input_csv"], config)
    df = normalize_readings(raw, config)
    expected_rows = int(config.get("metropt", {}).get("expected_rows", 1516948))

    # Profile 先建立“数据规模和时间范围”的基准，后续每一层行数异常都要回到这个基准比较。
    row_count = df.count()
    time_bounds = df.agg(F.min("event_time").alias("min_event_time"), F.max("event_time").alias("max_event_time")).first()
    failure_counts = {str(r["failure_type"]): int(r["count"]) for r in df.groupBy("failure_type").count().collect()}
    operating_state_counts = {str(r["operating_state"]): int(r["count"]) for r in df.groupBy("operating_state").count().collect()}
    null_counts_row = df.agg(
        *[F.sum(F.when(F.col(c).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(c) for c in ANALOG_SENSORS + DIGITAL_SENSORS]
    ).first()
    daily_sample_stats = df.groupBy("dt").count().agg(
        F.count("*").alias("active_days"),
        F.min("count").alias("min_daily_samples"),
        F.max("count").alias("max_daily_samples"),
        F.avg("count").alias("avg_daily_samples"),
    ).first()
    duplicate_event_time_count = df.groupBy("event_time").count().filter(F.col("count") > 1).count()
    # 采样间隔用于发现时间序列断点；它解释数据是否适合按分钟窗口聚合。
    interval_stats = (
        df.select(
            "event_time",
            (
                F.col("event_time").cast("long")
                - F.lag("event_time").over(Window.orderBy("event_time")).cast("long")
            ).alias("interval_seconds"),
        )
        .filter(F.col("interval_seconds").isNotNull())
        .agg(
            F.min("interval_seconds").alias("min_interval_seconds"),
            F.max("interval_seconds").alias("max_interval_seconds"),
            F.avg("interval_seconds").alias("avg_interval_seconds"),
        )
        .first()
    )
    # digital sensor 的取值分布可以快速暴露字段解析错误，例如本应 0/1 的开关列出现异常字符串。
    digital_value_counts = {
        col_name: {str(r[col_name]): int(r["count"]) for r in df.groupBy(col_name).count().collect()}
        for col_name in DIGITAL_SENSORS
    }

    payload = {
        "dataset": config.get("metropt", {}).get("dataset_name", "MetroPT-3 Dataset"),
        "dataset_url": config.get("metropt", {}).get("dataset_url"),
        "input_csv": paths["input_csv"],
        "row_count": row_count,
        "expected_rows": expected_rows,
        "row_count_matches_expected": row_count == expected_rows,
        "min_event_time": str(time_bounds["min_event_time"]),
        "max_event_time": str(time_bounds["max_event_time"]),
        "analog_sensor_count": len(ANALOG_SENSORS),
        "digital_sensor_count": len(DIGITAL_SENSORS),
        "failure_counts": failure_counts,
        "operating_state_counts": operating_state_counts,
        "daily_sample_stats": {
            "active_days": int(daily_sample_stats["active_days"] or 0),
            "min_daily_samples": int(daily_sample_stats["min_daily_samples"] or 0),
            "max_daily_samples": int(daily_sample_stats["max_daily_samples"] or 0),
            "avg_daily_samples": float(daily_sample_stats["avg_daily_samples"] or 0.0),
        },
        "duplicate_event_time_count": int(duplicate_event_time_count),
        "sampling_interval_seconds": {
            "min": int(interval_stats["min_interval_seconds"] or 0),
            "max": int(interval_stats["max_interval_seconds"] or 0),
            "avg": float(interval_stats["avg_interval_seconds"] or 0.0),
        },
        "digital_value_counts": digital_value_counts,
        "sensor_null_counts": {c: int(null_counts_row[c] or 0) for c in ANALOG_SENSORS + DIGITAL_SENSORS},
    }
    target = write_json_report(spark, payload, paths["profile_dir"], "dataset_profile_json")
    # profile JSON 是说明和验收都能复用的轻量证据；它比 Spark 控制台输出更适合作长期留档。
    print("MetroPT 数据验收报告已写入:", target)
    print(payload)
    spark.stop()


if __name__ == "__main__":
    main()
