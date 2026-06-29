# -*- coding: utf-8 -*-
"""Calculate MetroPT offline DWS KPIs."""
# 阅读提示：本文件是离线链路的 DWS 汇总层。
# 它把 ODS/DWD 明细压缩为整体 KPI、时间窗口 KPI 和传感器 KPI，供 Hive/BI/查询复验使用。
# 学习导读：
# - 链路位置：src/04，负责从明细层生成可查询、可展示、可复验的 DWS 指标层。
# - 主要输入：ODS readings 和 DWD sensor_long。
# - 主要输出：overall/window/sensor 三类 KPI Parquet。
# - 核心概念：DWS 是聚合结果，适合 dashboard、SQL 验证和后续分析基线。
# - 边界提醒：聚合指标能说明统计现象，但不能直接等同于故障诊断模型。
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F

from metropt_utils import ANALOG_SENSORS, DIGITAL_SENSORS, assert_path_exists, create_metropt_spark, load_metropt_config


def _failure_rate(total_col: str = "sample_count", failure_col: str = "failure_sample_count"):
    # failure rate 是报告和看板反复使用的指标；
    # 分母为 0 时显式给 0，避免 Spark SQL 聚合产生 null 或除零异常。
    return F.when(F.col(total_col) > 0, F.col(failure_col).cast("double") / F.col(total_col)).otherwise(F.lit(0.0))


def main() -> None:
    # 主流程分别产出 overall、window、sensor 三类 DWS。
    # 这些表是后续 Hive 发布、Doris 装载、P9 dashboard SQL 的基础输入。
    config = load_metropt_config()
    paths = config["paths"]
    spark = create_metropt_spark("MetroPT_04_KPI_Calc", config=config)
    spark.sparkContext.setLogLevel("WARN")

    assert_path_exists(spark, paths["ods_readings_parquet"], "MetroPT ODS readings")
    assert_path_exists(spark, paths["dwd_sensor_long"], "MetroPT DWD sensor_long")

    readings = spark.read.parquet(paths["ods_readings_parquet"])
    sensor_long = spark.read.parquet(paths["dwd_sensor_long"])

    # overall KPI 是全局摘要，用来快速回答数据集覆盖范围和故障窗口占比。
    overall = (
        readings.agg(
            F.count("*").alias("sample_count"),
            F.sum("is_failure_window").cast("long").alias("failure_sample_count"),
            F.min("event_time").alias("min_event_time"),
            F.max("event_time").alias("max_event_time"),
            F.countDistinct("dt").alias("active_day_count"),
        )
        .withColumn("failure_window_rate", _failure_rate())
        .withColumn("metric_type", F.lit("overall"))
    )

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
    digital_aggs = [F.sum(F.col(col_name).cast("double")).alias(f"active_count_{col_name}") for col_name in DIGITAL_SENSORS]

    # window KPI 是 dashboard 和 P9/P12 查询最常用的时间粒度：一分钟 + 运行状态。
    window_kpi = (
        readings.groupBy("event_minute", "dt", "operating_state")
        .agg(
            F.count("*").alias("sample_count"),
            F.sum("is_failure_window").cast("long").alias("failure_sample_count"),
            *analog_aggs,
            *digital_aggs,
        )
        .withColumn("failure_window_rate", _failure_rate())
        .withColumn("minute_bucket", F.date_format("event_minute", "yyyy-MM-dd HH:mm:00"))
    )

    # sensor KPI 从 DWD 长表聚合，可以统一比较不同传感器的分布和故障窗口占比。
    sensor_kpi = (
        sensor_long.groupBy("sensor_name", "sensor_type", "station_id", "unit")
        .agg(
            F.count("*").alias("sample_count"),
            F.sum("is_failure_window").cast("long").alias("failure_sample_count"),
            F.avg("sensor_value").alias("avg_sensor_value"),
            F.stddev("sensor_value").alias("std_sensor_value"),
            F.min("sensor_value").alias("min_sensor_value"),
            F.max("sensor_value").alias("max_sensor_value"),
        )
        .withColumn("failure_window_rate", _failure_rate())
    )

    overall.write.mode("overwrite").parquet(paths["dws_overall_kpi"])
    window_kpi.write.mode("overwrite").partitionBy("dt").parquet(paths["dws_window_kpi"])
    sensor_kpi.write.mode("overwrite").parquet(paths["dws_sensor_kpi"])

    print("MetroPT DWS overall 已写入:", paths["dws_overall_kpi"])
    print("MetroPT DWS window KPI 已写入:", paths["dws_window_kpi"])
    print("MetroPT DWS sensor KPI 已写入:", paths["dws_sensor_kpi"])
    outputs = [
        ("overall", paths["dws_overall_kpi"]),
        ("window", paths["dws_window_kpi"]),
        ("sensor", paths["dws_sensor_kpi"]),
    ]
    for label, path in outputs:
        # 写出后立即回读计数，防止 Spark lazy execution 把错误延迟到后续 Hive 发布阶段才暴露。
        out = spark.read.parquet(path)
        row_count = out.count()
        print(f"DWS {label} 输出记录数:", row_count)
        if row_count <= 0:
            raise RuntimeError(f"DWS {label} 输出为空: {path}")
        out.printSchema()
    spark.stop()


if __name__ == "__main__":
    main()
