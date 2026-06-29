# -*- coding: utf-8 -*-
"""Expand normalized MetroPT readings into a DWD sensor-long table."""
# 阅读提示：本文件把 ODS 宽表展开成 DWD sensor long。
# 长表让后续可以按 sensor_name、sensor_type、unit 做统一统计和 BI 查询。
# 学习导读：
# - 链路位置：src/03，负责 ODS 宽表到 DWD 传感器长表的表达转换。
# - 主要输入：ods/readings 宽表和传感器维表定义。
# - 主要输出：dwd/sensor_long，每个采样点会展开成多个传感器行。
# - 核心概念：DWD 不改变采样事实，只把“多列传感器”改成“传感器名称 + 数值”的标准明细。
# - 边界提醒：DWD 行数通常约等于 ODS 行数乘以传感器数量，行数变大是设计结果。
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark import StorageLevel
from pyspark.sql import functions as F

from metropt_utils import (
    ANALOG_SENSORS,
    DIGITAL_SENSORS,
    assert_path_exists,
    create_metropt_spark,
    load_metropt_config,
    sensor_dimension_rows,
)


def main() -> None:
    # DWD 仍保留采样级时间粒度，只改变表达方式：
    # 每个传感器变成一行，便于传感器维度分析和 sensor KPI 汇总。
    config = load_metropt_config()
    paths = config["paths"]
    spark = create_metropt_spark("MetroPT_03_DWD_Sensor_Long", config=config)
    spark.sparkContext.setLogLevel("WARN")

    assert_path_exists(spark, paths["ods_readings_parquet"], "MetroPT ODS readings")
    sensor_names = ANALOG_SENSORS + DIGITAL_SENSORS
    # sensor dimension 给长表补充传感器类型、站点和单位，避免 BI 层只能看到裸字段名。
    dim = spark.createDataFrame(sensor_dimension_rows(sensor_names))
    readings = spark.read.parquet(paths["ods_readings_parquet"]).persist(StorageLevel.MEMORY_AND_DISK)
    long_df = None

    try:
        # 用 array<struct> + explode 把宽表的 15 个传感器列展开成统一的 sensor_name/sensor_value。
        sensor_structs = [
            F.struct(
                F.lit(sensor_name).alias("sensor_name"),
                F.col(sensor_name).cast("double").alias("sensor_value"),
            )
            for sensor_name in sensor_names
        ]
        readings_count = readings.count()
        expected_long_count = readings_count * len(sensor_names)
        long_df = (
            readings.select(
                "raw_index",
                "event_time",
                "event_minute",
                "dt",
                "operating_state",
                "is_failure_window",
                "failure_type",
                F.explode(F.array(*sensor_structs)).alias("sensor"),
            )
            .select(
                "raw_index",
                "event_time",
                "event_minute",
                "dt",
                "operating_state",
                "is_failure_window",
                "failure_type",
                F.col("sensor.sensor_name").alias("sensor_name"),
                F.col("sensor.sensor_value").alias("sensor_value"),
            )
            .join(F.broadcast(dim), on="sensor_name", how="left")
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        long_df.write.mode("overwrite").partitionBy("dt", "sensor_type").parquet(paths["dwd_sensor_long"])
        output_count = spark.read.parquet(paths["dwd_sensor_long"]).count()
        print("MetroPT DWD sensor_long 已写入:", paths["dwd_sensor_long"])
        print("展开传感器数:", len(sensor_names))
        print("ODS 记录数:", readings_count, "期望 DWD 记录数:", expected_long_count, "实际 DWD 记录数:", output_count)
        if output_count != expected_long_count:
            # 展开长表必须行数可推导；这里失败通常说明传感器列表或 explode 逻辑被改坏。
            raise RuntimeError(f"DWD 展开行数不一致: expected={expected_long_count}, output={output_count}")
        spark.read.parquet(paths["dwd_sensor_long"]).printSchema()
    finally:
        if long_df is not None:
            long_df.unpersist()
        readings.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
