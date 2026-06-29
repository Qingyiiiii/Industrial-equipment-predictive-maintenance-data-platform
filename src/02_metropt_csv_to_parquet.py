# -*- coding: utf-8 -*-
"""Convert MetroPT-3 raw CSV into normalized ODS Parquet readings."""
# 阅读提示：本文件是 Raw -> ODS 的转换点。
# 这里把 CSV 字段统一成项目标准字段，并补充 event_date、operating_state、failure_window 等下游通用列。
# 学习导读：
# - 链路位置：src/02，负责把原始 CSV 落成可被 Spark/Hive 复用的 ODS Parquet。
# - 主要输入：Raw CSV、字段映射、failure_windows、输出路径配置。
# - 主要输出：ods/readings 宽表，一行仍代表一个采样点。
# - 核心概念：ODS 保留原始采样粒度，只统一字段和补充通用标签，不做聚合。
# - 边界提醒：ODS 行数应与输入行数一致；如果不一致，要先查这里而不是继续跑 DWD/DWS。
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark import StorageLevel
from pyspark.sql import functions as F

from metropt_utils import assert_path_exists, create_metropt_spark, load_metropt_config, normalize_readings, read_metropt_csv


def main() -> None:
    # ODS 输出仍是一行一个采样点的宽表；它保留原始传感器读数，
    # 但字段名和标签口径已经被标准化，供 DWD/DWS/Hive/analysis 复用。
    config = load_metropt_config()
    paths = config["paths"]
    spark = create_metropt_spark("MetroPT_02_CSV_To_Parquet", config=config)
    spark.sparkContext.setLogLevel("WARN")

    assert_path_exists(spark, paths["input_csv"], "MetroPT-3 CSV")
    # normalize_readings 是 Raw -> ODS 的核心 contract：标准字段、时间列、状态列和 weak-label 列都在这里生成。
    readings = (
        normalize_readings(read_metropt_csv(spark, paths["input_csv"], config), config)
        .withColumn("source_system", F.lit("metropt_csv"))
        .withColumn("source_file", F.lit(paths["input_csv"]))
        .withColumn("ods_loaded_at", F.current_timestamp())
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    try:
        input_count = readings.count()
        # ODS 按 dt 分区，后续 Hive、DWS 和 P9/P10 读数都能按日期裁剪。
        readings.write.mode("overwrite").partitionBy("dt").parquet(paths["ods_readings_parquet"])
        output = spark.read.parquet(paths["ods_readings_parquet"])
        output_count = output.count()
        print("MetroPT ODS readings 已写入:", paths["ods_readings_parquet"])
        print("输入记录数:", input_count, "输出记录数:", output_count, "字段数:", len(output.columns))
        if input_count != output_count:
            # Raw -> ODS 不应丢行；一旦行数不守恒，下游所有统计都要暂停解释。
            raise RuntimeError(f"ODS 写出行数不一致: input={input_count}, output={output_count}")
        output.printSchema()
    finally:
        readings.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
