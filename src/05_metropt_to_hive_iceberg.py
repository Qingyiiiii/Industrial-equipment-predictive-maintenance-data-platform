# -*- coding: utf-8 -*-
"""Publish MetroPT ODS/DWD/DWS Parquet datasets to Hive and optional Iceberg tables."""
# 阅读提示：本文件负责把 Parquet 数据层发布成可查询表。
# Hive 是当前 canonical 查询入口，Iceberg 是扩展表格式；发布后会立即做 count 校验。
# 学习导读：
# - 链路位置：src/05，负责把已经生成的 Parquet 层发布到查询层。
# - 主要输入：ODS/DWD/DWS Parquet 数据集和 Hive/Iceberg 配置。
# - 主要输出：Hive 表、可选 Iceberg 表，以及每张表的 count 校验。
# - 核心概念：Hive 是本项目当前主证据链，Iceberg 是兼容扩展，不改变 canonical 口径。
# - 边界提醒：表发布成功只说明查询层可见；BI views 还要由 src/06 单独创建和验证。
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from metropt_utils import assert_path_exists, create_metropt_spark, load_metropt_config, sql_identifier


def _save_hive_table(df, full_name: str, partition_cols=None) -> None:
    # Hive 表是后续 beeline、Trino/Iceberg 和 BI views 的主要依赖；
    # overwrite 是验收链路的可复现选择，确保本轮输出不会混入旧数据。
    writer = df.write.mode("overwrite").format("parquet")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(full_name)


def _save_iceberg_table(df, full_name: str, table_format_version: int) -> None:
    # Iceberg 发布是扩展查询能力，不改变 Hive canonical 表口径；
    # 如果环境不支持 Iceberg，下游仍以 Hive 表作为主要证据。
    (
        df.writeTo(full_name)
        .using("iceberg")
        .tableProperty("format-version", str(table_format_version))
        .createOrReplace()
    )


def _verify_table(spark, full_name: str) -> int:
    row = spark.sql(f"SELECT COUNT(*) AS cnt FROM {full_name}").first()
    count = int(row["cnt"] or 0)
    if count <= 0:
        raise RuntimeError(f"表验收失败，记录数为空: {full_name}")
    print(f"表验收通过: {full_name}, rows={count}")
    return count


def main() -> None:
    # 主流程按 ODS/DWD/DWS 顺序发布，便于读者把 Hive 表反向追溯到 Parquet 层级。
    config = load_metropt_config()
    paths = config["paths"]
    hive_cfg = config.get("hive", {})
    iceberg_cfg = config.get("iceberg", {})
    spark = create_metropt_spark("MetroPT_05_To_Hive_Iceberg", config=config, enable_hive_support=True)
    spark.sparkContext.setLogLevel("WARN")

    for label, path in [
        ("ODS readings", paths["ods_readings_parquet"]),
        ("DWD sensor long", paths["dwd_sensor_long"]),
        ("DWS overall", paths["dws_overall_kpi"]),
        ("DWS window", paths["dws_window_kpi"]),
        ("DWS sensor", paths["dws_sensor_kpi"]),
    ]:
        # 发布到查询层之前先确认每个 Parquet 数据层存在，避免创建空表或旧表误导后续验收。
        assert_path_exists(spark, path, label)

    database = sql_identifier(hive_cfg.get("database", "metropt_quality"))
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"USE {database}")

    table_specs = [
        # 每个 spec 绑定源 Parquet、目标 Hive 表名和分区列；读者可按此表追溯数据层级。
        (paths["ods_readings_parquet"], hive_cfg.get("ods_readings_table", "ods_metropt_readings"), ["dt"]),
        (paths["dwd_sensor_long"], hive_cfg.get("dwd_sensor_table", "dwd_metropt_sensor_long"), ["dt", "sensor_type"]),
        (paths["dws_overall_kpi"], hive_cfg.get("dws_overall_table", "dws_metropt_overall_kpi"), None),
        (paths["dws_window_kpi"], hive_cfg.get("dws_window_table", "dws_metropt_window_kpi"), ["dt"]),
        (paths["dws_sensor_kpi"], hive_cfg.get("dws_sensor_table", "dws_metropt_sensor_kpi"), None),
    ]

    loaded = []
    for source_path, table_name, partition_cols in table_specs:
        safe_table = sql_identifier(table_name)
        df = spark.read.parquet(source_path)
        _save_hive_table(df, f"{database}.{safe_table}", partition_cols=partition_cols)
        loaded.append((safe_table, df))
        print(f"Hive 表已写入: {database}.{safe_table}")
        # count 校验是发布层最小验收：表能被 metastore 找到，并且至少有数据。
        _verify_table(spark, f"{database}.{safe_table}")

    if bool(iceberg_cfg.get("enable", False)):
        # Iceberg 发布只在配置开启时执行，方便同一代码在不带 Iceberg 的演示环境里运行。
        catalog = sql_identifier(iceberg_cfg.get("catalog", "lakehouse"))
        iceberg_db = sql_identifier(iceberg_cfg.get("database", "metropt_quality_iceberg"))
        table_format_version = int(iceberg_cfg.get("table_format_version", 2))
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{iceberg_db}")
        for table_name, df in loaded:
            _save_iceberg_table(df, f"{catalog}.{iceberg_db}.{table_name}", table_format_version)
            print(f"Iceberg 表已写入: {catalog}.{iceberg_db}.{table_name}")
            _verify_table(spark, f"{catalog}.{iceberg_db}.{table_name}")

    spark.stop()


if __name__ == "__main__":
    main()
