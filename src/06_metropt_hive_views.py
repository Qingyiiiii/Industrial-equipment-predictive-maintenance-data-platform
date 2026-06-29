# -*- coding: utf-8 -*-
"""Create MetroPT BI-friendly Hive views."""
# 阅读提示：本文件在 Hive 表之上创建 BI-friendly views。
# 视图不重新计算事实数据，只把 DWS 指标组织成更适合 dashboard 查询的口径。
# 学习导读：
# - 链路位置：src/06，是离线链路最后一步，面向 BI/dashboard 读者。
# - 主要输入：src/05 发布后的 Hive DWS 表，以及 realtime 占位表定义。
# - 主要输出：PBI-friendly Hive views 和轻量查询验收结果。
# - 核心概念：view 是查询口径封装，不是新的数据层，也不会修正上游 DWS 错误。
# - 边界提醒：视图查询通过不等于 Trino/Doris 通过；P12 会单独验证扩展查询层。
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from metropt_utils import create_metropt_spark, load_metropt_config, sql_identifier


def _verify_query(spark, label: str, sql: str, allow_empty: bool = False) -> None:
    # 创建视图后立即跑小查询，确保 view 能解析且能返回样例；
    # allow_empty 只用于确实允许空结果的辅助检查，正式 KPI 视图应有数据。
    rows = spark.sql(sql).limit(20).collect()
    if not rows and not allow_empty:
        raise RuntimeError(f"{label} 验收查询无结果")
    print(f"{label} 验收通过，样例行数: {len(rows)}")
    for row in rows[:3]:
        print(row.asDict())


def main() -> None:
    # 主流程先确保 Hive 数据库和 realtime 占位表存在，再创建 dashboard view；
    # 后续 P9/P12/P14 的 SQL 证据会复用这些 BI 查询口径。
    config = load_metropt_config()
    hive_cfg = config.get("hive", {})
    spark = create_metropt_spark("MetroPT_06_Hive_Views", config=config, enable_hive_support=True)
    spark.sparkContext.setLogLevel("WARN")

    database = sql_identifier(hive_cfg.get("database", "metropt_quality"))
    window_table = sql_identifier(hive_cfg.get("dws_window_table", "dws_metropt_window_kpi"))
    sensor_table = sql_identifier(hive_cfg.get("dws_sensor_table", "dws_metropt_sensor_kpi"))
    realtime_table = sql_identifier(hive_cfg.get("dws_realtime_table", "dws_metropt_realtime_kpi_1min"))
    window_view = sql_identifier(hive_cfg.get("pbi_window_view", "vw_pbi_metropt_window_kpi"))
    sensor_view = sql_identifier(hive_cfg.get("pbi_sensor_view", "vw_pbi_metropt_sensor_kpi"))
    realtime_view = sql_identifier(hive_cfg.get("pbi_realtime_view", "vw_pbi_metropt_realtime_kpi_1m"))

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"USE {database}")
    # realtime 表先建占位结构，保证 BI view 在实时 demo 还没跑时也能被创建和引用。
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {realtime_table} (
          minute_bucket STRING,
          operating_state STRING,
          sample_count BIGINT,
          failure_sample_count BIGINT,
          failure_window_rate DOUBLE,
          avg_tp2 DOUBLE,
          avg_tp3 DOUBLE,
          avg_reservoirs DOUBLE,
          avg_oil_temperature DOUBLE,
          avg_motor_current DOUBLE
        ) PARTITIONED BY (dt STRING)
        STORED AS PARQUET
        """
    )

    # window view 对 DWS window KPI 做字段类型收敛，方便 BI 工具稳定读取。
    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {window_view} AS
        SELECT
          CAST(minute_bucket AS STRING) AS minute_bucket,
          CAST(event_minute AS TIMESTAMP) AS event_minute,
          CAST(operating_state AS STRING) AS operating_state,
          CAST(sample_count AS BIGINT) AS sample_count,
          CAST(failure_sample_count AS BIGINT) AS failure_sample_count,
          CAST(failure_window_rate AS DOUBLE) AS failure_window_rate,
          CAST(avg_tp2 AS DOUBLE) AS avg_tp2,
          CAST(avg_tp3 AS DOUBLE) AS avg_tp3,
          CAST(avg_reservoirs AS DOUBLE) AS avg_reservoirs,
          CAST(avg_oil_temperature AS DOUBLE) AS avg_oil_temperature,
          CAST(avg_motor_current AS DOUBLE) AS avg_motor_current,
          CAST(dt AS STRING) AS dt
        FROM {window_table}
        """
    )

    # sensor view 面向传感器维度分析，保留 station_id/unit 让图表更容易解释。
    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {sensor_view} AS
        SELECT
          CAST(sensor_name AS STRING) AS sensor_name,
          CAST(sensor_type AS STRING) AS sensor_type,
          CAST(station_id AS STRING) AS station_id,
          CAST(unit AS STRING) AS unit,
          CAST(sample_count AS BIGINT) AS sample_count,
          CAST(failure_sample_count AS BIGINT) AS failure_sample_count,
          CAST(failure_window_rate AS DOUBLE) AS failure_window_rate,
          CAST(avg_sensor_value AS DOUBLE) AS avg_sensor_value,
          CAST(std_sensor_value AS DOUBLE) AS std_sensor_value,
          CAST(min_sensor_value AS DOUBLE) AS min_sensor_value,
          CAST(max_sensor_value AS DOUBLE) AS max_sensor_value
        FROM {sensor_table}
        """
    )

    # realtime view 与离线 window view 采用相近字段，便于演示离线和实时 KPI 的口径衔接。
    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {realtime_view} AS
        SELECT
          CAST(minute_bucket AS STRING) AS minute_bucket,
          CAST(operating_state AS STRING) AS operating_state,
          CAST(sample_count AS BIGINT) AS sample_count,
          CAST(failure_sample_count AS BIGINT) AS failure_sample_count,
          CAST(failure_window_rate AS DOUBLE) AS failure_window_rate,
          CAST(avg_tp2 AS DOUBLE) AS avg_tp2,
          CAST(avg_tp3 AS DOUBLE) AS avg_tp3,
          CAST(avg_reservoirs AS DOUBLE) AS avg_reservoirs,
          CAST(avg_oil_temperature AS DOUBLE) AS avg_oil_temperature,
          CAST(avg_motor_current AS DOUBLE) AS avg_motor_current,
          CAST(dt AS STRING) AS dt
        FROM {realtime_table}
        """
    )

    print(f"MetroPT BI 视图已创建: {database}.{window_view}, {database}.{sensor_view}, {database}.{realtime_view}")
    # 三个 view 都做轻量查询；realtime view 允许为空，因为实时 demo 可能尚未写入数据。
    _verify_query(
        spark,
        f"{database}.{window_view}",
        f"SELECT minute_bucket, operating_state, sample_count, failure_sample_count, failure_window_rate FROM {window_view} ORDER BY minute_bucket",
    )
    _verify_query(
        spark,
        f"{database}.{sensor_view}",
        f"SELECT sensor_name, sensor_type, station_id, avg_sensor_value, sample_count FROM {sensor_view} ORDER BY sensor_name",
    )
    _verify_query(
        spark,
        f"{database}.{realtime_view}",
        f"SELECT minute_bucket, operating_state, sample_count FROM {realtime_view} ORDER BY minute_bucket DESC",
        allow_empty=True,
    )
    spark.stop()


if __name__ == "__main__":
    main()
