# -*- coding: utf-8 -*-
"""Flink SQL job for MetroPT Kafka replay events -> Hive DWS + Redis KPI cache."""
# 阅读提示：本文件是实时 KPI 链路：Kafka JSON event -> Flink SQL 校验/聚合 -> Hive sink + Redis cache。
# 它消费 replay 标准事件，生成 1 分钟 KPI；不计算 P9/P11 risk_score。
# 学习导读：
# - 链路位置：P6 realtime demo 的 KPI 作业，位于 Kafka replay 之后、风险评分之前。
# - 主要输入：Kafka topic 中的标准 JSON event、Hive conf、Redis URL、Flink connector jars。
# - 主要输出：Hive realtime ODS/KPI 表和 Redis 1-minute KPI cache。
# - 核心概念：Flink SQL 在这里做 schema 校验、DLQ 分流和分钟级聚合。
# - 边界提醒：该作业只产出实时 KPI，不产出 risk_score；短时 demo 作业结束不一定是失败。
import argparse
import importlib.util
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udf


ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from metropt_utils import load_metropt_config, sql_identifier  # type: ignore  # noqa: E402


DEFAULT_HIVE_CONF_DIR = "/export/server/hive/conf"


def _quoted(value: str) -> str:
    return str(value).replace("'", "''")


def _set_dialect(t_env: TableEnvironment, dialect: str) -> None:
    t_env.get_config().set("table.sql-dialect", dialect)


def _is_missing_hive_parser(exc: Exception) -> bool:
    text = str(exc)
    return "ParserFactory" in text and "identifier 'hive'" in text


def _normalize_startup_mode(mode: str) -> str:
    alias = {
        "earliest": "earliest-offset",
        "earliest-offset": "earliest-offset",
        "latest": "latest-offset",
        "latest-offset": "latest-offset",
        "group": "group-offsets",
        "group-offsets": "group-offsets",
    }
    normalized = alias.get((mode or "").strip().lower())
    if not normalized:
        raise ValueError("startup-mode 只支持 earliest-offset/latest-offset/group-offsets")
    return normalized


def _parse_redis_url(redis_url: str):
    parsed = urlparse(redis_url)
    if parsed.scheme != "redis":
        raise ValueError(f"暂不支持的 Redis URL: {redis_url}")
    return {
        "host": parsed.hostname or "127.0.0.1",
        "port": int(parsed.port or 6379),
        "db": int(parsed.path.lstrip("/") or "0"),
        "password": parsed.password,
    }


def _redis_hset(redis_client, key: str, mapping: dict) -> None:
    try:
        redis_client.hset(key, mapping=mapping)
    except Exception as exc:
        if "wrong number of arguments for 'hset'" not in str(exc).lower():
            raise
        pieces = []
        for field, value in mapping.items():
            pieces.extend([field, value])
        redis_client.execute_command("HMSET", key, *pieces)


def _parse_kafka_bootstrap_servers(kafka_servers: str) -> list:
    servers = [s.strip() for s in str(kafka_servers or "").split(",") if s.strip()]
    invalid = [s for s in servers if ":" not in s or not s.rsplit(":", 1)[-1].isdigit()]
    if not servers:
        raise ValueError("缺少 realtime.kafka_bootstrap_servers 或 --bootstrap 配置")
    if invalid:
        raise ValueError(f"Kafka bootstrap servers 格式错误: {invalid}")
    return servers


def _has_connector(jar_names: list, *tokens: str) -> bool:
    lowered = [n.lower() for n in jar_names]
    return any(all(token in name for token in tokens) for name in lowered)


def _build_redis_cache_udf(redis_url: str, key_prefix: str, ttl_seconds: int):
    # Redis UDF 是 side-effect cache，用于保存最新 1 分钟 KPI；Hive 表仍是可追溯的主落地结果。
    redis_opts = _parse_redis_url(redis_url)
    redis_client = None
    ttl = int(max(0, ttl_seconds))

    @udf(result_type=DataTypes.BIGINT(), deterministic=False)
    def redis_cache_kpi(minute_bucket, operating_state, sample_count, failure_sample_count, failure_window_rate):
        nonlocal redis_client
        if minute_bucket is None:
            return 0
        try:
            if redis_client is None:
                # Redis 连接在 UDF 第一次真正执行时懒加载，避免作业创建阶段就触发网络连接。
                import redis  # pylint: disable=import-outside-toplevel

                redis_client = redis.Redis(
                    host=redis_opts["host"],
                    port=redis_opts["port"],
                    db=redis_opts["db"],
                    password=redis_opts["password"],
                    decode_responses=True,
                    socket_connect_timeout=3,
                    socket_timeout=3,
                )
                redis_client.ping()
            key = f"{key_prefix}:{minute_bucket}:{operating_state}"
            _redis_hset(
                redis_client,
                key,
                {
                    "sample_count": int(sample_count or 0),
                    "failure_sample_count": int(failure_sample_count or 0),
                    "failure_window_rate": float(failure_window_rate or 0.0),
                },
            )
            if ttl > 0:
                redis_client.expire(key, ttl)
            return 1
        except Exception as exc:
            print(f"[redis_udf] write_failed key_prefix={key_prefix} error={exc}", file=sys.stderr)
            return 0

    return redis_cache_kpi


def _preflight(hive_conf_dir: str, kafka_servers: str, kafka_topic: str, dlq_topic: str, redis_url: str) -> None:
    # 启动前先检查 Kafka/Redis/Hive/Flink connector，避免 SQL 提交后才发现环境依赖缺失。
    _parse_kafka_bootstrap_servers(kafka_servers)
    if not str(kafka_topic or "").strip():
        raise ValueError("缺少 Kafka 主 topic: realtime.kafka_topic")
    if not str(dlq_topic or "").strip():
        raise ValueError("缺少 Kafka DLQ topic: realtime.kafka_dlq_topic")
    _parse_redis_url(redis_url)
    if importlib.util.find_spec("redis") is None:
        raise RuntimeError("缺少 Python redis 包，请安装 requirements.txt 中的 redis 依赖。")
    if importlib.util.find_spec("pkg_resources") is None:
        raise RuntimeError(
            "PyFlink 虚拟环境缺少 pkg_resources。pkg_resources 由 setuptools 提供，"
            "请执行: /export/server/venv/flink120/bin/python -m pip install -U "
            "'setuptools>=68,<80' wheel"
        )
    if not os.path.isdir(hive_conf_dir):
        raise FileNotFoundError(f"hive-conf-dir 不存在: {hive_conf_dir}")
    hive_site = Path(hive_conf_dir) / "hive-site.xml"
    if not hive_site.is_file():
        raise FileNotFoundError(f"hive-conf-dir 中未找到 hive-site.xml: {hive_site}")
    flink_home = os.getenv("FLINK_HOME", "").strip()
    if not flink_home:
        raise EnvironmentError("缺少 FLINK_HOME，请设置为 /export/server/flink")
    lib_dir = Path(flink_home) / "lib"
    if not lib_dir.is_dir():
        raise FileNotFoundError(f"Flink lib 目录不存在: {lib_dir}")
    names = [p.name for p in lib_dir.iterdir() if p.is_file() and p.suffix == ".jar"]
    if not _has_connector(names, "connector", "kafka"):
        raise RuntimeError(f"未在 {lib_dir} 找到 Kafka connector jar")
    if not _has_connector(names, "connector", "hive"):
        raise RuntimeError(f"未在 {lib_dir} 找到 Hive connector jar")
    if not _has_connector(names, "json"):
        print(f"[preflight] 未在 {lib_dir} 显式找到 JSON format jar；若作业启动失败，请补齐 Flink JSON format 相关 jar。", file=sys.stderr, flush=True)


def _set_hive_streaming_commit_policy(t_env: TableEnvironment, table_name: str) -> None:
    """Ensure partitioned Hive streaming sinks can commit visible partitions."""
    _set_dialect(t_env, "default")
    t_env.execute_sql(
        f"""
        ALTER TABLE {table_name} SET (
          'sink.partition-commit.trigger' = 'process-time',
          'sink.partition-commit.delay' = '0s',
          'sink.partition-commit.policy.kind' = 'metastore,success-file'
        )
        """
    )


def _create_hive_sink_tables(t_env: TableEnvironment, ods_event_table: str, realtime_table: str) -> None:
    """Create Hive-compatible sink tables, falling back when Hive SQL dialect is unavailable."""
    # sink 分为 ODS realtime events 和 DWS 1min KPI；Hive dialect 缺失时改用 default dialect + connector='hive'。
    try:
        _set_dialect(t_env, "hive")
        t_env.execute_sql(
            f"""
            CREATE TABLE IF NOT EXISTS {ods_event_table} (
              event_id STRING,
              raw_index BIGINT,
              event_time TIMESTAMP,
              ingest_time TIMESTAMP,
              source STRING,
              operating_state STRING,
              is_failure_window INT,
              failure_type STRING,
              tp2 DOUBLE,
              tp3 DOUBLE,
              h1 DOUBLE,
              dv_pressure DOUBLE,
              reservoirs DOUBLE,
              oil_temperature DOUBLE,
              motor_current DOUBLE,
              comp DOUBLE,
              dv_electric DOUBLE,
              towers DOUBLE,
              mpg DOUBLE,
              lps DOUBLE,
              pressure_switch DOUBLE,
              oil_level DOUBLE,
              caudal_impulses DOUBLE
            ) PARTITIONED BY (dt STRING)
            STORED AS PARQUET
            """
        )
        t_env.execute_sql(
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
        _set_hive_streaming_commit_policy(t_env, ods_event_table)
        _set_hive_streaming_commit_policy(t_env, realtime_table)
        return
    except Exception as exc:
        if not _is_missing_hive_parser(exc):
            raise
        # 某些 PyFlink 环境缺 Hive dialect parser，但 connector='hive' 仍可写表；这里保留兼容路径。
        print(
            "[preflight] 当前 PyFlink JVM classpath 缺少 Hive dialect ParserFactory，"
            "改用 default dialect + connector='hive' 创建 Hive-compatible 表。",
            file=sys.stderr,
            flush=True,
        )

    _set_dialect(t_env, "default")
    t_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ods_event_table} (
          event_id STRING,
          raw_index BIGINT,
          event_time TIMESTAMP(9),
          ingest_time TIMESTAMP(9),
          source STRING,
          operating_state STRING,
          is_failure_window INT,
          failure_type STRING,
          tp2 DOUBLE,
          tp3 DOUBLE,
          h1 DOUBLE,
          dv_pressure DOUBLE,
          reservoirs DOUBLE,
          oil_temperature DOUBLE,
          motor_current DOUBLE,
          comp DOUBLE,
          dv_electric DOUBLE,
          towers DOUBLE,
          mpg DOUBLE,
          lps DOUBLE,
          pressure_switch DOUBLE,
          oil_level DOUBLE,
          caudal_impulses DOUBLE,
          dt STRING
        ) PARTITIONED BY (dt) WITH (
          'connector' = 'hive',
          'sink.partition-commit.trigger' = 'process-time',
          'sink.partition-commit.delay' = '0s',
          'sink.partition-commit.policy.kind' = 'metastore,success-file'
        )
        """
    )
    t_env.execute_sql(
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
          avg_motor_current DOUBLE,
          dt STRING
        ) PARTITIONED BY (dt) WITH (
          'connector' = 'hive',
          'sink.partition-commit.trigger' = 'process-time',
          'sink.partition-commit.delay' = '0s',
          'sink.partition-commit.policy.kind' = 'metastore,success-file'
        )
        """
    )
    _set_hive_streaming_commit_policy(t_env, ods_event_table)
    _set_hive_streaming_commit_policy(t_env, realtime_table)


def main() -> None:
    # 主流程创建 Kafka source、DLQ、Hive sink 和 Redis side-effect sink，再用 StatementSet 一次提交。
    parser = argparse.ArgumentParser()
    parser.add_argument("--hive-conf-dir", default="")
    parser.add_argument("--group-id", default="metropt_quality_flink_v1")
    parser.add_argument("--startup-mode", default="")
    args = parser.parse_args()

    config = load_metropt_config()
    rt_cfg = config.get("realtime", {})
    hive_cfg = config.get("hive", {})
    hive_conf_dir = args.hive_conf_dir or os.getenv("HIVE_CONF_DIR") or DEFAULT_HIVE_CONF_DIR

    kafka_servers = str(rt_cfg.get("kafka_bootstrap_servers") or "")
    kafka_topic = str(rt_cfg.get("kafka_topic", "metropt.ods.compressor.reading.v1"))
    dlq_topic = str(rt_cfg.get("kafka_dlq_topic", "metropt.ods.compressor.reading.dlq.v1"))
    startup_mode = _normalize_startup_mode(args.startup_mode or rt_cfg.get("kafka_startup_mode", "group-offsets"))
    database = sql_identifier(hive_cfg.get("database", "metropt_quality"))
    realtime_table = sql_identifier(hive_cfg.get("dws_realtime_table", "dws_metropt_realtime_kpi_1min"))
    ods_event_table = sql_identifier(hive_cfg.get("ods_realtime_table", "ods_metropt_realtime_readings"))
    redis_url = str(rt_cfg.get("redis_url", "redis://127.0.0.1:6379/0"))
    redis_prefix = str(rt_cfg.get("redis_kpi_key_prefix", "metropt:kpi:1m"))
    redis_ttl = int(rt_cfg.get("redis_kpi_ttl_seconds", 7200))
    _preflight(hive_conf_dir, kafka_servers, kafka_topic, dlq_topic, redis_url)

    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)
    conf = t_env.get_config().get_configuration()
    # checkpoint/partition commit 配置保证 Hive 分区能被 metastore 及时发现，便于验收脚本查询。
    conf.set_string("pipeline.name", "metropt_quality_kafka_hive_redis")
    conf.set_string("execution.checkpointing.interval", "30 s")
    conf.set_string("table.exec.state.ttl", "24 h")
    conf.set_string("table.exec.mini-batch.enabled", "true")
    conf.set_string("table.exec.mini-batch.allow-latency", "30 s")
    conf.set_string("table.exec.mini-batch.size", "5000")

    t_env.execute_sql(
        f"""
        CREATE CATALOG hive_catalog WITH (
          'type' = 'hive',
          'default-database' = '{_quoted(database)}',
          'hive-conf-dir' = '{_quoted(hive_conf_dir)}'
        )
        """
    )
    t_env.execute_sql("USE CATALOG hive_catalog")
    t_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    t_env.execute_sql(f"USE {database}")

    _set_dialect(t_env, "default")
    t_env.execute_sql(
        f"""
        CREATE TEMPORARY TABLE kafka_metropt_events (
          event_id STRING,
          raw_index BIGINT,
          event_time TIMESTAMP(3),
          ingest_time TIMESTAMP(3),
          source STRING,
          operating_state STRING,
          is_failure_window INT,
          failure_type STRING,
          tp2 DOUBLE,
          tp3 DOUBLE,
          h1 DOUBLE,
          dv_pressure DOUBLE,
          reservoirs DOUBLE,
          oil_temperature DOUBLE,
          motor_current DOUBLE,
          comp DOUBLE,
          dv_electric DOUBLE,
          towers DOUBLE,
          mpg DOUBLE,
          lps DOUBLE,
          pressure_switch DOUBLE,
          oil_level DOUBLE,
          caudal_impulses DOUBLE,
          proc_time AS PROCTIME(),
          WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
          'connector' = 'kafka',
          'topic' = '{_quoted(kafka_topic)}',
          'properties.bootstrap.servers' = '{_quoted(kafka_servers)}',
          'properties.group.id' = '{_quoted(args.group_id)}',
          'scan.startup.mode' = '{_quoted(startup_mode)}',
          'format' = 'json',
          'json.ignore-parse-errors' = 'true',
          'json.timestamp-format.standard' = 'SQL'
        )
        """
    )
    t_env.execute_sql(
        f"""
        CREATE TEMPORARY TABLE kafka_metropt_dlq (
          reason STRING,
          event_id STRING,
          raw_index STRING,
          payload STRING,
          write_time STRING
        ) WITH (
          'connector' = 'kafka',
          'topic' = '{_quoted(dlq_topic)}',
          'properties.bootstrap.servers' = '{_quoted(kafka_servers)}',
          'format' = 'json'
        )
        """
    )
    t_env.execute_sql("CREATE TEMPORARY TABLE redis_side_effect_sink (ok BIGINT) WITH ('connector' = 'blackhole')")
    # blackhole 表承接 Redis side-effect UDF 返回值；真正的缓存写入发生在 UDF 内部。
    t_env.create_temporary_system_function(
        "redis_cache_kpi",
        _build_redis_cache_udf(redis_url=redis_url, key_prefix=redis_prefix, ttl_seconds=redis_ttl),
    )
    print(
        "MetroPT DLQ v1 范围: 解析后 event_id/raw_index/event_time/operating_state 或任一必需传感器字段为空的事件；"
        "原始坏 JSON 的 raw payload 捕获作为后续优化项。",
        flush=True,
    )

    print("MetroPT Flink 准备创建或更新 Hive sink 表。", flush=True)
    _create_hive_sink_tables(t_env, ods_event_table, realtime_table)
    print("MetroPT Flink Hive sink 表检查完成。", flush=True)

    _set_dialect(t_env, "default")
    # v_invalid/v_valid 是实时链路的 schema gate：坏事件进入 DLQ，合格事件进入 Hive ODS 与 KPI 聚合。
    t_env.execute_sql(
        """
        CREATE TEMPORARY VIEW v_invalid AS
        SELECT
          CASE
            WHEN event_id IS NULL THEN 'missing_event_id_or_parse_error'
            WHEN raw_index IS NULL THEN 'missing_raw_index'
            WHEN event_time IS NULL THEN 'missing_event_time'
            WHEN operating_state IS NULL THEN 'missing_operating_state'
            WHEN tp2 IS NULL OR tp3 IS NULL OR h1 IS NULL OR dv_pressure IS NULL OR reservoirs IS NULL
              OR oil_temperature IS NULL OR motor_current IS NULL OR comp IS NULL OR dv_electric IS NULL
              OR towers IS NULL OR mpg IS NULL OR lps IS NULL OR pressure_switch IS NULL OR oil_level IS NULL
              OR caudal_impulses IS NULL THEN 'missing_required_business_field'
            ELSE 'unknown'
          END AS reason,
          event_id,
          CAST(raw_index AS STRING) AS raw_index,
          CONCAT(
            'source=', COALESCE(source, 'null'),
            ',state=', COALESCE(operating_state, 'null'),
            ',failure_type=', COALESCE(failure_type, 'null')
          ) AS payload,
          DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') AS write_time
        FROM kafka_metropt_events
        WHERE event_id IS NULL OR raw_index IS NULL OR event_time IS NULL OR operating_state IS NULL
          OR tp2 IS NULL OR tp3 IS NULL OR h1 IS NULL OR dv_pressure IS NULL OR reservoirs IS NULL
          OR oil_temperature IS NULL OR motor_current IS NULL OR comp IS NULL OR dv_electric IS NULL
          OR towers IS NULL OR mpg IS NULL OR lps IS NULL OR pressure_switch IS NULL OR oil_level IS NULL
          OR caudal_impulses IS NULL
        """
    )
    t_env.execute_sql(
        """
        CREATE TEMPORARY VIEW v_valid AS
        SELECT *
        FROM kafka_metropt_events
        WHERE event_id IS NOT NULL AND raw_index IS NOT NULL AND event_time IS NOT NULL AND operating_state IS NOT NULL
          AND tp2 IS NOT NULL AND tp3 IS NOT NULL AND h1 IS NOT NULL AND dv_pressure IS NOT NULL AND reservoirs IS NOT NULL
          AND oil_temperature IS NOT NULL AND motor_current IS NOT NULL AND comp IS NOT NULL AND dv_electric IS NOT NULL
          AND towers IS NOT NULL AND mpg IS NOT NULL AND lps IS NOT NULL AND pressure_switch IS NOT NULL AND oil_level IS NOT NULL
          AND caudal_impulses IS NOT NULL
        """
    )
    t_env.execute_sql(
        """
        CREATE TEMPORARY VIEW v_kpi_1m AS
        SELECT
          DATE_FORMAT(TUMBLE_START(proc_time, INTERVAL '1' MINUTE), 'yyyy-MM-dd HH:mm:00') AS minute_bucket,
          COALESCE(operating_state, 'unknown') AS operating_state,
          COUNT(1) AS sample_count,
          SUM(CASE WHEN is_failure_window = 1 THEN 1 ELSE 0 END) AS failure_sample_count,
          CAST(SUM(CASE WHEN is_failure_window = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(1) AS failure_window_rate,
          AVG(tp2) AS avg_tp2,
          AVG(tp3) AS avg_tp3,
          AVG(reservoirs) AS avg_reservoirs,
          AVG(oil_temperature) AS avg_oil_temperature,
          AVG(motor_current) AS avg_motor_current,
          DATE_FORMAT(TUMBLE_START(proc_time, INTERVAL '1' MINUTE), 'yyyy-MM-dd') AS dt
        FROM v_valid
        GROUP BY TUMBLE(proc_time, INTERVAL '1' MINUTE), COALESCE(operating_state, 'unknown')
        """
    )

    # StatementSet 同时写 DLQ、ODS、DWS 和 Redis cache，确保同一批实时事件使用一致的过滤口径。
    stmt = t_env.create_statement_set()
    stmt.add_insert_sql("INSERT INTO kafka_metropt_dlq SELECT reason, event_id, raw_index, payload, write_time FROM v_invalid")
    stmt.add_insert_sql(
        f"""
        INSERT INTO {ods_event_table}
        SELECT
          event_id,
          raw_index,
          event_time,
          ingest_time,
          source,
          operating_state,
          is_failure_window,
          failure_type,
          tp2,
          tp3,
          h1,
          dv_pressure,
          reservoirs,
          oil_temperature,
          motor_current,
          comp,
          dv_electric,
          towers,
          mpg,
          lps,
          pressure_switch,
          oil_level,
          caudal_impulses,
          DATE_FORMAT(event_time, 'yyyy-MM-dd') AS dt
        FROM v_valid
        """
    )
    stmt.add_insert_sql(
        f"""
        INSERT INTO {realtime_table}
        SELECT
          minute_bucket,
          operating_state,
          sample_count,
          failure_sample_count,
          failure_window_rate,
          avg_tp2,
          avg_tp3,
          avg_reservoirs,
          avg_oil_temperature,
          avg_motor_current,
          dt
        FROM v_kpi_1m
        """
    )
    stmt.add_insert_sql(
        """
        INSERT INTO redis_side_effect_sink
        SELECT redis_cache_kpi(minute_bucket, operating_state, sample_count, failure_sample_count, failure_window_rate)
        FROM v_kpi_1m
        """
    )

    print("MetroPT Flink 正在提交 StatementSet。", flush=True)
    result = stmt.execute()
    print(f"MetroPT Flink 作业已提交: topic={kafka_topic}, dws={database}.{realtime_table}, redis={redis_prefix}", flush=True)
    result.wait()


if __name__ == "__main__":
    main()
