# -*- coding: utf-8 -*-
"""Flink SQL job for MetroPT Kafka replay events -> Hive/Redis realtime risk score.

This job turns the P9 realtime-risk field contract into an executable P11
streaming stage. The scorer intentionally preserves the current signal-proxy
logic from ``metropt_realtime_risk_score_plan.py``; it is online integration
evidence, not a claim that a production ML model has replaced the proxy.
"""
# 阅读提示：本文件把 realtime risk contract 落成 PyFlink 作业。
# risk_score 来自 raw-event signal proxy，用于联调 Hive/Redis 在线链路，不代表生产 ML 模型。
# 学习导读：
# - 链路位置：P11 realtime risk 作业，消费 Kafka replay event，写 Hive risk table 和 Redis latest risk。
# - 主要输入：Kafka topic、Flink/Hive/Redis 配置、P9 contract 中的传感器字段和阈值口径。
# - 主要输出：dws_metropt_realtime_risk_events、Redis latest risk key、DLQ 校验结果。
# - 核心概念：risk_score 是 signal-proxy integration evidence，用来验证在线链路闭环。
# - 边界提醒：这里没有加载生产模型服务，也没有替代 P9/P10 weak-label baseline 的离线评估。
import argparse
import importlib.util
import math
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udf


ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
STREAMING_DIR = ROOT / "streaming"
for path in (SRC_DIR, STREAMING_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from metropt_realtime_risk_score_plan import (  # type: ignore  # noqa: E402
    BASELINE_THRESHOLD,
    DIGITAL_SENSOR_FIELDS,
)
from metropt_utils import load_metropt_config, sql_identifier  # type: ignore  # noqa: E402


DEFAULT_HIVE_CONF_DIR = "/export/server/hive/conf"
DEFAULT_RISK_TABLE = "dws_metropt_realtime_risk_events"
DEFAULT_RISK_REDIS_PREFIX = "metropt_quality:risk:latest"
# 这些字段定义风险表和 Redis latest-state 的模型标识边界；source 中显式写明 not production model。
RISK_EQUIPMENT_ID = "compressor_1"
RISK_MODEL_NAME = "p11_flink_realtime_risk_contract"
RISK_MODEL_VERSION = "p11_flink_signal_proxy_20260607"
RISK_SCORE_SOURCE = "flink_signal_proxy_not_production_model"
MODEL_FEATURE_SET_VERSION = "p11_raw_event_signal_proxy_v1"


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


def _preflight(hive_conf_dir: str, kafka_servers: str, kafka_topic: str, dlq_topic: str, redis_url: str) -> None:
    # 启动前检查 Kafka/DLQ/Redis/Hive/Flink connector，避免长时间运行后才暴露环境缺口。
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


def _to_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        result = float(value)
        if not math.isfinite(result):
            return default
        return result
    except (TypeError, ValueError):
        return default


def _clip(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def _score_components(
    tp2,
    tp3,
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
) -> dict:
    # signal proxy 只使用当前 Kafka event 的压力、温度、电流和数字信号，不读取未来窗口或离线标签。
    pressure_delta = abs(_to_float(tp3) - _to_float(reservoirs))
    pressure_gap = abs(_to_float(tp2) - _to_float(tp3))
    digital_values = [comp, dv_electric, towers, mpg, lps, pressure_switch, oil_level, caudal_impulses]
    digital_activity = sum(_to_float(value) for value in digital_values) / max(len(DIGITAL_SENSOR_FIELDS), 1)
    return {
        "pressure_delta": pressure_delta,
        "pressure_gap": pressure_gap,
        "pressure_component": _clip((pressure_delta + pressure_gap * 0.05) / 2.0),
        "oil_component": _clip((_to_float(oil_temperature) - 55.0) / 20.0),
        "current_component": _clip((_to_float(motor_current) - 6.5) / 4.0),
        "digital_component": _clip(digital_activity),
    }


def _risk_score_from_components(components: dict) -> float:
    # 权重是 signal-proxy 经验组合，用于演示在线评分字段，不是训练得到的生产模型参数。
    score = _clip(
        0.12
        + 0.36 * components["pressure_component"]
        + 0.28 * components["oil_component"]
        + 0.18 * components["current_component"]
        + 0.06 * components["digital_component"]
    )
    return round(float(score), 6)


def _build_risk_score_udf():
    # Flink UDF 复刻 dry-run contract 的打分公式，输出 0-1 的 risk_score 供 Hive/Redis 联调验证。
    @udf(result_type=DataTypes.DOUBLE(), deterministic=True)
    def risk_signal_score(
        tp2,
        tp3,
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
    ):
        components = _score_components(
            tp2,
            tp3,
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
        )
        return _risk_score_from_components(components)

    return risk_signal_score


def _build_risk_reason_udf():
    @udf(result_type=DataTypes.STRING(), deterministic=True)
    def risk_signal_reason(
        tp2,
        tp3,
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
    ):
        components = _score_components(
            tp2,
            tp3,
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
        )
        reasons = []
        # reason 字段帮助说明和看板解释为什么分数升高，但它仍然来自规则化信号阈值。
        if components["pressure_delta"] > 0.35 or components["pressure_gap"] > 2.0:
            reasons.append("pressure_balance_shift")
        if _to_float(oil_temperature) > 60.0:
            reasons.append("oil_temperature_elevated")
        if _to_float(motor_current) > 7.5:
            reasons.append("motor_current_elevated")
        if not reasons:
            reasons.append("baseline_signal_level")
        return ",".join(reasons)

    return risk_signal_reason


def _build_redis_cache_risk_udf(redis_url: str, key_prefix: str, ttl_seconds: int):
    # Redis 只保存设备最新风险状态，方便实时看板读取；完整可追溯事件落在 Hive risk table。
    redis_opts = _parse_redis_url(redis_url)
    redis_client = None
    ttl = int(max(0, ttl_seconds))

    @udf(result_type=DataTypes.BIGINT(), deterministic=False)
    def redis_cache_risk(event_id, event_time, operating_state, risk_score, risk_level, risk_reason, scoring_time):
        nonlocal redis_client
        if event_id is None:
            return 0
        try:
            if redis_client is None:
                # Redis latest-state 是看板加速层；连接在 UDF 首次执行时建立，减少作业初始化阻塞。
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
            key = f"{key_prefix}:{RISK_EQUIPMENT_ID}"
            _redis_hset(
                redis_client,
                key,
                {
                    "equipment_id": RISK_EQUIPMENT_ID,
                    "event_id": str(event_id),
                    "event_time": str(event_time),
                    "operating_state": str(operating_state or "unknown"),
                    "risk_score": f"{float(risk_score or 0.0):.6f}",
                    "risk_level": str(risk_level or "unknown"),
                    "risk_reason": str(risk_reason or ""),
                    "risk_score_source": RISK_SCORE_SOURCE,
                    "risk_model_name": RISK_MODEL_NAME,
                    "risk_model_version": RISK_MODEL_VERSION,
                    "model_version": RISK_MODEL_VERSION,
                    "risk_threshold": f"{float(BASELINE_THRESHOLD):.12f}",
                    "model_feature_set_version": MODEL_FEATURE_SET_VERSION,
                    "scoring_time": str(scoring_time or ""),
                },
            )
            if ttl > 0:
                redis_client.expire(key, ttl)
            return 1
        except Exception as exc:
            print(f"[redis_risk_udf] write_failed key_prefix={key_prefix} error={exc}", file=sys.stderr)
            return 0

    return redis_cache_risk


def _set_hive_streaming_commit_policy(t_env: TableEnvironment, table_name: str) -> None:
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


def _create_hive_risk_table(t_env: TableEnvironment, risk_table: str) -> None:
    # 风险表保留原始关键传感器、risk metadata 和 dt 分区，便于离线复盘每条实时评分。
    try:
        _set_dialect(t_env, "hive")
        t_env.execute_sql(
            f"""
            CREATE TABLE IF NOT EXISTS {risk_table} (
              event_id STRING,
              raw_index BIGINT,
              event_time TIMESTAMP,
              ingest_time TIMESTAMP,
              source STRING,
              equipment_id STRING,
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
              risk_score DOUBLE,
              risk_level STRING,
              risk_reason STRING,
              risk_score_source STRING,
              risk_model_name STRING,
              risk_model_version STRING,
              model_version STRING,
              risk_threshold DOUBLE,
              feature_window_minutes STRING,
              feature_window_end TIMESTAMP,
              model_feature_set_version STRING,
              scoring_time STRING
            ) PARTITIONED BY (dt STRING)
            STORED AS PARQUET
            """
        )
        _set_hive_streaming_commit_policy(t_env, risk_table)
        return
    except Exception as exc:
        if not _is_missing_hive_parser(exc):
            raise
        print(
            "[preflight] 当前 PyFlink JVM classpath 缺少 Hive dialect ParserFactory，"
            "改用 default dialect + connector='hive' 创建 Hive-compatible 风险表。",
            file=sys.stderr,
            flush=True,
        )

    _set_dialect(t_env, "default")
    t_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {risk_table} (
          event_id STRING,
          raw_index BIGINT,
          event_time TIMESTAMP(9),
          ingest_time TIMESTAMP(9),
          source STRING,
          equipment_id STRING,
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
          risk_score DOUBLE,
          risk_level STRING,
          risk_reason STRING,
          risk_score_source STRING,
          risk_model_name STRING,
          risk_model_version STRING,
          model_version STRING,
          risk_threshold DOUBLE,
          feature_window_minutes STRING,
          feature_window_end TIMESTAMP(9),
          model_feature_set_version STRING,
          scoring_time STRING,
          dt STRING
        ) PARTITIONED BY (dt) WITH (
          'connector' = 'hive',
          'sink.partition-commit.trigger' = 'process-time',
          'sink.partition-commit.delay' = '0s',
          'sink.partition-commit.policy.kind' = 'metastore,success-file'
        )
        """
    )
    _set_hive_streaming_commit_policy(t_env, risk_table)


def main() -> None:
    # 主流程建立 Kafka source、DLQ、risk UDF、Hive sink 和 Redis side-effect sink，并一次提交 StatementSet。
    parser = argparse.ArgumentParser()
    parser.add_argument("--hive-conf-dir", default="")
    parser.add_argument("--group-id", default="metropt_quality_realtime_risk_v1")
    parser.add_argument("--startup-mode", default="")
    parser.add_argument("--risk-table", default="")
    parser.add_argument("--redis-risk-prefix", default="")
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
    risk_table = sql_identifier(args.risk_table or hive_cfg.get("dws_realtime_risk_table", DEFAULT_RISK_TABLE))
    redis_url = str(rt_cfg.get("redis_url", "redis://127.0.0.1:6379/0"))
    redis_prefix = str(args.redis_risk_prefix or rt_cfg.get("redis_risk_key_prefix", DEFAULT_RISK_REDIS_PREFIX))
    redis_ttl = int(rt_cfg.get("redis_risk_ttl_seconds", rt_cfg.get("redis_kpi_ttl_seconds", 7200)))
    _preflight(hive_conf_dir, kafka_servers, kafka_topic, dlq_topic, redis_url)

    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)
    conf = t_env.get_config().get_configuration()
    # P11 也使用 checkpoint/partition commit，使 Hive risk table 能被 P14/P11 验收脚本稳定查询。
    conf.set_string("pipeline.name", "metropt_quality_realtime_risk_score")
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
    # 注册 score/reason/cache 三个 UDF 后，后续 SQL 才能把 valid event 转成风险记录和 Redis latest-state。
    t_env.create_temporary_system_function("risk_signal_score", _build_risk_score_udf())
    t_env.create_temporary_system_function("risk_signal_reason", _build_risk_reason_udf())
    t_env.create_temporary_system_function(
        "redis_cache_risk",
        _build_redis_cache_risk_udf(redis_url=redis_url, key_prefix=redis_prefix, ttl_seconds=redis_ttl),
    )

    print(
        "MetroPT P11 risk scoring scope: Kafka raw replay events -> schema validation/DLQ -> "
        "signal-proxy risk fields -> Hive risk table + Redis latest risk state.",
        flush=True,
    )
    print(
        f"MetroPT P11 scorer version={RISK_MODEL_VERSION}, threshold={BASELINE_THRESHOLD}, "
        "score_source=flink_signal_proxy_not_production_model.",
        flush=True,
    )

    print("MetroPT P11 Flink 准备创建或更新 Hive risk sink 表。", flush=True)
    _create_hive_risk_table(t_env, risk_table)
    print("MetroPT P11 Flink Hive risk sink 表检查完成。", flush=True)

    _set_dialect(t_env, "default")
    # v_invalid/v_valid 先做 schema gate：坏事件写 DLQ，合格事件才允许进入 risk signal proxy。
    t_env.execute_sql(
        """
        CREATE TEMPORARY VIEW v_invalid AS
        SELECT
          CASE
            WHEN event_id IS NULL THEN 'missing_event_id_or_parse_error'
            WHEN raw_index IS NULL THEN 'missing_raw_index'
            WHEN event_time IS NULL THEN 'missing_event_time'
            WHEN ingest_time IS NULL THEN 'missing_ingest_time'
            WHEN operating_state IS NULL THEN 'missing_operating_state'
            WHEN operating_state NOT IN ('loaded', 'unloaded', 'stopped') THEN 'invalid_operating_state'
            WHEN is_failure_window IS NULL THEN 'missing_is_failure_window'
            WHEN is_failure_window NOT IN (0, 1) THEN 'invalid_is_failure_window'
            WHEN failure_type IS NULL THEN 'missing_failure_type'
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
        WHERE event_id IS NULL OR raw_index IS NULL OR event_time IS NULL OR ingest_time IS NULL
          OR operating_state IS NULL OR operating_state NOT IN ('loaded', 'unloaded', 'stopped')
          OR is_failure_window IS NULL OR is_failure_window NOT IN (0, 1) OR failure_type IS NULL
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
        WHERE event_id IS NOT NULL AND raw_index IS NOT NULL AND event_time IS NOT NULL AND ingest_time IS NOT NULL
          AND operating_state IN ('loaded', 'unloaded', 'stopped')
          AND is_failure_window IN (0, 1) AND failure_type IS NOT NULL
          AND tp2 IS NOT NULL AND tp3 IS NOT NULL AND h1 IS NOT NULL AND dv_pressure IS NOT NULL AND reservoirs IS NOT NULL
          AND oil_temperature IS NOT NULL AND motor_current IS NOT NULL AND comp IS NOT NULL AND dv_electric IS NOT NULL
          AND towers IS NOT NULL AND mpg IS NOT NULL AND lps IS NOT NULL AND pressure_switch IS NOT NULL AND oil_level IS NOT NULL
          AND caudal_impulses IS NOT NULL
        """
    )
    t_env.execute_sql(
        """
        CREATE TEMPORARY VIEW v_risk_base AS
        SELECT
          *,
          risk_signal_score(
            tp2, tp3, reservoirs, oil_temperature, motor_current,
            comp, dv_electric, towers, mpg, lps, pressure_switch, oil_level, caudal_impulses
          ) AS risk_score,
          risk_signal_reason(
            tp2, tp3, reservoirs, oil_temperature, motor_current,
            comp, dv_electric, towers, mpg, lps, pressure_switch, oil_level, caudal_impulses
          ) AS risk_reason,
          DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') AS scoring_time
        FROM v_valid
        """
    )
    t_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW v_risk_scored AS
        SELECT
          *,
          CASE
            WHEN risk_score >= {float(BASELINE_THRESHOLD)} THEN 'high'
            WHEN risk_score >= 0.40 THEN 'medium'
            ELSE 'low'
          END AS risk_level
        FROM v_risk_base
        WHERE risk_score IS NOT NULL AND risk_score >= 0.0 AND risk_score <= 1.0
        """
    )

    # StatementSet 同步写 DLQ、Hive risk table 和 Redis latest-state，保证在线展示和离线复盘口径一致。
    stmt = t_env.create_statement_set()
    stmt.add_insert_sql("INSERT INTO kafka_metropt_dlq SELECT reason, event_id, raw_index, payload, write_time FROM v_invalid")
    stmt.add_insert_sql(
        f"""
        INSERT INTO {risk_table}
        SELECT
          event_id,
          raw_index,
          event_time,
          ingest_time,
          source,
          '{RISK_EQUIPMENT_ID}' AS equipment_id,
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
          risk_score,
          risk_level,
          risk_reason,
          '{RISK_SCORE_SOURCE}' AS risk_score_source,
          '{RISK_MODEL_NAME}' AS risk_model_name,
          '{RISK_MODEL_VERSION}' AS risk_model_version,
          '{RISK_MODEL_VERSION}' AS model_version,
          {float(BASELINE_THRESHOLD)} AS risk_threshold,
          'raw_event' AS feature_window_minutes,
          event_time AS feature_window_end,
          '{MODEL_FEATURE_SET_VERSION}' AS model_feature_set_version,
          scoring_time,
          DATE_FORMAT(event_time, 'yyyy-MM-dd') AS dt
        FROM v_risk_scored
        """
    )
    stmt.add_insert_sql(
        """
        INSERT INTO redis_side_effect_sink
        SELECT redis_cache_risk(event_id, event_time, operating_state, risk_score, risk_level, risk_reason, scoring_time)
        FROM v_risk_scored
        """
    )

    print("MetroPT P11 Flink 正在提交 risk StatementSet。", flush=True)
    result = stmt.execute()
    print(
        f"MetroPT P11 Flink 风险作业已提交: topic={kafka_topic}, "
        f"risk_table={database}.{risk_table}, redis={redis_prefix}:{RISK_EQUIPMENT_ID}",
        flush=True,
    )
    result.wait()


if __name__ == "__main__":
    main()
