# -*- coding: utf-8 -*-
"""Common helpers for the MetroPT-3 air-compressor pipeline."""
# 阅读提示：本文件是离线链路的公共工具层。
# 它集中处理配置、Spark session、路径、字段标准化和 failure window 标签，避免各脚本各自定义口径。
# 学习导读：
# - 链路位置：src 公共层，被离线、analysis 和部分 realtime 脚本复用。
# - 主要输入：YAML 配置、Raw CSV/DataFrame、Spark 路径和 failure_windows。
# - 主要输出：标准字段 DataFrame、SparkSession、路径工具、报告写入工具。
# - 核心概念：公共工具的职责是固定“项目口径”，尤其是字段名、路径和弱标签窗口。
# - 边界提醒：不要在业务脚本里复制一份字段映射或标签逻辑，否则 P9/P10/P11 很容易口径漂移。
import json
import os
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
except ModuleNotFoundError:
    DataFrame = Any
    SparkSession = Any
    F = None


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_CONFIG = os.path.join(ROOT_DIR, "config", "metropt_quality.local.yaml")

RAW_TO_STANDARD = {
    # Raw CSV 中存在历史拼写差异，例如 DV_eletric / DV_electric。
    # 这里统一映射到项目标准字段，保证 src、streaming、analysis 的字段口径一致。
    "timestamp": "event_time_raw",
    "TP2": "tp2",
    "TP3": "tp3",
    "H1": "h1",
    "DV_pressure": "dv_pressure",
    "Reservoirs": "reservoirs",
    "Oil_temperature": "oil_temperature",
    "Motor_current": "motor_current",
    "COMP": "comp",
    "DV_eletric": "dv_electric",
    "DV_electric": "dv_electric",
    "Towers": "towers",
    "MPG": "mpg",
    "LPS": "lps",
    "Pressure_switch": "pressure_switch",
    "Oil_level": "oil_level",
    "Caudal_impulses": "caudal_impulses",
}

ANALOG_SENSORS = [
    "tp2",
    "tp3",
    "h1",
    "dv_pressure",
    "reservoirs",
    "oil_temperature",
    "motor_current",
]

DIGITAL_SENSORS = [
    "comp",
    "dv_electric",
    "towers",
    "mpg",
    "lps",
    "pressure_switch",
    "oil_level",
    "caudal_impulses",
]

SENSOR_UNITS = {
    "tp2": "bar",
    "tp3": "bar",
    "h1": "bar",
    "dv_pressure": "bar",
    "reservoirs": "bar",
    "oil_temperature": "celsius",
    "motor_current": "ampere",
    "comp": "binary",
    "dv_electric": "binary",
    "towers": "binary",
    "mpg": "binary",
    "lps": "binary",
    "pressure_switch": "binary",
    "oil_level": "binary",
    "caudal_impulses": "binary",
}


def _require_pyspark() -> None:
    """Import PySpark lazily so local config/preflight helpers can run without it."""
    global F, SparkSession
    if F is not None and hasattr(SparkSession, "builder"):
        return
    try:
        from pyspark.sql import SparkSession as _SparkSession
        from pyspark.sql import functions as _F
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "当前 Python 环境缺少 pyspark。请在虚拟机中使用 spark-submit 运行 Spark 作业，"
            "或先安装项目依赖: python -m pip install --user -r requirements.txt。"
        ) from exc
    SparkSession = _SparkSession
    F = _F


def _strip_comment(s: str) -> str:
    """Remove YAML comments while preserving # inside quotes."""
    in_single = False
    in_double = False
    for i, ch in enumerate(s):
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == "#" and not in_single and not in_double:
            return s[:i]
    return s


def _parse_scalar(value: str):
    """Parse the scalar subset used by MetroPT YAML configs."""
    v = value.strip()
    if v == "":
        return ""
    if v in {"null", "Null", "NULL", "~"}:
        return None
    if v in {"true", "True", "TRUE"}:
        return True
    if v in {"false", "False", "FALSE"}:
        return False
    if v == "{}":
        return {}
    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
        return v[1:-1]
    try:
        if "." in v:
            return float(v)
        return int(v)
    except ValueError:
        return v


def _simple_yaml_load(text: str) -> dict:
    """Small YAML subset parser for environments without PyYAML."""
    # 集群最小环境不一定安装 PyYAML，因此保留一个只覆盖项目配置格式的轻量解析器；
    # 这让 preflight 和 replay 这类基础脚本能在依赖不足时仍给出清晰错误。
    root = {}
    stack = [(-1, root)]
    for raw in text.splitlines():
        line = _strip_comment(raw).rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        stripped = line.strip()
        if ":" not in stripped:
            raise ValueError(f"无法解析配置行: {raw}")
        key, val = stripped.split(":", 1)
        key = key.strip()
        val = val.strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        if not stack:
            raise ValueError(f"YAML 缩进错误: {raw}")
        parent = stack[-1][1]
        if val == "":
            parent[key] = {}
            stack.append((indent, parent[key]))
        else:
            parent[key] = _parse_scalar(val)
    return root


def _load_yaml(path: str) -> dict:
    """Load YAML with PyYAML when available, otherwise use the local subset parser."""
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    try:
        import yaml  # type: ignore

        return yaml.safe_load(content)
    except ModuleNotFoundError:
        return _simple_yaml_load(content)


def is_distributed_path(path: str) -> bool:
    """Return whether a path is handled by Hadoop-compatible filesystems."""
    return urlparse(path).scheme.lower() in {"hdfs", "viewfs", "s3a", "abfs", "oss"}


def to_local_path(path: str) -> str:
    """Convert file:/// URIs to local OS paths; leave normal paths unchanged."""
    parsed = urlparse(path)
    if parsed.scheme.lower() != "file":
        return path
    local_path = parsed.path or path
    if os.name == "nt" and len(local_path) >= 3 and local_path[0] == "/" and local_path[2] == ":":
        local_path = local_path[1:]
    return local_path


def ensure_dir(path: str, spark: SparkSession = None) -> None:
    """Create a local/HDFS directory if it does not exist."""
    if is_distributed_path(path):
        if spark is None:
            return
        jvm = spark._jvm
        hconf = spark._jsc.hadoopConfiguration()
        uri = jvm.java.net.URI(path)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hconf)
        fs.mkdirs(jvm.org.apache.hadoop.fs.Path(path))
        return
    os.makedirs(to_local_path(path), exist_ok=True)


def path_exists(path: str, spark: SparkSession = None) -> bool:
    """Check local/HDFS path existence."""
    if not is_distributed_path(path):
        return os.path.exists(to_local_path(path))
    if spark is None:
        raise ValueError("检查分布式路径是否存在时必须传入 spark。")
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    uri = jvm.java.net.URI(path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hconf)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


def load_metropt_config(config_path: Optional[str] = None) -> dict:
    """Load MetroPT config, preferring METROPT_CONFIG over the local default."""
    # 配置优先级是复现时最常见的分叉点：
    # 显式参数 > METROPT_CONFIG 环境变量 > 本地默认配置。
    resolved = config_path or os.environ.get("METROPT_CONFIG") or DEFAULT_CONFIG
    return _load_yaml(resolved)


def create_metropt_spark(app_name: str, config: Optional[dict] = None, enable_hive_support: Optional[bool] = None) -> SparkSession:
    """Create a SparkSession for local or YARN MetroPT jobs."""
    # Spark session 的本地/YARN、Hive support、Iceberg extension 都从 config 派生；
    # 这样同一套脚本可以在本地轻量验证和三节点集群上复用。
    _require_pyspark()
    cfg = config or load_metropt_config()
    spark_cfg = cfg.get("spark", {})
    hive_cfg = cfg.get("hive", {})
    iceberg_cfg = cfg.get("iceberg", {})
    mode = str(spark_cfg.get("mode", "local")).lower()
    master = spark_cfg.get("master", "local[*]" if mode == "local" else "yarn")
    shuffle_parts = str(spark_cfg.get("shuffle_partitions", 8))
    timezone = spark_cfg.get("timezone", "Asia/Shanghai")
    driver_host = spark_cfg.get("driver_host", "127.0.0.1")

    builder = SparkSession.builder.appName(app_name).master(master)
    if mode != "yarn":
        builder = builder.config("spark.driver.host", driver_host)

    builder = (
        builder
        .config("spark.sql.session.timeZone", timezone)
        .config("spark.sql.shuffle.partitions", shuffle_parts)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    for k, v in spark_cfg.get("extra_conf", {}).items():
        # extra_conf 给集群复验保留弹性，例如 executor memory、queue、adaptive 参数；
        # 业务脚本不用知道这些 Spark 细节，只依赖统一 session。
        builder = builder.config(str(k), str(v))

    if bool(iceberg_cfg.get("enable", False)):
        # Iceberg 只在配置明确开启时挂载 catalog，避免本地轻量运行被缺失 jar 或 metastore 阻断。
        catalog = str(iceberg_cfg.get("catalog", "lakehouse"))
        metastore_uris = hive_cfg.get("metastore_uris")
        warehouse_dir = hive_cfg.get("warehouse_dir")
        builder = (
            builder
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{catalog}.type", "hive")
        )
        if metastore_uris:
            builder = builder.config(f"spark.sql.catalog.{catalog}.uri", str(metastore_uris))
        if warehouse_dir:
            builder = builder.config(f"spark.sql.catalog.{catalog}.warehouse", str(warehouse_dir))

    use_hive_support = spark_cfg.get("enable_hive_support", False)
    if enable_hive_support is not None:
        use_hive_support = bool(enable_hive_support)
    if use_hive_support:
        # Hive support 影响 saveAsTable、SQL view 和 metastore 访问；
        # 只有需要发布/查询 Hive 表的步骤才应该强制开启。
        metastore_uris = hive_cfg.get("metastore_uris")
        warehouse_dir = hive_cfg.get("warehouse_dir")
        if metastore_uris:
            builder = (
                builder
                .config("hive.metastore.uris", str(metastore_uris))
                .config("spark.hadoop.hive.metastore.uris", str(metastore_uris))
            )
        if warehouse_dir:
            builder = builder.config("spark.sql.warehouse.dir", str(warehouse_dir))
        builder = builder.enableHiveSupport()

    return builder.getOrCreate()


def join_path(base: str, *parts: str) -> str:
    """Join local paths and URI paths without breaking hdfs:/// prefixes."""
    suffix = "/".join(str(p).strip("/\\") for p in parts if p is not None and str(p).strip() != "")
    if not suffix:
        return base.rstrip("/\\")
    if "://" in base:
        return base.rstrip("/") + "/" + suffix.replace("\\", "/")
    return os.path.join(base, *parts)


def assert_path_exists(spark: SparkSession, path: str, label: str) -> None:
    """Fail early when an expected local/HDFS input is missing."""
    if not path_exists(path, spark=spark):
        raise FileNotFoundError(f"{label} 不存在: {path}")


def read_metropt_csv(spark: SparkSession, path: str, config: dict) -> DataFrame:
    """Read the MetroPT CSV with stable options."""
    mode = str(config.get("spark", {}).get("csv_read", {}).get("mode", "PERMISSIVE"))
    return spark.read.option("header", True).option("mode", mode).csv(path)


def _raw_index_column(columns: Sequence[str]) -> Optional[str]:
    """Find the unnamed index column produced by the MetroPT CSV."""
    for col_name in columns:
        if col_name is None:
            continue
        stripped = str(col_name).strip()
        if stripped == "" or stripped.startswith("_c"):
            return col_name
    return None


def normalize_readings(df: DataFrame, config: dict) -> DataFrame:
    """Normalize raw MetroPT columns and add timestamp, date, state, and failure-window labels."""
    # ODS 标准化在这里集中完成：字段重命名、时间解析、状态字段、failure window 标签。
    # 下游 DWD/DWS/analysis 不再重新解释 Raw CSV，避免口径漂移。
    _require_pyspark()
    out = df
    raw_idx = _raw_index_column(out.columns)
    if raw_idx and raw_idx != "raw_index":
        # MetroPT CSV 第一列常是无名索引列；保留下来便于抽查原始行和 replay event_id。
        out = out.withColumnRenamed(raw_idx, "raw_index")

    for raw_name, standard_name in RAW_TO_STANDARD.items():
        if raw_name in out.columns and raw_name != standard_name:
            # 字段标准化在最早处完成，后续代码只使用小写标准名。
            out = out.withColumnRenamed(raw_name, standard_name)

    required = ["event_time_raw", *ANALOG_SENSORS, *DIGITAL_SENSORS]
    missing = [c for c in required if c not in out.columns]
    if missing:
        # 缺字段是 schema contract 失败，不能让 Spark 静默把缺失传感器变成 null。
        raise ValueError(f"MetroPT CSV 缺少字段: {missing}")

    select_exprs = []
    if "raw_index" in out.columns:
        select_exprs.append(F.col("raw_index").cast("long").alias("raw_index"))
    else:
        select_exprs.append(F.monotonically_increasing_id().cast("long").alias("raw_index"))

    select_exprs.extend(
        [
            F.to_timestamp("event_time_raw", "yyyy-MM-dd HH:mm:ss").alias("event_time"),
            *[F.col(c).cast("double").alias(c) for c in ANALOG_SENSORS + DIGITAL_SENSORS],
        ]
    )
    normalized = out.select(*select_exprs).filter(F.col("event_time").isNotNull())
    # event_minute 是 DWS window KPI、P9 minute features 和 realtime KPI 对齐的共同时间粒度。
    normalized = normalized.withColumn("event_date", F.to_date("event_time"))
    normalized = normalized.withColumn("event_minute", F.date_trunc("minute", F.col("event_time")))
    normalized = normalized.withColumn("dt", F.date_format("event_time", "yyyy-MM-dd"))
    normalized = _add_failure_window_columns(normalized, config)
    return normalized.withColumn(
        "operating_state",
        # operating_state 是基于 motor_current 的工程化状态标签，用于 EDA 和 KPI 分组；
        # 不是设备控制系统真实状态回写。
        F.when(F.col("motor_current") >= 7.0, F.lit("loaded"))
        .when(F.col("motor_current") >= 1.0, F.lit("unloaded"))
        .otherwise(F.lit("stopped")),
    )


def parse_failure_windows(config: dict) -> List[Tuple[str, str, str]]:
    """Parse semicolon-delimited failure windows from config."""
    raw = str(config.get("metropt", {}).get("failure_windows", "") or "").strip()
    windows: List[Tuple[str, str, str]] = []
    for chunk in raw.split(";"):
        chunk = chunk.strip()
        if not chunk:
            continue
        pieces = [p.strip() for p in chunk.split("|")]
        if len(pieces) != 3:
            raise ValueError(f"failure_windows 配置格式错误: {chunk}")
        windows.append((pieces[0], pieces[1], pieces[2]))
    return windows


def _parse_failure_windows(config: dict) -> List[Tuple[str, str, str]]:
    """Backward-compatible private alias for older imports."""
    return parse_failure_windows(config)


def _add_failure_window_columns(df: DataFrame, config: dict) -> DataFrame:
    """Add is_failure_window and failure_type columns using known MetroPT failure intervals."""
    # failure window 来自数据说明中的故障时间段，是弱标签来源；
    # 它用于分析和 baseline，不应被解释成人工逐行标注。
    windows = parse_failure_windows(config)
    if not windows:
        return df.withColumn("is_failure_window", F.lit(0)).withColumn("failure_type", F.lit("normal"))

    condition = None
    failure_type_expr = F.lit("normal")
    for start, end, failure_type in windows:
        # 多个故障窗口取并集；failure_type 保留窗口来源，便于报告解释不同故障阶段。
        window_cond = (F.col("event_time") >= F.to_timestamp(F.lit(start))) & (F.col("event_time") <= F.to_timestamp(F.lit(end)))
        condition = window_cond if condition is None else (condition | window_cond)
        failure_type_expr = F.when(window_cond, F.lit(failure_type)).otherwise(failure_type_expr)
    return df.withColumn("is_failure_window", F.when(condition, F.lit(1)).otherwise(F.lit(0))).withColumn("failure_type", failure_type_expr)


def sensor_dimension_rows(sensor_names: Iterable[str]) -> List[Dict]:
    """Return a compact sensor dimension table for DWD joins."""
    rows = []
    for sensor_name in sensor_names:
        sensor_type = "analog" if sensor_name in ANALOG_SENSORS else "digital"
        if sensor_name in {"tp2", "tp3", "h1", "dv_pressure", "reservoirs"}:
            station_id = "air_pressure_path"
        elif sensor_name in {"oil_temperature", "motor_current"}:
            station_id = "compressor_motor"
        else:
            station_id = "electrical_control"
        rows.append(
            {
                "sensor_name": sensor_name,
                "sensor_type": sensor_type,
                "station_id": station_id,
                "unit": SENSOR_UNITS.get(sensor_name, "unknown"),
            }
        )
    return rows


def sql_identifier(value: str) -> str:
    """Validate a SQL identifier before interpolating it into Spark SQL."""
    text = (value or "").strip()
    if not text or any(ch in text for ch in " ;'\"`"):
        raise ValueError(f"非法 SQL 标识符: {value}")
    return text


def write_json_report(spark: SparkSession, payload: Dict, output_dir: str, name: str) -> str:
    """Write a JSON report as a single text dataset under local or HDFS output_dir."""
    ensure_dir(output_dir, spark=spark)
    target = join_path(output_dir, name)
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2)
    spark.createDataFrame([(text,)], ["value"]).coalesce(1).write.mode("overwrite").text(target)
    return target
