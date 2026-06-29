# -*- coding: utf-8 -*-
"""Shared helpers for MetroPT-3 analysis and baseline jobs."""
# 阅读提示：本模块是 analysis/ 的公共工具层，只放路径、Spark、输入检查和 artifact 写入能力。
# 各分析脚本应在这里复用配置和 I/O 规则，避免每个脚本自行解释 ODS/DWD/DWS 路径。
# 学习导读：
# - 链路位置：analysis 公共层，被 00-11 和 runner 复用。
# - 主要输入：MetroPT 配置、Spark/HDFS 路径、本地 artifact 路径。
# - 主要输出：统一的目录、输入状态、读写工具和 JSON/Markdown/figure 保存函数。
# - 核心概念：公共层固定分析产物的位置和输入检查规则，保证报告证据可追溯。
# - 边界提醒：这里不应放具体模型逻辑；模型和特征口径应留在 P9/P10 脚本或 p9_common.py。
import json
import math
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from metropt_utils import (  # noqa: E402
    ANALOG_SENSORS,
    DIGITAL_SENSORS,
    assert_path_exists,
    create_metropt_spark,
    is_distributed_path,
    load_metropt_config,
    path_exists,
    to_local_path,
)


ANALYSIS_ROOT = ROOT_DIR / "data" / "metropt_quality" / "analysis"
# 本地 analysis artifact 分为 reports、figures、models、logs，便于 README 和交付记录按类型索引。
REPORT_DIR = ANALYSIS_ROOT / "reports"
FIGURE_DIR = ANALYSIS_ROOT / "figures"
MODEL_DIR = ANALYSIS_ROOT / "models"
LOG_DIR = ANALYSIS_ROOT / "logs"

FULL_ANALYSIS_PATH_KEYS = [
    "ods_readings_parquet",
    "dwd_sensor_long",
    "dws_window_kpi",
    "dws_sensor_kpi",
]

INPUT_CHECKS = [
    # 输入清单描述每个分析依赖由哪个 src 步骤生产，方便第一次复现时定位缺失上游。
    {
        "key": "input_csv",
        "label": "Raw MetroPT CSV",
        "required_for": "light validation",
        "producer": "python src/00_metropt_preflight.py",
    },
    {
        "key": "profile_dir",
        "label": "Profile output directory",
        "required_for": "profile review",
        "producer": "spark-submit src/01_metropt_profile.py",
    },
    {
        "key": "ods_readings_parquet",
        "label": "ODS readings Parquet",
        "required_for": "data quality analysis",
        "producer": "spark-submit src/02_metropt_csv_to_parquet.py",
    },
    {
        "key": "dwd_sensor_long",
        "label": "DWD sensor long Parquet",
        "required_for": "sensor and failure-window analysis",
        "producer": "spark-submit src/03_metropt_dwd_sensor_long.py",
    },
    {
        "key": "dws_overall_kpi",
        "label": "DWS overall KPI Parquet",
        "required_for": "overall KPI review",
        "producer": "spark-submit src/04_metropt_kpi_calc.py",
    },
    {
        "key": "dws_window_kpi",
        "label": "DWS window KPI Parquet",
        "required_for": "multidimensional analysis and modeling",
        "producer": "spark-submit src/04_metropt_kpi_calc.py",
    },
    {
        "key": "dws_sensor_kpi",
        "label": "DWS sensor KPI Parquet",
        "required_for": "sensor KPI analysis",
        "producer": "spark-submit src/04_metropt_kpi_calc.py",
    },
]


def ensure_analysis_dirs() -> Dict[str, Path]:
    """Create stable local output directories for analysis artifacts."""
    dirs = {
        "root": ANALYSIS_ROOT,
        "reports": REPORT_DIR,
        "figures": FIGURE_DIR,
        "models": MODEL_DIR,
        "logs": LOG_DIR,
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def load_config() -> Dict[str, Any]:
    """Load the active MetroPT config through the existing project helper."""
    return load_metropt_config()


def uses_distributed_paths(config: Dict[str, Any], keys: Optional[Sequence[str]] = None) -> bool:
    """Return whether selected configured paths need a Spark/Hadoop filesystem check."""
    paths = config.get("paths", {})
    for key in keys or paths.keys():
        path = str(paths.get(key, "") or "")
        if path and is_distributed_path(path):
            return True
    return False


def collect_input_status(config: Dict[str, Any], spark=None) -> List[Dict[str, Any]]:
    """Check raw/profile/ODS/DWD/DWS paths without creating downstream data."""
    # 本检查只读取路径状态，不写下游产物；分布式路径必须通过 Spark/Hadoop filesystem 判断。
    paths = config.get("paths", {})
    statuses: List[Dict[str, Any]] = []
    for spec in INPUT_CHECKS:
        key = spec["key"]
        configured_path = str(paths.get(key, "") or "")
        # status 保留 label/producer，报告里才能直接告诉读者“缺什么”和“由哪个上游脚本产生”。
        status = {
            **spec,
            "path": configured_path,
            "exists": False,
            "check_error": "",
        }
        if not configured_path:
            status["check_error"] = f"paths.{key} is empty"
            statuses.append(status)
            continue
        try:
            if is_distributed_path(configured_path):
                if spark is None:
                    # HDFS/S3 类路径不能用本地 os.path 检查；没有 Spark 时记录诊断而不是误报不存在。
                    status["check_error"] = "distributed path requires Spark to check"
                else:
                    status["exists"] = bool(path_exists(configured_path, spark=spark))
            else:
                status["exists"] = os.path.exists(to_local_path(configured_path))
        except Exception as exc:  # Keep validation diagnostic, do not hide the path.
            status["check_error"] = str(exc)
        statuses.append(status)
    return statuses


def missing_full_inputs(statuses: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return missing datasets required by full analysis/modeling."""
    required = set(FULL_ANALYSIS_PATH_KEYS)
    return [item for item in statuses if item.get("key") in required and not item.get("exists")]


def build_missing_inputs_message(missing: Sequence[Dict[str, Any]]) -> str:
    """Format a clear upstream instruction for incomplete analysis inputs."""
    lines = [
        "MetroPT analysis inputs are incomplete.",
        "Run the offline chain to DWS before full analysis:",
        "  python src/run_metropt_offline.py --stop-after 04_metropt_kpi_calc.py",
        "",
        "Missing inputs:",
    ]
    for item in missing:
        suffix = f" ({item['check_error']})" if item.get("check_error") else ""
        lines.append(f"  - {item['label']}: {item['path']}{suffix}")
        lines.append(f"    producer: {item['producer']}")
    return "\n".join(lines)


def require_paths_ready(spark, config: Dict[str, Any], path_keys: Sequence[str]) -> None:
    """Fail early when a required configured path does not exist."""
    paths = config.get("paths", {})
    for key in path_keys:
        assert_path_exists(spark, paths[key], key)


def read_parquet(spark, config: Dict[str, Any], path_key: str):
    """Read a configured Parquet path after checking it exists."""
    require_paths_ready(spark, config, [path_key])
    return spark.read.parquet(config["paths"][path_key])


def prepare_spark_after_input_validation(app_name: str, config: Dict[str, Any], required_path_keys: Sequence[str]):
    """Validate required inputs before starting Spark when local paths can be checked directly."""
    # 先做输入完整性检查再启动真正任务，避免 DWS/模型阶段才暴露上游缺失。
    spark = None
    try:
        if uses_distributed_paths(config, required_path_keys):
            spark = create_metropt_spark(app_name, config=config)
            spark.sparkContext.setLogLevel("WARN")
            statuses = collect_input_status(config, spark=spark)
        else:
            statuses = collect_input_status(config, spark=None)

        missing = [item for item in statuses if item.get("key") in set(required_path_keys) and not item.get("exists")]
        if missing:
            # 缺输入时直接给上游修复命令，帮助说明阶段形成“先补数据层，再跑分析”的习惯。
            raise RuntimeError(build_missing_inputs_message(missing))

        if spark is None:
            # 本地路径已通过 os.path 检查后，再创建 Spark，减少不必要的 session 启动成本。
            spark = create_metropt_spark(app_name, config=config)
            spark.sparkContext.setLogLevel("WARN")
        return spark, statuses
    except Exception:
        if spark is not None:
            spark.stop()
        raise


def write_json(path: Path, payload: Dict[str, Any]) -> Path:
    """Write a UTF-8 JSON artifact with stable formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(to_jsonable(payload), f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    return path


def write_markdown(path: Path, content: str) -> Path:
    """Write a UTF-8 Markdown artifact."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(content.rstrip() + "\n")
    return path


def save_csv(path: Path, rows: Sequence[Dict[str, Any]], fieldnames: Sequence[str]) -> Path:
    """Write a small CSV artifact from collected analysis rows."""
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name) for name in fieldnames})
    return path


def save_figure(fig, path: Path) -> Path:
    """Save a matplotlib figure and verify a non-empty image was produced."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path, dpi=160, bbox_inches="tight")
    if not path.exists() or path.stat().st_size <= 0:
        raise RuntimeError(f"Figure output is empty: {path}")
    return path


def to_jsonable(value: Any) -> Any:
    """Convert Spark/pandas/numpy-friendly values into JSON-safe Python values."""
    if value is None:
        return None
    if isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else value
    if isinstance(value, (datetime, date)):
        return value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()
    if hasattr(value, "asDict"):
        return {k: to_jsonable(v) for k, v in value.asDict().items()}
    if hasattr(value, "item"):
        try:
            return to_jsonable(value.item())
        except Exception:
            pass
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_jsonable(v) for v in value]
    return str(value)


def now_run_id() -> str:
    """Return a timestamp id used by local runners."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def elapsed_text(start: datetime, end: datetime) -> str:
    """Format elapsed seconds as HH:MM:SS."""
    seconds = int((end - start).total_seconds())
    return f"{seconds // 3600:02d}:{seconds % 3600 // 60:02d}:{seconds % 60:02d}"


def figure_relpath(path: Path) -> str:
    """Return a report-friendly path relative to the project root."""
    try:
        return str(path.relative_to(ROOT_DIR)).replace("\\", "/")
    except ValueError:
        return str(path)


def available_columns(df, names: Iterable[str]) -> List[str]:
    """Return requested column names that exist in a Spark DataFrame."""
    existing = set(df.columns)
    return [name for name in names if name in existing]


def weighted_avg_expr(functions_module, value_col: str, weight_col: str = "sample_count"):
    """Build a weighted-average Spark expression for aggregated KPI rows."""
    F = functions_module
    return (
        F.sum(F.col(value_col) * F.col(weight_col))
        / F.when(F.sum(F.col(weight_col)) > 0, F.sum(F.col(weight_col))).otherwise(F.lit(None))
    )
