# -*- coding: utf-8 -*-
"""Replay MetroPT-3 air-compressor readings to Kafka."""
# 阅读提示：本文件把 MetroPT CSV 回放成 Kafka JSON event，是实时链路的源头模拟器。
# 它只做字段标准化、failure window 标注和发送节流，不做 Flink 聚合或风险模型计算。
# 学习导读：
# - 链路位置：streaming 源头，把静态 MetroPT CSV 模拟成实时 Kafka 事件流。
# - 主要输入：本地/集群 CSV、realtime.kafka 配置、failure_windows、发送速率参数。
# - 主要输出：Kafka JSON event；dry-run 模式只打印样例，不写 Kafka。
# - 核心概念：event contract 要稳定，因为下游 Flink KPI 和 P11 risk job 都按这个 JSON shape 解析。
# - 边界提醒：replay 是演示型数据源，不代表真实设备采集服务，也不计算 risk_score。
import argparse
import json
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import numpy as np
import pandas as pd

try:
    from kafka import KafkaProducer
except Exception:  # pragma: no cover
    KafkaProducer = None


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config" / "metropt_quality.local.yaml"

RAW_TO_STANDARD = {
    # Kafka event 使用项目标准列名；这里兼容原始 CSV 的 DV_eletric 拼写和修正后的 DV_electric。
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

SENSOR_COLUMNS = list(dict.fromkeys(RAW_TO_STANDARD.values()))

DEFAULT_FAILURE_WINDOWS = (
    # 默认故障窗口用于 replay event 的弱标签字段，便于下游 Flink/Hive 验证，不代表逐行人工标注。
    "2020-04-18 00:00:00|2020-04-18 23:59:59|air_leak_high_stress;"
    "2020-05-29 23:30:00|2020-05-30 06:00:00|air_leak_high_stress;"
    "2020-06-05 10:00:00|2020-06-07 14:30:00|air_leak_high_stress;"
    "2020-07-15 14:30:00|2020-07-15 19:00:00|air_leak_high_stress"
)


def _strip_comment(s: str) -> str:
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


def _load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        content = f.read()
    try:
        import yaml  # type: ignore

        return yaml.safe_load(content) or {}
    except ModuleNotFoundError:
        return _simple_yaml_load(content)


def _load_config(config_path: Optional[str] = None) -> dict:
    resolved = Path(config_path or os.environ.get("METROPT_CONFIG") or DEFAULT_CONFIG)
    if not resolved.exists():
        raise FileNotFoundError(f"MetroPT 配置文件不存在: {resolved}")
    return _load_yaml(resolved)


def _cfg_get(config: dict, *keys: str, default=None):
    cur = config
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def _positive_int(value, default: int) -> int:
    try:
        parsed = int(value)
        return parsed if parsed > 0 else default
    except Exception:
        return default


def _looks_like_windows_drive_path(path: str) -> bool:
    return len(path) >= 3 and path[1] == ":" and path[0].isalpha() and path[2] in {"/", "\\"}


def _validate_source_csv(source_csv: str, config_path: str) -> None:
    if os.name != "nt" and _looks_like_windows_drive_path(source_csv):
        raise FileNotFoundError(
            "replay_source_csv 指向 Windows 路径，当前在 Linux/VM 中无法读取: "
            f"{source_csv}\n"
            f"当前配置文件: {config_path}\n"
            "请使用 cluster 配置运行，例如: "
            "python streaming/metropt_replay_to_kafka.py "
            "--config /home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml "
            "--dry-run --print-sample 3 --max-events 3\n"
            "或显式指定 VM 本地 CSV: "
            "--source-csv /home/common/tmp/metropt_quality/MetroPT3_AirCompressor.csv"
        )
    if not Path(source_csv).exists():
        raise FileNotFoundError(
            f"replay_source_csv 文件不存在: {source_csv}\n"
            f"当前配置文件: {config_path}\n"
            "请先确认 CSV 已上传到 VM，或使用 --source-csv 指向实际文件。"
        )


def _parse_failure_windows(raw: str) -> List[Tuple[datetime, datetime, str]]:
    # replay 侧也解析 failure_windows，是为了让 Kafka event 自带和离线链路一致的弱标签字段。
    windows = []
    for chunk in (raw or "").split(";"):
        chunk = chunk.strip()
        if not chunk:
            continue
        start, end, label = [x.strip() for x in chunk.split("|")]
        windows.append(
            (
                datetime.strptime(start, "%Y-%m-%d %H:%M:%S"),
                datetime.strptime(end, "%Y-%m-%d %H:%M:%S"),
                label,
            )
        )
    return windows


def _label_for_time(ts: datetime, windows: List[Tuple[datetime, datetime, str]]) -> Tuple[int, str]:
    for start, end, label in windows:
        if start <= ts <= end:
            return 1, label
    return 0, "normal"


def _finite(value, default=0.0):
    try:
        fval = float(value)
    except Exception:
        return default
    return fval if math.isfinite(fval) else default


def _json_default(value):
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        fval = float(value)
        return fval if math.isfinite(fval) else None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _normalize_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    # 每个 CSV chunk 先统一字段名和数值类型；只有通过该规范化后的行才会进入 Kafka JSON。
    out = chunk.copy()
    unnamed_cols = [c for c in out.columns if str(c).startswith("Unnamed") or str(c).strip() == ""]
    if unnamed_cols:
        # raw_index 保留 CSV 原始行号语义，方便从 Kafka event 追溯回静态数据行。
        out = out.rename(columns={unnamed_cols[0]: "raw_index"})
    elif "raw_index" not in out.columns:
        out["raw_index"] = np.arange(len(out), dtype=np.int64)

    out = out.rename(columns=RAW_TO_STANDARD)
    if "timestamp" not in out.columns:
        raise ValueError("MetroPT CSV 必须包含 timestamp 列。")
    out["event_time"] = pd.to_datetime(out["timestamp"], errors="coerce")
    out = out.dropna(subset=["event_time"])
    for col in SENSOR_COLUMNS:
        if col not in out.columns:
            # replay event contract 要求 15 个传感器字段齐全；缺字段时宁可提前失败，也不要发半结构化事件。
            raise ValueError(f"MetroPT CSV 缺少字段: {col}")
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _iter_events(source_csv: str, chunksize: int, failure_windows: List[Tuple[datetime, datetime, str]], max_events: int) -> Iterable[Dict]:
    # 事件生成器把原始时间序列逐行转成 JSON payload，并补充 operating_state 与 failure window 弱标签。
    sent = 0
    for chunk in pd.read_csv(source_csv, chunksize=chunksize):
        data = _normalize_chunk(chunk)
        for _, row in data.iterrows():
            if max_events > 0 and sent >= max_events:
                return
            ts = row["event_time"].to_pydatetime()
            is_failure, failure_type = _label_for_time(ts, failure_windows)
            motor_current = _finite(row["motor_current"])
            if motor_current >= 7.0:
                operating_state = "loaded"
            elif motor_current >= 1.0:
                operating_state = "unloaded"
            else:
                operating_state = "stopped"
            # event 中同时带 raw sensor、operating_state 和 weak label，下游 Flink 不再重新读取 CSV。
            event = {
                "event_id": f"metropt-{int(row['raw_index'])}",
                "raw_index": int(row["raw_index"]),
                "event_time": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "ingest_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "source": "metropt_csv_replay",
                "operating_state": operating_state,
                "is_failure_window": is_failure,
                "failure_type": failure_type,
            }
            for col in SENSOR_COLUMNS:
                event[col] = _finite(row[col])
            sent += 1
            yield event


def main() -> None:
    # 主流程支持 dry-run 合约检查和真实 Kafka 发送；dry-run 会打印样例，不连接 Kafka。
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--config", default="", help="MetroPT YAML config; defaults to METROPT_CONFIG or local config")
    pre_args, _ = pre_parser.parse_known_args()
    config_path = str(Path(pre_args.config or os.environ.get("METROPT_CONFIG") or DEFAULT_CONFIG))
    config = _load_config(pre_args.config or None)

    parser = argparse.ArgumentParser(parents=[pre_parser])
    parser.add_argument(
        "--source-csv",
        default=str(_cfg_get(config, "realtime", "replay_source_csv", default=_cfg_get(config, "paths", "input_csv", default=""))),
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=str(_cfg_get(config, "realtime", "kafka_bootstrap_servers", default="")),
    )
    parser.add_argument("--topic", default=str(_cfg_get(config, "realtime", "kafka_topic", default="metropt.ods.compressor.reading.v1")))
    parser.add_argument("--chunksize", type=int, default=10000)
    parser.add_argument("--batch-size", type=int, default=_positive_int(_cfg_get(config, "realtime", "replay_batch_size", default=500), 500))
    parser.add_argument(
        "--rate",
        type=int,
        default=_positive_int(_cfg_get(config, "realtime", "replay_rate", default=100), 100),
        help="events per second; <=0 sends as fast as possible",
    )
    parser.add_argument("--max-events", type=int, default=0, help="0 means full file; dry-run defaults to printed sample size")
    parser.add_argument("--failure-windows", default=str(_cfg_get(config, "metropt", "failure_windows", default=DEFAULT_FAILURE_WINDOWS)))
    parser.add_argument("--dry-run", action="store_true", help="validate and print events without connecting to Kafka")
    parser.add_argument("--print-sample", type=int, default=0, help="print first N normalized JSON events")
    args = parser.parse_args()

    source_csv = str(args.source_csv or "").strip()
    if not source_csv:
        parser.error("缺少 --source-csv，且配置中没有 realtime.replay_source_csv 或 paths.input_csv")
    if args.chunksize <= 0:
        parser.error("--chunksize 必须大于 0")
    if args.batch_size <= 0:
        parser.error("--batch-size 必须大于 0")
    if args.max_events < 0:
        parser.error("--max-events 必须大于等于 0")
    is_windows_drive_path = len(source_csv) >= 2 and source_csv[1] == ":"
    parsed_source = urlparse(source_csv)
    if is_windows_drive_path:
        parsed_source = urlparse("")
    if parsed_source.scheme and parsed_source.scheme not in {"file"}:
        raise ValueError(f"replay producer 需要可被 pandas 直接读取的本地 CSV，不支持: {source_csv}")
    if parsed_source.scheme == "file":
        source_csv = parsed_source.path
        if os.name == "nt" and len(source_csv) >= 3 and source_csv[0] == "/" and source_csv[2] == ":":
            source_csv = source_csv[1:]
    _validate_source_csv(source_csv, config_path)

    if args.dry_run and args.max_events <= 0:
        # dry-run 默认只生成少量样例，用于学习 JSON shape 和字段含义；真实发送才需要 KafkaProducer。
        args.max_events = max(args.print_sample, 5)
    if args.dry_run and args.print_sample <= 0:
        args.print_sample = min(args.max_events, 5)

    if not args.dry_run and KafkaProducer is None:
        raise RuntimeError("缺少 kafka-python，请先安装 kafka-python。")
    if not args.dry_run and not str(args.bootstrap_servers or "").strip():
        parser.error("缺少 --bootstrap-servers，且配置中没有 realtime.kafka_bootstrap_servers")

    failure_windows = _parse_failure_windows(args.failure_windows)
    producer = None
    if not args.dry_run:
        # 只有真实发送模式才创建 KafkaProducer，便于在没有 Kafka 的本地环境做 contract 学习。
        producer = KafkaProducer(
            bootstrap_servers=[s.strip() for s in args.bootstrap_servers.split(",") if s.strip()],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=_json_default).encode("utf-8"),
            acks="all",
            linger_ms=20,
            retries=3,
        )

    sent = 0
    failed = 0
    printed = 0
    started = time.time()
    batch_started = time.time()
    first_event_time = None
    last_event_time = None
    for event in _iter_events(source_csv, args.chunksize, failure_windows, args.max_events):
        first_event_time = first_event_time or event["event_time"]
        last_event_time = event["event_time"]
        if args.print_sample > printed:
            print(json.dumps(event, ensure_ascii=False, default=_json_default))
            printed += 1

        if not args.dry_run and producer is not None:
            try:
                producer.send(args.topic, value=event)
            except Exception as exc:
                failed += 1
                print(f"[replay] send_failed event_id={event.get('event_id')} error={exc}", file=sys.stderr)
                continue
        sent += 1
        if not args.dry_run and sent % args.batch_size == 0 and producer is not None:
            producer.flush()
            if args.rate > 0:
                expected = args.batch_size / float(args.rate)
                elapsed = time.time() - batch_started
                if elapsed < expected:
                    time.sleep(expected - elapsed)
                batch_started = time.time()
            print(f"sent={sent} topic={args.topic}")

    if producer is not None:
        producer.flush()
        producer.close()
    elapsed_total = max(time.time() - started, 0.001)
    effective_rate = sent / elapsed_total
    mode = "dry-run" if args.dry_run else "send"
    print(
        "MetroPT replay 完成: "
        f"mode={mode}, sent={sent}, failed={failed}, topic={args.topic}, "
        f"elapsed_seconds={elapsed_total:.2f}, effective_rate={effective_rate:.2f}/s, "
        f"first_event_time={first_event_time}, last_event_time={last_event_time}"
    )


if __name__ == "__main__":
    main()
