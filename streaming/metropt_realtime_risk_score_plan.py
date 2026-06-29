# -*- coding: utf-8 -*-
"""Dry-run contract for MetroPT P9 realtime risk-score messages.

This helper does not connect to Kafka, Flink, Redis, Hive, or a model server.
It validates the current replay JSON shape and can attach a clearly marked
sample risk score for local contract review.
"""
# 阅读提示：本文件是 P9/P11 实时风险字段的 dry-run contract。
# 它只校验 Kafka JSON shape 并附加 sample risk_score，明确不连接 Kafka/Flink/Redis/Hive，也不是生产 ML 模型。
# 学习导读：
# - 链路位置：P9/P11 之间的本地契约检查器，用来解释实时风险字段应该长什么样。
# - 主要输入：内置样例或 JSONL payload。
# - 主要输出：valid/invalid/enriched 风险消息样例和字段校验结论。
# - 核心概念：这里的 score 是 signal-proxy 演示分，不是训练后的生产模型概率。
# - 边界提醒：本文件不连接 Kafka/Flink/Redis/Hive，不能作为实时链路已跑通的证据。
import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


ANALOG_SENSOR_FIELDS = [
    "tp2",
    "tp3",
    "h1",
    "dv_pressure",
    "reservoirs",
    "oil_temperature",
    "motor_current",
]

DIGITAL_SENSOR_FIELDS = [
    "comp",
    "dv_electric",
    "towers",
    "mpg",
    "lps",
    "pressure_switch",
    "oil_level",
    "caudal_impulses",
]

REQUIRED_EVENT_FIELDS = [
    # replay 事件必须包含时间、状态、弱标签和 15 个传感器字段；缺字段会在 contract 阶段直接报错。
    "event_id",
    "raw_index",
    "event_time",
    "ingest_time",
    "operating_state",
    "is_failure_window",
    "failure_type",
    *ANALOG_SENSOR_FIELDS,
    *DIGITAL_SENSOR_FIELDS,
]

VALID_OPERATING_STATES = {"loaded", "unloaded", "stopped"}
BASELINE_THRESHOLD = 0.5636003790254064
# 该阈值继承离线 baseline 的 contract 口径，只用于 dry-run 和 Flink signal-proxy 分级。
MODEL_VERSION = "p9_worker_dry_run_20260606"


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def _clip(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def _parse_time(value: str) -> bool:
    try:
        datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
        return True
    except ValueError:
        return False


def sample_valid_events() -> List[Dict]:
    return [
        {
            "event_id": "metropt-0",
            "raw_index": 0,
            "event_time": "2020-02-01 00:00:00",
            "ingest_time": "2026-06-06 12:47:43",
            "source": "metropt_csv_replay",
            "operating_state": "stopped",
            "is_failure_window": 0,
            "failure_type": "normal",
            "tp2": -0.0123,
            "tp3": 9.3274,
            "h1": 9.3111,
            "dv_pressure": -0.0229,
            "reservoirs": 9.3280,
            "oil_temperature": 53.5214,
            "motor_current": 0.0404,
            "comp": 1.0,
            "dv_electric": 0.0,
            "towers": 1.0,
            "mpg": 1.0,
            "lps": 0.0,
            "pressure_switch": 1.0,
            "oil_level": 1.0,
            "caudal_impulses": 1.0,
        },
        {
            "event_id": "metropt-7200",
            "raw_index": 7200,
            "event_time": "2020-02-01 02:00:00",
            "ingest_time": "2026-06-06 12:47:44",
            "source": "metropt_csv_replay",
            "operating_state": "unloaded",
            "is_failure_window": 0,
            "failure_type": "normal",
            "tp2": 1.842,
            "tp3": 8.915,
            "h1": 8.906,
            "dv_pressure": -0.024,
            "reservoirs": 8.934,
            "oil_temperature": 55.12,
            "motor_current": 3.95,
            "comp": 1.0,
            "dv_electric": 0.0,
            "towers": 1.0,
            "mpg": 0.0,
            "lps": 0.0,
            "pressure_switch": 1.0,
            "oil_level": 1.0,
            "caudal_impulses": 1.0,
        },
        {
            "event_id": "metropt-14400",
            "raw_index": 14400,
            "event_time": "2020-02-01 04:00:00",
            "ingest_time": "2026-06-06 12:47:45",
            "source": "metropt_csv_replay",
            "operating_state": "loaded",
            "is_failure_window": 0,
            "failure_type": "normal",
            "tp2": 6.112,
            "tp3": 8.74,
            "h1": 8.731,
            "dv_pressure": -0.022,
            "reservoirs": 8.768,
            "oil_temperature": 58.45,
            "motor_current": 7.32,
            "comp": 1.0,
            "dv_electric": 0.0,
            "towers": 1.0,
            "mpg": 1.0,
            "lps": 0.0,
            "pressure_switch": 1.0,
            "oil_level": 1.0,
            "caudal_impulses": 1.0,
        },
        {
            "event_id": "metropt-6696000",
            "raw_index": 6696000,
            "event_time": "2020-04-18 00:00:00",
            "ingest_time": "2026-06-06 12:47:46",
            "source": "metropt_csv_replay",
            "operating_state": "loaded",
            "is_failure_window": 1,
            "failure_type": "air_leak_high_stress",
            "tp2": 5.781,
            "tp3": 7.214,
            "h1": 7.196,
            "dv_pressure": -0.026,
            "reservoirs": 7.849,
            "oil_temperature": 64.3,
            "motor_current": 8.1,
            "comp": 1.0,
            "dv_electric": 1.0,
            "towers": 1.0,
            "mpg": 1.0,
            "lps": 0.0,
            "pressure_switch": 1.0,
            "oil_level": 1.0,
            "caudal_impulses": 1.0,
        },
        {
            "event_id": "metropt-14772000",
            "raw_index": 14772000,
            "event_time": "2020-07-15 14:30:00",
            "ingest_time": "2026-06-06 12:47:47",
            "source": "metropt_csv_replay",
            "operating_state": "loaded",
            "is_failure_window": 1,
            "failure_type": "air_leak_high_stress",
            "tp2": 4.96,
            "tp3": 6.98,
            "h1": 6.95,
            "dv_pressure": -0.029,
            "reservoirs": 7.91,
            "oil_temperature": 68.2,
            "motor_current": 8.75,
            "comp": 1.0,
            "dv_electric": 1.0,
            "towers": 1.0,
            "mpg": 1.0,
            "lps": 0.0,
            "pressure_switch": 1.0,
            "oil_level": 1.0,
            "caudal_impulses": 1.0,
        },
    ]


def sample_invalid_payloads() -> List[str]:
    missing_event_time = {
        k: v for k, v in sample_valid_events()[0].items() if k != "event_time"
    }
    missing_sensor = {
        k: v for k, v in sample_valid_events()[1].items() if k != "dv_electric"
    }
    bad_type = dict(sample_valid_events()[2])
    bad_type["motor_current"] = "not-a-number"
    return [
        json.dumps(missing_event_time, ensure_ascii=False, sort_keys=True),
        json.dumps(missing_sensor, ensure_ascii=False, sort_keys=True),
        json.dumps(bad_type, ensure_ascii=False, sort_keys=True),
        '{"event_id": "metropt-bad-json", "raw_index": 1, ',
    ]


def validate_event(event: Dict) -> List[str]:
    # 合约校验关注字段存在、时间格式、状态枚举和数值类型，避免坏 payload 进入下游 SQL sink。
    errors: List[str] = []
    for field in REQUIRED_EVENT_FIELDS:
        if field not in event or event[field] is None:
            errors.append(f"missing_required_field:{field}")
    if "event_time" in event and not _parse_time(event["event_time"]):
        errors.append("invalid_event_time_format")
    if "ingest_time" in event and not _parse_time(event["ingest_time"]):
        errors.append("invalid_ingest_time_format")
    if event.get("operating_state") not in VALID_OPERATING_STATES:
        errors.append("invalid_operating_state")
    if event.get("is_failure_window") not in {0, 1}:
        errors.append("invalid_is_failure_window")
    for field in ANALOG_SENSOR_FIELDS + DIGITAL_SENSOR_FIELDS:
        if field in event and event[field] is not None and not _is_number(event[field]):
            errors.append(f"non_numeric_sensor:{field}")
    return errors


def dry_run_risk_score(event: Dict) -> Tuple[float, List[str], str]:
    """Attach a contract-review score, not a production prediction."""
    # risk_score 优先透传已提供的离线/样例分数；否则用压力、温度、电流、数字信号构造 signal proxy。
    # 这个分数只用于字段契约和演示联调，不能解释为生产 ML 模型输出。
    if _is_number(event.get("risk_score")):
        score = _clip(float(event["risk_score"]))
        return score, ["passed_through_existing_risk_score"], "provided_risk_score"
    if _is_number(event.get("numpy_logistic_score")):
        score = _clip(float(event["numpy_logistic_score"]))
        return score, ["passed_through_numpy_logistic_score"], "p9_model_prediction_sample.tsv"

    pressure_delta = abs(float(event.get("tp3", 0.0)) - float(event.get("reservoirs", 0.0)))
    pressure_gap = abs(float(event.get("tp2", 0.0)) - float(event.get("tp3", 0.0)))
    oil_temperature = float(event.get("oil_temperature", 0.0))
    motor_current = float(event.get("motor_current", 0.0))
    digital_activity = sum(float(event.get(f, 0.0)) for f in DIGITAL_SENSOR_FIELDS) / max(len(DIGITAL_SENSOR_FIELDS), 1)

    # 这些 component 是工程化信号组合，目的是让实时链路有可解释风险字段可写入 Hive/Redis。
    pressure_component = _clip((pressure_delta + pressure_gap * 0.05) / 2.0)
    oil_component = _clip((oil_temperature - 55.0) / 20.0)
    current_component = _clip((motor_current - 6.5) / 4.0)
    digital_component = _clip(digital_activity)
    score = _clip(0.12 + 0.36 * pressure_component + 0.28 * oil_component + 0.18 * current_component + 0.06 * digital_component)

    reasons = []
    if pressure_component >= 0.25:
        reasons.append("pressure_balance_shift")
    if oil_component >= 0.25:
        reasons.append("oil_temperature_elevated")
    if current_component >= 0.25:
        reasons.append("motor_current_elevated")
    if not reasons:
        reasons.append("baseline_signal_level")
    return score, reasons, "dry_run_signal_proxy_not_production"


def enrich_event(event: Dict) -> Dict:
    score, reasons, source = dry_run_risk_score(event)
    risk_level = "high" if score >= BASELINE_THRESHOLD else "medium" if score >= 0.40 else "low"
    enriched = dict(event)
    # enriched event 明确写入 score_source/model_version，防止读者把 dry-run signal proxy 当成正式模型。
    enriched.update(
        {
            "risk_score": round(score, 6),
            "risk_level": risk_level,
            "risk_score_source": source,
            "risk_model_name": "p9_realtime_contract",
            "risk_model_version": MODEL_VERSION,
            "risk_threshold": BASELINE_THRESHOLD,
            "feature_window_minutes": [1, 5, 15, 60],
            "feature_window_end": event.get("event_time"),
            "model_feature_set_version": "p9_window_features_v1",
            "risk_reason": reasons,
            "scoring_time": _now_utc(),
        }
    )
    return enriched


def iter_input_payloads(path: str) -> Iterable[str]:
    if not path or path == "-":
        for line in sys.stdin:
            if line.strip():
                yield line.strip()
        return
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield line.strip()


def parse_payload(payload: str) -> Tuple[Dict, List[str]]:
    try:
        event = json.loads(payload)
    except json.JSONDecodeError as exc:
        return {}, [f"invalid_json:{exc.msg}"]
    if not isinstance(event, dict):
        return {}, ["payload_not_json_object"]
    return event, validate_event(event)


def emit_examples(kind: str, enrich: bool) -> int:
    payloads: List[str] = []
    if kind in {"valid", "all"}:
        for event in sample_valid_events():
            payloads.append(json.dumps(enrich_event(event) if enrich else event, ensure_ascii=False, sort_keys=True))
    if kind in {"invalid", "all"}:
        payloads.extend(sample_invalid_payloads())
    for payload in payloads:
        print(payload)
    return len(payloads)


def validate_payloads(input_jsonl: str, enrich: bool, max_events: int) -> int:
    total = 0
    invalid = 0
    for payload in iter_input_payloads(input_jsonl):
        if max_events > 0 and total >= max_events:
            break
        event, errors = parse_payload(payload)
        total += 1
        if errors:
            invalid += 1
            print(json.dumps({"status": "invalid", "errors": errors, "payload": payload}, ensure_ascii=False, sort_keys=True))
            continue
        out = enrich_event(event) if enrich else {"status": "valid", "event_id": event.get("event_id")}
        print(json.dumps(out, ensure_ascii=False, sort_keys=True))
    print(json.dumps({"summary": {"total": total, "invalid": invalid, "valid": total - invalid}}, ensure_ascii=False, sort_keys=True))
    return 1 if invalid else 0


def main() -> None:
    # 主流程提供样例输出和 JSONL 校验两种模式，用于在正式 Flink 作业前确认消息契约。
    parser = argparse.ArgumentParser()
    parser.add_argument("--emit-examples", choices=["valid", "invalid", "all"], default="")
    parser.add_argument("--input-jsonl", default="", help="JSONL input path, or '-' for stdin")
    parser.add_argument("--enrich", action="store_true", help="attach dry-run risk_score contract fields")
    parser.add_argument("--max-events", type=int, default=0)
    args = parser.parse_args()

    if args.max_events < 0:
        parser.error("--max-events must be >= 0")
    if args.emit_examples:
        count = emit_examples(args.emit_examples, args.enrich)
        print(json.dumps({"summary": {"emitted": count, "mode": "examples", "enriched": args.enrich}}, ensure_ascii=False, sort_keys=True))
        return
    if args.input_jsonl:
        sys.exit(validate_payloads(args.input_jsonl, args.enrich, args.max_events))
    parser.print_help()


if __name__ == "__main__":
    main()
