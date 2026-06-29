# -*- coding: utf-8 -*-
"""Build P9 sensor dictionary and weak-label documentation for MetroPT-3."""
# 阅读提示：本文件沉淀 P9 的传感器字典和 weak label 规则。
# 这里写的是建模口径说明，不会训练模型，也不会把弱标签包装成真实人工标注。
# - 链路位置：P9 起点，先把传感器含义和标签规则写清楚，再谈特征和模型。
# - 主要输入：配置中的 failure_windows、Raw CSV 时间范围和传感器定义。
# - 主要输出：sensor dictionary、label system 文档和 label summary。
# - 边界提醒：这一步只固化口径，不训练模型，也不代表故障诊断能力已经成立。
import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from analysis_common import MODEL_DIR, REPORT_DIR, ensure_analysis_dirs, write_json, write_markdown  # noqa: E402
from p9_common import (  # noqa: E402
    SENSOR_DETAILS,
    active_config,
    build_label_frame,
    configured_csv_path,
    failure_window_records,
    read_metropt_csv,
    relative_path,
    write_tsv,
)


def _sensor_dictionary_markdown(config) -> str:
    # 传感器字典把 raw_name、standard_name、物理含义和业务解释绑定，方便报告、BI 和特征命名复用。
    analog_count = sum(1 for row in SENSOR_DETAILS if row["sensor_type"] == "analog")
    digital_count = sum(1 for row in SENSOR_DETAILS if row["sensor_type"] == "digital")
    table_lines = [
        "| # | Type | Raw field | Standard field | Unit/value | Physical meaning | Modeling note |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in SENSOR_DETAILS:
        table_lines.append(
            "| {order} | {sensor_type} | `{raw_name}` | `{standard_name}` | {unit} | {physical_meaning} | {business_note} |".format(
                **row
            )
        )

    source_path = configured_csv_path(config)
    return f"""# P9 Sensor Dictionary

## Scope

- Dataset: `{config.get('metropt', {}).get('dataset_name', 'MetroPT-3 Dataset')}`.
- Local source checked by worker: `<WORKER_PROJECT_ROOT>/datas/{source_path.name}`.
- Sensor count: `{analog_count}` analog sensors and `{digital_count}` digital sensors.
- `DV_eletric` is the original CSV spelling. The project standard field is `dv_electric`.

The data description reports `15,169,480 data points`; the current project ODS baseline is `1,516,948` timestamp rows. These are different counting concepts and must not be mixed in model reports.

## Sensor Fields

{chr(10).join(table_lines)}

## Modeling Boundary

- Analog sensors can be used for rolling statistics, slopes, pressure differences, and correlation analysis.
- Digital sensors should be treated as binary state/control signals, with activation counts, toggles, and duration features.
- `operating_state` is derived from `motor_current`: `loaded` when `motor_current >= 7.0`, `unloaded` when `1.0 <= motor_current < 7.0`, and `stopped` otherwise.
- This dictionary is a worker-side P9 artifact. Any cluster table/schema confirmation is 待 master 验证.
"""


def _label_system_markdown(config, summary, label_summary_path: Path) -> str:
    # 标签体系来自 failure windows 和相对时间窗口，核心约束是时间切分和避免未来信息泄漏。
    window_lines = [
        "| Failure id | Start | End | Failure type | Severity | Label source |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for record in failure_window_records(config):
        window_lines.append(
            f"| {record['failure_id']} | `{record['start']}` | `{record['end']}` | `{record['failure_type']}` | `{record['severity']}` | Configured official failure interval |"
        )

    label_lines = [
        "| Label | Rule | Intended use | Leakage control | Limitation |",
        "| --- | --- | --- | --- | --- |",
        "| `failure_window` | `event_time` falls inside one configured failure interval. | Current-window weak classification target and EDA grouping. | Use only as target/grouping, never as feature. | Interval-derived weak label, not manually verified row-level truth. |",
        "| `pre_failure_1h` | `event_time` is in `[failure_start - 1h, failure_start)`. | Short-horizon early-warning target. | Computed from event calendar for offline labels; not available as online feature. | Positive windows are nested inside 6h and 24h labels. |",
        "| `pre_failure_6h` | `event_time` is in `[failure_start - 6h, failure_start)`. | Medium-horizon early-warning target. | Use time split before model evaluation. | Still weak because the failure report has interval granularity. |",
        "| `pre_failure_24h` | `event_time` is in `[failure_start - 24h, failure_start)`. | Main P9 early-warning baseline target. | Exclude the label and all future-derived fields from feature columns. | Includes normal-looking minutes near a reported failure. |",
        "| `post_maintenance` | `event_time` is in `(failure_end, failure_end + 24h]`. | Recovery/maintenance context exclusion flag. | Do not train normal class from recovery windows unless explicitly tested. | It is a pragmatic recovery window, not a verified maintenance work order. |",
        "| `normal_candidate` | Not `failure_window`, not `pre_failure_24h`, and not `post_maintenance`. | Conservative normal class candidate. | Still split by time; do not sample randomly across months. | It is only a candidate normal label because the dataset is unlabeled. |",
        "| `rul_seconds` | Seconds until the next configured failure start; `0` inside a configured failure window; null after the last known failure horizon. | Weak RUL regression target or report field. | Target only; never use as a model feature. | It is derived from failure intervals and is not true component remaining life. |",
    ]

    count_lines = [
        "| Label | Positive rows | Positive rate |",
        "| --- | ---: | ---: |",
    ]
    for item in summary["labels"]:
        count_lines.append(f"| `{item['label']}` | {item['positive_rows']} | {item['positive_rate']:.6f} |")

    return f"""# P9 Label System

## Source and Boundary

- Label source: `metropt.failure_windows` in the active MetroPT config.
- Worker local row count checked from CSV timestamps: `{summary['row_count']}`.
- Time range: `{summary['time_range']['min_event_time']}` to `{summary['time_range']['max_event_time']}`.
- Label summary artifact: `{relative_path(label_summary_path)}`.

The original MetroPT-3 dataset is unlabeled at row level. The company failure reports provide time intervals that can support failure prediction, anomaly detection, and RUL experiments, but they are weak labels. They must not be described as manually verified per-row fault truth.

## Configured Failure Windows

{chr(10).join(window_lines)}

## Label Rules

{chr(10).join(label_lines)}

## Local Label Distribution

{chr(10).join(count_lines)}

## Time Split Requirement

- Train/validation/test must be split by `event_time`, not random rows.
- Recommended P9 split for full CSV experiments:
  - train: before `2020-06-01 00:00:00`
  - validation: `2020-06-01 00:00:00` to before `2020-07-01 00:00:00`
  - test: from `2020-07-01 00:00:00`
- The split keeps earlier failure windows in train/validation and leaves the July failure for test-style evaluation.

## Leakage Notes

- `failure_window`, `pre_failure_*`, `post_maintenance`, `normal_candidate`, and `rul_seconds` are labels or evaluation masks, not feature inputs.
- Rolling features must be right-aligned and computed only from current/past sensor values.
- `rul_seconds` uses knowledge of the next configured failure start, so it is acceptable only as an offline target.
- Worker-side label generation is a local self-check; master cluster table alignment is 待 master 验证.
"""


def _label_summary(labels: pd.DataFrame) -> dict:
    # label summary 用来量化弱标签分布，尤其要看 pre_failure 和 normal_candidate 是否有足够样本。
    label_names = [
        "failure_window",
        "pre_failure_1h",
        "pre_failure_6h",
        "pre_failure_24h",
        "post_maintenance",
        "normal_candidate",
    ]
    row_count = int(len(labels))
    label_rows = []
    for label in label_names:
        positives = int(labels[label].sum())
        label_rows.append(
            {
                "label": label,
                "positive_rows": positives,
                "negative_rows": row_count - positives,
                "positive_rate": positives / row_count if row_count else 0.0,
            }
        )
    return {
        "row_count": row_count,
        "time_range": {
            "min_event_time": str(labels["event_time"].min()),
            "max_event_time": str(labels["event_time"].max()),
        },
        "labels": label_rows,
        "rul_non_null_rows": int(labels["rul_seconds"].notna().sum()),
        "rul_min_seconds": float(labels["rul_seconds"].min()) if labels["rul_seconds"].notna().any() else None,
        "rul_max_seconds": float(labels["rul_seconds"].max()) if labels["rul_seconds"].notna().any() else None,
    }


def main() -> None:
    # 主流程只生成文档和标签分布摘要，为后续 feature engineering 和 baseline 提供口径。
    ensure_analysis_dirs()
    config = active_config()
    timestamps = read_metropt_csv(config, columns=["event_time"])[["event_time"]]
    labels = build_label_frame(timestamps["event_time"], config)
    summary = _label_summary(labels)

    label_summary_path = MODEL_DIR / "p9_label_summary.tsv"
    # TSV 便于人工查看，JSON 便于后续验收或报告脚本读取。
    write_tsv(
        label_summary_path,
        summary["labels"],
        ["label", "positive_rows", "negative_rows", "positive_rate"],
    )
    json_path = write_json(MODEL_DIR / "p9_label_summary.json", summary)
    # 这两份 Markdown 是说明阶段解释 P9 的入口：先讲传感器，再讲标签边界。
    sensor_path = write_markdown(REPORT_DIR / "p9_sensor_dictionary.md", _sensor_dictionary_markdown(config))
    label_path = write_markdown(REPORT_DIR / "p9_label_system.md", _label_system_markdown(config, summary, label_summary_path))

    print("P9 label builder completed.")
    print("sensor_dictionary:", sensor_path)
    print("label_system:", label_path)
    print("label_summary_tsv:", label_summary_path)
    print("label_summary_json:", json_path)


if __name__ == "__main__":
    main()
