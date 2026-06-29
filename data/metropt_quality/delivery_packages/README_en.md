# delivery_packages Index

Language / 语言: [中文](README_zh.md) | [English](README_en.md)

This directory stores P8 delivery packages. The current formal entry is:

```text
p8_delivery_package_20260606_011332/delivery_index.md
```

Chinese guide files are consistently named `*.zh.md`, and English originals are kept without overwrite.

## Package Versions

| Directory | Status | Description |
| --- | --- | --- |
| `p8_delivery_package_20260606_010634/` | Historical package | Early delivery package kept for traceability |
| `p8_delivery_package_20260606_011100/` | Historical package | Early delivery package kept for traceability |
| `p8_delivery_package_20260606_011332/` | Current formal package | Current P8 delivery package entry |

## Current Package Reading Order

| Order | English original | Chinese guide | Purpose |
| --- | --- | --- | --- |
| 1 | `delivery_index.md` | `delivery_index.zh.md` | Main index |
| 2 | `package_summary.md` | `package_summary.zh.md` | Package summary and boundary |
| 3 | `project_overview.md` | `project_overview.zh.md` | Project overview |
| 4 | `acceptance_results.md` | `acceptance_results.zh.md` | Acceptance results |
| 5 | `metrics_and_queries.md` | `metrics_and_queries.zh.md` | Metrics and queries |
| 6 | `run_order.md` | `run_order.zh.md` | Reproduction order |
| 7 | `realtime_demo_steps.md` | `realtime_demo_steps.zh.md` | Real-time demo steps |
| 8 | `troubleshooting_entry.md` | `troubleshooting_entry.zh.md` | Troubleshooting entry |

## Reproduction Notes

The delivery package stores evidence, indexes, and documentation only. It does not copy raw CSV, HDFS data, or large Parquet files. For reproduction, return to the project root:

```bash
cd /home/common/tmp/pycharm_Design
bin/p7_ops_snapshot.sh
bin/p10_p9_master_validation.sh --mode standard --allow-swapoff --realtime-max-events 1000 --realtime-wait-seconds 60 --query-timeout 300
```

For a quick environment check, run:

```bash
bin/p10_p9_master_validation.sh --mode smoke
```

`smoke` cannot replace formal validation.

