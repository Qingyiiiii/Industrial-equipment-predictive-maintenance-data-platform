# delivery_packages 交付包索引

语言 / Language: [中文](README_zh.md) | [English](README_en.md)

本目录保存 P8 交付包。当前正式入口是：

```text
p8_delivery_package_20260606_011332/delivery_index.md
```

中文说明文件统一命名为 `*.zh.md`，英文原文保留不覆盖。

## 包版本

| 目录 | 状态 | 说明 |
| --- | --- | --- |
| `p8_delivery_package_20260606_010634/` | 历史包 | 早期交付包，保留追溯 |
| `p8_delivery_package_20260606_011100/` | 历史包 | 早期交付包，保留追溯 |
| `p8_delivery_package_20260606_011332/` | 当前正式包 | 当前 P8 delivery package 入口 |

## 当前正式包阅读顺序

| 顺序 | 英文原文 | 中文说明 | 用途 |
| --- | --- | --- | --- |
| 1 | `delivery_index.md` | `delivery_index.zh.md` | 总索引 |
| 2 | `package_summary.md` | `package_summary.zh.md` | 包摘要和边界 |
| 3 | `project_overview.md` | `project_overview.zh.md` | 项目概览 |
| 4 | `acceptance_results.md` | `acceptance_results.zh.md` | 验收结果 |
| 5 | `metrics_and_queries.md` | `metrics_and_queries.zh.md` | 指标和查询 |
| 6 | `run_order.md` | `run_order.zh.md` | 复现顺序 |
| 7 | `realtime_demo_steps.md` | `realtime_demo_steps.zh.md` | 实时 demo 步骤 |
| 8 | `troubleshooting_entry.md` | `troubleshooting_entry.zh.md` | 排错入口 |

## 复现提示

交付包只保存证据、索引和说明，不复制原始 CSV、HDFS 数据或大 Parquet 文件。复现时应回到项目根目录执行：

```bash
cd /home/common/tmp/pycharm_Design
bin/p7_ops_snapshot.sh
bin/p10_p9_master_validation.sh --mode standard --allow-swapoff --realtime-max-events 1000 --realtime-wait-seconds 60 --query-timeout 300
```

如果只想快速确认环境状态，可以先运行：

```bash
bin/p10_p9_master_validation.sh --mode smoke
```

`smoke` 不能替代正式验收。
