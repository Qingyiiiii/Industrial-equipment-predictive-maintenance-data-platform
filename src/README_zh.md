# src 离线链路说明

语言 / Language: [中文](README_zh.md) | [English](README_en.md)

`src/` 是 MetroPT-3 离线湖仓主链路目录，负责把原始 CSV 处理成 ODS、DWD、DWS、Hive/Iceberg 表和 BI views。

## 执行顺序

推荐优先使用统一入口：

```bash
cd /home/common/tmp/pycharm_Design
export METROPT_CONFIG=/home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml
python src/run_metropt_offline.py
```

只跑到 DWS：

```bash
python src/run_metropt_offline.py --stop-after 04_metropt_kpi_calc.py
```

从某一步继续：

```bash
python src/run_metropt_offline.py --start-at 03_metropt_dwd_sensor_long.py
```

输出日志在：

```text
data/metropt_quality/logs/<run_id>/
```

先看 `offline_run_summary.tsv`，再看失败步骤对应的 `.log`。

## 文件说明

| 文件 | 作用 | 输入 | 输出 |
| --- | --- | --- | --- |
| `00_metropt_preflight.py` | 运行前预检，确认配置、CSV、字段、HDFS 路径和 Spark 可读性 | `METROPT_CONFIG`、Raw CSV | 控制台检查结果，失败时阻断下游 |
| `01_metropt_profile.py` | 对原始 CSV 做 profile 和基础质量检查 | HDFS Raw CSV | profile 报告、行数、字段、时间范围 |
| `02_metropt_csv_to_parquet.py` | 将 CSV 标准化为 ODS Parquet | Raw CSV | `ods/readings` |
| `03_metropt_dwd_sensor_long.py` | 将宽表传感器展开为长表 | ODS Parquet | `dwd/sensor_long` |
| `04_metropt_kpi_calc.py` | 计算离线 DWS KPI | ODS/DWD | `dws/overall_kpi`、`dws/window_kpi`、`dws/sensor_kpi` |
| `05_metropt_to_hive_iceberg.py` | 发布 ODS/DWD/DWS 到 Hive 和可选 Iceberg | Parquet 数据集 | Hive 表、Iceberg 表 |
| `06_metropt_hive_views.py` | 创建 BI-friendly Hive views | Hive DWS 表 | BI views |
| `run_metropt_offline.py` | 串联 `00 -> 06`，记录每步日志和 return code | 以上脚本 | `offline_run_summary.tsv` 和每步 log |
| `metropt_utils.py` | 公共函数：配置读取、Spark session、字段标准化、路径解析 | 被其他脚本引用 | 不单独运行 |

## 核心数据分层

| 层 | 含义 | 典型路径 |
| --- | --- | --- |
| Raw | 原始 CSV | `hdfs:///lakehouse/projects/metropt_quality/raw/MetroPT3_AirCompressor.csv` |
| ODS | 标准化读数宽表 | `hdfs:///lakehouse/projects/metropt_quality/ods/readings` |
| DWD | 传感器长表 | `hdfs:///lakehouse/projects/metropt_quality/dwd/sensor_long` |
| DWS | KPI 汇总层 | `hdfs:///lakehouse/projects/metropt_quality/dws/*` |
| Hive/Iceberg | 查询发布层 | `metropt_quality.*`、`metropt_quality_iceberg.*` |

## 怎么看结果

1. 看 `offline_run_summary.tsv` 是否所有步骤 `return_code=0`。
2. 看 ODS 行数是否约为 `1,516,948`。
3. 看 DWD 行数是否约为 `22,754,220`。
4. 看 DWS sensor KPI 是否有 15 个传感器。
5. 看 Hive 表是否可查询。

## 常见问题

| 现象 | 处理方式 |
| --- | --- |
| 找不到 CSV | 检查 `METROPT_CONFIG` 指向的 raw 路径，确认 HDFS 已上传 |
| Spark 失败 | 先看该步骤 `.log`，再看 YARN application log |
| Hive COUNT 慢或 JDK 报错 | 使用 `bin/metropt_hive_mr_count_check.sh --mode offline` |
| 跑到一半失败 | 不要直接跳下游；修复后用 `--start-at` 从失败步骤继续 |
| 字段名不一致 | 先跑 `00_metropt_preflight.py`，它会检查关键字段和历史拼写差异 |
