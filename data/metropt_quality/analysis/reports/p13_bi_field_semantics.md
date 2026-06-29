# P13 BI Field Semantics

生成时间：2026-06-07

## 目的

本文档定义 P13 BI 看板字段口径，供 Power BI / Superset / Doris SQL / Trino SQL / 静态作品集页面使用。字段来源分为 Hive、Trino、Doris、P9 analysis、P10 analysis、P11 realtime evidence。不同来源不能混写成同一个验收等级。

## 来源等级

| 来源等级 | 说明 | 可展示口径 |
| --- | --- | --- |
| Hive | accepted ODS/DWD/DWS 和 BI views | 离线主链路 canonical 口径 |
| Trino / Iceberg | P12 已复验的 Iceberg 查询层 | 查询层复验口径 |
| Doris | P12 已装载 / 映射的 OLAP 查询层 | 看板加速和样例查询口径 |
| P9 analysis | local / cluster analysis artifact | 分析和特征解释口径 |
| P10 analysis | warehouse-derived model baseline artifact | 模型 baseline 对比口径 |
| P11 realtime evidence | Kafka/Flink/Redis/Hive 风险字段验收 | 在线 signal-proxy 风险证据 |

## 页面字段

### 1. 总体健康

| 字段 | 含义 | 来源 | 页面用法 | 边界 |
| --- | --- | --- | --- | --- |
| `ods_rows` | ODS timestamp row count | Trino `ods_metropt_readings` | 数据规模 KPI | 与数据说明中的 15,169,480 data points 不是同一概念 |
| `rows_in_long_table` | DWD sensor-long 行数 | Trino `dwd_metropt_sensor_long` | 传感器长表覆盖 KPI | 行数等于 timestamp rows 乘传感器数 |
| `window_rows` | DWS window KPI 行数 | Trino / Hive `dws_metropt_window_kpi` | 1-minute window 规模 KPI | 不等同于原始 CSV 行数 |
| `dt` | 日期分区 | Hive / Doris | 日期筛选、状态矩阵 | 日期粒度，不是秒级时间 |
| `operating_state` | derived compressor state | Hive DWS / P9 features | 状态分布 | 由 `motor_current` 阈值推导 |
| `sample_count` | 聚合组内样本数 | Hive / Doris DWS | 数据密度 | 不是设备数量 |

### 2. 传感器风险

| 字段 | 含义 | 来源 | 页面用法 | 边界 |
| --- | --- | --- | --- | --- |
| `sensor_name` | 标准传感器字段名 | Hive / Doris / P9 sensor dictionary | 传感器筛选和排行 | 原始 `DV_eletric` 展示时使用标准名 `dv_electric` |
| `sensor_type` | `analog` 或 `digital` | DWS sensor KPI / P9 sensor dictionary | 类型分组 | 数字信号不能当连续值解释 |
| `unit` | 单位或 0/1 | Hive BI view / sensor dictionary | 字段说明 | 数字信号单位显示为 binary |
| `avg_sensor_value` | 平均传感器值 | Hive / Doris sensor KPI | 趋势和对比 | 不代表故障因果 |
| `std_sensor_value` | 标准差 | Hive / Doris sensor KPI | 波动分析 | 数字信号的标准差只表示激活变化 |
| `failure_window_rate` | 故障窗口样本占比 | Hive / Doris DWS | 风险排序 | 弱标签统计，不是人工标注概率 |

### 3. 故障窗口

| 字段 | 含义 | 来源 | 页面用法 | 边界 |
| --- | --- | --- | --- | --- |
| `failure_window` | 配置故障区间内样本标记 | P9 label builder / DWS aggregation | 故障窗口标记 | 弱标签，不是逐行真实故障标签 |
| `pre_failure_1h` | 故障开始前 1 小时窗口 | P9 label builder | 预警上下文 | 弱标签 |
| `pre_failure_6h` | 故障开始前 6 小时窗口 | P9 label builder | 预警上下文 | 弱标签 |
| `pre_failure_24h` | 故障开始前 24 小时窗口 | P9 / P10 target | 模型 baseline target | target 字段，不得进入模型特征 |
| `post_maintenance` | 故障结束后恢复窗口 | P9 label builder | 恢复期说明 | 经验窗口 |
| `positive_rate` | 标签正例占比 | P9 label summary | 标签规模图 | 只描述当前配置规则 |

### 4. 模型表现

| 字段 | 含义 | 来源 | 页面用法 | 边界 |
| --- | --- | --- | --- | --- |
| `source_type` | `csv_derived` / `warehouse_derived` | P10 model comparison | 来源对比 | warehouse-derived 是当前官方 baseline 来源 |
| `model_name` | 模型名称 | P10 metrics | 模型维度 | baseline，不是生产模型 |
| `precision` | 查准率 | P10 metrics | 指标卡 | 弱标签下的实验指标 |
| `recall` | 召回率 | P10 metrics | 指标卡 | 弱标签下的实验指标 |
| `f1` | F1 score | P10 metrics | 指标卡 | 不单独代表可上线 |
| `pr_auc` | Precision-Recall AUC | P10 metrics | 模型比较 | 比 accuracy 更适合稀疏正例 |
| `false_alarms_per_day` | 每天误报负担 | P10 metrics | 可用性说明 | 需要业务阈值再评审 |
| `lead_time_model` | lead time 归属模型 | P10 metrics | lead time 图脚注 | 只允许是 `numpy_logistic_regression` |
| `mean_lead_time_hours` | 平均提前量 | P10 metrics | lead time 摘要 | 不适用于 RF/IF/anomaly score |

### 5. 实时风险

| 字段 | 含义 | 来源 | 页面用法 | 边界 |
| --- | --- | --- | --- | --- |
| `risk_score` | 在线风险分数 | P11 Flink signal-proxy | 风险仪表盘 | 非生产 ML model score |
| `risk_level` | 风险等级 | P11 Flink signal-proxy | 状态标签 | 阈值来自 signal-proxy 合同 |
| `risk_reason` | 风险原因 | P11 Flink signal-proxy | 解释字段 | 规则 / 信号代理解释 |
| `model_version` | 输出版本 | P11 risk event | 版本追踪 | 当前为 `p11_flink_signal_proxy_20260607` |
| `risk_score_source` | 分数来源 | P11 risk event / Redis | 页面脚注 | 必须展示 `flink_signal_proxy_not_production_model` |
| `model_feature_set_version` | 实时特征集合版本 | P11 risk event / Redis | 追踪字段 | 原始事件 signal-proxy 版本 |

## 禁用表达

| 禁用表达 | 替代表达 |
| --- | --- |
| 真实故障标签 | 配置故障窗口弱标签 |
| 人工标注故障行 | 根据故障时间区间推导的样本标记 |
| 生产预测维护模型 | warehouse-derived baseline / production-candidate evidence |
| 在线 ML 模型已上线 | Flink signal-proxy scorer 已在线生成风险字段 |
| Trino/Doris BI 已上线 | P12 已完成 P9 SQL 查询层复验 |

## 字段验收规则

- 每个图表必须显示字段来源：Hive / Trino / Doris / P9 analysis / P10 analysis / P11 realtime evidence。
- 标签字段只能作为 target、grouping、filter 或解释字段，不能作为模型输入字段展示。
- BI 文案必须保留弱标签、baseline、signal-proxy 和生产化边界。
- 后续新增字段必须先补 `p9_dashboard_field_dictionary.md` 或本文档，再进入看板页面。
