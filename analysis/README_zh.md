# analysis 分析建模说明

语言 / Language: [中文](README_zh.md) | [English](README_en.md)

`analysis/` 负责 MetroPT-3 的数据质量分析、多维 EDA、弱标签、特征工程、baseline model、P9/P10 生产化对齐和报告生成。

## 推荐运行方式

先检查输入：

```bash
python analysis/00_validate_analysis_inputs.py
```

运行早期分析链路：

```bash
python analysis/run_metropt_analysis.py
```

P9/P10 单步脚本通常由 P14 调用；手工复现时按文件编号执行。

如果只做文档复盘或演示预检，且已有 P10 产物，可以使用 fast path：

```bash
python analysis/08_p10_warehouse_feature_builder.py --reuse-existing
python analysis/09_p10_warehouse_feature_quality_check.py
python analysis/10_p10_warehouse_model_baseline.py --reuse-existing
python analysis/11_model_explainability_summary.py
```

`--reuse-existing` 只复用已有产物，不替代正式 P14 standard 重跑。

输出目录：

```text
data/metropt_quality/analysis/reports/
data/metropt_quality/analysis/figures/
data/metropt_quality/analysis/models/
data/metropt_quality/analysis/logs/
```

## 文件说明

| 文件 | 作用 | 输入 | 输出 |
| --- | --- | --- | --- |
| `analysis_common.py` | 公共工具：路径、配置、Spark、报告写入 | 被其他脚本引用 | 不单独运行 |
| `00_validate_analysis_inputs.py` | 检查 Raw/Profile/ODS/DWD/DWS 等上游输入是否存在 | 配置、数据目录 | `analysis_input_validation*.json` |
| `01_data_quality_analysis.py` | ODS 数据质量分析 | ODS Parquet | `metropt_data_quality_report.md/json`、质量图 |
| `02_multidim_analysis.py` | 多维分析和图表 | ODS/DWD/DWS | `metropt_multidim_analysis_report.md/json`、图表 |
| `03_model_baseline.py` | 基于 DWS window KPI 的早期 baseline model | DWS window KPI | `metropt_baseline_model_report.md`、metrics、图 |
| `p9_common.py` | P9 标签、特征、模型公共函数 | 被 P9/P10 脚本引用 | 不单独运行 |
| `04_p9_label_builder.py` | 生成 sensor dictionary 和 weak label 文档 | CSV / 配置 failure windows | `p9_sensor_dictionary.md`、`p9_label_system.md` |
| `05_p9_feature_engineering.py` | 生成 P9 EDA、minute features 和 feature dictionary | CSV-derived 数据 | `p9_window_features_1min.parquet`、P9 报告和图 |
| `06_p9_model_experiments.py` | 时间切分 baseline 实验 | P9 minute features | `p9_model_metrics.json`、`p9_model_baseline_report.md` |
| `07_p9_feature_quality_check.py` | 不重跑重任务，检查 P9 特征产物质量 | P9 reports/models | `p9_feature_quality_report.md`、checks json |
| `08_p10_warehouse_feature_builder.py` | 从 ODS/DWD/DWS 重建 warehouse-derived P9 features | accepted Parquet | `p9_window_features_1min_warehouse.parquet`、parity report |
| `09_p10_warehouse_feature_quality_check.py` | 检查 warehouse-derived features 和 leakage boundary | P10 feature artifacts | `p10_warehouse_feature_quality_report.md` |
| `10_p10_warehouse_model_baseline.py` | 使用 warehouse-derived features 重跑 baseline 并和 CSV-derived 对比 | P9/P10 features | `p10_model_metric_comparison.*`、模型报告 |
| `11_model_explainability_summary.py` | 从已有 metrics 和 logistic weights 生成解释性摘要，不训练新模型 | P9/P10 model artifacts | `p11_model_explainability_summary.json/md` |
| `run_metropt_analysis.py` | 串联早期 `00 -> 03` 分析任务 | 上游 Parquet | `analysis_run_summary.tsv`、每步 log |

## 关键结果怎么看

| 结果 | 重点 |
| --- | --- |
| `analysis/reports/*.md` | 阅读结论和边界；中文版本为 `*.zh.md` |
| `analysis/figures/*.png` | 看趋势、相关性、模型信号、故障窗口对比 |
| `analysis/models/*.json` | 看 metrics、feature summary、quality checks |
| `analysis/models/*.parquet` | P9/P10 minute feature table，不建议直接手工打开大文件 |
| `analysis/logs/<run_id>/analysis_run_summary.tsv` | 看每个分析步骤 return code |

## 关键指标

| 指标 | 含义 |
| --- | --- |
| `precision` / `recall` / `f1` | baseline classification 指标 |
| `pr_auc` | 弱标签不平衡时比 ROC 更有参考价值 |
| `lead_time` | 预警提前量，必须说明来自哪个 model |
| `false alarms per day` | 误报强度，用于判断可解释性 |
| `risk_score` | 实时 signal-proxy risk scorer 输出，不是生产模型概率 |
| `PASS/WARN/SKIP/FAIL` | 验收状态，不可手工改写 |

## 图表阅读方向

| 图表 | 关注点 |
| --- | --- |
| `daily_sample_count_trend.png` | 每日样本量是否稳定 |
| `sensor_correlation_heatmap.png` | 传感器相关性和冗余 |
| `failure_window_sensor_contrast.png` | 故障窗口前后传感器差异 |
| `baseline_confusion_matrices.png` | baseline model 误判结构 |
| `p9_risk_score_timeline.png` | 风险分数随时间变化 |
| `p9_logistic_feature_weights.png` | Logistic Regression 主要特征方向 |

## 扩展路线

| 文档 | 用途 |
| --- | --- |
| `data/metropt_quality/analysis/reports/p11_model_explainability_summary.md` | 解释当前 weak-label baseline 的指标、feature weights 和 production boundary |
| `data/metropt_quality/analysis/reports/rul_anomaly_extension_plan.md` | 说明 RUL regression 与 anomaly detection 如何在 P10 features 上继续扩展 |

## 常见问题

| 现象 | 处理方式 |
| --- | --- |
| 输入缺失 | 先跑离线 `src/run_metropt_offline.py`，或检查 `00_validate_analysis_inputs.py` 输出 |
| 指标看起来太好 | 检查时间切分和 leakage notes，不能使用随机切分冒充可预测性 |
| RF/IF skipped | 看报告中的 dependency / resource boundary，不要把 skipped 写成失败 |
| P9 与 P10 结果不完全一致 | 看 `p9_feature_parity_report.md`，warehouse-derived 与 CSV-derived 允许有可解释差异 |
| P10 重跑太慢 | 文档复盘可用 `--reuse-existing`，正式验收仍要跑 standard P14 |
| 中文/英文报告不一致 | 英文原文是生成时证据，中文版本用于读者理解；关键验收结论以原文和 run_dir 为准 |
