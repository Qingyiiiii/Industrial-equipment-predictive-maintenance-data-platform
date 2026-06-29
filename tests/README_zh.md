# 轻量测试说明

语言 / Language: [中文](README_zh.md) | [English](README_en.md)

本目录放置不依赖 Spark/Flink/Kafka/Redis/Hive 的本地测试，用于快速验证项目核心口径没有漂移。

运行入口：

```powershell
python bin/local_code_quality_check.py
```

检查范围：

- Python 文件 AST 语法检查。
- `src/metropt_utils.py` 中的配置、路径、failure window、字段映射 helper。
- `streaming/metropt_realtime_risk_score_plan.py` 中的 Kafka JSON contract、DLQ 错误分类和 `risk_score` signal-proxy 边界。
- `config/metropt_quality.local.yaml` 的关键配置项。

不检查：

- 不启动 Spark。
- 不连接 Kafka/Flink/Redis/Hive。
- 不生成 Parquet、图片或模型文件。

