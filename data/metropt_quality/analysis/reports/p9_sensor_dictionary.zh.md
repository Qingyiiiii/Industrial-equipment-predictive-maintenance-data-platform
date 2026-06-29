# P9 sensor dictionary（中文版）

原英文文件：`data/metropt_quality/analysis/reports/p9_sensor_dictionary.md`

## 这份文档是什么

P9 传感器字典，说明模拟信号、数字信号和业务含义。

## 输入是什么

MetroPT data description、CSV columns。

## 输出是什么

sensor dictionary 和建模边界。

## 怎么看

先看 Sensor Fields，再看 Modeling Boundary。

## 关键术语

- `P9`
- `sensor dictionary`
- `analog signal`
- `digital signal`
- `modeling boundary`

## 证据边界

字段解释用于分析和 BI，不代表新增业务标签。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。
