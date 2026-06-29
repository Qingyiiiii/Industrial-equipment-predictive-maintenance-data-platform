# P9/P11 legacy status overlay（中文版）

原英文文件：`data/metropt_quality/analysis/reports/p9_p11_legacy_status_overlay_20260608.md`

## 这份文档是什么

完善阶段旧 pending 覆盖表，说明 P9/P11 历史待办如何被 P10/P12/P14 关闭。

## 输入是什么

P9/P11 historical reports、P10/P12/P14 evidence。

## 输出是什么

旧状态到当前状态的映射表。

## 怎么看

先看每条 pending 的 current status 和 evidence path。

## 关键术语

- `legacy status`
- `pending`
- `P10`
- `P12`
- `P14`
- `PASS_WITH_WARNINGS`

## 证据边界

这是文档漂移修正入口，不是新增业务验收。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。
