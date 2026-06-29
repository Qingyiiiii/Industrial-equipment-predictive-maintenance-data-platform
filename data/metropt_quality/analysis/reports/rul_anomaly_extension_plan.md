# RUL and Anomaly Detection Extension Plan

## Scope

本文档说明如何把当前 P9/P10 weak-label baseline 扩展到 RUL regression 和 anomaly detection，同时不夸大当前已经完成的证据。

## RUL Baseline

目标：

- 使用配置中的 failure starts 生成 `rul_seconds`。
- 最后一个已知 failure horizon 之后的空值应按 excluded 或 censored 处理，不能当作真实无限寿命。

候选特征：

- 复用当前 P10 warehouse-derived minute features。
- 必须排除 `failure_window`、`pre_failure_*`、`post_maintenance`、`normal_candidate` 和 `rul_seconds`，避免 label leakage。

baseline models：

- `RandomForestRegressor`.
- 如果依赖可用，可补 gradient boosting regressor。
- 增加简单 quantile/bin baseline，方便解释。

metrics：

- MAE / RMSE，单位换算为 hours。
- 按 failure window 分组看误差。
- 按 predicted RUL range 做 calibration buckets。

## Anomaly Detection Baseline

训练数据：

- 只使用 early train period 中 `normal_candidate == 1` 的样本。
- 排除 failure window 和 post-maintenance window。

candidate models：

- Robust MAD distance.
- IsolationForest.
- AutoEncoder 只作为后续可选扩展，不进入当前默认路线。

metrics：

- `pre_failure_24h` 内的 detection rate。
- false alarms per day。
- 每个 configured failure 前后的 score timeline。

## Required Boundary

- 这些扩展只是 portfolio enhancements。
- 在重新运行并形成验收证据前，不能替代当前 P14 acceptance boundary。
- 必须保留 chronological split 和 weak-label caveats。
