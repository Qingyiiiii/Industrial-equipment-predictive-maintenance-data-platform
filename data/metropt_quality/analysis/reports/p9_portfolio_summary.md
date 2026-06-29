# P9 Portfolio Summary

## Scope

This document is the BI / QA / documentation entry for P9: `数据理解、标签体系与建模基线`.

Sources used:

- `data/metropt_quality/analysis/reports/p9_sensor_dictionary.md`
- `data/metropt_quality/analysis/reports/p9_label_system.md`
- `data/metropt_quality/analysis/reports/p9_eda_report.md`
- `data/metropt_quality/analysis/reports/p9_feature_dictionary.md`
- `data/metropt_quality/analysis/reports/p9_model_baseline_report.md`
- `data/metropt_quality/analysis/reports/p9_feature_quality_report.md`
- `data/metropt_quality/analysis/reports/p9_*.md`

P9 outputs were originally local analysis artifacts. They do not replace P8 final delivery evidence. Later cluster validation work has closed the main productionization gaps: P10 rebuilt warehouse-derived features and model baselines, P11 validated online Flink signal-proxy risk fields, P12 validated Trino / Doris P9 query samples, and P13 packaged the BI dashboard material. Current BI material entry: `data/metropt_quality/analysis/reports/p13_bi_dashboard_portfolio.md`.

## Portfolio Narrative

The portfolio story should be presented as a staged predictive-maintenance platform:

1. P0-P8 establish the data platform foundation: offline chain, realtime demo, query layer, operations snapshot, and delivery package.
2. P9 extends the project from platform delivery into equipment behavior understanding.
3. Sensor dictionary and label system clarify what can and cannot be modeled.
4. Deep EDA explains pressure, current, oil-temperature, and digital-state behavior around reported failure windows.
5. Feature engineering creates right-aligned multi-scale features for early-warning experiments.
6. Baseline models provide measurable but limited early-warning evidence.
7. QA documents keep weak labels, time split, leakage, and cluster validation boundaries explicit.

The model must be described as a baseline only. It is not a production predictive-maintenance model.

## Evidence Map

| Theme | Portfolio message | Evidence | Validation boundary |
| --- | --- | --- | --- |
| Platform foundation | P0-P8 have a reproducible delivery package and fixed validation entry points. | `data/metropt_quality/delivery_packages/p8_delivery_package_20260606_011332/delivery_index.md` | Historical master evidence; do not overwrite in P9. |
| Sensor semantics | 15 signals are separated into 7 analog and 8 digital sensors. | `p9_sensor_dictionary.md` | P9 was local; current BI field semantics are consolidated in `p13_bi_field_semantics.md`. |
| Label honesty | Failure and pre-failure labels are derived from configured intervals, not row-level ground truth. | `p9_label_system.md` | Cluster validation must confirm config parity before accepting counts. |
| Data behavior | Failure windows show oil-temperature, pressure, current, and state-machine contrasts. | `p9_eda_report.md`, `p9_*.png` figures | CSV-derived analysis remains historical P9 evidence; P10 warehouse-derived parity is now accepted. |
| Feature design | Features use right-aligned windows and exclude label/RUL fields from model inputs. | `p9_feature_dictionary.md`, `p9_feature_quality_report.md` | P10 warehouse-derived feature table is the current accepted feature source for follow-up modeling. |
| Baseline model | Chronological split and baseline metrics are reported with precision, recall, F1, PR-AUC, false alarms/day, and lead time. | `p9_model_baseline_report.md`, `p9_model_metrics.json` | Random Forest and Isolation Forest are explicitly recorded as skipped with environment-specific reasons. |
| QA boundary | Local checks are separated from cluster checks. | `p9_qa_checklist.md`, `p9_master_validation_checklist.md` | Cluster validation must record final pass/fail/skip evidence. |

## Dashboard Structure

| Page | Purpose | Core visuals | Main fields |
| --- | --- | --- | --- |
| Overview | Show platform scope, time range, sample volume, failure-window locations, and evidence status. | Timeline, KPI strip, failure-window markers | `event_time`, `dt`, `sample_count`, `failure_window`, `failure_window_rate` |
| Sensor Behavior | Compare analog and digital sensor behavior by state and failure context. | Sensor trend, distribution, correlation heatmap, volatility ranking | `tp2`, `tp3`, `reservoirs`, `oil_temperature`, `motor_current`, `operating_state` |
| Failure Context | Explain what changes before and inside configured failure windows. | Pre-failure delta chart, fault timeline, state-transition frequency | `pre_failure_1h`, `pre_failure_6h`, `pre_failure_24h`, `post_maintenance` |
| Feature Quality | Show feature families and leakage controls. | Feature-group matrix, quality checklist, artifact status | `roll*_`, `active_count_*`, `toggle_count_*`, `state_transition_count`, `rul_seconds` |
| Baseline Model | Present baseline metrics and risk timeline without overclaiming. | Confusion matrix, PR summary, feature weights, risk-score timeline | `model_name`, `precision`, `recall`, `f1`, `pr_auc`, `false_alarms_per_day`, `risk_score` |
| Cluster Validation | Separate local evidence from cluster evidence. | Status table and command checklist | `validation_level`, `status`, `run_id`, `evidence_path`, `pending_item` |

## Recommended Wording

Use:

- "P9 local baseline"
- "configured failure-window weak labels"
- "chronological train/validation/test split"
- "P9 local evidence; see P10/P11/P12/P13 cluster follow-up"
- "high recall with high false alarms"

Avoid:

- "production predictive-maintenance model"
- "row-level true fault labels"
- "cluster passed" unless master writes final validation evidence
- "Hive/Trino/Doris query passed" unless citing P12 evidence
- "accuracy proves model quality"
