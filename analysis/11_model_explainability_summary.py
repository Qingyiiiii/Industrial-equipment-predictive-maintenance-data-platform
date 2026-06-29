# -*- coding: utf-8 -*-
"""Build a lightweight model explainability summary from existing P9/P10 artifacts."""
# 学习导读：
# - 链路位置：P11 解释性摘要脚本，位于 P9/P10 模型产物之后。
# - 主要输入：已有 logistic weights、metrics 和 P10 comparison 文件。
# - 主要输出：p11_model_explainability_summary.json/md。
# - 核心概念：本脚本只汇总解释材料，不训练新模型，也不改变模型指标。
# - 边界提醒：解释性摘要帮助讲清 baseline，但不能把 weak-label baseline 包装成生产模型。
import csv
import json
from pathlib import Path
from typing import Any, Dict, List

from analysis_common import MODEL_DIR, REPORT_DIR, ensure_analysis_dirs, write_json, write_markdown
from p9_common import relative_path


WEIGHTS = MODEL_DIR / "p9_logistic_feature_weights.tsv"
METRICS = MODEL_DIR / "p9_model_metrics.json"
COMPARISON = MODEL_DIR / "p10_model_metric_comparison.tsv"
OUTPUT_JSON = MODEL_DIR / "p11_model_explainability_summary.json"
OUTPUT_MD = REPORT_DIR / "p11_model_explainability_summary.md"


def _read_tsv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _top_weights(rows: List[Dict[str, str]], limit: int = 15) -> List[Dict[str, Any]]:
    # 只取绝对值最大的 logistic weights，帮助讲解“哪些特征最影响 baseline 分数”。
    out = []
    for row in rows:
        try:
            coef = float(row.get("coefficient", "") or 0.0)
            abs_coef = float(row.get("abs_coefficient", "") or abs(coef))
        except ValueError:
            continue
        out.append({"feature": row.get("feature", ""), "coefficient": coef, "abs_coefficient": abs_coef})
    return sorted(out, key=lambda item: item["abs_coefficient"], reverse=True)[:limit]


def _markdown(payload: Dict[str, Any]) -> str:
    weights = "\n".join(
        f"| `{row['feature']}` | {row['coefficient']:.8f} | {row['abs_coefficient']:.8f} |"
        for row in payload["top_logistic_features"]
    )
    model_lines = []
    for name, item in payload.get("models", {}).items():
        if item.get("status") != "trained":
            model_lines.append(f"- `{name}`: skipped, {item.get('reason', 'no reason recorded')}")
            continue
        metrics = item.get("test_metrics", {})
        model_lines.append(
            f"- `{name}`: precision `{metrics.get('precision')}`, recall `{metrics.get('recall')}`, "
            f"F1 `{metrics.get('f1')}`, false alarms/day `{metrics.get('false_alarms_per_day')}`"
        )
    return f"""# P11 Model Explainability Summary

## Scope

- Metrics source: `{relative_path(METRICS)}`.
- Logistic weights source: `{relative_path(WEIGHTS)}`.
- P10 comparison source: `{relative_path(COMPARISON)}`.

This report explains existing baseline artifacts. It does not train a new model and does not promote the realtime signal proxy into a production ML model.

## Model Metrics

{chr(10).join(model_lines)}

## Top Logistic Features

| Feature | Coefficient | Abs coefficient |
| --- | ---: | ---: |
{weights}

## Interpretation Notes

- Positive coefficients increase the weak-label `pre_failure_24h` score in the numpy logistic baseline.
- Feature weights are meaningful only under the current chronological split and weak-label definition.
- Lead time is reported only for `numpy_logistic_regression`; RF/IF metrics do not carry lead-time evidence in this run.
- Any future RUL or anomaly-detection extension must keep `failure_window`, `pre_failure_*`, `post_maintenance`, `normal_candidate`, and `rul_seconds` out of model features.
"""


def main() -> None:
    ensure_analysis_dirs()
    # 解释性摘要只读取既有产物；如果上游 metrics/weights 缺失，应回到 P9/P10 生成，而不是在这里补训练。
    metrics = _read_json(METRICS)
    weights = _top_weights(_read_tsv(WEIGHTS))
    comparison_rows = _read_tsv(COMPARISON)
    payload = {
        # payload 同时保留来源路径和摘要值，便于报告读者追溯到原始 metrics/weights 文件。
        "metrics_source": relative_path(METRICS),
        "weights_source": relative_path(WEIGHTS),
        "comparison_source": relative_path(COMPARISON),
        "target": metrics.get("target"),
        "feature_count": metrics.get("feature_count"),
        "models": metrics.get("models", {}),
        "lead_time": metrics.get("lead_time", {}),
        "top_logistic_features": weights,
        "comparison_row_count": len(comparison_rows),
    }
    write_json(OUTPUT_JSON, payload)
    write_markdown(OUTPUT_MD, _markdown(payload))
    print("Model explainability summary built.")
    print("json:", OUTPUT_JSON)
    print("report:", OUTPUT_MD)


if __name__ == "__main__":
    main()
