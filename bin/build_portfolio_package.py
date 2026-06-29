# -*- coding: utf-8 -*-
"""Build a compact portfolio package from current docs and report evidence."""
import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = ROOT / "data" / "metropt_quality" / "delivery_packages"

DOCS = [
    "README.md",
    "README_zh.md",
    "README_en.md",
    "项目接口文档.md",
    "通用大数据流程配置.md",
    "MetroPT-3虚拟机测试执行清单.md",
    "src/README_zh.md",
    "src/README_en.md",
    "analysis/README_zh.md",
    "analysis/README_en.md",
    "streaming/README_zh.md",
    "streaming/README_en.md",
    "bin/README_zh.md",
    "bin/README_en.md",
    "data/metropt_quality/README_zh.md",
    "data/metropt_quality/README_en.md",
    "data/metropt_quality/analysis/reports/README.md",
    "data/metropt_quality/analysis/reports/README_zh.md",
    "data/metropt_quality/analysis/reports/README_en.md",
    "data/metropt_quality/delivery_packages/README.md",
    "data/metropt_quality/delivery_packages/README_zh.md",
    "data/metropt_quality/delivery_packages/README_en.md",
    "tests/README_zh.md",
    "tests/README_en.md",
    "api/README_zh.md",
    "api/README_en.md",
    "api/metropt_portfolio_api.py",
]

REPORTS = [
    "data/metropt_quality/analysis/reports/p9_phase_closure_20260607.zh.md",
    "data/metropt_quality/analysis/reports/p9_model_baseline_report.zh.md",
    "data/metropt_quality/analysis/reports/p10_model_baseline_comparison_report.zh.md",
    "data/metropt_quality/analysis/reports/p11_realtime_risk_scoring_validation_report.zh.md",
    "data/metropt_quality/analysis/reports/p12_query_layer_validation_report.zh.md",
    "data/metropt_quality/analysis/reports/p13_bi_dashboard_portfolio.zh.md",
    "data/metropt_quality/analysis/reports/p14_master_validation_report_20260607_054200.zh.md",
    "data/metropt_quality/analysis/reports/p14_master_validation_report_20260609_020821.zh.md",
    "data/metropt_quality/analysis/reports/p14_master_validation_report_20260609_020821.md",
    "data/metropt_quality/analysis/reports/p14_summary_20260609_020821.tsv",
    "data/metropt_quality/analysis/reports/p14_master_validation_steps_20260609_020821.tsv",
    "data/metropt_quality/analysis/reports/p14_hive_results_20260609_020821.tsv",
    "data/metropt_quality/analysis/reports/p11_model_explainability_summary.md",
    "data/metropt_quality/analysis/reports/rul_anomaly_extension_plan.md",
]


def copy_file(package_dir: Path, rel: str, group: str, rows: list) -> None:
    src = ROOT / rel
    if not src.exists():
        rows.append({"group": group, "path": rel, "status": "MISSING", "bytes": 0})
        return
    dst = package_dir / group / rel.replace("/", "__").replace("\\", "__")
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    rows.append({"group": group, "path": rel, "status": "COPIED", "bytes": src.stat().st_size, "package_file": str(dst.relative_to(package_dir)).replace("\\", "/")})


def write_index(package_dir: Path, rows: list, run_id: str) -> None:
    copied = [row for row in rows if row["status"] == "COPIED"]
    missing = [row for row in rows if row["status"] != "COPIED"]
    lines = [
        "# MetroPT-3 Portfolio Final Package",
        "",
        f"- Build time: `{run_id}`.",
        "- Purpose: compact portfolio package for reading, review, and reproduction.",
        "- Boundary: no raw CSV, Parquet, logs, jars, or service data are copied.",
        "",
        "## Contents",
        "",
        "| Group | Source | Package file | Bytes |",
        "| --- | --- | --- | ---: |",
    ]
    for row in copied:
        lines.append(f"| {row['group']} | `{row['path']}` | `{row['package_file']}` | {row['bytes']} |")
    if missing:
        lines.extend(["", "## Missing References", "", "| Group | Source |", "| --- | --- |"])
        for row in missing:
            lines.append(f"| {row['group']} | `{row['path']}` |")
    (package_dir / "delivery_index.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--package-name", default="")
    args = parser.parse_args()

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    package_name = args.package_name or f"portfolio_final_{run_id}"
    package_dir = Path(args.output_root) / package_name
    package_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for rel in DOCS:
        copy_file(package_dir, rel, "docs", rows)
    for rel in REPORTS:
        copy_file(package_dir, rel, "reports", rows)

    manifest = package_dir / "manifest.json"
    manifest.write_text(json.dumps({"run_id": run_id, "rows": rows}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_index(package_dir, rows, run_id)
    print("Portfolio package built")
    print("package_dir:", package_dir)
    print("index:", package_dir / "delivery_index.md")


if __name__ == "__main__":
    main()
