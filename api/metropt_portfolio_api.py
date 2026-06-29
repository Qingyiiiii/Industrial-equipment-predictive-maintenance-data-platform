# -*- coding: utf-8 -*-
"""FastAPI demo for the MetroPT-3 portfolio."""
from pathlib import Path
from typing import Dict

from fastapi import FastAPI


ROOT = Path(__file__).resolve().parents[1]

app = FastAPI(
    title="MetroPT-3 Portfolio API",
    version="0.1.0",
    description="Lightweight API demo for the MetroPT-3 predictive-maintenance portfolio.",
)


@app.get("/health")
def health() -> Dict:
    return {
        "status": "ok",
        "project": "MetroPT-3 industrial predictive-maintenance data platform",
        "boundary": "portfolio demo; not a production serving system",
    }


@app.get("/project/summary")
def project_summary() -> Dict:
    return {
        "domain": "metropt_quality",
        "main_readme": str(ROOT / "README.md"),
        "validation_checklist": str(ROOT / "MetroPT-3虚拟机测试执行清单.md"),
        "validation_boundary": "latest documented standard P14 validation is PASS with pass=18 warn=0 skip=0 fail=0",
        "key_capabilities": [
            "offline lakehouse pipeline",
            "Kafka/Flink realtime pipeline",
            "weak-label baseline modeling",
            "Trino/Doris query validation",
            "BI and delivery documentation",
        ],
    }


@app.get("/risk/latest")
def latest_risk() -> Dict:
    return {
        "equipment_id": "compressor_1",
        "risk_score": 0.5636,
        "risk_level": "high",
        "risk_reason": ["portfolio_demo_sample"],
        "risk_score_source": "demo_static_sample_not_production",
        "real_source": "cluster Redis key metropt_quality:risk:latest:compressor_1",
    }
