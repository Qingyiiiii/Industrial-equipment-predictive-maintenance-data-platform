# MetroPT-3 FastAPI Demo

Language / 语言: [中文](README_zh.md) | [English](README_en.md)

This directory provides a lightweight FastAPI demo showing how the portfolio can be extended into a query service.

Run:

```bash
pip install -r requirements.txt
```

```bash
uvicorn api.metropt_portfolio_api:app --host 0.0.0.0 --port 8000
```

## Endpoints

| Endpoint | Purpose |
| --- | --- |
| `/health` | Returns service health and project boundary information |
| `/risk/latest` | Returns a local latest-risk sample; a real cluster version should read Redis key `metropt_quality:risk:latest:compressor_1` |
| `/project/summary` | Returns project phase, evidence, and reading entry points |

## Boundary

- This is a portfolio API demo, not a production service.
- It does not connect to Redis, Hive, Trino, or Doris by default.
- It shows one possible service-oriented extension of the project.
- If `fastapi` or `uvicorn` is missing in the current Python environment, install dependencies from the project root `requirements.txt`.

