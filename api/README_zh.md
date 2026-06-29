# MetroPT-3 FastAPI Demo

语言 / Language: [中文](README_zh.md) | [English](README_en.md)

本目录提供一个轻量 FastAPI demo，用于展示作品集可以扩展成查询服务。

运行方式：

```bash
pip install -r requirements.txt
```

```bash
uvicorn api.metropt_portfolio_api:app --host 0.0.0.0 --port 8000
```

接口：

| 接口 | 用途 |
| --- | --- |
| `/health` | 返回服务健康和项目边界 |
| `/risk/latest` | 返回本地样例 latest-risk；真实集群应读取 Redis key `metropt_quality:risk:latest:compressor_1` |
| `/project/summary` | 返回项目阶段、证据和阅读入口 |

边界：

- 这是作品集 API demo，不是生产服务。
- 默认不连接 Redis/Hive/Trino/Doris。
- 用于展示或演示“项目可以如何服务化”。
- 如果当前 Python 环境没有安装 `fastapi` 或 `uvicorn`，先按项目根目录 `requirements.txt` 安装依赖。
