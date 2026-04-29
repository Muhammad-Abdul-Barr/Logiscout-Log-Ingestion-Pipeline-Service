<div align="center">

# LogiScout — Logs Ingestion Pipeline

**Fault-tolerant log ingestion that consumes telemetry from ClickHouse, reconstructs request traces, and stores semantically enriched documents in Qdrant for AI-powered incident resolution.**

[![Python 3.13](https://img.shields.io/badge/Python-3.13-3776AB?logo=python&logoColor=white)](https://python.org)
[![Docker Ready](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](#quick-start)
[![Qdrant](https://img.shields.io/badge/Qdrant-Vector_DB-DC382D?logo=qdrant&logoColor=white)](https://qdrant.tech)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-OLAP-FFCC01?logo=clickhouse&logoColor=black)](https://clickhouse.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

[Quick Start](#quick-start) ·
[Architecture](#how-it-works) ·
[Pipeline Stages](#pipeline-stages) ·
[Gatekeeper](#gatekeeper) ·
[Fault Tolerance](#fault-tolerance) ·
[Configuration](#configuration)

</div>

---

## Overview

The **Logs Ingestion Pipeline** is a core service in the LogiScout incident resolution platform. It continuously polls structured log data from a **ClickHouse** OLAP store, reconstructs full request traces by correlation ID, applies multi-tier intelligent filtering, and stores only _operationally significant_ traces as semantically embedded vectors in **Qdrant**.

If you're familiar with ETL pipelines, this is one — except the **L** (Load) step is a vector database, and a 5-tier gatekeeper sits in the middle to drop ~95% of healthy production traffic before it ever touches Qdrant.

## Highlights

-  **Zero data loss** — watermark-based deduplication with atomic writes and backup recovery.
-  **Noise reduction** — 5-tier gatekeeper drops ~95% of healthy traffic before it reaches Qdrant.
-  **Semantic search** — traces are converted into retrieval-optimized natural-language digests and embedded with `BAAI/bge-small-en-v1.5`.
-  **Self-healing** — inline retry with exponential backoff, circuit breaker, and poison-pill isolation.
-  **Split-trace integrity** — merge-on-duplicate detects partial batches and re-fetches complete trace history.
-  **Lightweight** — FastEmbed (ONNX runtime) replaces PyTorch/CUDA; no JVM, no Spark.
-  **Container-ready** — single `docker compose up` spins up the pipeline + Qdrant.

## Quick Start

### Docker (Recommended)

```bash
# 1. Clone the repository
git clone https://github.com/Muhammad-Abdul-Barr/Logiscout-Log-Ingestion-Pipeline-Service.git
cd Logiscout-Log-Ingestion-Pipeline-Service

# 2. Configure environment
cp .env.example .env
# Edit .env with your ClickHouse credentials and Qdrant URL

# 3. Start services (Qdrant + Pipeline)
docker compose up -d

# 4. View logs
docker compose logs -f pipeline
```

### Local Development

```bash
# 1. Create virtual environment
python -m venv .venv
.venv\Scripts\activate     # Windows
# source .venv/bin/activate  # Linux/macOS

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
# Edit .env — set QDRANT_URL=http://localhost:6333 for local Qdrant

# 4. Run the pipeline
python main.py
```

### Programmatic Usage

```python
from config import Config
from pipeline import LogProcessingPipeline

# Single trace processing
pipeline = LogProcessingPipeline(config=None)
trace_doc = pipeline.process_trace(raw_trace_dict)

# Scheduled batch mode
config = Config.from_env()
pipeline = LogProcessingPipeline(config)
pipeline.run()  # starts the polling loop
```

**Requirements**

| Dependency         | Version   |
| ------------------ | --------- |
| Python             | `>= 3.13` |
| pydantic           | `2.13.3`  |
| fastembed           | `0.8.0`   |
| qdrant-client      | `1.17.1`  |
| clickhouse-connect | `0.15.1`  |
| python-dotenv      | `1.2.2`   |

## How It Works

```
┌──────────────────────────────────────────────────────────────────┐
│                      Scheduled Poll Loop                        │
│                  (every N minutes, drain-to-cutoff)              │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
          ┌────────────────────────┐
          │   ClickHouse Fetcher   │  ← Watermark-based batch fetch
          │   (clickhouse-connect) │  ← Split-trace trimming
          └────────────┬───────────┘  ← Merge-on-duplicate detection
                       │
              Flat OLAP rows → Nested traces
                       │
                       ▼
          ┌────────────────────────┐
          │      Ingestion         │  ← Pydantic schema validation
          └────────────┬───────────┘
                       │
                       ▼
          ┌────────────────────────┐
          │    Transformation      │  ← Flatten, normalize timestamps,
          └────────────┬───────────┘    resolve service names
                       │
                       ▼
          ┌────────────────────────┐
          │      Enrichment        │  ← Fingerprint, severity score,
          └────────────┬───────────┘    outcome, error types, keywords
                       │
                       ▼
          ┌────────────────────────┐
          │  ★ GATEKEEPER ★        │  ← 5-tier promote/drop decision
          │  (promote or drop)     │    (drops ~95% healthy traffic)
          └────────────┬───────────┘
                       │
                  [PROMOTED]
                       │
                       ▼
          ┌────────────────────────┐
          │    Indexing Prep        │  ← Semantic text builder +
          └────────────┬───────────┘    vector metadata assembly
                       │
                       ▼
          ┌────────────────────────┐
          │    Vector Store         │  ← FastEmbed (ONNX, no PyTorch)
          │    (Qdrant)            │  ← Batch upsert with retry
          └────────────────────────┘
```

The pipeline runs as a continuous **drain loop** — every N minutes it snapshots a cutoff timestamp, fetches all rows up to that point in batches, processes them, and advances the watermark. This guarantees every row is processed exactly once (or idempotently reprocessed via fingerprint dedup).

## Pipeline Stages

### 1. ClickHouse Fetcher

Reads flat log rows from ClickHouse via the HTTP-native `clickhouse-connect` driver. Uses a **high-watermark** strategy to avoid reprocessing:

-  **Watermark persistence** — atomic file writes with `.bak` backup for crash recovery.
-  **Split-trace guard** — when a batch hits the row limit, trailing correlation IDs are trimmed to prevent partial processing.
-  **Merge-on-duplicate** — compares batch row counts against ClickHouse totals per CID; re-fetches the complete history when a split is detected.

### 2. Ingestion

Validates raw trace dictionaries against strict **Pydantic** schemas (`RawTrace`, `RawLogEntry`, `RequestInfo`). Normalizes log levels across variants (`WARNING` → `WARN`, `CRITICAL` → `FATAL`).

### 3. Transformation

Flattens the nested trace structure into `TransformedLog` dataclasses:

-  Resolved service names (from `loggerName` → `component` → `projectName`).
-  Normalized ISO timestamps + Unix epoch seconds.
-  Extracted HTTP request context (`method`, `path`, `statusCode`).

### 4. Enrichment

Computes trace-level analytics across all logs sharing a correlation ID:

| Feature             | Description                                                                              |
| ------------------- | ---------------------------------------------------------------------------------------- |
| Fingerprint         | `MD5(sorted_services \| max_level \| normalized_message)` → `TRC-XXXXXXXX`               |
| Severity Score      | 1–10 scale based on max log level, critical keywords, and outcome                        |
| Outcome             | Classified as `success`, `warning`, `client_error`, `server_error`, or `failure`         |
| Error Types         | Regex extraction of exception class names (`TimeoutError`, `NullPointerException`, etc.) |
| Suspicious Keywords | Content scan across ALL log levels for 35+ failure indicators                            |
| Path Normalization  | `/users/42` → `/users/{id}` for endpoint grouping                                        |

### 5. Gatekeeper

See the dedicated [Gatekeeper](#gatekeeper) section below.

### 6. Indexing Prep

Builds a **retrieval-optimized semantic digest** for embedding (~512 tokens max):

```
Layer 1 → Operation + user-provided meta context (front-loaded for retrieval)
Layer 2 → Service flow (compact path, stated once)
Layer 3 → Behavioral digest (deduplicated unique messages)
Layer 4 → Outcome + timing
```

Also assembles 14+ indexed metadata fields for Qdrant's filtered search.

### 7. Vector Store

-  **Embedding** — `BAAI/bge-small-en-v1.5` via [FastEmbed](https://github.com/qdrant/fastembed) (ONNX, no PyTorch).
-  **Storage** — Qdrant with COSINE distance, 384-dimensional vectors.
-  **Collections** — dynamic `{project_id}_logs` per project.
-  **Deduplication** — fingerprint-based; duplicates increment `occurrence_count` instead of creating new points.

## Gatekeeper

The gatekeeper sits between Enrichment and Indexing Prep. It evaluates traces in order of computational cost — cheap deterministic checks first, expensive Qdrant lookups last:

| Tier  | Name               |  Cost  | Signal                                                                                       |
| :---: | ------------------ | :----: | -------------------------------------------------------------------------------------------- |
| **0** | Severity Gate      |  Free  | `severity ≥ 7`, outcome is `failure` / `server_error`, or trace has `ERROR` / `FATAL`        |
| **1** | Content Scan       |  Free  | Error type classes detected, or suspicious keywords in any log level                         |
| **5** | Cold Start         | Qdrant | Endpoint has < N known fingerprints — promote to build a baseline                            |
| **2** | Deploy Window      | Qdrant | Non-success outcome or new fingerprint within 60 min of a commit                             |
| **3** | Fingerprint Change | Qdrant | New fingerprint on a previously stable endpoint                                              |
|   —   | **Default**        |   —    | **DROP** — healthy traffic, no promotion criteria met                                        |

**Result:** ~95% of production traffic (healthy `200 OK` requests) is dropped before reaching the vector store.

## Fault Tolerance

Three resilience layers with **zero external dependencies** (just `json`, `os`, `time`):

### Inline Retry with Exponential Backoff

Each trace gets up to `RETRY_MAX_ATTEMPTS` (default: 3) processing attempts. Backoff: `base × 2^attempt` seconds.

### Circuit Breaker

A three-state machine that pauses the pipeline when systemic failures are detected:

```
CLOSED  →  normal operation
  │ (N consecutive failures)
  ▼
OPEN    →  pipeline sleeps for cooldown period
  │ (cooldown elapsed)
  ▼
HALF-OPEN → probe with one cycle
  │ (success)
  ▼
CLOSED  →  resume normal operation
```

### Poison Pill Tracker

Correlation IDs that fail across multiple cycles are persisted to `.poison_pills.json` and skipped automatically. Supports manual clearing after a fix:

```python
tracker.clear("problematic-correlation-id")
```

### Safe Watermark Advancement

On partial batch failures, the watermark advances only up to the row _before_ the first failed trace. Fingerprint-based deduplication ensures idempotent reprocessing of anything after that point.

## Configuration

All settings are managed through environment variables (`.env`) with sensible defaults in `config.py`.

### Required

| Variable        | Description                   | Example              |
| --------------- | ----------------------------- | -------------------- |
| `OLAP_HOST`     | ClickHouse server hostname/IP | `47.130.208.43`      |
| `OLAP_PORT`     | ClickHouse HTTP port          | `8123`               |
| `OLAP_DATABASE` | ClickHouse database name      | `logging`            |
| `OLAP_TABLE`    | ClickHouse table name         | `logs`               |
| `OLAP_USER`     | ClickHouse username           | `default`            |
| `OLAP_PASSWORD` | ClickHouse password           | `****`               |
| `QDRANT_URL`    | Qdrant connection URL         | `http://qdrant:6333` |

### Optional (with defaults)

| Variable                           | Default      | Description                                              |
| ---------------------------------- | ------------ | -------------------------------------------------------- |
| `FETCH_INTERVAL_MINUTES`           | `5`          | Polling interval between drain cycles                    |
| `FETCH_BATCH_SIZE`                 | `1000`       | Max rows per ClickHouse query                            |
| `WATERMARK_PATH`                   | `.watermark` | File path for watermark persistence                      |
| `SEVERITY_THRESHOLD`               | `7`          | Minimum severity score for Tier 0 promotion              |
| `DEPLOY_WINDOW_MINUTES`            | `60`         | Post-deployment relaxed filtering window                 |
| `COLD_START_BASELINE_COUNT`        | `3`          | Fingerprints needed before change-detection mode         |
| `MAX_SEMANTIC_CHARS`               | `2000`       | Max characters for semantic text (~512 tokens)           |
| `UPSERT_BATCH_SIZE`                | `100`        | Qdrant upsert batch size                                 |
| `RETRY_MAX_ATTEMPTS`               | `3`          | Max retry attempts per trace                             |
| `RETRY_BACKOFF_BASE`               | `1.0`        | Base delay (seconds) for exponential backoff             |
| `CIRCUIT_BREAKER_THRESHOLD`        | `3`          | Failures before circuit breaker trips                    |
| `CIRCUIT_BREAKER_COOLDOWN_MINUTES` | `10`         | Cooldown when circuit breaker is open                    |
| `POISON_PILL_MAX_FAILURES`         | `3`          | Failures before a CID is marked as poison pill           |

## Project Structure

```
.
├── main.py                          # Entry point — starts the scheduled pipeline loop
├── config.py                        # Centralized configuration (dataclass + env vars)
├── pipeline.py                      # Orchestrator — coordinates all pipeline stages
│
├── pipeline_services/
│   ├── __init__.py                  # Public API exports
│   ├── models.py                    # Pydantic schemas (RawTrace, TraceDocument, etc.)
│   ├── ingestion.py                 # Stage 1 — schema validation
│   ├── transformation.py            # Stage 2 — flatten + normalize
│   ├── enrichment.py                # Stage 3 — fingerprint, severity, outcome
│   ├── gatekeeper.py                # Stage 4 — 5-tier promote/drop decision
│   ├── indexing_prep.py             # Stage 5 — semantic text + metadata builder
│   ├── vector_store.py              # Stage 6 — embedding + Qdrant upsert
│   ├── clickhouse_fetcher.py        # ClickHouse batch reader + watermark
│   └── fault_tolerance.py           # Circuit breaker + poison pill tracker
│
├── Dockerfile                       # Python 3.13-slim with baked-in embedding model
├── docker-compose.yml               # Pipeline + Qdrant orchestration
├── requirements.txt                 # Python dependencies
├── .env.example                     # Environment variable template
├── .gitignore
└── LICENSE                          # MIT
```

## License

[MIT](LICENSE) © Muhammad Abdul Barr

## Links

- **Source**: https://github.com/Muhammad-Abdul-Barr/Logiscout-Log-Ingestion-Pipeline-Service
- **Issues**: https://github.com/Muhammad-Abdul-Barr/Logiscout-Log-Ingestion-Pipeline-Service/issues
- **LogiScout Platform**: https://github.com/Muhammad-Abdul-Barr
