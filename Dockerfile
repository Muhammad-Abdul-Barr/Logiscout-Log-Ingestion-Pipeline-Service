# ════════════════════════════════════════════════════════════════
#  LogiScout Logs Ingestion Pipeline (Without Spark)
#  Base: Python 3.13 — no Java required
# ════════════════════════════════════════════════════════════════

FROM python:3.13-slim

# ── 1. Install minimal system deps ───────────────────────────
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# ── 2. Set up working directory ──────────────────────────────
WORKDIR /app

# ── 3. Install Python dependencies ──────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── 4. Pre-download the embedding model (baked into image) ───
RUN python -c "from fastembed import TextEmbedding; TextEmbedding(model_name='BAAI/bge-small-en-v1.5')"

# ── 5. Copy application code ────────────────────────────────
COPY . .

# ── 6. Create data directory for watermark persistence ───────
RUN mkdir -p /app/data

# ── 7. Run ───────────────────────────────────────────────────
CMD ["python", "main.py"]
