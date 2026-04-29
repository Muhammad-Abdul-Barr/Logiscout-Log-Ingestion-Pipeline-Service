"""Centralized configuration for LogiScout pipeline (Without Spark).

All tunable parameters live here. Secrets come from .env via os.getenv().
"""
import os
from dataclasses import dataclass, field
from typing import Tuple, List


@dataclass
class Config:
    """Pipeline configuration settings."""

    # ══════════════════════════════════════════════════════════════════
    #  SECRETS (loaded from .env — never hardcode here)
    # ══════════════════════════════════════════════════════════════════

    # ── LLM ───────────────────────────────────────────────────────────
    gemini_key: str = field(default_factory=lambda: os.getenv("GEMINI_KEY", ""))
    models_to_try: Tuple[str, ...] = ("gemini-2.0-flash", "gemini-2.5-flash", "gemini-2.5-flash-lite")

    # ── OLAP / ClickHouse (HTTP native — no JDBC) ────────────────────
    olap_host: str = field(default_factory=lambda: os.getenv("OLAP_HOST", ""))
    olap_port: int = field(default_factory=lambda: int(os.getenv("OLAP_PORT", "8123")))
    olap_database: str = field(default_factory=lambda: os.getenv("OLAP_DATABASE", "logging"))
    olap_table: str = field(default_factory=lambda: os.getenv("OLAP_TABLE", "logs"))
    olap_user: str = field(default_factory=lambda: os.getenv("OLAP_USER", "default"))
    olap_password: str = field(default_factory=lambda: os.getenv("OLAP_PASSWORD", ""))
    olap_connect_timeout: int = field(default_factory=lambda: int(os.getenv("OLAP_CONNECT_TIMEOUT", "30")))
    olap_send_receive_timeout: int = field(default_factory=lambda: int(os.getenv("OLAP_SEND_RECEIVE_TIMEOUT", "120")))

    # ── Qdrant ────────────────────────────────────────────────────────
    qdrant_url: str = field(default_factory=lambda: os.getenv("QDRANT_URL", ""))

    # ══════════════════════════════════════════════════════════════════
    #  TUNABLE PARAMETERS (override via .env or environment variables)
    # ══════════════════════════════════════════════════════════════════

    # ── Vector Store ──────────────────────────────────────────────────
    vector_size: int = 384                      # BGE-small embedding dimensions (fixed by model)
    upsert_batch_size: int = field(default_factory=lambda: int(os.getenv("UPSERT_BATCH_SIZE", "100")))

    # ── Collection names (fallback — dynamic {project_id}_logs is preferred)
    collection_incidents: str = field(default_factory=lambda: os.getenv("COLLECTION_INCIDENTS", "logiscout_incidents"))
    collection_commits: str = field(default_factory=lambda: os.getenv("COLLECTION_COMMITS", "logiscout_commits"))

    # ── Scheduler / Batch ─────────────────────────────────────────────
    fetch_interval_minutes: int = field(default_factory=lambda: int(os.getenv("FETCH_INTERVAL_MINUTES", "5")))
    fetch_batch_size: int = field(default_factory=lambda: int(os.getenv("FETCH_BATCH_SIZE", "1000")))
    watermark_path: str = field(default_factory=lambda: os.getenv("WATERMARK_PATH", ".watermark"))

    # ── Gatekeeper Thresholds ─────────────────────────────────────────
    severity_threshold: int = field(default_factory=lambda: int(os.getenv("SEVERITY_THRESHOLD", "7")))
    deploy_window_minutes: int = field(default_factory=lambda: int(os.getenv("DEPLOY_WINDOW_MINUTES", "60")))
    cold_start_baseline_count: int = field(default_factory=lambda: int(os.getenv("COLD_START_BASELINE_COUNT", "3")))

    # ── Suspicious Keywords (Tier 1 content scan) ─────────────────────
    suspicious_keywords: List[str] = field(default_factory=lambda: [
        # Connection & network
        "timeout", "timed out", "connection refused", "connection reset",
        "unreachable", "broken pipe", "connection closed", "eof",
        "ssl error", "certificate", "handshake failed",

        # Memory & resources
        "oom", "out of memory", "disk full", "no space",
        "stack overflow", "stackoverflow",

        # Concurrency
        "deadlock",

        # Critical failures
        "panic", "segfault", "crash", "fatal",
        "corruption", "corrupted",
        "null pointer", "nullptr", "nullpointerexception",

        # Resilience patterns (indicate something IS failing)
        "circuit breaker", "rate limit", "quota exceeded",
        "retry", "retrying", "fallback", "degraded",

        # Access & auth
        "unavailable", "rejected", "denied",
        "forbidden", "unauthorized", "permission denied",

        # Soft failure verbs (catch INFO-level errors)
        "failed to", "unable to", "could not", "cannot connect",
    ])

    # ── Severity Score Boost Keywords ─────────────────────────────────
    critical_keywords: List[str] = field(default_factory=lambda: [
        "timeout", "deadlock", "oom", "out of memory", "crash",
        "fatal", "panic", "segfault", "corruption", "unreachable",
    ])

    # ── Semantic Text ─────────────────────────────────────────────────
    max_semantic_chars: int = field(default_factory=lambda: int(os.getenv("MAX_SEMANTIC_CHARS", "2000")))

    # ── Fault Tolerance ───────────────────────────────────────────────
    retry_max_attempts: int = field(
        default_factory=lambda: int(os.getenv("RETRY_MAX_ATTEMPTS", "3")))
    retry_backoff_base: float = field(
        default_factory=lambda: float(os.getenv("RETRY_BACKOFF_BASE", "1.0")))
    circuit_breaker_threshold: int = field(
        default_factory=lambda: int(os.getenv("CIRCUIT_BREAKER_THRESHOLD", "3")))
    circuit_breaker_cooldown_minutes: int = field(
        default_factory=lambda: int(os.getenv("CIRCUIT_BREAKER_COOLDOWN_MINUTES", "10")))
    poison_pill_max_failures: int = field(
        default_factory=lambda: int(os.getenv("POISON_PILL_MAX_FAILURES", "3")))
    poison_pill_path: str = field(
        default_factory=lambda: os.getenv("POISON_PILL_PATH", ".poison_pills.json"))

    @classmethod
    def from_env(cls) -> "Config":
        """Create config from environment variables. All fields already read from os.getenv()."""
        return cls()
