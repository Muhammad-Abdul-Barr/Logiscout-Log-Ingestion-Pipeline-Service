"""Fault Tolerance primitives for the LogiScout ingestion pipeline.

PoisonPillTracker — persists correlation IDs that consistently fail processing
                    so they can be skipped on future cycles.
CircuitBreaker    — detects systemic failures within a batch cycle and pauses
                    the pipeline to prevent thrashing.

Zero external dependencies — uses only json, os, time, logging.
"""

import json
import os
import time
import logging
from typing import Dict

logger = logging.getLogger(__name__)


# ── Poison Pill Tracker ──────────────────────────────────────────────────────

class PoisonPillTracker:
    """Tracks correlation_ids that consistently fail processing.

    After *max_failures* consecutive failures across cycles, the trace is
    marked as a poison pill and will be skipped automatically.  The registry
    is persisted to a small JSON file (same pattern as the watermark file).
    """

    def __init__(self, path: str = ".poison_pills.json", max_failures: int = 3):
        self._path = path
        self._max_failures = max_failures
        self._registry: Dict[str, dict] = self._load()

    # ── persistence ──────────────────────────────────────────────────

    def _load(self) -> Dict[str, dict]:
        if os.path.exists(self._path):
            try:
                with open(self._path, "r") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        return data
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("Failed to load poison pill registry: %s", e)
        return {}

    def _save(self) -> None:
        tmp = self._path + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump(self._registry, f, indent=2)
            os.replace(tmp, self._path)
        except OSError as e:
            logger.error("Failed to save poison pill registry: %s", e)

    # ── public API ───────────────────────────────────────────────────

    def is_poison_pill(self, correlation_id: str) -> bool:
        """True if this trace has exceeded max_failures across cycles."""
        entry = self._registry.get(correlation_id)
        if entry is None:
            return False
        return entry.get("count", 0) >= self._max_failures

    def record_failure(self, correlation_id: str, error: str) -> bool:
        """Increments count.  Returns True if trace just became a poison pill."""
        now = time.strftime("%Y-%m-%dT%H:%M:%S")
        entry = self._registry.get(correlation_id)

        if entry is None:
            entry = {"count": 0, "last_error": "", "first_seen": now, "last_seen": now}
            self._registry[correlation_id] = entry

        entry["count"] = entry.get("count", 0) + 1
        entry["last_error"] = error
        entry["last_seen"] = now

        self._save()

        became_poison = entry["count"] == self._max_failures
        return became_poison

    def get_all(self) -> Dict[str, dict]:
        """All tracked failures — for manual inspection / alerting."""
        return dict(self._registry)

    def clear(self, correlation_id: str) -> None:
        """Remove from tracking after manual fix."""
        if correlation_id in self._registry:
            del self._registry[correlation_id]
            self._save()
            logger.info("Cleared poison pill entry for %s", correlation_id)


# ── Circuit Breaker ──────────────────────────────────────────────────────────

class CircuitBreaker:
    """Detects systemic failures and pauses the pipeline.

    State machine:
        CLOSED  → normal operation
        OPEN    → too many failures, pipeline pauses
        HALF-OPEN → cooldown elapsed, pipeline probes with one cycle

    All state is in-memory — no persistence needed.
    """

    def __init__(self, failure_threshold: int = 3, cooldown_seconds: int = 600):
        self._failure_threshold = failure_threshold
        self._cooldown_seconds = cooldown_seconds
        self._cycle_failures = 0
        self._is_open = False
        self._opened_at: float = 0

    def record_failure(self) -> None:
        """Increments cycle failure count."""
        self._cycle_failures += 1
        if self._cycle_failures >= self._failure_threshold and not self._is_open:
            self._is_open = True
            self._opened_at = time.time()
            logger.critical(
                "Circuit breaker OPENED after %d failures (cooldown: %ds)",
                self._cycle_failures, self._cooldown_seconds,
            )

    def is_tripped(self) -> bool:
        """True if failures >= threshold."""
        return self._cycle_failures >= self._failure_threshold

    def should_attempt(self) -> bool:
        """True if breaker is closed OR cooldown has elapsed (half-open)."""
        if not self._is_open:
            return True
        elapsed = time.time() - self._opened_at
        if elapsed >= self._cooldown_seconds:
            logger.info("Circuit breaker cooldown elapsed — entering HALF-OPEN state")
            return True
        return False

    def reset_cycle(self) -> None:
        """Called at start of each batch cycle."""
        self._cycle_failures = 0

    def close(self) -> None:
        """Reset after successful health probe."""
        if self._is_open:
            logger.info("Circuit breaker CLOSED — pipeline resuming normal operation")
        self._is_open = False
        self._cycle_failures = 0
        self._opened_at = 0
