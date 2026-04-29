"""Indexing Prep Service: Builds trace-level semantic text and metadata for VectorDB."""

import re
from typing import Dict, Any, List
from .transformation import TransformedLog
from .models import TraceDocument


# Keys from meta to skip (already in payload or redundant)
_SKIP_META_KEYS = {"logger", "loggername", "correlationId", "correlation_id", "level", "timestamp"}


class IndexingPrepService:
    """Prepares trace-level documents for vector embedding and Qdrant storage."""

    def __init__(self, config=None):
        self._max_semantic_chars = config.max_semantic_chars if config else 2000

    # ── 1. Helpers ──────────────────────────────────────────────────────

    def _collect_meta_context(self, sorted_logs: List[TransformedLog]) -> Dict[str, str]:
        """
        Scans all meta fields across every log in the trace and collects
        user-provided context, skipping internal/redundant keys.

        Meta is optional and user-defined — it may contain entity IDs,
        debug strings, arrays, custom comments, or be absent entirely.

        Returns:
            Dict of unique {key: formatted_value} pairs, preserving first
            occurrence of each key across the trace.
        """
        context: Dict[str, str] = {}
        for log in sorted_logs:
            if not log.log_entry.meta:
                continue
            for k, v in log.log_entry.meta.items():
                if k.lower() in _SKIP_META_KEYS:
                    continue
                if k in context:
                    continue  # keep first occurrence
                # Format based on type
                if v is None:
                    continue
                elif isinstance(v, (str, int, float, bool)):
                    context[k] = str(v)
                elif isinstance(v, list) and len(v) <= 5:
                    context[k] = str(v)
                # Skip large arrays and dicts — too noisy for embedding budget
        return context

    def _normalize_for_dedup(self, message: str) -> str:
        """
        Creates a normalized form of a log message for deduplication.
        Strips UUIDs, hex addresses, IPs, and numbers so that messages
        differing only in dynamic values are treated as identical.
        """
        msg = message.lower().strip()
        msg = re.sub(r'\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b', '', msg)
        msg = re.sub(r'0x[0-9a-fA-F]+', '', msg)
        msg = re.sub(r'\b\d+\b', '', msg)
        msg = re.sub(r'\s+', ' ', msg).strip()
        return msg

    def _deduplicate_messages(self, sorted_logs: List[TransformedLog]) -> List[str]:
        """
        Returns a list of unique, formatted log message lines.

        - Normalizes each message to detect semantic duplicates
        - Keeps only the first occurrence of each unique pattern
        - Prefixes WARN/ERROR/FATAL with a level tag (INFO/DEBUG are the norm)
        """
        seen_normalized = set()
        unique_lines = []

        for log in sorted_logs:
            msg = log.log_entry.message.strip()
            if not msg:
                continue

            norm = self._normalize_for_dedup(msg)
            if norm in seen_normalized:
                continue
            seen_normalized.add(norm)

            # Prefix with level only for non-normal severities
            if log.log_entry.level.value in ("WARN", "ERROR", "FATAL"):
                unique_lines.append(f"[{log.log_entry.level.value}] {msg}")
            else:
                unique_lines.append(msg)

        return unique_lines

    # ── 2. Semantic Text Builder (per trace) ──────────────────────────

    def build_trace_semantic_text(
        self,
        sorted_logs: List[TransformedLog],
        enrichment: dict,
    ) -> str:
        """
        Constructs a retrieval-optimized semantic digest of the entire trace,
        designed for embedding-based search (BGE-small, 512-token budget).

        Optimized for production INFO/DEBUG logs where users query LogiScout
        when they don't get expected results. The LLM receives full raw logs
        separately — this text's only job is retrieval accuracy.

        Structure (4 layers):
            1. Operation + user-provided context from meta (front-loaded)
            2. Service flow (compact path, stated once)
            3. Behavioral digest (deduplicated unique messages)
            4. Outcome + timing

        Fields like project, environment, and correlation_id are NOT included —
        they live in filterable Qdrant payload fields.
        """
        trace = sorted_logs[0].trace
        parts: List[str] = []

        # ── Layer 1: Operation + Context ──────────────────────────────
        req = trace.request
        path_pattern = enrichment.get("request_path_pattern")

        if req and req.method:
            # Use normalized path pattern for generalization when available
            display_path = path_pattern or req.path or ""
            parts.append(f"{req.method} {display_path}".strip())
        elif trace.durationMs is not None:
            parts.append("Trace executed")

        # Append user-provided meta context (optional, any shape)
        meta_context = self._collect_meta_context(sorted_logs)
        if meta_context:
            ctx_str = "; ".join(f"{k}={v}" for k, v in meta_context.items())
            parts.append(f"Context: {ctx_str}")

        # ── Layer 2: Service Flow ─────────────────────────────────────
        services = enrichment.get("services", [])
        if len(services) > 1:
            parts.append(f"Flow: {' → '.join(services)}")
        elif len(services) == 1:
            parts.append(f"Service: {services[0]}")

        # ── Layer 3: Behavioral Digest ────────────────────────────────
        unique_messages = self._deduplicate_messages(sorted_logs)
        parts.extend(unique_messages)

        # ── Layer 4: Outcome + Timing ─────────────────────────────────
        outcome = enrichment.get("outcome", "")
        status_code = req.statusCode if req else None
        duration = trace.durationMs

        outcome_parts = []
        if outcome:
            outcome_parts.append(outcome.replace("_", " "))
        if status_code is not None:
            outcome_parts.append(str(status_code))
        if duration is not None:
            outcome_parts.append(f"{duration}ms")

        if outcome_parts:
            parts.append(f"Outcome: {', '.join(outcome_parts)}")

        # ── Assemble ──────────────────────────────────────────────────
        semantic_text = ". ".join(parts)

        # Truncate to stay within embedding model's token budget (~512 tokens ≈ ~2000 chars)
        if len(semantic_text) > self._max_semantic_chars:
            semantic_text = semantic_text[:self._max_semantic_chars] + "..."

        return semantic_text

    # ── 3. Vector Metadata Builder (per trace) ────────────────────────

    def build_trace_metadata(
        self,
        sorted_logs: List[TransformedLog],
        enrichment: dict,
    ) -> Dict[str, Any]:
        """
        Builds the flat metadata dict for Qdrant's filterable payload.

        All fields here are indexed for fast filtered search.
        """
        trace = sorted_logs[0].trace
        req = trace.request

        meta: Dict[str, Any] = {
            # Identity
            "correlation_id": trace.correlationId,
            "project_name": trace.projectName,
            "project_id": trace.project_id,

            # Request context
            "request_method": req.method if req else None,
            "request_path": req.path if req else None,
            "request_path_pattern": enrichment.get("request_path_pattern"),
            "request_status_code": req.statusCode if req else None,

            # Trace-level enrichment
            "services": enrichment["services"],
            "max_level": enrichment["max_level"],
            "outcome": enrichment["outcome"],
            "severity_score": enrichment["severity_score"],
            "fingerprint": enrichment["fingerprint"],
            "has_errors": enrichment["has_errors"],
            "has_warnings": enrichment["has_warnings"],
            "log_count": enrichment["log_count"],
            "levels_present": enrichment["levels_present"],

            # Environment
            "environment": trace.environment,

            # Timing
            "duration_ms": trace.durationMs,
            "timestamp_unix": sorted_logs[0].ls_ts_unix,
        }

        # Error types (if any)
        if enrichment["error_types"]:
            meta["error_types"] = enrichment["error_types"]

        # Suspicious keywords (if any)
        if enrichment.get("suspicious_keywords"):
            meta["suspicious_keywords"] = enrichment["suspicious_keywords"]

        # Occurrence tracking (for deduplication)
        meta["occurrence_count"] = 1
        meta["last_seen"] = sorted_logs[0].ls_ts_unix

        return meta

    # ── 4. Prepare Trace Document ─────────────────────────────────────

    def prepare_trace_document(
        self,
        sorted_logs: List[TransformedLog],
        enrichment: dict,
    ) -> TraceDocument:
        """
        Creates a single TraceDocument from a group of enriched logs.
        One TraceDocument = one vector in Qdrant.
        """
        trace = sorted_logs[0].trace

        semantic_text = self.build_trace_semantic_text(sorted_logs, enrichment)
        vector_metadata = self.build_trace_metadata(sorted_logs, enrichment)

        return TraceDocument(
            correlation_id=trace.correlationId,
            project_name=trace.projectName,
            project_id=trace.project_id,
            semantic_text=semantic_text,
            vector_metadata=vector_metadata,
        )
