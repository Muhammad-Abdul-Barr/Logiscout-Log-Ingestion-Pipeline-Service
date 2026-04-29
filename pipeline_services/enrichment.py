"""Enrichment Layer: Trace-level error extraction, fingerprinting, severity, and outcome."""

import re
import hashlib
from typing import Optional, List, Tuple
from .models import LogLevels
from .transformation import TransformedLog


# Level priority for determining max severity
_LEVEL_PRIORITY = {
    LogLevels.DEBUG: 0,
    LogLevels.INFO:  1,
    LogLevels.WARN:  2,
    LogLevels.ERROR: 3,
    LogLevels.FATAL: 4,
}


class EnrichmentService:
    """Enriches a complete trace (all logs sharing one correlation ID)."""

    def __init__(self, config=None):
        """
        Args:
            config: Pipeline Config object. If provided, keywords and thresholds
                    come from config. If None, uses sensible defaults.
        """
        if config:
            self._suspicious_keywords = config.suspicious_keywords
            self._critical_keywords = config.critical_keywords
        else:
            # Defaults for standalone/test usage without config
            self._suspicious_keywords = [
                "timeout", "connection refused", "deadlock", "oom",
                "out of memory", "panic", "segfault", "unreachable",
                "failed to", "unable to", "retry", "crash", "fatal",
            ]
            self._critical_keywords = [
                "timeout", "deadlock", "oom", "out of memory", "crash",
                "fatal", "panic", "segfault", "corruption", "unreachable",
            ]

    # ── 1. Message Normalization ──────────────────────────────────────

    def normalize_message(self, message: str) -> str:
        """Strips dynamic values (UUIDs, IPs, hex addresses, numbers) to create a stable template."""
        normalized = re.sub(r'\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b', '<UUID>', message)
        normalized = re.sub(r'0x[0-9a-fA-F]+', '<HEX_ADDR>', normalized)
        normalized = re.sub(r'\b\d{1,3}(\.\d{1,3}){3}(:\d+)?\b', '<IP>', normalized)
        normalized = re.sub(r'\b\d+\b', '<NUM>', normalized)
        return normalized

    # ── 2. Error Type Extraction ──────────────────────────────────────

    def extract_error_type(self, message: str, meta: Optional[dict] = None) -> Optional[str]:
        """Extracts the error type/class name from the log message or meta."""
        match = re.search(r'\b([A-Z][A-Za-z0-9]*(?:Error|Exception|Timeout|Failure|Refused|Denied|Overflow|Violation))\b', message)
        if match:
            return match.group(1)

        if ":" in message:
            candidate = message.split(":")[0].strip()
            if " " not in candidate and len(candidate) <= 50:
                return candidate

        if meta and 'error_type' in meta:
            return str(meta['error_type'])

        return None

    # ── 3. Suspicious Keyword Detection ───────────────────────────────

    def _has_suspicious_keywords(self, message: str) -> bool:
        """Quick check: does this message contain any suspicious keyword?"""
        msg_lower = message.lower()
        return any(kw in msg_lower for kw in self._suspicious_keywords)

    def detect_suspicious_keywords(self, logs: List[TransformedLog]) -> List[str]:
        """
        Scans ALL log messages (including INFO/DEBUG) for suspicious keywords.

        Returns a deduplicated list of matched keywords found across the trace.
        """
        found = []
        seen = set()
        for log in logs:
            msg_lower = log.log_entry.message.lower()
            for kw in self._suspicious_keywords:
                if kw in msg_lower and kw not in seen:
                    seen.add(kw)
                    found.append(kw)
        return found

    # ── 4. Max Level in Trace ─────────────────────────────────────────

    def compute_max_level(self, logs: List[TransformedLog]) -> LogLevels:
        """Returns the highest severity level across all logs in the trace."""
        max_level = LogLevels.DEBUG
        for log in logs:
            if _LEVEL_PRIORITY.get(log.log_entry.level, 0) > _LEVEL_PRIORITY.get(max_level, 0):
                max_level = log.log_entry.level
        return max_level

    # ── 5. Outcome Classification ─────────────────────────────────────

    def classify_outcome(self, status_code: Optional[int], max_level: LogLevels) -> str:
        """
        Derives a human-readable outcome for the trace.

        Categories: success, warning, client_error, server_error, failure
        """
        if max_level in (LogLevels.FATAL, LogLevels.ERROR):
            return "failure"
        if status_code is not None and status_code >= 500:
            return "server_error"
        if status_code is not None and status_code >= 400:
            return "client_error"
        if max_level == LogLevels.WARN:
            return "warning"
        return "success"

    # ── 6. Severity Score (trace-level) ───────────────────────────────

    def compute_trace_severity(self, max_level: LogLevels, outcome: str, logs: List[TransformedLog]) -> int:
        """Assigns a numeric severity score (1-10) for the entire trace."""
        base = {
            LogLevels.DEBUG: 1,
            LogLevels.INFO:  2,
            LogLevels.WARN:  4,
            LogLevels.ERROR: 7,
            LogLevels.FATAL: 9,
        }.get(max_level, 2)

        # Boost for critical keywords in any log message
        for log in logs:
            msg_lower = log.log_entry.message.lower()
            if any(kw in msg_lower for kw in self._critical_keywords):
                base = min(base + 2, 10)
                break

        # Boost for server errors
        if outcome in ("server_error", "failure"):
            base = min(base + 1, 10)

        return base

    # ── 7. Request Path Normalization ─────────────────────────────────

    def normalize_request_path(self, path: Optional[str]) -> Optional[str]:
        """
        Replaces dynamic path segments with {id} placeholders.

        /users/2         → /users/{id}
        /orders/ORD-123  → /orders/{id}
        /api/v2/products → /api/v2/products  (no change)
        """
        if not path:
            return None

        segments = path.split("/")
        normalized = []
        for seg in segments:
            if not seg:
                normalized.append(seg)
                continue
            # Replace segments that look like IDs (numbers, UUIDs, alphanumeric IDs)
            if re.match(r'^(\d+|[0-9a-fA-F-]{8,}|[A-Za-z]+-\d+|[A-Za-z0-9]{20,})$', seg):
                normalized.append("{id}")
            else:
                normalized.append(seg)
        return "/".join(normalized)

    # ── 8. Fingerprint Message Selection ──────────────────────────────

    def _pick_fingerprint_message(self, sorted_logs: List[TransformedLog]) -> str:
        """
        Selects the most representative message for fingerprint generation.

        Priority:
        1. First ERROR/FATAL message (most likely the actual error)
        2. First message with suspicious keywords (catches INFO-level errors)
        3. Last message (fallback — original behavior)

        This prevents different error traces from collapsing into the same
        fingerprint when they all end with "Request completed" or "Response sent".
        """
        # Priority 1: First ERROR/FATAL message
        for log in sorted_logs:
            if log.log_entry.level in (LogLevels.ERROR, LogLevels.FATAL):
                return log.log_entry.message

        # Priority 2: First message with suspicious keywords
        for log in sorted_logs:
            if self._has_suspicious_keywords(log.log_entry.message):
                return log.log_entry.message

        # Fallback: last message
        return sorted_logs[-1].log_entry.message

    # ── 9. Trace-Level Fingerprint ────────────────────────────────────

    def compute_trace_fingerprint(
        self,
        services: List[str],
        max_level: LogLevels,
        fingerprint_message: str,
    ) -> str:
        """
        Creates a deterministic fingerprint for the entire trace.

        Groups identical request flow patterns together.
        Formula: MD5(sorted_services | max_level | normalized_fingerprint_message)
        """
        services_path = "→".join(sorted(set(services)))
        normalized_msg = self.normalize_message(fingerprint_message)
        raw = f"{services_path}|{max_level.value}|{normalized_msg}".encode('utf-8')
        digest = hashlib.md5(raw).hexdigest()
        return f"TRC-{digest[:8].upper()}"

    # ── 10. Orchestrator ──────────────────────────────────────────────

    def enrich_trace(self, logs: List[TransformedLog]) -> dict:
        """
        Orchestrates all trace-level enrichment for a group of logs
        sharing one correlation ID.

        Returns a dict of enriched trace-level attributes.
        """
        if not logs:
            return {}

        # Use the first log's trace for request context
        trace = logs[0].trace

        # Sorted by timestamp for chronological order
        sorted_logs = sorted(logs, key=lambda l: l.ls_ts)

        # Services involved (ordered by first appearance)
        seen = set()
        services = []
        for log in sorted_logs:
            if log.service not in seen:
                seen.add(log.service)
                services.append(log.service)

        # Max level and outcome
        max_level = self.compute_max_level(sorted_logs)
        status_code = sorted_logs[0].request_status_code
        outcome = self.classify_outcome(status_code, max_level)
        severity_score = self.compute_trace_severity(max_level, outcome, sorted_logs)

        # Fingerprint (uses error/suspicious message, not blindly last message)
        fingerprint_message = self._pick_fingerprint_message(sorted_logs)
        fingerprint = self.compute_trace_fingerprint(services, max_level, fingerprint_message)

        # Error types — scan ALL log levels, not just WARN/ERROR/FATAL
        # Developers log real errors at INFO level constantly
        error_types = []
        for log in sorted_logs:
            et = self.extract_error_type(log.log_entry.message, log.log_entry.meta)
            if et and et not in error_types:
                error_types.append(et)

        # Suspicious keywords — scan all messages for hints
        suspicious_keywords = self.detect_suspicious_keywords(sorted_logs)

        # Request path normalization
        request_path = trace.request.path if trace.request else None
        request_path_pattern = self.normalize_request_path(request_path)

        # Level flags
        levels_present = list(set(log.log_entry.level.value for log in sorted_logs))

        return {
            "sorted_logs": sorted_logs,
            "services": services,
            "max_level": max_level.value,
            "outcome": outcome,
            "severity_score": severity_score,
            "fingerprint": fingerprint,
            "error_types": error_types,
            "suspicious_keywords": suspicious_keywords,
            "request_path_pattern": request_path_pattern,
            "levels_present": levels_present,
            "has_errors": max_level in (LogLevels.ERROR, LogLevels.FATAL),
            "has_warnings": LogLevels.WARN in [l.log_entry.level for l in sorted_logs],
            "log_count": len(sorted_logs),
        }
