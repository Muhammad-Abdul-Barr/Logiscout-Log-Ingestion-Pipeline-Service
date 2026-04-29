"""Transformation Layer: Flattens traces and normalizes fields into a standard internal format."""

from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Tuple
from .models import RawTrace, RawLogEntry


@dataclass
class TransformedLog:
    """Internal representation of a single flattened log with resolved context."""
    service: str
    environment: str
    ls_ts: str
    ls_ts_unix: int

    # Request context carried forward
    request_method: str | None
    request_path: str | None
    request_status_code: int | None

    # Original raw data pointers
    log_entry: RawLogEntry
    trace: RawTrace


class TransformationService:
    """Flattens the trace's log array and resolves contextual fields per entry."""

    def resolve_service(self, log: RawLogEntry, trace: RawTrace) -> str:
        """Determines the service name from loggername or trace-level component."""
        if log.loggername:
            return log.loggername
        if trace.component:
            return trace.component
        return trace.projectName or "unknown-service"

    def normalize_timestamp(self, raw_ts: str) -> Tuple[str, int]:
        """Parses a raw timestamp string into ISO format and Unix epoch seconds."""
        try:
            ts_str = raw_ts.replace('Z', '+00:00')
            dt = datetime.fromisoformat(ts_str)
            return dt.isoformat(), int(dt.timestamp())
        except ValueError:
            now = datetime.now(timezone.utc)
            return now.isoformat(), int(now.timestamp())

    def extract_request_context(self, trace: RawTrace) -> Tuple[str | None, str | None, int | None]:
        """Extracts HTTP request context from the trace if available."""
        if trace.request:
            return trace.request.method, trace.request.path, trace.request.statusCode
        return None, None, None

    def transform_log(self, log: RawLogEntry, trace: RawTrace) -> TransformedLog:
        """Transforms a single raw log entry into a flattened, normalized structure."""
        service = self.resolve_service(log, trace)
        ls_ts, ls_ts_unix = self.normalize_timestamp(log.timestamp)
        req_method, req_path, req_status = self.extract_request_context(trace)

        return TransformedLog(
            service=service,
            environment=trace.environment,
            ls_ts=ls_ts,
            ls_ts_unix=ls_ts_unix,
            request_method=req_method,
            request_path=req_path,
            request_status_code=req_status,
            log_entry=log,
            trace=trace,
        )

    def transform_trace(self, trace: RawTrace) -> list[TransformedLog]:
        """Flattens all logs in a trace into individually transformed entries."""
        return [self.transform_log(log, trace) for log in trace.logs]
