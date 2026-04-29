"""ClickHouse Fetcher Service: Reads flat log rows from OLAP via clickhouse-connect, deduplicates via watermark, reassembles into RawTrace dicts."""

import json
import os
import shutil
import time
import logging
from typing import List, Dict, Any, TypeVar, Callable
from collections import defaultdict

import clickhouse_connect

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ClickHouseFetcherService:
    """Fetches flattened log rows from ClickHouse in batches, avoids duplicates, and reassembles nested traces."""

    def __init__(self, config):
        self.config = config
        self._client = None

    # ── 1. ClickHouse Client ─────────────────────────────────────────

    def get_client(self):
        """Creates or reuses a ClickHouse HTTP client."""
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self.config.olap_host,
                port=self.config.olap_port,
                database=self.config.olap_database,
                username=self.config.olap_user,
                password=self.config.olap_password,
                connect_timeout=self.config.olap_connect_timeout,
                send_receive_timeout=self.config.olap_send_receive_timeout,
            )
            logger.info("ClickHouse client connected to %s:%s/%s",
                        self.config.olap_host, self.config.olap_port, self.config.olap_database)
        return self._client

    def _reset_client(self) -> None:
        """Drops the cached client so the next get_client() reconnects."""
        self._client = None

    def _execute_with_retry(self, operation: Callable[[], T], label: str) -> T:
        """Wraps a ClickHouse operation with retry + exponential backoff.

        On connection errors the cached client is reset so the next
        attempt establishes a fresh connection.  Uses the same retry
        settings as per-trace processing (retry_max_attempts / retry_backoff_base).
        """
        max_attempts = self.config.retry_max_attempts
        backoff_base = self.config.retry_backoff_base

        for attempt in range(max_attempts):
            try:
                return operation()
            except Exception as e:
                is_last = attempt >= max_attempts - 1
                # Reset client on connection-level errors so we reconnect
                self._reset_client()

                if is_last:
                    logger.error(
                        "ClickHouse %s failed after %d attempts: %s",
                        label, max_attempts, e,
                    )
                    raise

                wait = backoff_base * (2 ** attempt)
                logger.warning(
                    "ClickHouse %s — retry %d/%d in %.1fs: %s",
                    label, attempt + 1, max_attempts, wait, e,
                )
                time.sleep(wait)

    # ── 2. Watermark Persistence (dedup) ──────────────────────────────

    def load_watermark(self) -> str:
        """Reads the last-processed timestamp from the watermark file.

        Falls back to the .bak backup if the primary file is missing or corrupt.
        Returns epoch zero on first run.
        """
        path = self.config.watermark_path
        backup = path + ".bak"

        for candidate in (path, backup):
            if os.path.exists(candidate):
                try:
                    with open(candidate, "r") as f:
                        ts = f.read().strip()
                        if ts:
                            if candidate == backup:
                                logger.warning("Watermark recovered from backup file")
                            return ts
                except OSError:
                    continue

        return "1970-01-01 00:00:00.000"

    def save_watermark(self, timestamp: str) -> None:
        """Persists the new high-watermark timestamp with atomic write + backup."""
        path = self.config.watermark_path
        backup = path + ".bak"
        tmp = path + ".tmp"

        # Backup current watermark before overwriting
        if os.path.exists(path):
            try:
                shutil.copy2(path, backup)
            except OSError:
                pass

        # Atomic write: write to tmp, then rename
        with open(tmp, "w") as f:
            f.write(timestamp)
        os.replace(tmp, path)  # atomic on both Linux and Windows

        logger.info("Watermark updated to: %s", timestamp)

    # ── 3. Batch Fetcher ─────────────────────────────────────────────

    def fetch_batch(self, watermark: str, cutoff_ts: str = None) -> List[Dict[str, Any]]:
        """Queries ClickHouse and returns rows as Python dicts.

        When the batch hits the LIMIT, the last correlationId's rows may be
        split across two fetches.  To prevent processing a partial trace, we
        trim rows belonging to that trailing correlationId — they'll be
        re-fetched as a complete trace in the next cycle.

        Args:
            watermark: Only fetch rows after this timestamp.
            cutoff_ts: Only fetch rows up to this timestamp (inclusive).
                       Used to bound the drain loop to a snapshot time.
        """
        table = self.config.olap_table
        batch_size = self.config.fetch_batch_size

        where = f"WHERE timestamp > toDateTime64(%(wm)s, 3)"
        params = {"wm": watermark, "limit": batch_size}

        if cutoff_ts:
            where += f" AND timestamp <= toDateTime64(%(cutoff)s, 3)"
            params["cutoff"] = cutoff_ts

        query = (
            f"SELECT * FROM {table} "
            f"{where} "
            f"ORDER BY timestamp ASC, correlationId ASC "
            f"LIMIT %(limit)s"
        )

        def _do_fetch():
            client = self.get_client()
            return client.query(query, parameters=params)

        result = self._execute_with_retry(_do_fetch, "fetch_batch")

        # Convert to list of dicts using column names
        columns = result.column_names
        rows = [dict(zip(columns, row)) for row in result.result_rows]

        # Guard against split traces when the batch is full
        if len(rows) == batch_size and rows:
            tail_cid = rows[-1].get("correlationId")
            head_cid = rows[0].get("correlationId")
            # Only trim if there are multiple CIDs — if ALL rows are one CID,
            # keep them to avoid an infinite loop of always trimming
            if tail_cid != head_cid:
                rows = [r for r in rows if r.get("correlationId") != tail_cid]
                logger.info(
                    "Trimmed trailing correlationId %s to avoid split trace "
                    "(%d rows kept)", tail_cid, len(rows),
                )

        logger.info(f"Fetched {len(rows)} rows from ClickHouse")
        return rows

    def bulk_count_cids(self, correlation_ids: List[str]) -> Dict[str, int]:
        """Counts total rows per correlationId in ClickHouse.

        Used to detect partial traces: if ClickHouse has more rows for a CID
        than the current batch, prior rows exist and the trace needs merging.
        """
        if not correlation_ids:
            return {}

        table = self.config.olap_table

        query = (
            f"SELECT correlationId, count(*) as cnt FROM {table} "
            f"WHERE correlationId IN %(cids)s "
            f"GROUP BY correlationId"
        )

        def _do_count():
            client = self.get_client()
            return client.query(query, parameters={"cids": correlation_ids})

        result = self._execute_with_retry(_do_count, "bulk_count_cids")
        counts = {row[0]: row[1] for row in result.result_rows}
        logger.debug("Bulk CID counts: %d CIDs checked", len(counts))
        return counts

    def fetch_rows_for_cid(self, correlation_id: str) -> List[Dict[str, Any]]:
        """Fetches ALL rows for a specific correlationId from ClickHouse.

        Ignores the watermark — pulls the complete history for this CID.
        Used when a partial trace is detected via bulk_count_cids().
        """
        table = self.config.olap_table

        query = (
            f"SELECT * FROM {table} "
            f"WHERE correlationId = %(cid)s "
            f"ORDER BY timestamp ASC"
        )

        def _do_fetch_cid():
            client = self.get_client()
            return client.query(query, parameters={"cid": correlation_id})

        result = self._execute_with_retry(_do_fetch_cid, "fetch_rows_for_cid")
        columns = result.column_names
        rows = [dict(zip(columns, row)) for row in result.result_rows]

        logger.info("Re-fetched %d rows for correlationId %s", len(rows), correlation_id)
        return rows

    # ── 4. Reassemble Flat Rows → Nested Traces ──────────────────────

    def reassemble_traces(self, flat_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Groups flat OLAP rows by correlationId and rebuilds nested RawTrace dicts.

        Actual OLAP columns (camelCase):
            projectId, correlationId, timestamp, level, loggerName,
            message, meta, exception,
            sessionStartedAt, sessionEndedAt, durationMs,
            requestMethod, requestPath, statusCode
        """
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        trace_context: Dict[str, Dict[str, Any]] = {}

        for row in flat_rows:
            cid = row.get("correlationId", "unknown")
            grouped[cid].append(row)

            # Capture trace-level fields from the first row per correlationId
            if cid not in trace_context:
                request = None
                if row.get("requestMethod") or row.get("requestPath"):
                    request = {
                        "method": row.get("requestMethod"),
                        "path": row.get("requestPath"),
                        "statusCode": row.get("statusCode"),
                    }

                trace_context[cid] = {
                    "projectName": row.get("projectId", ""),
                    "project_id": row.get("projectId", ""),
                    "environment": "",
                    "correlationId": cid,
                    "component": row.get("loggerName"),
                    "startedAt": str(row.get("sessionStartedAt", "")),
                    "endedAt": str(row.get("sessionEndedAt")) if row.get("sessionEndedAt") else None,
                    "durationMs": row.get("durationMs"),
                    "request": request,
                }

        traces = []
        for cid, rows in grouped.items():
            trace = trace_context[cid]
            trace["logs"] = []
            for r in rows:
                # Parse meta from JSON string if stored as text
                meta = r.get("meta")
                if isinstance(meta, str):
                    try:
                        meta = json.loads(meta)
                    except (json.JSONDecodeError, TypeError):
                        meta = None

                # Parse exception from JSON string if stored as text
                exception = r.get("exception")
                if isinstance(exception, str):
                    try:
                        exception = json.loads(exception)
                    except (json.JSONDecodeError, TypeError):
                        exception = None

                trace["logs"].append({
                    "timestamp": str(r.get("timestamp", "")),
                    "level": r.get("level", "INFO"),
                    "message": r.get("message", ""),
                    "loggername": r.get("loggerName", trace.get("component", "")),
                    "meta": meta,
                    "exception": exception,
                })
            traces.append(trace)

        logger.info(f"Reassembled {len(flat_rows)} flat rows into {len(traces)} traces")
        return traces

    # ── 5. Compute New Watermark ──────────────────────────────────────

    def compute_new_watermark(self, flat_rows: List[Dict[str, Any]], current_watermark: str) -> str:
        """Returns the latest timestamp value from the batch, or the current watermark if empty."""
        if not flat_rows:
            return current_watermark
        # Rows are ordered ASC by timestamp, so the last row has the latest
        latest = flat_rows[-1].get("timestamp", current_watermark)
        return str(latest)

    def compute_safe_watermark(
        self,
        flat_rows: List[Dict[str, Any]],
        current_watermark: str,
        failed_cids: List[str],
    ) -> str:
        """
        Computes watermark only up to the row just before the first failed trace.

        Walks rows in timestamp-ASC order and stops as soon as a row belongs
        to a failed correlationId.  Everything at or after that timestamp
        will be re-fetched on the next cycle.  Successful rows that happen to
        sit after the failed ones will be re-processed, but deduplication
        (fingerprint check) makes this idempotent.
        """
        failed_set = set(failed_cids)
        safe_watermark = current_watermark

        for row in flat_rows:
            cid = row.get("correlationId", "")
            if cid in failed_set:
                break  # stop — don't advance past any failed trace
            safe_watermark = str(row.get("timestamp", current_watermark))

        return safe_watermark
