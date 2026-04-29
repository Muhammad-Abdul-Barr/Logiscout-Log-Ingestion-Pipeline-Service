"""
LogiScout Log Ingestion Pipeline — Orchestrator (Without Spark).

Usage (manual / server integration):
    from pipeline import LogProcessingPipeline
    pipeline = LogProcessingPipeline(config)
    trace_doc = pipeline.process_trace(raw_trace_dict)

Usage (scheduled batch mode via clickhouse-connect):
    from pipeline import LogProcessingPipeline
    from config import Config
    pipeline = LogProcessingPipeline(Config.from_env())
    pipeline.run()
"""

import time
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

from pipeline_services import (
    IngestionService,
    TransformationService,
)
from pipeline_services.enrichment import EnrichmentService
from pipeline_services.gatekeeper import GatekeeperService
from pipeline_services.indexing_prep import IndexingPrepService
from pipeline_services.models import TraceDocument
from pipeline_services.clickhouse_fetcher import ClickHouseFetcherService
from pipeline_services.vector_store import VectorStoreService
from pipeline_services.fault_tolerance import PoisonPillTracker, CircuitBreaker

logger = logging.getLogger(__name__)


# ── Batch Result ──────────────────────────────────────────────────────────────

@dataclass
class BatchResult:
    """Outcome of a single process_batch() call."""
    promoted: List[TraceDocument] = field(default_factory=list)
    dropped: int = 0
    failed_cids: List[str] = field(default_factory=list)
    circuit_breaker_tripped: bool = False


class LogProcessingPipeline:
    """
    Entry point for the log processing pipeline.

    Coordinates:
        ClickHouse Fetch → Ingestion → Transformation → Enrichment
        → GATEKEEPER (promote/drop) → Indexing Prep → VectorDB Store

    The gatekeeper sits between enrichment and indexing prep.
    It drops ~95% of healthy traffic, only promoting traces that
    match one of the promotion tiers (severity, content, fingerprint change, etc.).
    """

    def __init__(self, config=None):
        self.config = config
        self.ingestion = IngestionService()
        self.transformation = TransformationService()
        self.enrichment = EnrichmentService(config)
        self.indexing_prep = IndexingPrepService(config)
        self.vector_store = VectorStoreService(config) if config else None
        self.gatekeeper = GatekeeperService(config, self.vector_store) if config else None

        # ── Fault tolerance components ────────────────────────────────
        self.poison_pills = PoisonPillTracker(
            path=config.poison_pill_path,
            max_failures=config.poison_pill_max_failures,
        ) if config else None
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=config.circuit_breaker_threshold,
            cooldown_seconds=config.circuit_breaker_cooldown_minutes * 60,
        ) if config else None
        self._fetcher = None  # set in run() for merge-on-duplicate support

    # ── Single-Trace Entry Point (for server/developer use) ───────────

    def process_trace(self, raw_trace_dict: Dict[str, Any]) -> Optional[TraceDocument]:
        """
        Accepts a single raw trace dict, returns a vector-ready TraceDocument.
        Returns None if the trace is dropped by the gatekeeper or deduplicated.
        """
        is_merge = raw_trace_dict.pop("_is_merge", False)

        # 1. Ingestion: validate
        validated_trace = self.ingestion.validate_trace(raw_trace_dict)
        project_id = validated_trace.project_id
        cid = validated_trace.correlationId

        # 2. Transformation: flatten + normalize
        transformed_logs = self.transformation.transform_trace(validated_trace)
        if not transformed_logs:
            return None

        # 3. Enrichment: trace-level features (fingerprint, outcome, severity, keywords)
        enrichment = self.enrichment.enrich_trace(transformed_logs)

        # 4. Gatekeeper: decide PROMOTE or DROP
        if self.gatekeeper:
            verdict = self.gatekeeper.evaluate(enrichment, project_id)

            if not verdict.is_promote:
                logger.debug(
                    "DROPPED trace %s — %s", cid, verdict.reason,
                )
                # If this is a merge and gatekeeper drops it, clean up old partial doc
                if is_merge and self.vector_store:
                    collection_name = f"{project_id}_logs"
                    self.vector_store.delete_by_cid(collection_name, cid)
                    logger.info("MERGE DROP — deleted old partial for %s", cid)
                return None

            logger.info(
                "PROMOTED trace %s — [%s] %s", cid, verdict.tier, verdict.reason,
            )

        # 5. Deduplication: check if fingerprint already in Qdrant
        #    Skip for merges — we're replacing, not deduping
        collection_name = f"{project_id}_logs"
        fingerprint = enrichment.get("fingerprint", "")

        if not is_merge:
            if self.vector_store and fingerprint:
                if self.vector_store.fingerprint_exists(collection_name, fingerprint):
                    self.vector_store.increment_occurrence(collection_name, fingerprint)
                    logger.info(
                        "DEDUP — trace %s, fingerprint %s already in %s, count++",
                        cid, fingerprint, collection_name,
                    )
                    return None

        # 6. Indexing Prep: semantic text + vector metadata → TraceDocument
        trace_doc = self.indexing_prep.prepare_trace_document(
            enrichment["sorted_logs"],
            enrichment,
        )

        # 7. Vector Store: for merges, delete old partial first, then upsert
        if self.vector_store:
            if is_merge:
                self.vector_store.delete_by_cid(collection_name, cid)
                logger.info("MERGE UPSERT — replacing CID %s with complete trace", cid)
            self.vector_store.upsert_traces([trace_doc], collection_name=collection_name)

        return trace_doc

    # ── Batch Entry Point (with fault tolerance) ──────────────────────

    def process_batch(self, traces: List[Dict[str, Any]]) -> BatchResult:
        """Processes traces with inline retry, circuit breaker, and poison-pill isolation."""
        promoted = []
        failed_cids = []
        dropped = 0

        for trace in traces:
            cid = trace.get("correlationId", "unknown")

            # Skip known poison pills
            if self.poison_pills and self.poison_pills.is_poison_pill(cid):
                logger.warning("POISON PILL — skipping %s", cid)
                continue

            # Inline retry with exponential backoff
            doc = None
            last_error = None
            max_attempts = self.config.retry_max_attempts if self.config else 1
            backoff_base = self.config.retry_backoff_base if self.config else 1.0

            for attempt in range(max_attempts):
                try:
                    doc = self.process_trace(trace)
                    last_error = None
                    break  # success
                except Exception as e:
                    last_error = e
                    if attempt < max_attempts - 1:
                        wait = backoff_base * (2 ** attempt)
                        logger.warning(
                            "Retry %d/%d for %s in %.1fs: %s",
                            attempt + 1, max_attempts, cid, wait, e,
                        )
                        time.sleep(wait)

            if last_error and doc is None:
                # All retries exhausted
                logger.error(
                    "FAILED trace %s after %d attempts: %s",
                    cid, max_attempts, last_error,
                )
                failed_cids.append(cid)

                if self.poison_pills:
                    became_poison = self.poison_pills.record_failure(cid, str(last_error))
                    if became_poison:
                        logger.critical(
                            "POISON PILL DETECTED — %s will be skipped in future cycles", cid,
                        )

                if self.circuit_breaker:
                    self.circuit_breaker.record_failure()
                    if self.circuit_breaker.is_tripped():
                        logger.critical(
                            "CIRCUIT BREAKER TRIPPED — %d failures this cycle, pausing",
                            self.circuit_breaker._cycle_failures,
                        )
                        break  # stop processing remaining traces
            elif doc:
                promoted.append(doc)
            else:
                dropped += 1  # gatekeeper dropped or deduped

        logger.info(
            "Batch complete — promoted: %d, dropped/deduped: %d, failed: %d, total: %d",
            len(promoted), dropped, len(failed_cids), len(traces),
        )

        return BatchResult(
            promoted=promoted,
            dropped=dropped,
            failed_cids=failed_cids,
            circuit_breaker_tripped=(
                self.circuit_breaker.is_tripped() if self.circuit_breaker else False
            ),
        )

    def run(self) -> None:
        """
        Starts the scheduled pipeline loop.
        Fetches batches from ClickHouse via clickhouse-connect, processes, and stores in VectorDB.
        """
        if not self.config:
            raise ValueError("Config is required for scheduled mode. Pass config to __init__.")

        fetcher = ClickHouseFetcherService(self.config)
        self._fetcher = fetcher
        interval_sec = self.config.fetch_interval_minutes * 60

        logger.info(
            f"Pipeline scheduler started — fetching every {self.config.fetch_interval_minutes} min "
            f"(batch size: {self.config.fetch_batch_size})"
        )

        while True:
            # Circuit breaker check
            if self.circuit_breaker:
                self.circuit_breaker.reset_cycle()
                if not self.circuit_breaker.should_attempt():
                    logger.warning(
                        "Circuit breaker OPEN — sleeping %d min",
                        self.config.circuit_breaker_cooldown_minutes,
                    )
                    time.sleep(self.config.circuit_breaker_cooldown_minutes * 60)
                    continue

            # Snapshot cutoff: process everything up to NOW, then sleep
            cutoff = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            logger.info("Drain cycle started — cutoff: %s", cutoff)

            try:
                batches_processed = 0

                while True:
                    watermark = fetcher.load_watermark()

                    # Stop draining if watermark has caught up to cutoff
                    if watermark >= cutoff:
                        logger.info("Watermark %s >= cutoff %s — caught up", watermark, cutoff)
                        break

                    flat_rows = fetcher.fetch_batch(watermark)

                    if not flat_rows:
                        logger.info("No more rows before cutoff. Done draining.")
                        break

                    traces = fetcher.reassemble_traces(flat_rows)

                    # ── Merge-on-duplicate: detect partial traces ─────
                    batch_cids = [t.get("correlationId", "") for t in traces]
                    batch_counts = {
                        t.get("correlationId", ""): len(t.get("logs", []))
                        for t in traces
                    }
                    ch_counts = fetcher.bulk_count_cids(batch_cids)

                    for i, trace in enumerate(traces):
                        cid = trace.get("correlationId", "")
                        if ch_counts.get(cid, 0) > batch_counts.get(cid, 0):
                            logger.info(
                                "PARTIAL TRACE — CID %s: %d rows in batch, %d in ClickHouse. Re-fetching.",
                                cid, batch_counts[cid], ch_counts[cid],
                            )
                            full_rows = fetcher.fetch_rows_for_cid(cid)
                            full_traces = fetcher.reassemble_traces(full_rows)
                            if full_traces:
                                traces[i] = full_traces[0]
                                traces[i]["_is_merge"] = True

                    result = self.process_batch(traces)
                    batches_processed += 1

                    logger.info(
                        "Batch %d — promoted: %d, dropped: %d, failed: %d",
                        batches_processed, len(result.promoted), result.dropped, len(result.failed_cids),
                    )

                    if not result.circuit_breaker_tripped:
                        if result.failed_cids:
                            new_wm = fetcher.compute_safe_watermark(
                                flat_rows, watermark, result.failed_cids,
                            )
                        else:
                            new_wm = fetcher.compute_new_watermark(flat_rows, watermark)
                        fetcher.save_watermark(new_wm)

                        # Successful cycle — close breaker if it was half-open
                        if self.circuit_breaker:
                            self.circuit_breaker.close()
                    else:
                        logger.critical("Watermark NOT advanced — circuit breaker tripped")
                        break  # stop draining on circuit breaker trip

                logger.info(
                    "Drain complete — %d batch(es) processed. Sleeping %d min.",
                    batches_processed, self.config.fetch_interval_minutes,
                )

            except Exception as e:
                logger.error(f"Batch cycle failed: {e}", exc_info=True)

            time.sleep(interval_sec)
