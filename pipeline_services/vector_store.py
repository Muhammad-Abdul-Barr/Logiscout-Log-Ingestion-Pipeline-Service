"""Vector Store Service: Embeds trace semantic text and upserts into Qdrant."""

import logging
import uuid
import time
from typing import List, Set, Optional

from .models import TraceDocument

logger = logging.getLogger(__name__)

MODEL_NAME = "BAAI/bge-small-en-v1.5"


class QdrantUpsertError(Exception):
    """Raised when Qdrant upsert fails after all retries."""
    pass


class VectorStoreService:
    """Handles embedding generation and Qdrant upsert operations."""

    def __init__(self, config):
        self.config = config
        self._model = None
        self._client = None

    def get_embedding_model(self):
        """Loads the FastEmbed model (ONNX-based, no PyTorch required)."""
        if self._model is None:
            from fastembed import TextEmbedding

            self._model = TextEmbedding(model_name=MODEL_NAME)
            logger.info("FastEmbed model loaded: %s", MODEL_NAME)
        return self._model

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Vectorizes a list of semantic text strings."""
        model = self.get_embedding_model()
        embeddings = list(model.embed(texts, batch_size=64))
        return [e.tolist() for e in embeddings]

    def get_qdrant_client(self):
        """Creates and caches a Qdrant client connection."""
        if self._client is None:
            from qdrant_client import QdrantClient

            self._client = QdrantClient(url=self.config.qdrant_url)
            logger.info("Qdrant client connected: %s", self.config.qdrant_url)
        return self._client

    def health_check(self) -> bool:
        """Pings Qdrant to check connectivity. Used by circuit breaker probe."""
        try:
            client = self.get_qdrant_client()
            client.get_collections()
            return True
        except Exception:
            return False

    # ── Collection Setup ──────────────────────────────────────────────

    def ensure_collection(self, collection_name: str) -> None:
        """Creates the Qdrant collection if it doesn't exist, then ensures payload indexes."""
        from qdrant_client.models import Distance, VectorParams

        client = self.get_qdrant_client()

        collections = [c.name for c in client.get_collections().collections]
        if collection_name not in collections:
            client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(
                    size=self.config.vector_size,
                    distance=Distance.COSINE,
                ),
            )
            logger.info("Created Qdrant collection: %s", collection_name)
        else:
            logger.info("Qdrant collection already exists: %s", collection_name)

        self._ensure_payload_indexes(client, collection_name)

    def _ensure_payload_indexes(self, client, collection_name: str) -> None:
        """Creates payload indexes on fields used for filtering and grouping."""
        from qdrant_client.models import PayloadSchemaType

        indexed_fields = {
            "correlation_id":       PayloadSchemaType.KEYWORD,
            "project_name":         PayloadSchemaType.KEYWORD,
            "project_id":           PayloadSchemaType.KEYWORD,
            "services":             PayloadSchemaType.KEYWORD,
            "request_method":       PayloadSchemaType.KEYWORD,
            "request_path_pattern": PayloadSchemaType.KEYWORD,
            "request_status_code":  PayloadSchemaType.INTEGER,
            "outcome":              PayloadSchemaType.KEYWORD,
            "max_level":            PayloadSchemaType.KEYWORD,
            "fingerprint":          PayloadSchemaType.KEYWORD,
            "environment":          PayloadSchemaType.KEYWORD,
            "severity_score":       PayloadSchemaType.INTEGER,
            "duration_ms":          PayloadSchemaType.INTEGER,
            "timestamp_unix":       PayloadSchemaType.INTEGER,
            "occurrence_count":     PayloadSchemaType.INTEGER,
        }

        for field_name, field_type in indexed_fields.items():
            client.create_payload_index(
                collection_name=collection_name,
                field_name=field_name,
                field_schema=field_type,
            )

        logger.info(
            "Payload indexes ensured for %s field(s) on '%s'",
            len(indexed_fields), collection_name,
        )

    # ── Gatekeeper Support Methods ────────────────────────────────────

    def fingerprint_exists(self, collection_name: str, fingerprint: str) -> bool:
        """Checks if a trace with this fingerprint already exists in the collection."""
        from qdrant_client.models import Filter, FieldCondition, MatchValue

        client = self.get_qdrant_client()

        try:
            # Check if the collection even exists
            collections = [c.name for c in client.get_collections().collections]
            if collection_name not in collections:
                return False

            result = client.scroll(
                collection_name=collection_name,
                scroll_filter=Filter(
                    must=[FieldCondition(key="fingerprint", match=MatchValue(value=fingerprint))]
                ),
                limit=1,
            )
            points, _ = result
            return len(points) > 0
        except Exception as e:
            logger.warning("fingerprint_exists lookup failed: %s", e)
            return False

    def get_known_fingerprints(self, collection_name: str, request_path_pattern: str) -> Set[str]:
        """
        Returns the set of known fingerprints for a given request_path_pattern.
        Used by the gatekeeper for change detection and cold start checks.
        """
        from qdrant_client.models import Filter, FieldCondition, MatchValue

        client = self.get_qdrant_client()

        try:
            collections = [c.name for c in client.get_collections().collections]
            if collection_name not in collections:
                return set()

            points, _ = client.scroll(
                collection_name=collection_name,
                scroll_filter=Filter(
                    must=[FieldCondition(
                        key="request_path_pattern",
                        match=MatchValue(value=request_path_pattern),
                    )]
                ),
                limit=100,
                with_payload=["fingerprint"],
            )

            fingerprints = set()
            for point in points:
                fp = point.payload.get("fingerprint")
                if fp:
                    fingerprints.add(fp)

            return fingerprints
        except Exception as e:
            logger.warning("get_known_fingerprints lookup failed: %s", e)
            return set()

    def increment_occurrence(self, collection_name: str, fingerprint: str) -> bool:
        """
        Increments occurrence_count and updates last_seen for a fingerprint
        that already exists in Qdrant. Used for deduplication.
        """
        from qdrant_client.models import Filter, FieldCondition, MatchValue

        client = self.get_qdrant_client()

        try:
            points, _ = client.scroll(
                collection_name=collection_name,
                scroll_filter=Filter(
                    must=[FieldCondition(key="fingerprint", match=MatchValue(value=fingerprint))]
                ),
                limit=1,
                with_payload=["occurrence_count"],
            )

            if not points:
                return False

            point = points[0]
            current_count = point.payload.get("occurrence_count", 1)

            client.set_payload(
                collection_name=collection_name,
                payload={
                    "occurrence_count": current_count + 1,
                    "last_seen": int(time.time()),
                },
                points=[point.id],
            )

            logger.debug(
                "Dedup — fingerprint %s count: %d → %d",
                fingerprint, current_count, current_count + 1,
            )
            return True
        except Exception as e:
            logger.warning("increment_occurrence failed: %s", e)
            return False

    def get_latest_commit_timestamp(self, commits_collection: str) -> Optional[int]:
        """
        Queries the {project_id}_commits Qdrant collection to find the most
        recent commit timestamp. Used by the gatekeeper's deployment window tier.

        Returns Unix timestamp (int) or None if no commits exist.
        """
        from qdrant_client.models import Filter

        client = self.get_qdrant_client()

        try:
            collections = [c.name for c in client.get_collections().collections]
            if commits_collection not in collections:
                return None

            # Scroll with ordering by timestamp_unix descending, take 1
            points, _ = client.scroll(
                collection_name=commits_collection,
                limit=1,
                with_payload=["timestamp_unix"],
                order_by="timestamp_unix",
            )

            if not points:
                return None

            return points[0].payload.get("timestamp_unix")
        except Exception as e:
            logger.warning("get_latest_commit_timestamp failed: %s", e)
            return None

    # ── Merge-on-Duplicate Support ──────────────────────────────────

    def delete_by_cid(self, collection_name: str, correlation_id: str) -> None:
        """Deletes all points matching a correlationId.

        Called during merge-on-duplicate to remove old partial documents
        before upserting the complete trace. No-op if nothing exists.
        """
        from qdrant_client.models import Filter, FieldCondition, MatchValue

        client = self.get_qdrant_client()
        try:
            collections = [c.name for c in client.get_collections().collections]
            if collection_name not in collections:
                return

            client.delete(
                collection_name=collection_name,
                points_selector=Filter(
                    must=[FieldCondition(
                        key="correlation_id",
                        match=MatchValue(value=correlation_id),
                    )]
                ),
            )
            logger.info("Deleted existing point(s) for CID %s from %s",
                         correlation_id, collection_name)
        except Exception as e:
            logger.warning("delete_by_cid failed: %s", e)

    # ── Upsert Traces ────────────────────────────────────────────────

    def upsert_traces(self, trace_documents: List[TraceDocument], collection_name: str) -> int:
        """
        Embeds and upserts TraceDocuments into Qdrant.

        Each point represents one complete trace (correlation ID):
        - id: generated UUID
        - vector: embedding of the trace's semantic_text
        - payload: vector_metadata + semantic_text
        """
        from qdrant_client.models import PointStruct

        if not trace_documents:
            return 0

        client = self.get_qdrant_client()
        self.ensure_collection(collection_name)

        texts = [doc.semantic_text for doc in trace_documents]
        vectors = self.embed_batch(texts)

        points = []
        for doc, vector in zip(trace_documents, vectors):
            payload = {
                **doc.vector_metadata,
                "semantic_text": doc.semantic_text,
            }

            points.append(PointStruct(
                id=str(uuid.uuid4()),
                vector=vector,
                payload=payload,
            ))

        batch_size = self.config.upsert_batch_size
        for i in range(0, len(points), batch_size):
            batch = points[i:i + batch_size]
            last_err = None
            for attempt in range(3):
                try:
                    client.upsert(collection_name=collection_name, points=batch)
                    last_err = None
                    break
                except Exception as e:
                    last_err = e
                    wait = 1.0 * (2 ** attempt)
                    logger.warning("Qdrant upsert retry %d/3 in %.1fs: %s", attempt + 1, wait, e)
                    time.sleep(wait)
            if last_err:
                raise QdrantUpsertError(
                    f"Qdrant upsert failed after 3 attempts: {last_err}"
                ) from last_err

        logger.info(
            "Upserted %s trace(s) into Qdrant collection '%s'",
            len(points),
            collection_name,
        )
        return len(points)
