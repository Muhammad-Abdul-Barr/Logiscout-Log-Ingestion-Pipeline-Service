"""
Gatekeeper Service: Decides which traces get promoted from OLAP to Qdrant.

Sits between Enrichment and Indexing Prep. Evaluates promotion tiers in order
of cost (cheap deterministic checks first, expensive Qdrant lookups last).
Returns a verdict: PROMOTE or DROP.

95% of production traffic is healthy. This service keeps it out of Qdrant.
"""

import logging
from dataclasses import dataclass
from typing import Optional, Set

logger = logging.getLogger(__name__)


@dataclass
class GatekeeperVerdict:
    """Result of a gatekeeper evaluation."""
    action: str     # "PROMOTE" or "DROP"
    tier: str       # which tier triggered the decision
    reason: str     # human-readable explanation

    @property
    def is_promote(self) -> bool:
        return self.action == "PROMOTE"


def _promote(tier: str, reason: str) -> GatekeeperVerdict:
    return GatekeeperVerdict(action="PROMOTE", tier=tier, reason=reason)


def _drop(reason: str) -> GatekeeperVerdict:
    return GatekeeperVerdict(action="DROP", tier="default", reason=reason)


class GatekeeperService:
    """
    Evaluates whether a trace should be promoted to Qdrant or dropped.

    Tier evaluation order (cheapest first):
        Tier 0 — Severity gate (deterministic, zero history)
        Tier 1 — Content scan (deterministic, zero history)
        Tier 5 — Cold start (Qdrant lookup — checked before 2/3 because they need history)
        Tier 2 — Deployment window (Qdrant lookup for recent commits)
        Tier 3 — Fingerprint change detection (Qdrant lookup for known fingerprints)
        Default — DROP
    """

    def __init__(self, config, vector_store=None):
        self.config = config
        self.vector_store = vector_store

    # ── Main Entry Point ──────────────────────────────────────────────

    def evaluate(self, enrichment: dict, project_id: str) -> GatekeeperVerdict:
        """
        Evaluates promotion tiers in order. Returns on first match.

        Args:
            enrichment: Dict from EnrichmentService.enrich_trace()
            project_id: Project identifier for Qdrant collection routing

        Returns:
            GatekeeperVerdict with action, tier, and reason
        """
        # ── Tier 0: Severity Gate ─────────────────────────────────────
        verdict = self._tier0_severity_gate(enrichment)
        if verdict:
            return verdict

        # ── Tier 1: Content Scan ──────────────────────────────────────
        verdict = self._tier1_content_scan(enrichment)
        if verdict:
            return verdict

        # ── Tiers requiring Qdrant lookups ────────────────────────────
        if self.vector_store:
            collection_name = f"{project_id}_logs"
            request_path = enrichment.get("request_path_pattern")
            fingerprint = enrichment.get("fingerprint", "")

            # ── Tier 5: Cold Start (before 2/3 — they need history) ───
            verdict = self._tier5_cold_start(collection_name, request_path)
            if verdict:
                return verdict

            # ── Tier 2: Deployment Window ─────────────────────────────
            verdict = self._tier2_deploy_window(
                enrichment, project_id, collection_name, fingerprint, request_path,
            )
            if verdict:
                return verdict

            # ── Tier 3: Fingerprint Change Detection ──────────────────
            verdict = self._tier3_fingerprint_change(
                collection_name, fingerprint, request_path,
            )
            if verdict:
                return verdict

        # ── Default: DROP ─────────────────────────────────────────────
        return _drop("healthy_traffic — no promotion criteria met")

    # ── Tier 0: Severity Gate ─────────────────────────────────────────

    def _tier0_severity_gate(self, enrichment: dict) -> Optional[GatekeeperVerdict]:
        """
        Deterministic, zero history. Catches explicit errors.
        """
        severity = enrichment.get("severity_score", 0)
        outcome = enrichment.get("outcome", "")
        has_errors = enrichment.get("has_errors", False)

        if severity >= self.config.severity_threshold:
            return _promote("tier0_severity", f"severity_score={severity} >= {self.config.severity_threshold}")

        if outcome in ("server_error", "failure"):
            return _promote("tier0_outcome", f"outcome={outcome}")

        if has_errors:
            return _promote("tier0_has_errors", "trace contains ERROR/FATAL logs")

        return None

    # ── Tier 1: Content Scan ──────────────────────────────────────────

    def _tier1_content_scan(self, enrichment: dict) -> Optional[GatekeeperVerdict]:
        """
        Deterministic, zero history. Catches error types and suspicious keywords
        that may appear in INFO/DEBUG logs.
        """
        error_types = enrichment.get("error_types", [])
        suspicious = enrichment.get("suspicious_keywords", [])

        if error_types:
            return _promote("tier1_error_types", f"error_types detected: {error_types}")

        if suspicious:
            return _promote("tier1_keywords", f"suspicious keywords: {suspicious}")

        return None

    # ── Tier 2: Deployment Window ─────────────────────────────────────

    def _tier2_deploy_window(
        self,
        enrichment: dict,
        project_id: str,
        collection_name: str,
        fingerprint: str,
        request_path: Optional[str],
    ) -> Optional[GatekeeperVerdict]:
        """
        Checks if a recent deployment happened for this project.
        If yes, applies relaxed promotion criteria.
        """
        import time

        commits_collection = f"{project_id}_commits"
        latest_commit_ts = self.vector_store.get_latest_commit_timestamp(commits_collection)

        if latest_commit_ts is None:
            return None  # no commit history — skip this tier

        now = int(time.time())
        window_seconds = self.config.deploy_window_minutes * 60
        time_since_deploy = now - latest_commit_ts

        if time_since_deploy > window_seconds:
            return None  # outside deployment window

        # Inside window — relax criteria
        outcome = enrichment.get("outcome", "success")
        if outcome != "success":
            return _promote(
                "tier2_deploy_window",
                f"outcome={outcome} within {self.config.deploy_window_minutes}min of deploy",
            )

        # Check if fingerprint is new (not in known set for this endpoint)
        if request_path:
            known = self.vector_store.get_known_fingerprints(collection_name, request_path)
            if fingerprint not in known:
                return _promote(
                    "tier2_deploy_new_fp",
                    f"new fingerprint {fingerprint} within deploy window",
                )

        return None

    # ── Tier 3: Fingerprint Change Detection ──────────────────────────

    def _tier3_fingerprint_change(
        self,
        collection_name: str,
        fingerprint: str,
        request_path: Optional[str],
    ) -> Optional[GatekeeperVerdict]:
        """
        Checks if this fingerprint is new on a previously stable endpoint.
        This is the core change detection signal.
        """
        if not request_path:
            return None

        known = self.vector_store.get_known_fingerprints(collection_name, request_path)

        if not known:
            # No fingerprints at all — handled by tier 5 (cold start)
            return None

        if fingerprint not in known:
            return _promote(
                "tier3_new_fingerprint",
                f"new fingerprint {fingerprint} on stable endpoint {request_path} "
                f"(known: {len(known)} fingerprints)",
            )

        return None

    # ── Tier 5: Cold Start ────────────────────────────────────────────

    def _tier5_cold_start(
        self,
        collection_name: str,
        request_path: Optional[str],
    ) -> Optional[GatekeeperVerdict]:
        """
        For endpoints with no Qdrant history: promote to build a baseline.
        After enough fingerprints are known, switch to change-detection mode.
        """
        if not request_path:
            return None

        known = self.vector_store.get_known_fingerprints(collection_name, request_path)

        if len(known) < self.config.cold_start_baseline_count:
            return _promote(
                "tier5_cold_start",
                f"building baseline for {request_path} "
                f"({len(known)}/{self.config.cold_start_baseline_count} fingerprints known)",
            )

        return None
