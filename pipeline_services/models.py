"""Pydantic models for the LogiScout log processing pipeline."""

from typing import List, Optional, Dict, Any
from enum import Enum
from pydantic import BaseModel, validator


# ============================================ #
#  Input Schemas (from application traces)     #
# ============================================ #

class LogLevels(str, Enum):
    INFO = "info"
    WARN = "warn"
    ERROR = "error"
    DEBUG = "debug"
    FATAL = "fatal"


# Canonical mapping: all known variants → enum value
_LEVEL_MAP = {
    "info": LogLevels.INFO,
    "warn": LogLevels.WARN,
    "warning": LogLevels.WARN,
    "error": LogLevels.ERROR,
    "debug": LogLevels.DEBUG,
    "fatal": LogLevels.FATAL,
    "critical": LogLevels.FATAL,
}


class RequestInfo(BaseModel):
    method: Optional[str] = None
    path: Optional[str] = None
    statusCode: Optional[int] = None


class RawLogEntry(BaseModel):
    timestamp: str
    level: LogLevels
    message: str
    meta: Optional[Dict[str, Any]] = None
    loggername: str
    exception: Optional[Dict[str, Any]] = None

    @validator("level", pre=True)
    def normalize_level(cls, v):
        """Accepts INFO/info/Info, FATAL/fatal/CRITICAL/critical etc."""
        if isinstance(v, LogLevels):
            return v
        key = str(v).strip().lower()
        mapped = _LEVEL_MAP.get(key)
        if mapped is not None:
            return mapped
        raise ValueError(f"Unknown log level: {v!r}. Expected one of: {list(_LEVEL_MAP.keys())}")


class RawTrace(BaseModel):
    projectName: str
    project_id: str
    environment: str
    correlationId: str
    component: Optional[str] = None
    startedAt: str
    endedAt: Optional[str] = None
    durationMs: Optional[int] = None
    request: Optional[RequestInfo] = None
    logs: List[RawLogEntry]


# ============================================ #
#  Output Schema (vector-ready trace document) #
# ============================================ #

class TraceDocument(BaseModel):
    """
    Final vector-ready document. One per correlation ID (trace).

    This replaces the old per-log EnrichedDocument.
    """
    # Identity
    correlation_id: str
    project_name: str
    project_id: str

    # Pre-built natural-language string optimized for embedding
    semantic_text: str

    # Flat metadata dict for Qdrant filterable payload
    vector_metadata: Dict[str, Any]
