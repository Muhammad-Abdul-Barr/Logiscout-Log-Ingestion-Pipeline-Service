from typing import Dict, Any
from .models import RawTrace

class IngestionService:
    """Ingestion Layer: Validates raw input dictionaries against expected Pydantic schemas."""
    
    def validate_trace(self, raw_trace_dict: Dict[str, Any]) -> RawTrace:
        """Parses and validates the raw JSON payload into a RawTrace object."""
        return RawTrace(**raw_trace_dict)
