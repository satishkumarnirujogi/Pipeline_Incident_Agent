"""Incident state and the shared vocabulary of the agent system.

Phase 3 defines the root-cause categories; IncidentState and the Pydantic
models for diagnosis and actions arrive in Phase 4.
"""

from enum import StrEnum


class RootCause(StrEnum):
    """The Diagnostics agent must answer with exactly one of these."""

    SCHEMA_DRIFT = "SCHEMA_DRIFT"
    TYPE_MISMATCH = "TYPE_MISMATCH"
    MISSING_SOURCE = "MISSING_SOURCE"
    NULL_SPIKE = "NULL_SPIKE"
    DUPLICATE_LOAD = "DUPLICATE_LOAD"
    OUT_OF_RANGE_VALUES = "OUT_OF_RANGE_VALUES"
    ORPHAN_KEYS = "ORPHAN_KEYS"
    UPSTREAM_API_OUTAGE = "UPSTREAM_API_OUTAGE"
    INCOMPLETE_API_PAYLOAD = "INCOMPLETE_API_PAYLOAD"
    CREDENTIALS = "CREDENTIALS"
