"""Severity scorer — maps neutral pipeline facts to agent severity + risk score."""

from __future__ import annotations

EVENT_TYPE_SCORES = {
    "KPI_DEVIATION": 5,
    "CONSTRAINT_VIOLATION": 4,
    "FUNNEL_VIOLATION": 3,
    "DTYPE_MISMATCH": 6,
    "COLUMN_MISSING": 6,
    "DUPLICATE_ROWS": 2,
    "NULL_FIELD": 2,
    "ROW_COUNT_DROP": 2,
    "CRM_LAG_DETECTED": 2,
    "COLUMN_UNEXPECTED": 1,
    "ROW_COUNT_ZERO": 4,
    "LATE_DATA_DETECTED": 1,
}


def score_event(event: dict) -> tuple[int, str]:
    """Returns (risk_score, agent_severity)."""
    score = 0

    # Event type weight
    score += EVENT_TYPE_SCORES.get(event.get("event_type", ""), 1)

    # Deviation magnitude
    dev = abs(event.get("deviation_pct") or 0)
    if dev > 100:
        score += 5
    elif dev > 50:
        score += 3
    elif dev > 10:
        score += 1

    # Finance domain multiplier
    if event.get("domain") == "finance":
        score += 5

    # Repeat failures
    repeats = event.get("historical_count", 0)
    if repeats >= 3:
        score += 3
    elif repeats >= 1:
        score += 1

    # Map to severity
    if score >= 10:
        severity = "CRITICAL"
    elif score >= 6:
        severity = "HIGH"
    elif score >= 3:
        severity = "MEDIUM"
    else:
        severity = "LOW"

    return score, severity
