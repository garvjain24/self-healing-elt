"""Context builder — enriches events with historical counts and metrics."""

from __future__ import annotations

from datetime import date as _date, timedelta

from agent_service.db.connection import get_conn
from agent_service.memory.memory_store import make_signature


def build_context(event: dict, fetcher) -> dict:
    """Add historical_count, repeat_failures, and metrics_context to event."""
    enriched = dict(event)
    date = event.get("date", "")

    # Historical count: how many times this (domain, event_type) appeared in last 7 days
    conn = get_conn()
    try:
        if isinstance(date, str):
            ref_date = _date.fromisoformat(date)
        else:
            ref_date = date
        week_ago = ref_date - timedelta(days=7)

        row = conn.execute(
            "SELECT COUNT(*) FROM agent_processed_events WHERE domain = ? AND event_type = ? AND date >= ?",
            [event.get("domain"), event.get("event_type"), week_ago.isoformat()],
        ).fetchone()
        enriched["historical_count"] = row[0] if row else 0
    except Exception:
        enriched["historical_count"] = 0

    # Repeat failures from memory
    try:
        signature = make_signature(event)
        row = conn.execute(
            "SELECT failure_count FROM agent_memory WHERE signature = ?",
            [signature],
        ).fetchone()
        enriched["repeat_failures"] = row[0] if row else 0
    except Exception:
        enriched["repeat_failures"] = 0

    conn.close()

    # Metrics context from fetcher
    try:
        context = fetcher.fetch_event_context(event)
        enriched["metrics_context"] = context.get("metrics_context", [])
        enriched["staged_context"] = context.get("staged_context", [])
    except Exception:
        enriched["metrics_context"] = []
        enriched["staged_context"] = []

    return enriched
