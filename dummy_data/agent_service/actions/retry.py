"""Auto-retry handler — for late data, CRM lag, row count drops."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def handle_auto_retry(event: dict, payload: dict) -> bool:
    """
    For CRM_LAG_DETECTED, ROW_COUNT_DROP etc:
    Log that we're waiting for late data. The agent doesn't control the pipeline.
    On next poll cycle, the event_fetcher checks if new quality events of the
    same type/date/domain exist — if not, data arrived and event is resolved.
    """
    domain = payload.get("domain", event.get("domain", "unknown"))
    wait = payload.get("wait_seconds", 15)

    logger.info(
        "[AUTO_RETRY] %s on %s (date=%s) — waiting for late data, re-check in %ds",
        event.get("event_type"), domain, event.get("date"), wait,
    )
    # The wait IS the action — returning True means the retry was logged
    return True
