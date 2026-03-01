"""Human escalation handler — writes to the review queue."""

from __future__ import annotations

import json
import logging

from agent_service.db.connection import get_conn

logger = logging.getLogger(__name__)


def handle_human_escalation(event: dict, payload: dict) -> bool:
    """Write event to human_review_queue with status='pending'."""
    conn = get_conn()
    conn.execute(
        """INSERT INTO human_review_queue
           (pipeline_event_id, date, domain, event_type, agent_severity,
            evidence, suggested_fix, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')""",
        [
            event.get("id"),
            event.get("date"),
            event.get("domain"),
            event.get("event_type"),
            event.get("agent_severity"),
            json.dumps(event, default=str),
            json.dumps(payload, default=str),
        ],
    )
    conn.close()

    logger.info(
        "[ESCALATE] %s on %s (date=%s) severity=%s → pending human review",
        event.get("event_type"), event.get("domain"),
        event.get("date"), event.get("agent_severity"),
    )
    return True
