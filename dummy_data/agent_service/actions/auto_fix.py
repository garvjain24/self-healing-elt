"""Auto-fix handler — deterministic schema/type corrections."""

from __future__ import annotations

import json
import logging

from agent_service.db.connection import get_conn
from agent_service.memory.memory_store import MemoryStore, make_signature

logger = logging.getLogger(__name__)


def handle_auto_fix(event: dict, payload: dict) -> bool:
    """
    Deterministic fixes for schema and type issues.
    Agent cannot modify pipeline DB — stores correction records in its own DB.
    Also writes to human_review_queue with status='auto_resolved' for audit.
    """
    fix_type = payload.get("fix_type", "schema_correction")
    detail = payload.get("detail") or {}
    # detail can be a JSON string from the API — parse it
    if isinstance(detail, str):
        try:
            detail = json.loads(detail)
        except (json.JSONDecodeError, TypeError):
            detail = {"raw": detail}

    if fix_type == "schema_correction":
        if event["event_type"] == "COLUMN_MISSING":
            fix_payload = {
                "column_expected": detail.get("column", "unknown"),
                "domain": event["domain"],
                "action": "map_renamed_column",
            }
        elif event["event_type"] == "COLUMN_UNEXPECTED":
            fix_payload = {
                "column_actual": detail.get("column", "unknown"),
                "domain": event["domain"],
                "action": "ignore_or_map_column",
            }
        elif event["event_type"] == "DTYPE_MISMATCH":
            fix_payload = {
                "field": detail.get("field", "unknown"),
                "from_type": "str",
                "to_type": "float",
                "action": "cast_correction",
            }
        else:
            fix_payload = {"action": "generic_schema_fix", "detail": detail}
    else:
        fix_payload = {"action": fix_type, "detail": detail}

    # Store in memory
    memory = MemoryStore()
    signature = make_signature(event)
    memory.update(
        signature=signature,
        domain=event["domain"],
        event_type=event["event_type"],
        action="auto_fix",
        payload=fix_payload,
        success=True,
        llm_cost=0,
    )

    # Write to human_review_queue with status='auto_resolved' for audit
    conn = get_conn()
    conn.execute(
        """INSERT INTO human_review_queue
           (pipeline_event_id, date, domain, event_type, agent_severity,
            evidence, suggested_fix, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'auto_resolved')""",
        [
            event.get("id"),
            event.get("date"),
            event.get("domain"),
            event.get("event_type"),
            event.get("agent_severity"),
            json.dumps(event, default=str),
            json.dumps(fix_payload),
        ],
    )
    conn.close()

    logger.info(
        "[AUTO_FIX] %s on %s — fix_type=%s payload=%s",
        event.get("event_type"), event.get("domain"), fix_type, fix_payload,
    )
    return True
