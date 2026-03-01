"""Memory store — persistent experience for the agent."""

from __future__ import annotations

import json

from agent_service.db.connection import get_conn


def make_signature(event: dict) -> str:
    """Deterministic key: event_type:domain:pipeline_stage:metric_name."""
    metric = event.get("metric_name") or "none"
    stage = event.get("pipeline_stage") or "none"
    return f"{event['event_type']}:{event['domain']}:{stage}:{metric}"


class MemoryStore:
    def get(self, signature: str) -> dict | None:
        conn = get_conn()
        row = conn.execute(
            "SELECT * FROM agent_memory WHERE signature = ?", [signature]
        ).fetchone()
        if not row:
            conn.close()
            return None
        columns = [desc[0] for desc in conn.description]
        conn.close()
        result = dict(zip(columns, row))
        # Ensure action_payload is a string for json.loads later
        if result.get("action_payload") and not isinstance(result["action_payload"], str):
            result["action_payload"] = json.dumps(result["action_payload"])
        return result

    def update(
        self,
        signature: str,
        domain: str,
        event_type: str,
        action: str,
        payload: dict,
        success: bool,
        llm_cost: float = 0,
    ) -> None:
        conn = get_conn()
        payload_json = json.dumps(payload) if payload else "{}"

        existing = conn.execute(
            "SELECT id, success_count, failure_count, total_llm_cost FROM agent_memory WHERE signature = ?",
            [signature],
        ).fetchone()

        if existing:
            mem_id, s_count, f_count, prev_cost = existing
            new_s = (s_count or 0) + (1 if success else 0)
            new_f = (f_count or 0) + (0 if success else 1)
            conn.execute(
                """UPDATE agent_memory
                   SET action_taken = ?, action_payload = ?,
                       success_count = ?, failure_count = ?,
                       total_llm_cost = ?, last_used = current_timestamp
                   WHERE id = ?""",
                [action, payload_json, new_s, new_f,
                 (prev_cost or 0) + llm_cost, mem_id],
            )
        else:
            # New entry — generate id from max+1
            max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM agent_memory").fetchone()[0]
            conn.execute(
                """INSERT INTO agent_memory
                   (id, signature, domain, event_type, action_taken, action_payload,
                    success_count, failure_count, total_llm_cost, last_used)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)""",
                [max_id + 1, signature, domain, event_type, action, payload_json,
                 1 if success else 0, 0 if success else 1, llm_cost],
            )
        conn.close()

    def success_rate(self, signature: str) -> float:
        conn = get_conn()
        row = conn.execute(
            "SELECT success_count, failure_count FROM agent_memory WHERE signature = ?",
            [signature],
        ).fetchone()
        conn.close()
        if not row:
            return 0.0
        s, f = row[0] or 0, row[1] or 0
        total = s + f
        return s / total if total > 0 else 0.0

    def mark_human_forced(self, signature: str) -> None:
        conn = get_conn()
        conn.execute(
            "UPDATE agent_memory SET human_forced = true WHERE signature = ?",
            [signature],
        )
        conn.close()
