"""Policy engine — core decision function mapping events to actions."""

from __future__ import annotations

import json

from agent_service.memory.memory_store import MemoryStore, make_signature
from agent_service.decision.cost_estimator import estimate_cost


def decide_action(event: dict, memory: MemoryStore, policy: dict) -> tuple[str, dict]:
    """Returns (action_name, payload_dict)."""
    severity = event["agent_severity"]
    signature = make_signature(event)

    # Step 1: Check human_forced memory
    mem = memory.get(signature)
    if mem and mem.get("human_forced"):
        return "human_escalation", {"reason": "human_forced_pattern", "signature": signature}

    # Step 2: Memory hit — reuse successful past action
    if mem:
        s_count = mem.get("success_count", 0) or 0
        f_count = mem.get("failure_count", 0) or 0
        total = s_count + f_count
        if total > 0 and (s_count / total) > policy["memory_confidence_threshold"]:
            avg_cost = (mem.get("total_llm_cost", 0) or 0) / max(total, 1)
            if avg_cost < policy["max_cost_per_action"]:
                try:
                    payload = json.loads(mem.get("action_payload") or "{}")
                except (json.JSONDecodeError, TypeError):
                    payload = {}
                return mem["action_taken"], payload

    # Step 3: CRITICAL → always escalate
    if severity == "CRITICAL":
        return "human_escalation", {
            "reason": f"CRITICAL: {event['event_type']} on {event['domain']}",
            "deviation_pct": event.get("deviation_pct"),
            "metric_name": event.get("metric_name"),
            "reference_ids": event.get("reference_ids"),
        }

    # Step 4: HIGH → attempt fix
    if severity == "HIGH":
        if event["event_type"] in ("COLUMN_MISSING", "COLUMN_UNEXPECTED", "DTYPE_MISMATCH"):
            return "auto_fix", {
                "fix_type": "schema_correction",
                "event_type": event["event_type"],
                "domain": event["domain"],
                "detail": event.get("detail"),
            }
        if event["event_type"] == "CONSTRAINT_VIOLATION":
            if policy.get("allow_llm"):
                if estimate_cost(event) < policy["max_cost_per_action"]:
                    return "llm_fix", {"event": event}
        # HIGH NULL_FIELD → try LLM
        if event["event_type"] == "NULL_FIELD":
            if policy.get("allow_llm"):
                if estimate_cost(event) < policy["max_cost_per_action"]:
                    return "llm_fix", {"event": event}
        return "human_escalation", {
            "reason": f"HIGH severity {event['event_type']} — no deterministic fix",
        }

    # Step 5: MEDIUM → retry or flag
    if severity == "MEDIUM":
        if event["event_type"] in ("CRM_LAG_DETECTED", "ROW_COUNT_DROP", "LATE_DATA_DETECTED"):
            return "auto_retry", {"wait_seconds": policy["retry_wait"], "domain": event["domain"]}
        return "noop", {"reason": "MEDIUM severity flagged and logged"}

    # Step 6: LOW → noop
    return "noop", {"reason": "LOW severity — logged only"}
