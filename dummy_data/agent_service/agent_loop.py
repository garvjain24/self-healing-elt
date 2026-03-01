"""Agent main polling loop."""

from __future__ import annotations

import json
import logging
import time

from agent_service.config import POLICY
from agent_service.db.connection import get_conn, init_db
from agent_service.observer.event_fetcher import EventFetcher
from agent_service.observer.context_builder import build_context
from agent_service.interpreter.severity_scorer import score_event
from agent_service.memory.memory_store import MemoryStore, make_signature
from agent_service.decision.policy_engine import decide_action
from agent_service.actions.retry import handle_auto_retry
from agent_service.actions.auto_fix import handle_auto_fix
from agent_service.actions.llm_fix import handle_llm_fix
from agent_service.actions.escalate import handle_human_escalation

logger = logging.getLogger(__name__)


def dispatch_action(action: str, event: dict, payload: dict, policy: dict) -> tuple[bool, float]:
    """Execute the chosen action, return (success, llm_cost)."""
    if action == "auto_retry":
        return handle_auto_retry(event, payload), 0.0
    elif action == "auto_fix":
        return handle_auto_fix(event, payload), 0.0
    elif action == "llm_fix":
        return handle_llm_fix(event, payload, policy)
    elif action == "human_escalation":
        return handle_human_escalation(event, payload), 0.0
    elif action == "noop":
        logger.info("[NOOP] %s on %s — %s", event.get("event_type"), event.get("domain"), payload.get("reason", ""))
        return True, 0.0
    return False, 0.0


def record_processed_event(
    event: dict, action: str, payload: dict,
    severity: str, risk_score: int, success: bool, llm_cost: float,
) -> None:
    """Write to agent_processed_events."""
    conn = get_conn()
    conn.execute(
        """INSERT INTO agent_processed_events
           (pipeline_event_id, date, domain, event_type, agent_severity,
            risk_score, action_taken, action_payload, action_success, llm_cost)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            event.get("id"),
            event.get("date"),
            event.get("domain"),
            event.get("event_type"),
            severity,
            risk_score,
            action,
            json.dumps(payload, default=str),
            success,
            llm_cost,
        ],
    )
    conn.close()


def write_audit_log(
    pipeline_event_id: int, action: str, success: bool, llm_cost: float,
    memory_hit: bool = False, llm_used: bool = False, llm_confidence: float | None = None,
) -> None:
    conn = get_conn()
    conn.execute(
        """INSERT INTO agent_audit_log
           (pipeline_event_id, action, memory_hit, llm_used, llm_cost,
            llm_confidence, success, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            pipeline_event_id, action, memory_hit, llm_used,
            llm_cost, llm_confidence, success,
            f"action={action} success={success}",
        ],
    )
    conn.close()


def process_events_once(
    fetcher: EventFetcher, memory: MemoryStore, policy: dict,
    date_range_days: int = 7,
    start_date: str | None = None,
    end_date: str | None = None,
) -> int:
    """Process one batch of unprocessed events. Returns count processed."""
    raw_events = fetcher.fetch_unprocessed(
        date_range_days=date_range_days,
        start_date=start_date,
        end_date=end_date,
    )
    if not raw_events:
        return 0

    processed = 0
    for raw_event in raw_events:
        # Build context
        event = build_context(raw_event, fetcher)

        # Score severity
        risk_score, agent_severity = score_event(event)
        event["risk_score"] = risk_score
        event["agent_severity"] = agent_severity

        # Check memory hit
        signature = make_signature(event)
        mem = memory.get(signature)
        memory_hit = mem is not None and (mem.get("success_count", 0) or 0) > 0

        # Decide action
        action, payload = decide_action(event, memory, policy)

        # Is this an LLM call?
        llm_used = action == "llm_fix"

        # Execute
        success, llm_cost = dispatch_action(action, event, payload, policy)

        # Record
        record_processed_event(event, action, payload, agent_severity, risk_score, success, llm_cost)

        # Update memory (not for noop)
        if action != "noop":
            memory.update(
                signature, event["domain"], event["event_type"],
                action, payload, success, llm_cost,
            )

        # Audit log
        write_audit_log(
            event.get("id"), action, success, llm_cost,
            memory_hit=memory_hit, llm_used=llm_used,
        )

        processed += 1
        logger.info(
            "  → [%s] %s on %s.%s | risk=%d severity=%s | success=%s cost=$%.4f",
            action.upper(), event.get("event_type"), event.get("domain"),
            event.get("metric_name", ""), risk_score, agent_severity,
            success, llm_cost,
        )

    return processed


def run_agent_loop(poll_interval: int | None = None):
    """Main polling loop — runs until interrupted."""
    if poll_interval is None:
        poll_interval = POLICY["poll_interval"]

    init_db()
    fetcher = EventFetcher()
    memory = MemoryStore()
    policy = POLICY

    print(f"\n{'='*60}")
    print(f"  AGENT SERVICE — polling every {poll_interval}s")
    print(f"  Pipeline API: {fetcher.warehouse_url}")
    print(f"{'='*60}\n")

    cycle = 0
    while True:
        cycle += 1
        count = process_events_once(fetcher, memory, policy, date_range_days=policy["date_range_days"])
        if count > 0:
            print(f"  Cycle {cycle}: processed {count} events")
        time.sleep(poll_interval)
