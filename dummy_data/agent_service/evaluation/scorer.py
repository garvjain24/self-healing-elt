"""Evaluation scorer — measures agent accuracy vs ground truth from scenario YAMLs."""

from __future__ import annotations

import os
from datetime import date as _date, timedelta

import yaml

from agent_service.config import SCENARIOS_DIR
from agent_service.db.connection import get_conn

# Mapping from scenario YAML expected_agent_action → agent action names
ACTION_MAP = {
    "auto_retry": "auto_retry",
    "flag_and_continue": "noop",
    "auto_fix": "auto_fix",
    "human_escalation": "human_escalation",
    "llm_fix": "llm_fix",
}

# Mapping from generator name to domain
GENERATOR_DOMAIN = {
    "ads": "ads",
    "analytics": "analytics",
    "crm": "crm",
    "finance": "finance",
}


def get_ground_truth(scenario_id: str) -> list[dict]:
    """Parse scenario YAML, return list of expected failures with actions."""
    yaml_path = os.path.join(SCENARIOS_DIR, f"{scenario_id}.yaml")
    if not os.path.exists(yaml_path):
        raise FileNotFoundError(f"Scenario YAML not found: {yaml_path}")

    with open(yaml_path) as f:
        scenario = yaml.safe_load(f)

    failures = []
    for fc in scenario.get("failure_config", []):
        domain = GENERATOR_DOMAIN.get(fc.get("generator"), fc.get("generator"))
        expected = ACTION_MAP.get(fc.get("expected_agent_action"), fc.get("expected_agent_action"))
        failures.append({
            "day_offset": fc.get("day_offset"),
            "domain": domain,
            "failure_type": fc.get("failure_type"),
            "expected_action": expected,
            "params": fc.get("params", {}),
        })

    return failures


def run_evaluation(scenario_id: str, start_date: str, days: int) -> dict:
    """Run evaluation: compare agent decisions vs ground truth."""
    ground_truth = get_ground_truth(scenario_id)
    start = _date.fromisoformat(start_date)

    # Query agent's processed events
    conn = get_conn()
    end = start + timedelta(days=days)
    rows = conn.execute(
        """SELECT pipeline_event_id, date, domain, event_type, agent_severity,
                  action_taken, action_success, llm_cost, processed_at, resolved_at
           FROM agent_processed_events
           WHERE date >= ? AND date < ?
           ORDER BY date, domain""",
        [start.isoformat(), end.isoformat()],
    ).fetchall()
    columns = [d[0] for d in conn.description]
    processed = [dict(zip(columns, r)) for r in rows]

    # Also count audit stats
    audit_stats = conn.execute(
        """SELECT
             COUNT(*) FILTER (WHERE llm_used) AS llm_calls,
             SUM(llm_cost) AS total_cost,
             COUNT(*) FILTER (WHERE memory_hit) AS memory_hits,
             COUNT(*) AS total_actions
           FROM agent_audit_log
           WHERE pipeline_event_id IN (
             SELECT pipeline_event_id FROM agent_processed_events
             WHERE date >= ? AND date < ?
           )""",
        [start.isoformat(), end.isoformat()],
    ).fetchone()
    conn.close()

    llm_calls = audit_stats[0] or 0
    total_llm_cost = audit_stats[1] or 0.0
    memory_hits = audit_stats[2] or 0
    total_actions = audit_stats[3] or 0

    # Match processed events to ground truth
    correct_actions = 0
    detected_failures = 0
    human_escalations = 0
    auto_resolved = 0

    for gt in ground_truth:
        target_date = (start + timedelta(days=gt["day_offset"])).isoformat()
        # Find matching processed events
        matches = [
            p for p in processed
            if str(p.get("date")) == target_date and p.get("domain") == gt["domain"]
        ]
        if matches:
            detected_failures += 1
            for m in matches:
                if m["action_taken"] == gt["expected_action"]:
                    correct_actions += 1
                    break

    total_injected = len(ground_truth)

    for p in processed:
        if p.get("action_taken") == "human_escalation":
            human_escalations += 1
        elif p.get("action_taken") in ("auto_retry", "auto_fix", "llm_fix", "noop"):
            auto_resolved += 1

    # Compute metrics
    non_critical = len([p for p in processed if p.get("agent_severity") != "CRITICAL"])
    naive_llm_cost = len(processed) * 0.02  # 2 calls at $0.01 each per event

    results = {
        "scenario_id": scenario_id,
        "total_events_processed": len(processed),
        "total_injected_failures": total_injected,
        "detection_accuracy": round(detected_failures / max(total_injected, 1), 4),
        "correct_action_rate": round(correct_actions / max(total_injected, 1), 4),
        "autonomy_rate": round(auto_resolved / max(non_critical, 1), 4),
        "human_escalations": human_escalations,
        "llm_calls": llm_calls,
        "total_llm_cost_usd": round(total_llm_cost, 4),
        "llm_cost_saved_vs_naive": round(naive_llm_cost - total_llm_cost, 4),
        "memory_hits": memory_hits,
        "memory_hit_rate": round(memory_hits / max(total_actions, 1), 4),
        "avg_time_to_resolve_seconds": 0.0,  # Would need timestamps comparison in production
    }

    return results


def print_evaluation_report(results: dict) -> None:
    """Pretty-print evaluation results."""
    print(f"\n{'='*60}")
    print(f"  EVALUATION REPORT: {results['scenario_id']}")
    print(f"{'='*60}")
    print(f"  Events processed:      {results['total_events_processed']}")
    print(f"  Injected failures:     {results['total_injected_failures']}")
    print(f"{'─'*60}")
    print(f"  Detection accuracy:    {results['detection_accuracy']:.1%}")
    print(f"  Correct action rate:   {results['correct_action_rate']:.1%}")
    print(f"  Autonomy rate:         {results['autonomy_rate']:.1%}")
    print(f"{'─'*60}")
    print(f"  Human escalations:     {results['human_escalations']}")
    print(f"  LLM calls:             {results['llm_calls']}")
    print(f"  Total LLM cost:        ${results['total_llm_cost_usd']:.4f}")
    print(f"  LLM cost saved:        ${results['llm_cost_saved_vs_naive']:.4f}")
    print(f"  Memory hits:           {results['memory_hits']}")
    print(f"  Memory hit rate:       {results['memory_hit_rate']:.1%}")
    print(f"{'='*60}\n")
