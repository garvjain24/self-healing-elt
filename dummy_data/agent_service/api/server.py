"""Human review dashboard API — FastAPI server at port 8003."""

from __future__ import annotations

import json
from datetime import datetime

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from agent_service.config import AGENT_DB_PATH, PIPELINE_WAREHOUSE_URL, AGENT_API_PORT
from agent_service.db.connection import get_conn, init_db
from agent_service.memory.memory_store import MemoryStore, make_signature
from agent_service.evaluation.scorer import run_evaluation, print_evaluation_report

app = FastAPI(title="Agent Service — Review Dashboard", version="1.0.0")


# ── Request Models ──

class ResolveRequest(BaseModel):
    resolution_notes: str = ""
    mark_pattern_critical: bool = False


# ── Health ──

@app.get("/health")
def health():
    init_db()
    conn = get_conn()
    pending = conn.execute(
        "SELECT COUNT(*) FROM human_review_queue WHERE status = 'pending'"
    ).fetchone()[0]
    conn.close()

    pipeline_status = "unreachable"
    try:
        resp = httpx.get(f"{PIPELINE_WAREHOUSE_URL}/health", timeout=5)
        if resp.status_code == 200:
            pipeline_status = "reachable"
    except Exception:
        pass

    return {
        "status": "ok",
        "agent_db": AGENT_DB_PATH,
        "pipeline_api": pipeline_status,
        "pending_reviews": pending,
    }


# ── Review Queue ──

@app.get("/review/queue")
def review_queue():
    conn = get_conn()
    rows = conn.execute(
        """SELECT id, pipeline_event_id, date, domain, event_type,
                  agent_severity, evidence, suggested_fix, status, created_at
           FROM human_review_queue
           WHERE status = 'pending'
           ORDER BY created_at DESC"""
    ).fetchall()
    columns = [d[0] for d in conn.description]
    conn.close()
    return [_row_to_dict(columns, r) for r in rows]


@app.post("/review/resolve/{queue_id}")
def resolve_review(queue_id: int, body: ResolveRequest):
    conn = get_conn()

    # Get the review item
    row = conn.execute(
        "SELECT * FROM human_review_queue WHERE id = ?", [queue_id]
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Review item {queue_id} not found")

    columns = [d[0] for d in conn.description]
    item = dict(zip(columns, row))

    # Resolve
    conn.execute(
        """UPDATE human_review_queue
           SET status = 'resolved', resolution_notes = ?, resolved_at = current_timestamp
           WHERE id = ?""",
        [body.resolution_notes, queue_id],
    )

    # If marking pattern critical + the event has enough info
    if body.mark_pattern_critical:
        event_data = {}
        try:
            event_data = json.loads(item.get("evidence") or "{}")
        except Exception:
            pass
        if event_data:
            signature = make_signature(event_data)
            memory = MemoryStore()
            memory.mark_human_forced(signature)
            # Also ensure the memory entry exists
            memory.update(
                signature=signature,
                domain=item.get("domain", ""),
                event_type=item.get("event_type", ""),
                action="human_escalation",
                payload={"reason": "human_forced"},
                success=True,
                llm_cost=0,
            )

    # Audit log
    conn.execute(
        """INSERT INTO agent_audit_log
           (pipeline_event_id, action, notes, success)
           VALUES (?, 'human_resolve', ?, true)""",
        [item.get("pipeline_event_id"), body.resolution_notes],
    )
    conn.close()

    return {"status": "resolved", "queue_id": queue_id, "pattern_critical": body.mark_pattern_critical}


@app.get("/review/resolved")
def review_resolved():
    conn = get_conn()
    rows = conn.execute(
        """SELECT * FROM human_review_queue WHERE status = 'resolved'
           ORDER BY resolved_at DESC"""
    ).fetchall()
    columns = [d[0] for d in conn.description]
    conn.close()
    return [_row_to_dict(columns, r) for r in rows]


# ── Agent Memory ──

@app.get("/agent/memory")
def agent_memory():
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM agent_memory ORDER BY last_used DESC"
    ).fetchall()
    columns = [d[0] for d in conn.description]
    conn.close()
    return [_row_to_dict(columns, r) for r in rows]


@app.get("/agent/memory/{signature}")
def agent_memory_by_sig(signature: str):
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM agent_memory WHERE signature = ?", [signature]
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Memory entry not found: {signature}")
    columns = [d[0] for d in conn.description]
    conn.close()
    return _row_to_dict(columns, row)


# ── Audit Log ──

@app.get("/agent/audit")
def agent_audit(limit: int = 100):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM agent_audit_log ORDER BY created_at DESC LIMIT ?", [limit]
    ).fetchall()
    columns = [d[0] for d in conn.description]
    conn.close()
    return [_row_to_dict(columns, r) for r in rows]


# ── Agent Stats ──

@app.get("/agent/stats")
def agent_stats():
    conn = get_conn()
    total = conn.execute("SELECT COUNT(*) FROM agent_processed_events").fetchone()[0]
    by_action = conn.execute(
        """SELECT action_taken, COUNT(*) FROM agent_processed_events
           GROUP BY action_taken"""
    ).fetchall()
    total_cost = conn.execute(
        "SELECT COALESCE(SUM(llm_cost), 0) FROM agent_processed_events"
    ).fetchone()[0]

    audit_rows = conn.execute(
        """SELECT
             COUNT(*) FILTER (WHERE memory_hit) AS hits,
             COUNT(*) AS total
           FROM agent_audit_log"""
    ).fetchone()
    memory_hit_rate = (audit_rows[0] / audit_rows[1]) if audit_rows[1] else 0.0

    pending = conn.execute(
        "SELECT COUNT(*) FROM human_review_queue WHERE status = 'pending'"
    ).fetchone()[0]
    conn.close()

    return {
        "total_processed": total,
        "by_action": {r[0]: r[1] for r in by_action},
        "total_llm_cost": round(total_cost, 4),
        "memory_hit_rate": round(memory_hit_rate, 4),
        "pending_reviews": pending,
    }


# ── Evaluation ──

@app.get("/evaluation/run")
def run_eval(scenario: str, start: str = "2024-01-01", days: int = 7):
    try:
        results = run_evaluation(scenario, start, days)
        return results
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))


# ── Helpers ──

def _row_to_dict(columns: list[str], row: tuple) -> dict:
    d = {}
    for col, val in zip(columns, row):
        if isinstance(val, datetime):
            d[col] = val.isoformat()
        else:
            d[col] = val
    return d
