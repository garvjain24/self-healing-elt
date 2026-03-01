"""
Read-only FastAPI server for querying the data pipeline warehouse.

No writes. No triggers. Just reads.
The agent service (Project 3) queries this API to inspect warehouse state.
"""

from __future__ import annotations

import json
from typing import Any, Optional

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from data_pipeline.config import API_PORT, DB_PATH, DOMAINS
from data_pipeline.db.connection import get_conn, verify_tables

app = FastAPI(
    title="Data Pipeline — Warehouse API",
    description="Read-only access to raw, staged, metrics, and quality event data.",
    version="1.0.0",
)


@app.get("/health")
def health():
    try:
        tables = verify_tables()
        conn = get_conn()
        counts = {}
        for t in tables:
            row = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
            counts[t] = row[0]
        conn.close()
        return {"status": "ok", "db_path": DB_PATH, "table_counts": counts}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "detail": str(e)})


@app.get("/warehouse/raw/{domain}")
def get_raw(domain: str, date: str = Query(...)):
    if domain not in DOMAINS:
        return JSONResponse(status_code=400, content={"error": f"Unknown domain: {domain}"})

    conn = get_conn()
    raw_table = f"raw_{domain}_data"
    rows = conn.execute(
        f"""
        SELECT raw_id, payload, source_url, scenario, errors_injected, ingested_at
        FROM {raw_table}
        WHERE json_extract_string(payload, '$.date') = ?
           OR json_extract_string(payload, '$.created_at') = ?
        ORDER BY raw_id
        """, [date, date]
    ).fetchall()
    conn.close()

    return [
        {
            "raw_id": r[0],
            "payload": json.loads(r[1]) if isinstance(r[1], str) else r[1],
            "source_url": r[2],
            "scenario": r[3],
            "errors_injected": r[4],
            "ingested_at": str(r[5]) if r[5] else None,
        }
        for r in rows
    ]


@app.get("/warehouse/staged/{domain}")
def get_staged(domain: str, date: str = Query(...)):
    if domain not in DOMAINS:
        return JSONResponse(status_code=400, content={"error": f"Unknown domain: {domain}"})

    conn = get_conn()
    stg_table = f"stg_{domain}_data"
    date_col = "created_at" if domain == "crm" else "date"

    rows = conn.execute(
        f"SELECT * FROM {stg_table} WHERE {date_col} = ? ORDER BY id", [date]
    ).fetchall()
    columns = [desc[0] for desc in conn.description]
    conn.close()

    return [
        {col: (str(val) if not isinstance(val, (int, float, bool, str, type(None), list)) else val)
         for col, val in zip(columns, row)}
        for row in rows
    ]


@app.get("/warehouse/metrics")
def get_metrics(date: str = Query(...)):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM daily_campaign_metrics WHERE date = ? ORDER BY campaign_id", [date]
    ).fetchall()
    columns = [desc[0] for desc in conn.description]
    conn.close()

    return [
        {col: (str(val) if not isinstance(val, (int, float, bool, str, type(None))) else val)
         for col, val in zip(columns, row)}
        for row in rows
    ]


@app.get("/warehouse/quality-events")
def get_quality_events(
    date: Optional[str] = Query(None),
    domain: Optional[str] = Query(None),
):
    conn = get_conn()
    query = "SELECT * FROM data_quality_events WHERE 1=1"
    params: list = []

    if date:
        query += " AND date = ?"
        params.append(date)
    if domain:
        query += " AND domain = ?"
        params.append(domain)

    query += " ORDER BY id"
    rows = conn.execute(query, params).fetchall()
    columns = [desc[0] for desc in conn.description]
    conn.close()

    return [_row_to_dict(columns, row) for row in rows]


@app.get("/warehouse/quality-events/range")
def get_quality_events_range(
    start: str = Query(...),
    end: str = Query(...),
    domain: Optional[str] = Query(None),
):
    conn = get_conn()
    query = "SELECT * FROM data_quality_events WHERE date BETWEEN ? AND ?"
    params: list = [start, end]

    if domain:
        query += " AND domain = ?"
        params.append(domain)

    query += " ORDER BY date, id"
    rows = conn.execute(query, params).fetchall()
    columns = [desc[0] for desc in conn.description]
    conn.close()

    return [_row_to_dict(columns, row) for row in rows]


def _row_to_dict(columns: list[str], row: tuple) -> dict:
    d = {}
    for col, val in zip(columns, row):
        if isinstance(val, (int, float, bool, str, type(None), list)):
            d[col] = val
        else:
            d[col] = str(val)
    return d
