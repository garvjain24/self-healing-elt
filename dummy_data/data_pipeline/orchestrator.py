"""
Orchestrator — runs the full pipeline for a given date or date range.
"""

from __future__ import annotations

import logging
from datetime import date as _date, timedelta

from data_pipeline.config import DOMAINS
from data_pipeline.db.connection import get_conn, init_db
from data_pipeline.ingestion.fetcher import DataServiceClient
from data_pipeline.ingestion.raw_loader import load_raw
from data_pipeline.staging.stager import stage_domain
from data_pipeline.metrics.aggregator import compute_daily_metrics
from data_pipeline.observability.emitter import emit_quality_events

logger = logging.getLogger(__name__)


def run_pipeline(date: str, client: DataServiceClient | None = None) -> dict:
    """
    Full pipeline for a single date:
    1. Fetch all 4 domains
    2. Load raw
    3. Stage (validate + write)
    4. Compute daily metrics
    5. Emit quality events
    6. Return summary report
    """
    if client is None:
        client = DataServiceClient()

    # Ensure DB is initialized
    init_db()

    summary = {
        "date": date,
        "raw": {},
        "staging": {},
        "metrics": {},
        "quality_events": [],
    }

    # ── 1. Fetch & Load Raw ──
    for domain in DOMAINS:
        try:
            response = client.fetch(domain, date)
            raw_ids = load_raw(domain, response, date)
            summary["raw"][domain] = {"rows_ingested": len(raw_ids)}
        except Exception as e:
            logger.error("Ingestion failed for %s on %s: %s", domain, date, e)
            summary["raw"][domain] = {"rows_ingested": 0, "error": str(e)}

    # ── 2. Stage ──
    for domain in DOMAINS:
        try:
            result = stage_domain(domain, date)
            summary["staging"][domain] = result
        except Exception as e:
            logger.error("Staging failed for %s on %s: %s", domain, date, e)
            summary["staging"][domain] = {"error": str(e)}

    # ── 3. Metrics ──
    try:
        metrics_result = compute_daily_metrics(date)
        summary["metrics"] = metrics_result
    except Exception as e:
        logger.error("Metrics computation failed for %s: %s", date, e)
        summary["metrics"] = {"error": str(e)}

    # ── 4. Quality Events ──
    for domain in DOMAINS:
        try:
            event_ids = emit_quality_events(domain, date)
            if event_ids:
                # Fetch the event details
                conn = get_conn()
                for eid in event_ids:
                    row = conn.execute(
                        "SELECT event_type, domain, metric_name, observed_value, expected_value, deviation_pct FROM data_quality_events WHERE id = ?",
                        [eid]
                    ).fetchone()
                    if row:
                        summary["quality_events"].append({
                            "id": eid,
                            "event_type": row[0],
                            "domain": row[1],
                            "metric_name": row[2],
                            "observed_value": row[3],
                            "expected_value": row[4],
                            "deviation_pct": row[5],
                        })
                conn.close()
        except Exception as e:
            logger.error("Quality events failed for %s on %s: %s", domain, date, e)

    return summary


def run_pipeline_range(start_date: str, days: int, client: DataServiceClient | None = None) -> list[dict]:
    """Run pipeline day by day for N days starting from start_date."""
    if client is None:
        client = DataServiceClient()

    start = _date.fromisoformat(start_date)
    summaries: list[dict] = []

    for i in range(days):
        current = start + timedelta(days=i)
        date_str = current.isoformat()
        summary = run_pipeline(date_str, client)
        summaries.append(summary)

    return summaries


def get_pipeline_status(date: str) -> dict:
    """
    Returns counts: raw rows, staged rows, valid %, events by event_type.
    For the agent to poll without needing internal access.
    """
    conn = get_conn()
    status: dict = {"date": date}

    for domain in DOMAINS:
        raw_table = f"raw_{domain}_data"
        stg_table = f"stg_{domain}_data"
        date_col = "created_at" if domain == "crm" else "date"

        raw_count = conn.execute(
            f"SELECT COUNT(*) FROM {raw_table} WHERE json_extract_string(payload, '$.date') = ? OR json_extract_string(payload, '$.created_at') = ?",
            [date, date]
        ).fetchone()[0]

        stg_count = conn.execute(
            f"SELECT COUNT(*) FROM {stg_table} WHERE {date_col} = ?", [date]
        ).fetchone()[0]

        valid_count = conn.execute(
            f"SELECT COUNT(*) FROM {stg_table} WHERE {date_col} = ? AND is_valid = true", [date]
        ).fetchone()[0]

        status[domain] = {
            "raw_rows": raw_count,
            "staged_rows": stg_count,
            "valid_pct": round((valid_count / stg_count * 100), 1) if stg_count > 0 else 0,
        }

    # Quality events
    events = conn.execute(
        "SELECT event_type, COUNT(*) FROM data_quality_events WHERE date = ? GROUP BY event_type",
        [date]
    ).fetchall()
    status["quality_events"] = {row[0]: row[1] for row in events}

    conn.close()
    return status
