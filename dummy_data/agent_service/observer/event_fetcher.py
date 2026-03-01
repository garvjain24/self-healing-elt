"""Event fetcher — polls the pipeline warehouse API for unprocessed quality events."""

from __future__ import annotations

import logging
from datetime import date as _date, timedelta

import httpx

from agent_service.config import PIPELINE_WAREHOUSE_URL
from agent_service.db.connection import get_conn

logger = logging.getLogger(__name__)


class EventFetcher:
    def __init__(self, warehouse_url: str | None = None):
        self.warehouse_url = (warehouse_url or PIPELINE_WAREHOUSE_URL).rstrip("/")

    def fetch_unprocessed(
        self,
        date_range_days: int = 7,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict]:
        """Fetch quality events not yet processed by the agent."""
        # Build date range — if explicit dates given, use them;
        # otherwise look back N days from today
        if start_date and end_date:
            start, end = start_date, end_date
        else:
            today = _date.today()
            start = (today - timedelta(days=date_range_days)).isoformat()
            end = today.isoformat()

        # 1. Get all events from warehouse API
        try:
            with httpx.Client(timeout=15.0) as client:
                resp = client.get(
                    f"{self.warehouse_url}/warehouse/quality-events/range",
                    params={"start": start, "end": end},
                )
                resp.raise_for_status()
                all_events = resp.json()
        except Exception as e:
            logger.error("Failed to fetch quality events: %s", e)
            return []

        if not all_events:
            return []

        # 2. Get already-processed event IDs
        conn = get_conn()
        try:
            rows = conn.execute(
                "SELECT pipeline_event_id FROM agent_processed_events"
            ).fetchall()
            processed_ids = {r[0] for r in rows}
        except Exception:
            processed_ids = set()
        finally:
            conn.close()

        # 3. Filter out already-processed
        unprocessed = [e for e in all_events if e.get("id") not in processed_ids]

        # 4. Sort: date ASC, KPI_DEVIATION last (needs metrics computed first)
        def sort_key(e):
            kpi_last = 1 if e.get("event_type") == "KPI_DEVIATION" else 0
            return (str(e.get("date", "")), kpi_last, e.get("id", 0))

        unprocessed.sort(key=sort_key)
        return unprocessed

    def fetch_event_context(self, event: dict) -> dict:
        """Enrich a single event with metrics and staged data context."""
        enriched = dict(event)
        date = event.get("date", "")
        domain = event.get("domain", "")

        try:
            with httpx.Client(timeout=15.0) as client:
                # Metrics
                resp = client.get(
                    f"{self.warehouse_url}/warehouse/metrics",
                    params={"date": date},
                )
                if resp.status_code == 200:
                    enriched["metrics_context"] = resp.json()

                # Staged data
                resp = client.get(
                    f"{self.warehouse_url}/warehouse/staged/{domain}",
                    params={"date": date},
                )
                if resp.status_code == 200:
                    enriched["staged_context"] = resp.json()
        except Exception as e:
            logger.warning("Failed to fetch context for event %s: %s", event.get("id"), e)

        return enriched
