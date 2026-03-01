"""
Raw loader — stores raw API payloads into raw_* tables, untouched.
"""

from __future__ import annotations

import json
from typing import Any

from data_pipeline.db.connection import get_conn
from data_pipeline.config import FAKE_DATA_SERVICE_URL


def load_raw(domain: str, response: dict[str, Any], date: str) -> list[int]:
    """
    Store every record in response['data'] as a separate row in raw_{domain}_data.

    Each row stores the record as JSON payload, plus envelope metadata.
    Returns list of raw_ids of inserted rows.
    """
    data = response.get("data", [])
    scenario = response.get("scenario_id")
    errors_injected = json.dumps(response.get("errors_injected", []))
    source_url = f"{FAKE_DATA_SERVICE_URL}/{domain}?date={date}"

    table = f"raw_{domain}_data"
    conn = get_conn()
    raw_ids: list[int] = []

    for record in data:
        payload_json = json.dumps(record)
        conn.execute(
            f"""
            INSERT INTO {table} (payload, source_url, scenario, errors_injected)
            VALUES (?, ?, ?, ?)
            """,
            [payload_json, source_url, scenario, errors_injected],
        )
        # Fetch the last inserted raw_id
        result = conn.execute(f"SELECT max(raw_id) FROM {table}").fetchone()
        raw_ids.append(result[0])

    conn.close()
    return raw_ids
