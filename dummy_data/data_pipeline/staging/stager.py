"""
Stager — reads raw_* tables, validates each record, writes to stg_* tables.

Invalid records are KEPT (is_valid=False) — never discarded.
"""

from __future__ import annotations

import json
from typing import Any

from data_pipeline.db.connection import get_conn
from data_pipeline.staging.validator import (
    validate_ads,
    validate_analytics,
    validate_crm,
    validate_finance,
    attempt_cast,
)

_VALIDATOR = {
    "ads": validate_ads,
    "analytics": validate_analytics,
    "crm": validate_crm,
    "finance": validate_finance,
}

# Mapping from domain to (stg_table, column_list, cast_spec)
# cast_spec: field → target_type for graceful type coercion
_DOMAIN_CONFIG: dict[str, dict[str, Any]] = {
    "ads": {
        "stg_table": "stg_ads_data",
        "raw_table": "raw_ads_data",
        "columns": ["date", "campaign_id", "platform", "impressions", "clicks",
                     "spend", "conversions", "cpc"],
        "casts": {
            "impressions": int, "clicks": int, "conversions": int,
            "spend": float, "cpc": float,
        },
    },
    "analytics": {
        "stg_table": "stg_analytics_data",
        "raw_table": "raw_analytics_data",
        "columns": ["date", "sessions", "pageviews", "funnel_step_1",
                     "funnel_step_2", "funnel_step_3", "conversion_events", "source"],
        "casts": {
            "sessions": int, "pageviews": int,
            "funnel_step_1": int, "funnel_step_2": int, "funnel_step_3": int,
            "conversion_events": int,
        },
    },
    "crm": {
        "stg_table": "stg_crm_data",
        "raw_table": "raw_crm_data",
        "columns": ["lead_id", "created_at", "status", "revenue",
                     "source_campaign", "conversion_lag_days"],
        "casts": {"revenue": float, "conversion_lag_days": int},
    },
    "finance": {
        "stg_table": "stg_finance_data",
        "raw_table": "raw_finance_data",
        "columns": ["date", "total_spend", "total_revenue", "roas", "cac", "profit"],
        "casts": {
            "total_spend": float, "total_revenue": float,
            "roas": float, "cac": float, "profit": float,
        },
    },
}


def stage_domain(domain: str, date: str) -> dict:
    """
    Read raw_{domain}_data rows for *date*, validate, and write to stg_{domain}_data.

    Returns: {total, valid, invalid, validation_errors: list[str]}
    """
    cfg = _DOMAIN_CONFIG[domain]
    validate_fn = _VALIDATOR[domain]
    conn = get_conn()

    # 1. Read all raw rows for this date
    raw_rows = conn.execute(
        f"""
        SELECT raw_id, payload FROM {cfg['raw_table']}
        WHERE json_extract_string(payload, '$.date') = ?
           OR json_extract_string(payload, '$.created_at') = ?
        ORDER BY raw_id
        """,
        [date, date],
    ).fetchall()

    total = len(raw_rows)
    valid_count = 0
    invalid_count = 0
    all_errors: list[str] = []

    for raw_id, payload_raw in raw_rows:
        record = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw

        # 2. Attempt type casting
        for field, target_type in cfg["casts"].items():
            if field in record and record[field] is not None:
                casted, ok = attempt_cast(record, field, target_type)
                if ok:
                    record[field] = casted

        # 3. Validate
        is_valid, violations = validate_fn(record)
        if is_valid:
            valid_count += 1
        else:
            invalid_count += 1
            all_errors.extend(violations)

        # 4. Write to stg table
        _insert_staged(conn, domain, cfg, raw_id, record, is_valid, violations)

    conn.close()
    return {
        "total": total,
        "valid": valid_count,
        "invalid": invalid_count,
        "validation_errors": all_errors,
    }


def _insert_staged(
    conn, domain: str, cfg: dict, raw_id: int,
    record: dict, is_valid: bool, violations: list[str],
) -> None:
    """Insert a single record into the stage table."""
    stg = cfg["stg_table"]

    if domain == "ads":
        conn.execute(
            f"""INSERT INTO {stg} (raw_id, date, campaign_id, platform,
                impressions, clicks, spend, conversions, cpc,
                is_valid, validation_errors)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            [raw_id,
             record.get("date"), record.get("campaign_id"), record.get("platform"),
             _safe_int(record.get("impressions")), _safe_int(record.get("clicks")),
             _safe_float(record.get("spend")), _safe_int(record.get("conversions")),
             _safe_float(record.get("cpc")),
             is_valid, violations],
        )
    elif domain == "analytics":
        conn.execute(
            f"""INSERT INTO {stg} (raw_id, date, sessions, pageviews,
                funnel_step_1, funnel_step_2, funnel_step_3,
                conversion_events, source, is_valid, validation_errors)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            [raw_id,
             record.get("date"), _safe_int(record.get("sessions")),
             _safe_int(record.get("pageviews")),
             _safe_int(record.get("funnel_step_1")), _safe_int(record.get("funnel_step_2")),
             _safe_int(record.get("funnel_step_3")),
             _safe_int(record.get("conversion_events")), record.get("source"),
             is_valid, violations],
        )
    elif domain == "crm":
        rev = record.get("revenue")
        import math
        if isinstance(rev, float) and math.isnan(rev):
            rev = None
        conn.execute(
            f"""INSERT INTO {stg} (raw_id, lead_id, created_at, status,
                revenue, source_campaign, conversion_lag_days,
                is_valid, validation_errors)
                VALUES (?,?,?,?,?,?,?,?,?)""",
            [raw_id,
             record.get("lead_id"), record.get("created_at"), record.get("status"),
             _safe_float(rev), record.get("source_campaign"),
             _safe_int(record.get("conversion_lag_days")),
             is_valid, violations],
        )
    elif domain == "finance":
        conn.execute(
            f"""INSERT INTO {stg} (raw_id, date, total_spend, total_revenue,
                reported_roas, reported_cac, reported_profit,
                is_valid, validation_errors)
                VALUES (?,?,?,?,?,?,?,?,?)""",
            [raw_id,
             record.get("date"),
             _safe_float(record.get("total_spend")),
             _safe_float(record.get("total_revenue")),
             _safe_float(record.get("roas")),
             _safe_float(record.get("cac")),
             _safe_float(record.get("profit")),
             is_valid, violations],
        )


def _safe_int(val) -> int | None:
    if val is None:
        return None
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        import math
        f = float(val)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None
