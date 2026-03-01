"""
Observability Emitter — detects anomalies and writes neutral data quality events.

This module makes NO decisions and assigns NO severity.
It only records objective measurements about data quality.
"""

from __future__ import annotations

import json
from typing import Any

from data_pipeline.db.connection import get_conn
from data_pipeline.config import THRESHOLDS, DOMAIN_SCHEMAS


def _emit(conn, *, date: str, pipeline_stage: str, domain: str,
          event_type: str, metric_name: str | None = None,
          observed_value: float | None = None,
          expected_value: float | None = None,
          deviation_pct: float | None = None,
          detail: dict | None = None,
          reference_table: str | None = None,
          reference_ids: list[int] | None = None) -> int:
    """Insert a single data_quality_event and return its id."""
    conn.execute(
        """
        INSERT INTO data_quality_events
            (date, pipeline_stage, domain, event_type, metric_name,
             observed_value, expected_value, deviation_pct, detail,
             reference_table, reference_ids)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [date, pipeline_stage, domain, event_type, metric_name,
         observed_value, expected_value, deviation_pct,
         json.dumps(detail) if detail else None,
         reference_table, reference_ids],
    )
    return conn.execute("SELECT max(id) FROM data_quality_events").fetchone()[0]


# ─────────────────────────────────────────────────────────────────────────
# Master function
# ─────────────────────────────────────────────────────────────────────────

def emit_quality_events(domain: str, date: str) -> list[int]:
    """Run all detectors for a domain/date. Returns list of event ids created."""
    ids: list[int] = []
    ids.extend(check_row_counts(domain, date))
    ids.extend(check_null_fields(domain, date))
    ids.extend(check_schema_drift(domain, date))
    ids.extend(check_dtype_issues(domain, date))
    ids.extend(check_duplicates(domain, date))
    ids.extend(check_constraint_violations(domain, date))
    if domain == "analytics":
        ids.extend(check_funnel_violations(date))
    if domain == "finance":
        ids.extend(check_kpi_deviation(date))
    if domain == "crm":
        ids.extend(check_crm_lag(date))
    return ids


# ─────────────────────────────────────────────────────────────────────────
# Individual detectors
# ─────────────────────────────────────────────────────────────────────────

def check_row_counts(domain: str, date: str) -> list[int]:
    ids: list[int] = []
    conn = get_conn()
    raw_table = f"raw_{domain}_data"

    # Today's count
    today_count = conn.execute(
        f"""
        SELECT COUNT(*) FROM {raw_table}
        WHERE json_extract_string(payload, '$.date') = ?
           OR json_extract_string(payload, '$.created_at') = ?
        """, [date, date]
    ).fetchone()[0]

    if today_count == 0:
        eid = _emit(conn, date=date, pipeline_stage="RAW", domain=domain,
                    event_type="ROW_COUNT_ZERO",
                    metric_name="row_count",
                    observed_value=0, expected_value=None,
                    reference_table=raw_table)
        ids.append(eid)
        conn.close()
        return ids

    # Rolling average
    window = THRESHOLDS["rolling_avg_window_days"]
    avg_row = conn.execute(
        f"""
        SELECT AVG(cnt) FROM (
            SELECT COUNT(*) as cnt
            FROM {raw_table}
            GROUP BY json_extract_string(payload, '$.date')
        )
        """
    ).fetchone()
    rolling_avg = avg_row[0] if avg_row and avg_row[0] else today_count

    if rolling_avg > 0:
        drop_pct = ((rolling_avg - today_count) / rolling_avg) * 100
        if drop_pct > THRESHOLDS["row_count_drop_pct"]:
            eid = _emit(conn, date=date, pipeline_stage="RAW", domain=domain,
                        event_type="ROW_COUNT_DROP",
                        metric_name="row_count",
                        observed_value=float(today_count),
                        expected_value=float(rolling_avg),
                        deviation_pct=round(drop_pct, 2),
                        reference_table=raw_table)
            ids.append(eid)

    conn.close()
    return ids


def check_null_fields(domain: str, date: str) -> list[int]:
    ids: list[int] = []
    conn = get_conn()
    stg_table = f"stg_{domain}_data"
    expected_fields = DOMAIN_SCHEMAS.get(domain, [])

    date_col = "created_at" if domain == "crm" else "date"

    for field in expected_fields:
        try:
            row = conn.execute(
                f"SELECT COUNT(*) FROM {stg_table} WHERE {date_col} = ? AND {field} IS NULL",
                [date]
            ).fetchone()
            null_count = row[0] if row else 0
        except Exception:
            continue

        if null_count > 0:
            total = conn.execute(
                f"SELECT COUNT(*) FROM {stg_table} WHERE {date_col} = ?", [date]
            ).fetchone()[0]
            eid = _emit(conn, date=date, pipeline_stage="STAGED", domain=domain,
                        event_type="NULL_FIELD",
                        metric_name=field,
                        observed_value=float(null_count),
                        expected_value=0,
                        detail={"field": field, "null_count": null_count, "total_rows": total},
                        reference_table=stg_table)
            ids.append(eid)

    conn.close()
    return ids


def check_schema_drift(domain: str, date: str) -> list[int]:
    ids: list[int] = []
    conn = get_conn()
    raw_table = f"raw_{domain}_data"
    expected_keys = set(DOMAIN_SCHEMAS.get(domain, []))

    rows = conn.execute(
        f"""
        SELECT raw_id, payload FROM {raw_table}
        WHERE json_extract_string(payload, '$.date') = ?
           OR json_extract_string(payload, '$.created_at') = ?
        LIMIT 10
        """, [date, date]
    ).fetchall()

    if not rows:
        conn.close()
        return ids

    # Check first row's keys
    sample = json.loads(rows[0][1]) if isinstance(rows[0][1], str) else rows[0][1]
    actual_keys = set(sample.keys())

    for missing in expected_keys - actual_keys:
        eid = _emit(conn, date=date, pipeline_stage="RAW", domain=domain,
                    event_type="COLUMN_MISSING",
                    detail={"column": missing},
                    reference_table=raw_table,
                    reference_ids=[rows[0][0]])
        ids.append(eid)

    for extra in actual_keys - expected_keys:
        if extra in ("requires_human_review",):
            continue  # Skip envelope fields that leak into some payloads
        eid = _emit(conn, date=date, pipeline_stage="RAW", domain=domain,
                    event_type="COLUMN_UNEXPECTED",
                    detail={"column": extra},
                    reference_table=raw_table,
                    reference_ids=[rows[0][0]])
        ids.append(eid)

    conn.close()
    return ids


def check_dtype_issues(domain: str, date: str) -> list[int]:
    ids: list[int] = []
    conn = get_conn()
    stg_table = f"stg_{domain}_data"
    date_col = "created_at" if domain == "crm" else "date"

    rows = conn.execute(
        f"""
        SELECT id, validation_errors FROM {stg_table}
        WHERE {date_col} = ? AND is_valid = false
        """, [date]
    ).fetchall()

    cast_issues: dict[str, int] = {}
    sample_ids: list[int] = []
    for row_id, errors in rows:
        if errors:
            for err in errors:
                if "cast_failed" in str(err):
                    field = str(err).split(":")[1] if ":" in str(err) else "unknown"
                    cast_issues[field] = cast_issues.get(field, 0) + 1
                    if len(sample_ids) < 5:
                        sample_ids.append(row_id)

    for field, count in cast_issues.items():
        eid = _emit(conn, date=date, pipeline_stage="STAGED", domain=domain,
                    event_type="DTYPE_MISMATCH",
                    detail={"field": field, "affected_rows": count},
                    reference_table=stg_table,
                    reference_ids=sample_ids)
        ids.append(eid)

    conn.close()
    return ids


def check_duplicates(domain: str, date: str) -> list[int]:
    ids: list[int] = []
    conn = get_conn()
    stg_table = f"stg_{domain}_data"

    if domain in ("ads", "analytics"):
        date_col = "date"
        key_expr = "campaign_id" if domain == "ads" else "source"
        key_fields = ["date", key_expr]
        dup_row = conn.execute(
            f"""
            SELECT COUNT(*) - COUNT(DISTINCT ({date_col}, {key_expr})) as dups
            FROM {stg_table} WHERE {date_col} = ?
            """, [date]
        ).fetchone()
    elif domain == "crm":
        dup_row = conn.execute(
            f"""
            SELECT COUNT(*) - COUNT(DISTINCT lead_id) as dups
            FROM {stg_table} WHERE created_at = ?
            """, [date]
        ).fetchone()
        key_fields = ["lead_id"]
    else:
        conn.close()
        return ids

    dup_count = dup_row[0] if dup_row else 0
    if dup_count > 0:
        eid = _emit(conn, date=date, pipeline_stage="STAGED", domain=domain,
                    event_type="DUPLICATE_ROWS",
                    observed_value=float(dup_count),
                    detail={"duplicate_count": dup_count, "key_fields": key_fields},
                    reference_table=stg_table)
        ids.append(eid)

    conn.close()
    return ids


def check_constraint_violations(domain: str, date: str) -> list[int]:
    ids: list[int] = []
    conn = get_conn()
    stg_table = f"stg_{domain}_data"
    date_col = "created_at" if domain == "crm" else "date"

    rows = conn.execute(
        f"""
        SELECT id, validation_errors FROM {stg_table}
        WHERE {date_col} = ? AND is_valid = false
        """, [date]
    ).fetchall()

    # Group by unique violation string
    violation_counts: dict[str, list[int]] = {}
    for row_id, errors in rows:
        if errors:
            for err in errors:
                err_str = str(err)
                if "constraint" in err_str:
                    violation_counts.setdefault(err_str, []).append(row_id)

    for violation, affected_ids in violation_counts.items():
        eid = _emit(conn, date=date, pipeline_stage="STAGED", domain=domain,
                    event_type="CONSTRAINT_VIOLATION",
                    detail={"violation": violation, "affected_rows": len(affected_ids)},
                    reference_table=stg_table,
                    reference_ids=affected_ids[:10])
        ids.append(eid)

    conn.close()
    return ids


def check_kpi_deviation(date: str) -> list[int]:
    """Compare stg_finance_data reported KPIs vs daily_campaign_metrics computed KPIs."""
    ids: list[int] = []
    conn = get_conn()
    threshold = THRESHOLDS["kpi_deviation_pct"]

    # Get reported values from stg_finance_data
    fin_row = conn.execute(
        """
        SELECT id, reported_roas, reported_cac, reported_profit, total_spend, total_revenue
        FROM stg_finance_data WHERE date = ? LIMIT 1
        """, [date]
    ).fetchone()

    if not fin_row:
        conn.close()
        return ids

    fin_id, reported_roas, reported_cac, reported_profit, total_spend, total_revenue = fin_row

    # Aggregate computed values from daily_campaign_metrics
    agg = conn.execute(
        """
        SELECT SUM(spend), SUM(conversions), SUM(revenue),
               SUM(computed_roas), SUM(computed_cac), SUM(computed_profit)
        FROM daily_campaign_metrics WHERE date = ?
        """, [date]
    ).fetchone()

    if not agg or agg[0] is None:
        conn.close()
        return ids

    total_comp_spend = float(agg[0]) if agg[0] else 0
    total_comp_conv = int(agg[1]) if agg[1] else 0
    total_comp_revenue = float(agg[2]) if agg[2] else 0

    # Recompute aggregate KPIs
    computed_roas = (total_comp_revenue / total_comp_spend) if total_comp_spend > 0 else 0
    computed_cac = (total_comp_spend / total_comp_conv) if total_comp_conv > 0 else 0
    computed_profit = total_comp_revenue - total_comp_spend

    # Compare each KPI
    kpi_checks = [
        ("roas", reported_roas, computed_roas),
        ("cac", reported_cac, computed_cac),
        ("profit", reported_profit, computed_profit),
    ]

    for metric_name, reported, computed in kpi_checks:
        if reported is None or computed is None:
            continue
        if abs(computed) < 0.001:
            # Can't compute meaningful deviation % when computed is ~0
            if abs(reported) > 0.1:
                eid = _emit(conn, date=date, pipeline_stage="METRICS", domain="finance",
                            event_type="KPI_DEVIATION",
                            metric_name=metric_name,
                            observed_value=float(reported),
                            expected_value=float(computed),
                            deviation_pct=100.0,
                            reference_table="stg_finance_data",
                            reference_ids=[fin_id])
                ids.append(eid)
            continue

        dev_pct = abs((reported - computed) / computed) * 100
        if dev_pct > threshold:
            eid = _emit(conn, date=date, pipeline_stage="METRICS", domain="finance",
                        event_type="KPI_DEVIATION",
                        metric_name=metric_name,
                        observed_value=float(reported),
                        expected_value=round(float(computed), 4),
                        deviation_pct=round(dev_pct, 2),
                        reference_table="stg_finance_data",
                        reference_ids=[fin_id])
            ids.append(eid)

    conn.close()
    return ids


def check_funnel_violations(date: str) -> list[int]:
    ids: list[int] = []
    conn = get_conn()

    rows = conn.execute(
        """
        SELECT id, validation_errors FROM stg_analytics_data
        WHERE date = ? AND is_valid = false
        """, [date]
    ).fetchall()

    funnel_ids: list[int] = []
    for row_id, errors in rows:
        if errors:
            for err in errors:
                if "funnel" in str(err).lower():
                    funnel_ids.append(row_id)
                    break

    if funnel_ids:
        eid = _emit(conn, date=date, pipeline_stage="STAGED", domain="analytics",
                    event_type="FUNNEL_VIOLATION",
                    detail={"affected_rows": len(funnel_ids)},
                    reference_table="stg_analytics_data",
                    reference_ids=funnel_ids[:10])
        ids.append(eid)

    conn.close()
    return ids


def check_crm_lag(date: str, lag_window_days: int | None = None) -> list[int]:
    ids: list[int] = []
    conn = get_conn()
    window = lag_window_days or THRESHOLDS["crm_lag_window_days"]

    # Count ad conversions for this date
    ad_conv = conn.execute(
        "SELECT COALESCE(SUM(conversions), 0) FROM stg_ads_data WHERE date = ? AND is_valid = true",
        [date]
    ).fetchone()[0]

    if ad_conv == 0:
        conn.close()
        return ids

    # Count CRM leads in window
    crm_leads = conn.execute(
        f"""
        SELECT COUNT(*) FROM stg_crm_data
        WHERE created_at BETWEEN ? AND (CAST(? AS DATE) + INTERVAL '{window}' DAY)
        """, [date, date]
    ).fetchone()[0]

    if crm_leads == 0:
        eid = _emit(conn, date=date, pipeline_stage="METRICS", domain="crm",
                    event_type="CRM_LAG_DETECTED",
                    detail={"ad_conversions": int(ad_conv), "crm_leads_in_window": 0,
                            "window_days": window},
                    reference_table="stg_crm_data")
        ids.append(eid)

    conn.close()
    return ids
