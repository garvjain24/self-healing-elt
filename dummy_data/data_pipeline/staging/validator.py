"""
Validator — pure domain validation functions.

Every function returns (is_valid: bool, violations: list[str]).
These are objective data checks — no severity levels, no agent hints.
"""

from __future__ import annotations

import math
from datetime import date as _date
from typing import Any


# ── Helpers ──────────────────────────────────────────────────────────────

def attempt_cast(record: dict, field: str, to_type: type) -> tuple[Any, bool]:
    """
    Try to cast record[field] to *to_type*.
    Returns (cast_value, success_bool).
    """
    val = record.get(field)
    if val is None:
        return None, False
    try:
        if to_type is int:
            return int(float(val)), True
        return to_type(val), True
    except (TypeError, ValueError):
        return val, False


def _is_numeric(val: Any) -> bool:
    if val is None:
        return False
    if isinstance(val, (int, float)):
        return not math.isnan(val) if isinstance(val, float) else True
    try:
        float(val)
        return True
    except (TypeError, ValueError):
        return False


def _as_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        f = float(val)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


def _as_int(val: Any) -> int | None:
    if val is None:
        return None
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None


# ── Domain validators ────────────────────────────────────────────────────

def validate_ads(record: dict) -> tuple[bool, list[str]]:
    violations: list[str] = []

    required = ["date", "campaign_id", "platform", "impressions", "clicks", "spend", "conversions", "cpc"]
    for f in required:
        if f not in record or record[f] is None:
            violations.append(f"missing_or_null:{f}")

    impressions = _as_int(record.get("impressions"))
    clicks = _as_int(record.get("clicks"))
    conversions = _as_int(record.get("conversions"))
    spend = _as_float(record.get("spend"))
    cpc = _as_float(record.get("cpc"))

    # Numeric type checks
    for fname, val in [("impressions", impressions), ("clicks", clicks),
                       ("conversions", conversions), ("spend", spend), ("cpc", cpc)]:
        if record.get(fname) is not None and val is None:
            violations.append(f"cast_failed:{fname}:expected_numeric")

    if impressions is not None and impressions < 0:
        violations.append(f"negative_value:impressions={impressions}")
    if clicks is not None and clicks < 0:
        violations.append(f"negative_value:clicks={clicks}")
    if spend is not None and spend < 0:
        violations.append(f"negative_value:spend={spend}")

    # Funnel constraints
    if impressions is not None and clicks is not None and impressions < clicks:
        violations.append(f"constraint:impressions({impressions})<clicks({clicks})")
    if clicks is not None and conversions is not None and clicks < conversions:
        violations.append(f"constraint:clicks({clicks})<conversions({conversions})")

    # Spend vs CPC check
    if spend is not None and clicks is not None and cpc is not None and spend > 0:
        expected_spend = clicks * cpc
        tolerance = spend * 0.05
        if abs(spend - expected_spend) > max(tolerance, 0.01):
            violations.append(
                f"constraint:spend({spend})!=clicks*cpc({round(expected_spend, 2)})"
            )

    return (len(violations) == 0, violations)


def validate_analytics(record: dict) -> tuple[bool, list[str]]:
    violations: list[str] = []

    required = ["date", "sessions", "pageviews", "funnel_step_1", "funnel_step_2",
                "funnel_step_3", "conversion_events", "source"]
    for f in required:
        if f not in record or record[f] is None:
            violations.append(f"missing_or_null:{f}")

    sessions = _as_int(record.get("sessions"))
    conversion_events = _as_int(record.get("conversion_events"))
    f1 = _as_int(record.get("funnel_step_1"))
    f2 = _as_int(record.get("funnel_step_2"))
    f3 = _as_int(record.get("funnel_step_3"))

    if sessions is not None and conversion_events is not None:
        if conversion_events > sessions:
            violations.append(
                f"constraint:conversion_events({conversion_events})>sessions({sessions})"
            )

    # Funnel monotonicity
    if f1 is not None and f2 is not None and f1 < f2:
        violations.append(f"funnel_violation:step1({f1})<step2({f2})")
    if f2 is not None and f3 is not None and f2 < f3:
        violations.append(f"funnel_violation:step2({f2})<step3({f3})")

    # Funnel steps <= sessions
    if sessions is not None:
        for step_name, step_val in [("funnel_step_1", f1), ("funnel_step_2", f2), ("funnel_step_3", f3)]:
            if step_val is not None and step_val > sessions:
                violations.append(f"constraint:{step_name}({step_val})>sessions({sessions})")

    return (len(violations) == 0, violations)


def validate_crm(record: dict) -> tuple[bool, list[str]]:
    violations: list[str] = []

    # lead_id non-null
    if not record.get("lead_id"):
        violations.append("missing_or_null:lead_id")

    # status check
    valid_statuses = {"new", "qualified", "closed_won", "closed_lost"}
    status = record.get("status")
    if status not in valid_statuses:
        violations.append(f"constraint:invalid_status({status})")

    # created_at parseable
    created_at = record.get("created_at")
    if created_at:
        try:
            _date.fromisoformat(str(created_at))
        except (ValueError, TypeError):
            violations.append(f"cast_failed:created_at:expected_date")
    else:
        violations.append("missing_or_null:created_at")

    # revenue logic
    revenue = record.get("revenue")
    # Treat NaN as None
    if isinstance(revenue, float) and math.isnan(revenue):
        revenue = None

    if status == "closed_won" and (revenue is None or _as_float(revenue) is None):
        violations.append("constraint:closed_won_missing_revenue")
    if status != "closed_won" and revenue is not None:
        violations.append(f"constraint:non_closed_won_has_revenue({revenue})")

    return (len(violations) == 0, violations)


def validate_finance(record: dict) -> tuple[bool, list[str]]:
    """
    Staging validation for finance — only checks field presence and castability.
    Does NOT validate KPI math (that's the observability layer's job).
    """
    violations: list[str] = []

    required = ["date", "total_spend", "total_revenue", "roas", "cac", "profit"]
    for f in required:
        if f not in record:
            violations.append(f"missing_field:{f}")

    total_spend = _as_float(record.get("total_spend"))
    total_revenue = _as_float(record.get("total_revenue"))

    if record.get("total_spend") is not None and total_spend is None:
        violations.append("cast_failed:total_spend:expected_numeric")
    if record.get("total_revenue") is not None and total_revenue is None:
        violations.append("cast_failed:total_revenue:expected_numeric")

    # Check castability of KPI fields
    for f in ["roas", "cac", "profit"]:
        if record.get(f) is not None and _as_float(record.get(f)) is None:
            violations.append(f"cast_failed:{f}:expected_numeric")

    if total_spend is not None and total_spend < 0:
        violations.append(f"constraint:total_spend({total_spend})<0")
    if total_revenue is not None and total_revenue < 0:
        violations.append(f"constraint:total_revenue({total_revenue})<0")

    return (len(violations) == 0, violations)
