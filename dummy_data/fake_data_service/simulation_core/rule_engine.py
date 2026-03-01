"""
Rule Engine — validates generated records against domain-specific business rules.
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Tuple


class RuleEngine:
    """Validates a single record (dict) against the rules for a given domain."""

    # Tolerance for floating-point comparisons
    _FLOAT_TOL = 0.01

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate(self, domain: str, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate *record* against the rules for *domain*.

        Returns:
            (is_valid, violations) where *violations* is a list of human-readable
            rule-violation descriptions.  Empty list when valid.
        """
        handler = self._DOMAIN_HANDLERS.get(domain)
        if handler is None:
            return True, []
        return handler(self, record)

    # ------------------------------------------------------------------
    # Domain-specific validators
    # ------------------------------------------------------------------

    def _validate_ads(self, rec: Dict[str, Any]) -> Tuple[bool, List[str]]:
        violations: List[str] = []

        impressions = rec.get("impressions", 0)
        clicks = rec.get("clicks", 0)
        conversions = rec.get("conversions", 0)
        spend = rec.get("spend", 0.0)
        cpc = rec.get("cpc", 0.0)

        if impressions < clicks:
            violations.append(
                f"impressions ({impressions}) < clicks ({clicks})"
            )
        if clicks < conversions:
            violations.append(
                f"clicks ({clicks}) < conversions ({conversions})"
            )

        expected_spend = round(clicks * cpc, 2)
        if abs(spend - expected_spend) > self._FLOAT_TOL:
            violations.append(
                f"spend ({spend}) != clicks*cpc ({expected_spend})"
            )

        return (len(violations) == 0, violations)

    def _validate_finance(self, rec: Dict[str, Any]) -> Tuple[bool, List[str]]:
        violations: List[str] = []

        try:
            total_spend = float(rec.get("total_spend", 0))
            total_revenue = float(rec.get("total_revenue", 0))
            roas = float(rec.get("roas", 0))
            profit = float(rec.get("profit", 0))
        except (TypeError, ValueError) as e:
            violations.append(f"type_error: cannot convert field to numeric ({e})")
            return (False, violations)

        # ROAS = revenue / spend (guard against zero-division)
        if total_spend > 0:
            expected_roas = round(total_revenue / total_spend, 4)
            if abs(roas - expected_roas) > self._FLOAT_TOL:
                violations.append(
                    f"roas ({roas}) != revenue/spend ({expected_roas})"
                )
        elif roas != 0:
            violations.append(f"roas ({roas}) should be 0 when spend is 0")

        expected_profit = round(total_revenue - total_spend, 2)
        if abs(profit - expected_profit) > self._FLOAT_TOL:
            violations.append(
                f"profit ({profit}) != revenue-spend ({expected_profit})"
            )

        # Sanity: negative values
        if total_spend < 0:
            violations.append(f"total_spend ({total_spend}) is negative")
        if roas < 0:
            violations.append(f"roas ({roas}) is negative")

        return (len(violations) == 0, violations)

    def _validate_analytics(self, rec: Dict[str, Any]) -> Tuple[bool, List[str]]:
        violations: List[str] = []

        sessions = rec.get("sessions", 0)
        conversion_events = rec.get("conversion_events", 0)

        if conversion_events > sessions:
            violations.append(
                f"conversion_events ({conversion_events}) > sessions ({sessions})"
            )

        # Funnel monotonicity
        funnel_keys = ["funnel_step_1", "funnel_step_2", "funnel_step_3"]
        prev_val = sessions
        for key in funnel_keys:
            val = rec.get(key, 0)
            if val > prev_val:
                violations.append(
                    f"{key} ({val}) > previous step ({prev_val})"
                )
            prev_val = val

        if conversion_events > rec.get("funnel_step_3", conversion_events):
            violations.append(
                "conversion_events exceeds funnel_step_3"
            )

        return (len(violations) == 0, violations)

    def _validate_crm(self, rec: Dict[str, Any]) -> Tuple[bool, List[str]]:
        violations: List[str] = []

        status = rec.get("status", "")
        revenue = rec.get("revenue")

        # pandas converts None → NaN; treat NaN as None
        if revenue is not None and (isinstance(revenue, float) and math.isnan(revenue)):
            revenue = None

        if status == "closed_won" and (revenue is None or revenue <= 0):
            violations.append(
                f"closed_won lead has invalid revenue ({revenue})"
            )
        if status != "closed_won" and revenue is not None:
            violations.append(
                f"non-closed_won lead (status={status}) has revenue ({revenue})"
            )

        return (len(violations) == 0, violations)

    # ------------------------------------------------------------------
    # Dispatch table
    # ------------------------------------------------------------------

    _DOMAIN_HANDLERS = {
        "ads": _validate_ads,
        "finance": _validate_finance,
        "analytics": _validate_analytics,
        "crm": _validate_crm,
    }
