"""
Finance Generator — produces financial KPI records (ROAS, CAC, profit).
"""

from __future__ import annotations

import random
from typing import Optional

import pandas as pd


class FinanceGenerator:
    """Generate a single-row daily finance summary."""

    def __init__(self, seed: Optional[int] = None):
        if seed is not None:
            random.seed(seed)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(
        self,
        date_str: str,
        ads_df: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """
        Generate a finance summary for *date_str*.

        If *ads_df* is provided (the day's ads data), spend/conversions are
        derived from it.  Otherwise synthetic values are used.

        Returns:
            DataFrame with columns: date, total_spend, total_revenue, roas,
            cac, profit
        """
        if ads_df is not None and not ads_df.empty:
            total_spend = round(float(ads_df["spend"].sum()), 2)
            total_conversions = int(ads_df["conversions"].sum())
        else:
            total_spend = round(random.uniform(500, 5000), 2)
            total_conversions = random.randint(5, 80)

        # Revenue is a random multiplier on spend (simulate ROAS 1.5–4x)
        roas_multiplier = round(random.uniform(1.5, 4.0), 2)
        total_revenue = round(total_spend * roas_multiplier, 2)

        roas = round(total_revenue / total_spend, 4) if total_spend > 0 else 0.0
        profit = round(total_revenue - total_spend, 2)
        cac = (
            round(total_spend / total_conversions, 2) if total_conversions > 0 else 0.0
        )

        row = {
            "date": date_str,
            "total_spend": total_spend,
            "total_revenue": total_revenue,
            "roas": roas,
            "cac": cac,
            "profit": profit,
        }

        return pd.DataFrame([row])
