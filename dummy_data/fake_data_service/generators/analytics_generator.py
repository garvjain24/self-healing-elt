"""
Analytics Generator — produces GA4-style web analytics records.
"""

from __future__ import annotations

import random
from typing import List, Optional

import pandas as pd


class AnalyticsGenerator:
    """Generate GA4-style web analytics data for a given date."""

    _SOURCES = [
        "google_cpc",
        "meta_cpc",
        "organic_search",
        "direct",
        "email",
        "referral",
    ]

    def __init__(self, seed: Optional[int] = None):
        if seed is not None:
            random.seed(seed)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self, date_str: str, num_sources: int = 4) -> pd.DataFrame:
        """
        Generate analytics rows — one per traffic source.

        Returns:
            DataFrame with columns: date, sessions, pageviews, funnel_step_1,
            funnel_step_2, funnel_step_3, conversion_events, source
        """
        sources = random.sample(self._SOURCES, min(num_sources, len(self._SOURCES)))
        rows: List[dict] = []

        for src in sources:
            sessions = random.randint(200, 8000)
            pageviews = sessions + random.randint(0, sessions * 2)

            # Monotonically decreasing funnel
            funnel_1 = random.randint(int(sessions * 0.40), int(sessions * 0.75))
            funnel_2 = random.randint(int(funnel_1 * 0.30), int(funnel_1 * 0.70))
            funnel_3 = random.randint(int(funnel_2 * 0.25), int(funnel_2 * 0.65))
            conversion_events = random.randint(
                max(0, int(funnel_3 * 0.30)), max(1, int(funnel_3 * 0.80))
            )

            rows.append(
                {
                    "date": date_str,
                    "sessions": sessions,
                    "pageviews": pageviews,
                    "funnel_step_1": funnel_1,
                    "funnel_step_2": funnel_2,
                    "funnel_step_3": funnel_3,
                    "conversion_events": conversion_events,
                    "source": src,
                }
            )

        return pd.DataFrame(rows)
