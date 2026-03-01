"""
Ads Generator — produces Google/Meta paid-ads records with realistic daily variation.
"""

from __future__ import annotations

import random
from typing import List, Optional

import pandas as pd
from faker import Faker

fake = Faker()


class AdsGenerator:
    """Generate paid-ads records for a given date."""

    _PLATFORMS = ["google", "meta"]
    _CAMPAIGN_PREFIXES = ["brand", "retarget", "prospecting", "lookalike", "seasonal"]

    def __init__(self, seed: Optional[int] = None):
        if seed is not None:
            random.seed(seed)
            Faker.seed(seed)

        # Random-walk state: one value per campaign slot
        self._prev_impressions: dict[str, int] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self, date_str: str, num_campaigns: int = 5) -> pd.DataFrame:
        """
        Generate *num_campaigns* ad records for *date_str*.

        Returns:
            DataFrame with columns: date, campaign_id, platform, impressions,
            clicks, spend, conversions, cpc
        """
        rows: List[dict] = []
        for i in range(num_campaigns):
            campaign_id = self._campaign_id(i)
            platform = self._PLATFORMS[i % len(self._PLATFORMS)]

            impressions = self._next_impressions(campaign_id)
            clicks = random.randint(
                max(1, int(impressions * 0.01)),
                max(2, int(impressions * 0.12)),
            )
            conversions = random.randint(0, max(1, int(clicks * 0.30)))
            cpc = round(random.uniform(0.20, 3.50), 2)
            spend = round(clicks * cpc, 2)

            rows.append(
                {
                    "date": date_str,
                    "campaign_id": campaign_id,
                    "platform": platform,
                    "impressions": impressions,
                    "clicks": clicks,
                    "spend": spend,
                    "conversions": conversions,
                    "cpc": cpc,
                }
            )

        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _campaign_id(self, idx: int) -> str:
        prefix = self._CAMPAIGN_PREFIXES[idx % len(self._CAMPAIGN_PREFIXES)]
        return f"cmp_{prefix}_{idx + 1:03d}"

    def _next_impressions(self, campaign_id: str) -> int:
        """Random-walk impressions (gradual daily variation)."""
        base = self._prev_impressions.get(campaign_id, random.randint(5000, 50000))
        delta = int(base * random.uniform(-0.15, 0.15))
        value = max(500, base + delta)
        self._prev_impressions[campaign_id] = value
        return value
