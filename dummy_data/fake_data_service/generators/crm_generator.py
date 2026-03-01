"""
CRM Generator — produces CRM leads and pipeline records.
"""

from __future__ import annotations

import random
import uuid
from typing import List, Optional

import pandas as pd
from faker import Faker

fake = Faker()


class CRMGenerator:
    """Generate CRM lead records for a given date."""

    _STATUSES = ["new", "qualified", "closed_won", "closed_lost"]
    _CAMPAIGN_IDS = [
        "cmp_brand_001",
        "cmp_retarget_002",
        "cmp_prospecting_003",
        "cmp_lookalike_004",
        "cmp_seasonal_005",
    ]

    def __init__(self, seed: Optional[int] = None):
        if seed is not None:
            random.seed(seed)
            Faker.seed(seed)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self, date_str: str, num_leads: int = 8) -> pd.DataFrame:
        """
        Generate *num_leads* CRM lead records for *date_str*.

        Returns:
            DataFrame with columns: lead_id, created_at, status, revenue,
            source_campaign, conversion_lag_days
        """
        rows: List[dict] = []

        for _ in range(num_leads):
            status = random.choice(self._STATUSES)
            conversion_lag = random.randint(1, 7)
            revenue = (
                round(random.uniform(200.0, 15000.0), 2)
                if status == "closed_won"
                else None
            )

            rows.append(
                {
                    "lead_id": str(uuid.uuid4()),
                    "created_at": date_str,
                    "status": status,
                    "revenue": revenue,
                    "source_campaign": random.choice(self._CAMPAIGN_IDS),
                    "conversion_lag_days": conversion_lag,
                }
            )

        return pd.DataFrame(rows)
