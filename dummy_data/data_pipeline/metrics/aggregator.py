"""
Aggregator — computes daily campaign metrics from staged data.

CRITICAL: Finance KPIs are recomputed from Ads + CRM — never trust source values.
"""

from __future__ import annotations

from data_pipeline.db.connection import get_conn


def compute_daily_metrics(date: str) -> dict:
    """
    Join stg_ads_data (spend, conversions) with stg_crm_data (closed_won revenue)
    grouped by campaign_id / platform. Write to daily_campaign_metrics.

    Returns: {date, campaigns_processed}
    """
    conn = get_conn()

    # Join ads spend/conversions with CRM closed_won revenue by campaign
    rows = conn.execute(
        """
        SELECT
            a.date,
            a.campaign_id,
            a.platform,
            SUM(a.spend)        AS total_spend,
            SUM(a.conversions)  AS total_conversions,
            COALESCE(crm_rev.total_revenue, 0) AS total_revenue
        FROM stg_ads_data a
        LEFT JOIN (
            SELECT source_campaign,
                   SUM(revenue) AS total_revenue
            FROM stg_crm_data
            WHERE status = 'closed_won'
              AND revenue IS NOT NULL
              AND created_at = ?
            GROUP BY source_campaign
        ) crm_rev ON a.campaign_id = crm_rev.source_campaign
        WHERE a.date = ?
          AND a.is_valid = true
        GROUP BY a.date, a.campaign_id, a.platform, crm_rev.total_revenue
        ORDER BY a.campaign_id
        """,
        [date, date],
    ).fetchall()

    campaigns_processed = 0
    for row in rows:
        dt, campaign_id, platform, spend, conversions, revenue = row
        spend = float(spend) if spend else 0.0
        conversions = int(conversions) if conversions else 0
        revenue = float(revenue) if revenue else 0.0

        computed_roas = round(revenue / spend, 4) if spend > 0 else 0.0
        computed_cac = round(spend / conversions, 2) if conversions > 0 else 0.0
        computed_profit = round(revenue - spend, 2)

        conn.execute(
            """
            INSERT INTO daily_campaign_metrics
                (date, campaign_id, platform, spend, conversions, revenue,
                 computed_roas, computed_cac, computed_profit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [dt, campaign_id, platform, spend, conversions, revenue,
             computed_roas, computed_cac, computed_profit],
        )
        campaigns_processed += 1

    conn.close()
    return {"date": date, "campaigns_processed": campaigns_processed}
