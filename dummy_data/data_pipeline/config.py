"""
Pipeline configuration — constants, thresholds, and domain schemas.
"""

from __future__ import annotations

FAKE_DATA_SERVICE_URL = "http://localhost:8099"
DB_PATH = "data_pipeline.duckdb"
API_PORT = 8001

THRESHOLDS = {
    "row_count_drop_pct": 30,
    "kpi_deviation_pct": 1,
    "crm_lag_window_days": 2,
    "spend_cpc_tolerance_pct": 5,
    "rolling_avg_window_days": 7,
}

DOMAIN_SCHEMAS: dict[str, list[str]] = {
    "ads": ["date", "campaign_id", "platform", "impressions", "clicks", "spend", "conversions", "cpc"],
    "analytics": ["date", "sessions", "pageviews", "funnel_step_1", "funnel_step_2", "funnel_step_3", "conversion_events", "source"],
    "crm": ["lead_id", "created_at", "status", "revenue", "source_campaign", "conversion_lag_days"],
    "finance": ["date", "total_spend", "total_revenue", "roas", "cac", "profit"],
}

# Domains that the pipeline processes
DOMAINS = ["ads", "analytics", "crm", "finance"]
