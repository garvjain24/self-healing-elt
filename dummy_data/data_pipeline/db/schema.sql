-- ─────────────────────────────────────────
-- RAW LAYER: append-only, never mutated
-- ─────────────────────────────────────────

CREATE SEQUENCE IF NOT EXISTS raw_ads_seq;
CREATE TABLE IF NOT EXISTS raw_ads_data (
  raw_id     INTEGER PRIMARY KEY DEFAULT nextval('raw_ads_seq'),
  payload    JSON    NOT NULL,
  source_url TEXT,
  scenario   TEXT,
  errors_injected TEXT,
  ingested_at TIMESTAMP DEFAULT current_timestamp
);

CREATE SEQUENCE IF NOT EXISTS raw_analytics_seq;
CREATE TABLE IF NOT EXISTS raw_analytics_data (
  raw_id     INTEGER PRIMARY KEY DEFAULT nextval('raw_analytics_seq'),
  payload    JSON    NOT NULL,
  source_url TEXT,
  scenario   TEXT,
  errors_injected TEXT,
  ingested_at TIMESTAMP DEFAULT current_timestamp
);

CREATE SEQUENCE IF NOT EXISTS raw_crm_seq;
CREATE TABLE IF NOT EXISTS raw_crm_data (
  raw_id     INTEGER PRIMARY KEY DEFAULT nextval('raw_crm_seq'),
  payload    JSON    NOT NULL,
  source_url TEXT,
  scenario   TEXT,
  errors_injected TEXT,
  ingested_at TIMESTAMP DEFAULT current_timestamp
);

CREATE SEQUENCE IF NOT EXISTS raw_finance_seq;
CREATE TABLE IF NOT EXISTS raw_finance_data (
  raw_id     INTEGER PRIMARY KEY DEFAULT nextval('raw_finance_seq'),
  payload    JSON    NOT NULL,
  source_url TEXT,
  scenario   TEXT,
  errors_injected TEXT,
  ingested_at TIMESTAMP DEFAULT current_timestamp
);

-- ─────────────────────────────────────────
-- STAGED LAYER: validated, typed, annotated
-- ─────────────────────────────────────────

CREATE SEQUENCE IF NOT EXISTS stg_ads_seq;
CREATE TABLE IF NOT EXISTS stg_ads_data (
  id                INTEGER PRIMARY KEY DEFAULT nextval('stg_ads_seq'),
  raw_id            INTEGER REFERENCES raw_ads_data(raw_id),
  date              DATE,
  campaign_id       TEXT,
  platform          TEXT,
  impressions       BIGINT,
  clicks            BIGINT,
  spend             DOUBLE,
  conversions       BIGINT,
  cpc               DOUBLE,
  is_valid          BOOLEAN,
  validation_errors TEXT[],
  processed_at      TIMESTAMP DEFAULT current_timestamp
);

CREATE SEQUENCE IF NOT EXISTS stg_analytics_seq;
CREATE TABLE IF NOT EXISTS stg_analytics_data (
  id                INTEGER PRIMARY KEY DEFAULT nextval('stg_analytics_seq'),
  raw_id            INTEGER REFERENCES raw_analytics_data(raw_id),
  date              DATE,
  sessions          BIGINT,
  pageviews         BIGINT,
  funnel_step_1     BIGINT,
  funnel_step_2     BIGINT,
  funnel_step_3     BIGINT,
  conversion_events BIGINT,
  source            TEXT,
  is_valid          BOOLEAN,
  validation_errors TEXT[],
  processed_at      TIMESTAMP DEFAULT current_timestamp
);

CREATE SEQUENCE IF NOT EXISTS stg_crm_seq;
CREATE TABLE IF NOT EXISTS stg_crm_data (
  id                  INTEGER PRIMARY KEY DEFAULT nextval('stg_crm_seq'),
  raw_id              INTEGER REFERENCES raw_crm_data(raw_id),
  lead_id             TEXT,
  created_at          DATE,
  status              TEXT,
  revenue             DOUBLE,
  source_campaign     TEXT,
  conversion_lag_days INTEGER,
  is_valid            BOOLEAN,
  validation_errors   TEXT[],
  processed_at        TIMESTAMP DEFAULT current_timestamp
);

CREATE SEQUENCE IF NOT EXISTS stg_finance_seq;
CREATE TABLE IF NOT EXISTS stg_finance_data (
  id              INTEGER PRIMARY KEY DEFAULT nextval('stg_finance_seq'),
  raw_id          INTEGER REFERENCES raw_finance_data(raw_id),
  date            DATE,
  total_spend     DOUBLE,
  total_revenue   DOUBLE,
  reported_roas   DOUBLE,
  reported_cac    DOUBLE,
  reported_profit DOUBLE,
  is_valid        BOOLEAN,
  validation_errors TEXT[],
  processed_at    TIMESTAMP DEFAULT current_timestamp
);

-- ─────────────────────────────────────────
-- METRICS LAYER: recomputed, never trust source KPIs
-- ─────────────────────────────────────────

CREATE SEQUENCE IF NOT EXISTS metrics_seq;
CREATE TABLE IF NOT EXISTS daily_campaign_metrics (
  id           INTEGER PRIMARY KEY DEFAULT nextval('metrics_seq'),
  date         DATE,
  campaign_id  TEXT,
  platform     TEXT,
  spend        DOUBLE,
  conversions  INTEGER,
  revenue      DOUBLE,
  computed_roas   DOUBLE,
  computed_cac    DOUBLE,
  computed_profit DOUBLE,
  computed_at  TIMESTAMP DEFAULT current_timestamp
);

-- ─────────────────────────────────────────
-- OBSERVABILITY LAYER: neutral facts only
-- ─────────────────────────────────────────

CREATE SEQUENCE IF NOT EXISTS dqe_seq;
CREATE TABLE IF NOT EXISTS data_quality_events (
  id               INTEGER PRIMARY KEY DEFAULT nextval('dqe_seq'),
  created_at       TIMESTAMP DEFAULT current_timestamp,
  date             DATE,
  pipeline_stage   TEXT,
  domain           TEXT,
  event_type       TEXT,
  metric_name      TEXT,
  observed_value   DOUBLE,
  expected_value   DOUBLE,
  deviation_pct    DOUBLE,
  detail           JSON,
  reference_table  TEXT,
  reference_ids    INTEGER[]
);
