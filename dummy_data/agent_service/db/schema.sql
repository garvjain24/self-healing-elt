-- Agent Service database — SEPARATE from pipeline DB
-- The agent NEVER writes to data_pipeline.duckdb

-- Tracks which data_quality_events the agent has seen and acted on.
CREATE SEQUENCE IF NOT EXISTS agent_events_seq;
CREATE TABLE IF NOT EXISTS agent_processed_events (
  id               INTEGER PRIMARY KEY DEFAULT nextval('agent_events_seq'),
  pipeline_event_id INTEGER NOT NULL UNIQUE,
  date             DATE,
  domain           TEXT,
  event_type       TEXT,
  agent_severity   TEXT,
  risk_score       INTEGER,
  action_taken     TEXT,
  action_payload   JSON,
  action_success   BOOLEAN,
  llm_cost         DOUBLE DEFAULT 0,
  processed_at     TIMESTAMP DEFAULT current_timestamp,
  resolved_at      TIMESTAMP
);

-- Agent's persistent experience store
CREATE TABLE IF NOT EXISTS agent_memory (
  id              INTEGER PRIMARY KEY,
  signature       TEXT UNIQUE NOT NULL,
  domain          TEXT,
  event_type      TEXT,
  action_taken    TEXT,
  action_payload  JSON,
  success_count   INTEGER DEFAULT 0,
  failure_count   INTEGER DEFAULT 0,
  total_llm_cost  DOUBLE DEFAULT 0,
  human_forced    BOOLEAN DEFAULT false,
  last_used       TIMESTAMP,
  created_at      TIMESTAMP DEFAULT current_timestamp
);

-- Human review queue
CREATE SEQUENCE IF NOT EXISTS review_seq;
CREATE TABLE IF NOT EXISTS human_review_queue (
  id               INTEGER PRIMARY KEY DEFAULT nextval('review_seq'),
  pipeline_event_id INTEGER NOT NULL,
  date             DATE,
  domain           TEXT,
  event_type       TEXT,
  agent_severity   TEXT,
  evidence         JSON,
  suggested_fix    JSON,
  status           TEXT DEFAULT 'pending',
  resolution_notes TEXT,
  created_at       TIMESTAMP DEFAULT current_timestamp,
  resolved_at      TIMESTAMP
);

-- Full audit trail
CREATE SEQUENCE IF NOT EXISTS audit_seq;
CREATE TABLE IF NOT EXISTS agent_audit_log (
  id               INTEGER PRIMARY KEY DEFAULT nextval('audit_seq'),
  pipeline_event_id INTEGER,
  action           TEXT,
  memory_hit       BOOLEAN DEFAULT false,
  llm_used         BOOLEAN DEFAULT false,
  llm_cost         DOUBLE DEFAULT 0,
  llm_confidence   DOUBLE,
  success          BOOLEAN,
  notes            TEXT,
  created_at       TIMESTAMP DEFAULT current_timestamp
);
