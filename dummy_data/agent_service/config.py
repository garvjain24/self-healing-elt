"""Agent service configuration."""

from __future__ import annotations

PIPELINE_WAREHOUSE_URL = "http://localhost:8002"
FAKE_DATA_SERVICE_URL = "http://localhost:8099"
AGENT_DB_PATH = "agent_service.duckdb"
SCENARIOS_DIR = "fake_data_service/scenarios"
AGENT_API_PORT = 8003

OPENROUTER_API_KEY = "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"
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_MODEL = "arcee-ai/trinity-large-preview:free"
LLM_COST_PER_CALL = 0.01  # $0.01 per API call

POLICY = {
    "allow_llm": True,
    "max_cost_per_action": 0.05,
    "llm_confidence_threshold": 0.75,
    "memory_confidence_threshold": 0.80,
    "retry_wait": 15,
    "poll_interval": 15,
    "date_range_days": 7,
}
