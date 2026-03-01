# Project 3: Agent Service

The defining "agentic" component. It reads neutral data quality events from the pipeline warehouse and applies business logic, historical context, and LLM reasoning to decide the best course of action.

## Architecture

1. **Observer**: `event_fetcher.py` + `context_builder.py` – Polls the pipeline API and enriches events with historical counts.
2. **Interpreter**: `severity_scorer.py` – Converts pipeline deviations into `LOW`, `MEDIUM`, `HIGH`, or `CRITICAL` agent risk scores based on explicit rules.
3. **Memory**: `memory_store.py` – Persistent DuckDB ledger of the agent’s past actions. Signatures are built deterministically (`event_type:domain:stage:metric`).
4. **Decision**: `policy_engine.py` – Routes events cascade-style:
   - Check if the pattern is flagged as `human_forced`
   - Check memory for past successful actions
   - Execute deterministic routing (e.g., CRITICAL -> escalate, HIGH -> LLM or auto-fix)
5. **Actions**: Handlers for each outcome:
   - `noop`: Log and ignore.
   - `auto_retry`: Wait for late data to arrive (pipeline handles naturally).
   - `auto_fix`: Deterministic schema corrections stored in agent memory (no modification to pipeline DB).
   - `llm_fix`: Two-call OpenRouter logic with self-review and confidence calibration.
   - `escalate`: Written to `human_review_queue` for manual intervention.

## Setup

```bash
# Optional: create a fresh virtualenv
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Ensure OpenRouter API key is properly set in `agent_service/config.py`.

## Running the Agent

You can start the agent loop to continuously poll the warehouse:

```bash
python -m agent_service.main run --poll 15
```

For the human review dashboard API (runs on port 8003):

```bash
python -m agent_service.main serve --port 8003
```

To see agent status, memory stats, and cost:

```bash
python -m agent_service.main status
```

To run a scenario evaluation against ground truth:

```bash
python -m agent_service.main evaluate --scenario corrupted_finance --date 2024-01-01 --days 7
```

## How It Interacts

- **Reads** extensively from `data_pipeline` via HTTP API (`http://localhost:8002/warehouse/*`).
- **Cannot write** to `data_pipeline.duckdb`. It has its own independent database (`agent_service.duckdb`) tracking what it has processed, its learned memory, and human escalations.
