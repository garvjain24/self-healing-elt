#!/usr/bin/env bash

set -e

# Configuration
SCENARIO=${1:-corrupted_finance}
DAYS=${2:-7}
START_DATE=${3:-2024-01-01}

echo "============================================================"
echo "    END-TO-END DEMO: $SCENARIO ($DAYS days from $START_DATE)"
echo "============================================================"
echo "This script runs all three projects sequentially to demonstrate"
echo "the full Agentic Self-Healing Data Pipeline."
echo ""

# Activate virtual environment
if [ ! -d "venv" ]; then
    echo "ERROR: venv not found. Please create it and install requirements."
    exit 1
fi
source venv/bin/activate

# Step 0: Cleanup previous state
echo "🧹 Cleaning up previous databases and background processes..."
rm -f data_pipeline.duckdb data_pipeline.duckdb.wal
rm -f agent_service.duckdb agent_service.duckdb.wal
lsof -ti :8099 | xargs kill -9 2>/dev/null || true
lsof -ti :8002 | xargs kill -9 2>/dev/null || true
lsof -ti :8003 | xargs kill -9 2>/dev/null || true
sleep 1

# Step 1: Start Fake Data Service (Project 1)
echo "🚀 [Project 1] Starting Fake Data Simulator (port 8099)..."
python -m uvicorn fake_data_service.outputs.api_server:app --host 0.0.0.0 --port 8099 --log-level warning &
PID_P1=$!
sleep 2

# Step 2: Run Data Pipeline (Project 2)
echo "🚀 [Project 2] Running Data Pipeline for $DAYS days..."
echo "              Extracting from Simulator -> Validating -> Computing Metrics -> Emitting Quality Events"
python -m data_pipeline.main run --scenario "$SCENARIO" --days "$DAYS" --date "$START_DATE"

# Step 3: Start Pipeline Warehouse API
echo "🚀 [Project 2] Starting Pipeline Warehouse API (port 8002)..."
python -m uvicorn data_pipeline.api:app --host 0.0.0.0 --port 8002 --log-level warning &
PID_P2=$!
sleep 2

# END_DATE calculation
END_DATE=$(python -c "from datetime import date, timedelta; print((date.fromisoformat('$START_DATE') + timedelta(days=$DAYS)).isoformat())")

# Step 4: Run Agent Service (Project 3)
echo "🚀 [Project 3] Running Agent Service..."
echo "              Reading Quality Events -> Applying Reasoning -> Executing Fixes/Escalations"
echo "------------------------------------------------------------"

python -c "
import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)-6s %(message)s')
from agent_service.agent_loop import process_events_once, init_db
from agent_service.observer.event_fetcher import EventFetcher
from agent_service.memory.memory_store import MemoryStore
from agent_service.config import POLICY

init_db()
count = process_events_once(
    EventFetcher(), MemoryStore(), POLICY,
    start_date='$START_DATE', end_date='$END_DATE'
)
print(f'\n✓ Agent autonomously processed {count} events!')
" 2>&1 | grep -v "HTTP Request"

echo "------------------------------------------------------------"
echo "📊 Agent Evaluation Results versus Ground Truth YAML:"
python -m agent_service.main evaluate --scenario "$SCENARIO" --date "$START_DATE" --days "$DAYS"

# Cleanup
echo ""
echo "🧹 Cleaning up background APIs..."
kill $PID_P1 $PID_P2 2>/dev/null || true

echo "✅ Demo Complete!"
echo "Run './run_demo.sh <scenario_name>' to try other scenarios (e.g. normal_flow, attribution_delay)."
