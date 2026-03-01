#!/usr/bin/env python3
"""
CLI entry point for the data pipeline.

Usage:
  python -m data_pipeline.main run --scenario normal_flow --days 7
  python -m data_pipeline.main run --scenario corrupted_finance --days 7 --date 2024-01-01
  python -m data_pipeline.main status --date 2024-01-03
  python -m data_pipeline.main reset
  python -m data_pipeline.main serve --port 8001
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from data_pipeline.config import API_PORT
from data_pipeline.db.connection import init_db, reset_db
from data_pipeline.ingestion.fetcher import DataServiceClient
from data_pipeline.orchestrator import run_pipeline_range, get_pipeline_status

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)


def cmd_run(args: argparse.Namespace) -> None:
    client = DataServiceClient()

    # Health check
    if not client.health_check():
        print("ERROR: Fake data service is not running at", client.base_url)
        sys.exit(1)

    # Activate scenario
    print(f"Activating scenario: {args.scenario}")
    client.activate_scenario(args.scenario)

    start_date = args.date or "2024-01-01"
    days = args.days or 7

    print(f"\n{'='*60}")
    print(f"  Pipeline: {args.scenario}")
    print(f"  Date range: {start_date} → {days} days")
    print(f"{'='*60}\n")

    # Ensure clean DB
    init_db()

    summaries = run_pipeline_range(start_date, days, client)

    # Print summary
    total_events = 0
    for s in summaries:
        date = s["date"]
        events = s.get("quality_events", [])
        total_events += len(events)

        staging_summary = []
        for domain, result in s.get("staging", {}).items():
            if isinstance(result, dict) and "total" in result:
                staging_summary.append(f"{domain}: {result['valid']}/{result['total']} valid")

        event_types = [e["event_type"] for e in events]

        print(f"── {date} ─{'─'*45}")
        if staging_summary:
            print(f"   Staging: {', '.join(staging_summary)}")
        if event_types:
            for e in events:
                det = f" ({e.get('metric_name', '')})" if e.get('metric_name') else ""
                dev = f" dev={e['deviation_pct']}%" if e.get('deviation_pct') else ""
                print(f"   ⚡ {e['event_type']}{det}{dev}  [{e['domain']}]")
        else:
            print("   ✓ No quality events")

    print(f"\n{'='*60}")
    print(f"  PIPELINE COMPLETE")
    print(f"  Total quality events: {total_events}")
    print(f"{'='*60}\n")


def cmd_status(args: argparse.Namespace) -> None:
    init_db()
    status = get_pipeline_status(args.date)
    print(json.dumps(status, indent=2, default=str))


def cmd_reset(args: argparse.Namespace) -> None:
    reset_db()
    print("Database reset and recreated.")


def cmd_serve(args: argparse.Namespace) -> None:
    import uvicorn
    port = args.port or API_PORT
    print(f"Starting data pipeline API on port {port}...")
    uvicorn.run("data_pipeline.api:app", host="0.0.0.0", port=port, reload=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Data Pipeline — Warehouse & Quality Engine")
    sub = parser.add_subparsers(dest="command")

    # run
    run_p = sub.add_parser("run", help="Run pipeline for a scenario")
    run_p.add_argument("--scenario", required=True)
    run_p.add_argument("--days", type=int, default=7)
    run_p.add_argument("--date", default=None, help="Start date (YYYY-MM-DD)")

    # status
    stat_p = sub.add_parser("status", help="Show pipeline status for a date")
    stat_p.add_argument("--date", required=True)

    # reset
    sub.add_parser("reset", help="Drop and recreate all tables")

    # serve
    serve_p = sub.add_parser("serve", help="Start read-only API server")
    serve_p.add_argument("--port", type=int, default=None)

    args = parser.parse_args()
    if args.command == "run":
        cmd_run(args)
    elif args.command == "status":
        cmd_status(args)
    elif args.command == "reset":
        cmd_reset(args)
    elif args.command == "serve":
        cmd_serve(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
