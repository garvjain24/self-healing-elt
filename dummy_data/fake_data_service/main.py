#!/usr/bin/env python3
"""
main.py — CLI entry point for the Controlled Reality Simulator.

Usage examples:
    python -m fake_data_service.main --scenario normal_flow --days 7 --export csv
    python -m fake_data_service.main --scenario corrupted_finance --days 7 --export csv
    python -m fake_data_service.main --serve   # start FastAPI server
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

from fake_data_service.generators.ads_generator import AdsGenerator
from fake_data_service.generators.analytics_generator import AnalyticsGenerator
from fake_data_service.generators.crm_generator import CRMGenerator
from fake_data_service.generators.finance_generator import FinanceGenerator
from fake_data_service.generators.manual_generator import ManualGenerator
from fake_data_service.scenario_loader import ScenarioLoader
from fake_data_service.simulation_core.failure_engine import FailureEngine
from fake_data_service.simulation_core.rule_engine import RuleEngine
from fake_data_service.simulation_core.time_engine import TimeEngine


def run_simulation(args: argparse.Namespace) -> None:
    """Load a scenario, run the time engine for N days, and export results."""
    loader = ScenarioLoader()
    scenario = loader.load(args.scenario)

    start_date = scenario.get("start_date", "2024-01-01")
    duration = args.days or scenario.get("duration_days", 7)
    generators_cfg = scenario.get("generators", {})
    failure_configs = scenario.get("failure_config", [])

    time_engine = TimeEngine(start_date)
    failure_engine = FailureEngine(failure_configs)
    rule_engine = RuleEngine()

    # Generators
    ads_gen = AdsGenerator()
    analytics_gen = AnalyticsGenerator()
    crm_gen = CRMGenerator()
    finance_gen = FinanceGenerator()
    manual_gen = ManualGenerator()

    # Accumulators
    all_results: dict[str, list[pd.DataFrame]] = {
        "ads": [], "analytics": [], "crm": [], "finance": [], "manual": [],
    }
    summary_rows: list[dict] = []

    print(f"\n{'=' * 60}")
    print(f"  Scenario : {scenario['scenario_id']}")
    print(f"  Duration : {duration} days  (start: {start_date})")
    print(f"  Export   : {args.export}")
    print(f"{'=' * 60}\n")

    for day in range(duration):
        current_date = time_engine.get_current_date()
        date_str = current_date.isoformat()
        day_idx = time_engine.get_day_index()

        print(f"── Day {day_idx} ({date_str}) {'─' * 40}")

        # ── Generate ──
        ads_df = analytics_df = crm_df = finance_df = manual_df = pd.DataFrame()

        if generators_cfg.get("ads"):
            ads_df = ads_gen.generate(date_str)
        if generators_cfg.get("analytics"):
            analytics_df = analytics_gen.generate(date_str)
        if generators_cfg.get("crm"):
            crm_df = crm_gen.generate(date_str)
        if generators_cfg.get("finance"):
            finance_df = finance_gen.generate(date_str, ads_df=ads_df if not ads_df.empty else None)
        if generators_cfg.get("manual"):
            manual_df = manual_gen.generate_dataframe(date_str, inject_human_errors=True)

        # ── Inject Failures ──
        domain_dfs = {
            "ads": ads_df, "analytics": analytics_df,
            "crm": crm_df, "finance": finance_df, "manual": manual_df,
        }

        for domain, df in domain_dfs.items():
            if df.empty:
                continue

            df_out = failure_engine.inject_failures(domain, df, day_idx)
            log = failure_engine.get_injected_log()
            severity = failure_engine.get_max_severity()

            # ── Validate ──
            violations_all: list[str] = []
            for _, row in df_out.iterrows():
                valid, viols = rule_engine.validate(domain, row.to_dict())
                violations_all.extend(viols)

            # Tag CRITICAL records
            if severity == "CRITICAL":
                df_out["requires_human_review"] = True
            else:
                df_out["requires_human_review"] = False

            all_results[domain].append(df_out)

            if log:
                for entry in log:
                    ft = entry["failure_type"]
                    sev = entry["severity"]
                    action = entry["details"].get("expected_agent_action", "n/a")
                    print(f"   ⚠  [{sev}] {domain}: {ft}  →  expected: {action}")
                    summary_rows.append({
                        "day": day_idx,
                        "domain": domain,
                        "failure_type": ft,
                        "severity": sev,
                        "expected_action": action,
                    })

            if violations_all:
                for v in violations_all[:3]:
                    print(f"   ✗  Validation: {v}")

        time_engine.advance_day()

    # ── Export ──
    base_dir = Path(__file__).resolve().parent / "outputs"
    if args.export in ("csv", "both"):
        csv_dir = base_dir / "csv_exports"
        csv_dir.mkdir(parents=True, exist_ok=True)
        for domain, frames in all_results.items():
            if frames:
                combined = pd.concat(frames, ignore_index=True)
                path = csv_dir / f"{args.scenario}_{domain}.csv"
                combined.to_csv(path, index=False)
                print(f"   📄  Exported {path.name} ({len(combined)} rows)")

    if args.export in ("pdf", "both") and generators_cfg.get("manual"):
        pdf_dir = str(base_dir / "pdf_exports")
        manual_gen.generate_pdf(start_date, pdf_dir)
        print(f"   📄  PDF exported to {pdf_dir}")

    # ── Summary ──
    print(f"\n{'=' * 60}")
    print("  SIMULATION SUMMARY")
    print(f"{'=' * 60}")
    if summary_rows:
        summary_df = pd.DataFrame(summary_rows)
        severity_counts = summary_df["severity"].value_counts().to_dict()
        print(f"  Total errors injected : {len(summary_rows)}")
        for sev in ("LOW", "MEDIUM", "HIGH", "CRITICAL"):
            if sev in severity_counts:
                print(f"    {sev:10s} : {severity_counts[sev]}")
        print("\n  Expected Agent Actions:")
        for _, r in summary_df.iterrows():
            print(f"    Day {r['day']} | {r['domain']:10s} | {r['failure_type']:15s} | {r['severity']:8s} | → {r['expected_action']}")
    else:
        print("  No errors injected (clean run).")
    print(f"{'=' * 60}\n")


def start_server(args: argparse.Namespace) -> None:
    """Start the FastAPI server via uvicorn."""
    import uvicorn
    print("Starting Fake Data Service API server …")
    uvicorn.run(
        "fake_data_service.outputs.api_server:app",
        host=args.host,
        port=args.port,
        reload=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Controlled Reality Simulator — fake marketing data pipeline",
    )
    subparsers = parser.add_subparsers(dest="command")

    # ── run sub-command ──
    run_parser = subparsers.add_parser("run", help="Run a simulation scenario")
    run_parser.add_argument("--scenario", required=True, help="Scenario ID (e.g. normal_flow)")
    run_parser.add_argument("--days", type=int, default=None, help="Override duration_days")
    run_parser.add_argument("--export", choices=["csv", "pdf", "both", "none"], default="csv")

    # ── serve sub-command ──
    serve_parser = subparsers.add_parser("serve", help="Start the FastAPI REST server")
    serve_parser.add_argument("--host", default="0.0.0.0")
    serve_parser.add_argument("--port", type=int, default=8000)

    # ── Flat flags (backward compat) ──
    parser.add_argument("--scenario", dest="flat_scenario", default=None)
    parser.add_argument("--days", dest="flat_days", type=int, default=None)
    parser.add_argument("--export", dest="flat_export", choices=["csv", "pdf", "both", "none"], default="csv")
    parser.add_argument("--serve", action="store_true")

    args = parser.parse_args()

    # Handle flat-flag style: python main.py --scenario corrupted_finance --days 7
    if args.command is None:
        if args.serve:
            ns = argparse.Namespace(host="0.0.0.0", port=8000)
            start_server(ns)
        elif args.flat_scenario:
            ns = argparse.Namespace(
                scenario=args.flat_scenario,
                days=args.flat_days,
                export=args.flat_export,
            )
            run_simulation(ns)
        else:
            parser.print_help()
    elif args.command == "run":
        run_simulation(args)
    elif args.command == "serve":
        start_server(args)


if __name__ == "__main__":
    main()
