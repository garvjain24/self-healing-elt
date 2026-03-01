"""Agent service CLI entry point."""

from __future__ import annotations

import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-6s %(message)s",
)


def cmd_run(args):
    from agent_service.agent_loop import run_agent_loop
    run_agent_loop(poll_interval=args.poll)


def cmd_serve(args):
    import uvicorn
    from agent_service.db.connection import init_db
    init_db()
    uvicorn.run("agent_service.api.server:app", host="0.0.0.0", port=args.port, log_level="info")


def cmd_evaluate(args):
    from agent_service.evaluation.scorer import run_evaluation, print_evaluation_report
    results = run_evaluation(args.scenario, args.date, args.days)
    print_evaluation_report(results)


def cmd_reset(args):
    from agent_service.db.connection import reset_db
    reset_db()
    print("Agent database reset.")


def cmd_status(args):
    from agent_service.db.connection import get_conn, init_db
    init_db()
    conn = get_conn()

    total = conn.execute("SELECT COUNT(*) FROM agent_processed_events").fetchone()[0]
    by_action = conn.execute(
        "SELECT action_taken, COUNT(*) FROM agent_processed_events GROUP BY action_taken"
    ).fetchall()
    pending = conn.execute(
        "SELECT COUNT(*) FROM human_review_queue WHERE status = 'pending'"
    ).fetchone()[0]
    memory_count = conn.execute("SELECT COUNT(*) FROM agent_memory").fetchone()[0]
    total_cost = conn.execute(
        "SELECT COALESCE(SUM(llm_cost), 0) FROM agent_processed_events"
    ).fetchone()[0]
    conn.close()

    print(f"\n{'='*50}")
    print("  AGENT STATUS")
    print(f"{'='*50}")
    print(f"  Events processed:  {total}")
    print(f"  Pending reviews:   {pending}")
    print(f"  Memory entries:    {memory_count}")
    print(f"  Total LLM cost:    ${total_cost:.4f}")
    print(f"{'─'*50}")
    for action, count in by_action:
        print(f"    {action:20s}: {count}")
    print(f"{'='*50}\n")


def main():
    parser = argparse.ArgumentParser(prog="agent_service", description="Self-healing data pipeline agent")
    sub = parser.add_subparsers(dest="command")

    # run
    p_run = sub.add_parser("run", help="Start agent polling loop")
    p_run.add_argument("--poll", type=int, default=15, help="Poll interval in seconds")
    p_run.set_defaults(func=cmd_run)

    # serve
    p_serve = sub.add_parser("serve", help="Start review dashboard API")
    p_serve.add_argument("--port", type=int, default=8003, help="API port")
    p_serve.set_defaults(func=cmd_serve)

    # evaluate
    p_eval = sub.add_parser("evaluate", help="Run evaluation against ground truth")
    p_eval.add_argument("--scenario", required=True, help="Scenario ID")
    p_eval.add_argument("--date", default="2024-01-01", help="Start date")
    p_eval.add_argument("--days", type=int, default=7, help="Number of days")
    p_eval.set_defaults(func=cmd_evaluate)

    # reset
    p_reset = sub.add_parser("reset", help="Reset agent database")
    p_reset.set_defaults(func=cmd_reset)

    # status
    p_status = sub.add_parser("status", help="Print agent stats")
    p_status.set_defaults(func=cmd_status)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
