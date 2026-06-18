#!/usr/bin/env python3

"""Refresh materialized analytical views without rebuilding upstream layers."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from project_config import add_db_connection_args


MATERIALIZED_VIEWS = [
    "marts.overall_ctr_summary",
    "marts.event_day_ctr_trend",
    "marts.numeric_bucket_ctr",
    "marts.feature_ctr_summary",
    "marts.missing_value_impact",
    "marts.feature_interaction_ctr",
    "marts.feature_ctr_ranked",
    "marts.event_day_ctr_rolling",
    "marts.high_value_segments",
    "marts.low_performing_segments",
    "marts.feature_ctr_lift_ranked",
    "marts.feature_interaction_ranked",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh analytical materialized views")
    add_db_connection_args(parser)
    return parser.parse_args()


def build_connection_command(base_command: list[str], database: str, args: argparse.Namespace) -> list[str]:
    command = base_command.copy()
    if args.host:
        command.extend(["-h", args.host])
    if args.port:
        command.extend(["-p", str(args.port)])
    if args.user:
        command.extend(["-U", args.user])
    command.extend(["-d", database])
    return command


def run_query(query: str, args: argparse.Namespace) -> None:
    env = os.environ.copy()
    if args.password:
        env["PGPASSWORD"] = args.password
    else:
        env.pop("PGPASSWORD", None)
    command = build_connection_command(["psql"], args.database, args)
    command.extend(["-v", "ON_ERROR_STOP=1", "-c", query])
    subprocess.run(command, check=True, env=env)


def main() -> None:
    args = parse_args()
    for view_name in MATERIALIZED_VIEWS:
        print(f"Refreshing {view_name}")
        run_query(f"refresh materialized view {view_name};", args)
    print("Analytical materialized views refreshed successfully.")


if __name__ == "__main__":
    main()
