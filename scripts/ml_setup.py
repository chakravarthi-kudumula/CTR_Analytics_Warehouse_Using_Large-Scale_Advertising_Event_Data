#!/usr/bin/env python3

"""Set up the ML schema and metadata tables."""

from __future__ import annotations

import argparse

from pipeline_tracking import run_psql, run_scalar_query
from project_config import PROJECT_ROOT, SQL_DIR, add_db_connection_args


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create the ML schema and metadata tables")
    add_db_connection_args(parser)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    sql_dir = SQL_DIR

    run_psql(sql_dir / "01_create_schemas.sql", args.database, args)
    run_psql(sql_dir / "21_ml_schema.sql", args.database, args)

    table_count = int(
        run_scalar_query(
            """
            select count(*)
            from information_schema.tables
            where table_schema = 'ml'
              and table_name in ('model_registry', 'training_runs', 'model_metrics', 'prediction_scores');
            """,
            args.database,
            args,
        )
    )
    view_count = int(
        run_scalar_query(
            """
            select count(*)
            from information_schema.views
            where table_schema = 'ml'
              and table_name = 'latest_training_metrics';
            """,
            args.database,
            args,
        )
    )

    print(f"Project root: {PROJECT_ROOT}")
    print(f"ML tables created or confirmed: {table_count}")
    print(f"ML views created or confirmed: {view_count}")
    print("ML schema foundation is ready.")


if __name__ == "__main__":
    main()
