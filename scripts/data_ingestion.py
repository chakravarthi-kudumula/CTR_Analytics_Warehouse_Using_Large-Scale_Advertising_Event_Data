#!/usr/bin/env python3

"""Load sampled Criteo files into the raw PostgreSQL layer."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
import subprocess
import tempfile

from pipeline_tracking import (
    fetch_batch_details,
    create_pipeline_run,
    create_pipeline_step,
    complete_pipeline_run,
    complete_pipeline_step,
    ensure_pipeline_metadata,
    register_batch,
    run_psql,
    run_scalar_query,
    sql_literal,
    update_batch_status,
)
from project_config import PROJECT_ROOT, SAMPLE_FILES, SQL_DIR, add_db_connection_args


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load Criteo sample data into raw.criteo_events")
    parser.add_argument("--sample", choices=SAMPLE_FILES, help="Sample file to load into the raw layer.")
    parser.add_argument("--source-path", help="Explicit CSV file path for an incoming batch.")
    parser.add_argument("--batch-name")
    parser.add_argument("--sample-scale")
    parser.add_argument("--triggered-by", default="manual")
    add_db_connection_args(parser, include_maintenance_database=True)
    args = parser.parse_args()
    if not args.sample and not args.source_path:
        parser.error("Either --sample or --source-path is required.")
    return args


def build_connection_command(
    base_command: list[str],
    database: str,
    args: argparse.Namespace,
) -> list[str]:
    command = base_command.copy()
    if args.host:
        command.extend(["-h", args.host])
    if args.port:
        command.extend(["-p", str(args.port)])
    if args.user:
        command.extend(["-U", args.user])
    command.extend(["-d", database])
    return command

def ensure_database_exists(args: argparse.Namespace) -> None:
    exists = run_scalar_query(
        f"select 1 from pg_database where datname = '{args.database}';",
        args.maintenance_database,
        args,
    )
    if exists == "1":
        return

    env = os.environ.copy()
    if args.password:
        env["PGPASSWORD"] = args.password
    else:
        env.pop("PGPASSWORD", None)

    command = ["createdb"]
    if args.host:
        command.extend(["-h", args.host])
    if args.port:
        command.extend(["-p", str(args.port)])
    if args.user:
        command.extend(["-U", args.user])
    command.extend(["-T", "template0", args.database])
    subprocess.run(command, check=True, env=env)


def count_csv_rows(csv_file: Path) -> int:
    with csv_file.open("r", newline="") as source:
        return sum(1 for _ in source) - 1


def build_load_sql(csv_file: Path, batch_id: int) -> str:
    source_name = csv_file.name
    csv_path = sql_literal(str(csv_file))
    return f"""
delete from raw.criteo_events
where batch_id = {batch_id};

create temporary table temp_criteo_load
as
select
    label, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13,
    c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13,
    c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26
from raw.criteo_events
with no data;

\\copy temp_criteo_load (label, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26) from '{csv_path}' with (format csv, header true, null '');

insert into raw.criteo_events (
    batch_id,
    label, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13,
    c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13,
    c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26,
    source_file, ingested_at
)
select
    {batch_id},
    label, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13,
    c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13,
    c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26,
    '{source_name}', now()
from temp_criteo_load;
"""


def build_audit_insert_sql(source_file: str, row_count: int, batch_id: int, pipeline_run_id: int) -> str:
    escaped_source = sql_literal(source_file)
    message = sql_literal(f"Raw layer load completed successfully for {source_file}.")
    return (
        "insert into quality.load_audit "
        "(batch_id, pipeline_run_id, layer_name, table_name, source_file, row_count, check_status, check_message) "
        f"values ({batch_id}, {pipeline_run_id}, 'raw', 'raw.criteo_events', '{escaped_source}', {row_count}, 'SUCCESS', '{message}') "
        "returning audit_id;"
    )


def write_temp_sql(content: str) -> Path:
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as handle:
        handle.write(content)
        return Path(handle.name)


def main() -> None:
    args = parse_args()
    project_root = PROJECT_ROOT
    sql_dir = SQL_DIR
    if args.sample:
        source_file_path = project_root / "data" / "sample" / SAMPLE_FILES[args.sample]
        sample_scale = args.sample
        batch_name = args.batch_name or f"criteo_{args.sample}_batch"
    else:
        source_file_path = Path(args.source_path).expanduser().resolve()
        sample_scale = args.sample_scale or "incoming"
        if not args.batch_name:
            raise ValueError("--batch-name is required when using --source-path.")
        batch_name = args.batch_name

    if not source_file_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_file_path}")

    expected_rows = count_csv_rows(source_file_path)
    print(f"Loading file: {source_file_path}")
    print(f"Expected rows: {expected_rows:,}")

    ensure_database_exists(args)
    ensure_pipeline_metadata(sql_dir, args.database, args)
    run_psql(sql_dir / "02_raw_tables.sql", args.database, args)
    run_psql(sql_dir / "06_quality_checks.sql", args.database, args)

    if args.source_path:
        batch_details = fetch_batch_details(batch_name, args.database, args)
        batch_id = int(batch_details["batch_id"])
        source_file_name = str(batch_details["source_file"])
    else:
        batch_id = register_batch(
            batch_name=batch_name,
            source_file=source_file_path.name,
            source_path=str(source_file_path),
            sample_scale=sample_scale,
            expected_row_count=expected_rows,
            notes="Registered by data_ingestion.py",
            database=args.database,
            args=args,
        )
        source_file_name = source_file_path.name
    pipeline_run_id = create_pipeline_run(
        batch_id=batch_id,
        pipeline_name="raw_layer_load",
        layer_name="raw",
        source_file=source_file_name,
        triggered_by=args.triggered_by,
        database=args.database,
        args=args,
    )
    pipeline_step_id = create_pipeline_step(
        pipeline_run_id=pipeline_run_id,
        batch_id=batch_id,
        step_name="load_raw_criteo_events",
        layer_name="raw",
        target_table="raw.criteo_events",
        source_file=source_file_name,
        database=args.database,
        args=args,
    )
    update_batch_status(
        batch_id=batch_id,
        batch_status="RAW_LOADING",
        last_successful_stage=None,
        row_column=None,
        row_count=None,
        mark_started=True,
        mark_completed=False,
        database=args.database,
        args=args,
    )

    try:
        load_sql_file = write_temp_sql(build_load_sql(source_file_path, batch_id))
        try:
            run_psql(load_sql_file, args.database, args)
        finally:
            load_sql_file.unlink(missing_ok=True)

        loaded_rows = int(
            run_scalar_query(f"select count(*) from raw.criteo_events where batch_id = {batch_id};", args.database, args)
        )
        distinct_source_files = run_scalar_query(
            f"select string_agg(distinct source_file, ', ') from raw.criteo_events where batch_id = {batch_id};",
            args.database,
            args,
        )

        print(f"Loaded rows: {loaded_rows:,}")
        print(f"Source file tag: {distinct_source_files}")

        if loaded_rows != expected_rows:
            raise RuntimeError(
                f"Row count mismatch: expected {expected_rows:,}, loaded {loaded_rows:,}"
            )

        update_batch_status(
            batch_id=batch_id,
            batch_status="RAW_LOADED",
            last_successful_stage="raw",
            row_column="actual_raw_row_count",
            row_count=loaded_rows,
            mark_started=False,
            mark_completed=False,
            database=args.database,
            args=args,
        )
        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="SUCCESS",
            rows_processed=loaded_rows,
            step_message=f"Raw load completed successfully for {source_file_name}.",
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="SUCCESS",
            run_message=f"Raw layer load completed successfully for {source_file_name}.",
            database=args.database,
            args=args,
        )

        audit_id = run_scalar_query(
            build_audit_insert_sql(source_file_name, loaded_rows, batch_id, pipeline_run_id),
            args.database,
            args,
        )

        print("Raw-layer load validation passed.")
        print(f"Batch ID: {batch_id}")
        print(f"Pipeline run ID: {pipeline_run_id}")
        print(f"Audit row created: {audit_id}")
    except Exception as exc:
        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="FAILED",
            rows_processed=None,
            step_message=str(exc),
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="FAILED",
            run_message=str(exc),
            database=args.database,
            args=args,
        )
        update_batch_status(
            batch_id=batch_id,
            batch_status="RAW_LOAD_FAILED",
            last_successful_stage=None,
            row_column=None,
            row_count=None,
            mark_started=False,
            mark_completed=False,
            database=args.database,
            args=args,
        )
        raise


if __name__ == "__main__":
    main()
