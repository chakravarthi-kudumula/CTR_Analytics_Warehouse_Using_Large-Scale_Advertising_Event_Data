#!/usr/bin/env python3

"""Capture benchmark snapshots for the latest completed batch pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline_tracking import (
    complete_pipeline_run,
    complete_pipeline_step,
    create_pipeline_run,
    create_pipeline_step,
    ensure_pipeline_metadata,
    fetch_batch_details,
    resolve_batch_context,
    run_query,
    run_scalar_query,
    sql_literal,
)
from project_config import PROCESSED_DIR, PROJECT_ROOT, SQL_DIR, add_db_connection_args


TABLES_TO_MEASURE = {
    "raw": "raw.criteo_events",
    "staging": "staging.stg_criteo_events",
    "warehouse": "warehouse.fact_ad_events",
    "feature_store": "feature_store.ctr_training_features",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture pipeline benchmark snapshots")
    parser.add_argument("--batch-name")
    parser.add_argument("--triggered-by", default="manual")
    add_db_connection_args(parser)
    return parser.parse_args()


def quote_nullable(value: str | None) -> str:
    return "null" if value is None else f"'{sql_literal(value)}'"


def capture_stage_benchmarks(batch_id: int, args: argparse.Namespace) -> None:
    query = f"""
insert into ops.benchmark_snapshots (
    batch_id,
    pipeline_run_id,
    benchmark_name,
    layer_name,
    object_name,
    row_count,
    duration_seconds,
    rows_per_second,
    storage_mb,
    notes
)
select
    prs.batch_id,
    prs.pipeline_run_id,
    'pipeline_stage_runtime',
    prs.layer_name,
    prs.pipeline_name,
    b.expected_row_count,
    prs.duration_seconds,
    case
        when prs.duration_seconds > 0 and b.expected_row_count is not null then round((b.expected_row_count::numeric / prs.duration_seconds), 3)
        else null
    end,
    null,
    'Captured from ops.pipeline_run_summary for the latest successful batch run.'
from ops.pipeline_run_summary prs
join ops.batch_registry b
  on b.batch_id = prs.batch_id
where prs.batch_id = {batch_id}
  and prs.run_status = 'SUCCESS';
"""
    run_query(query, args.database, args)


def capture_storage_benchmarks(batch_id: int, args: argparse.Namespace) -> None:
    for layer_name, object_name in TABLES_TO_MEASURE.items():
        if object_name in {"raw.criteo_events", "staging.stg_criteo_events", "warehouse.fact_ad_events", "feature_store.ctr_training_features"}:
            row_count = int(run_scalar_query(f"select count(*) from {object_name} where batch_id = {batch_id};", args.database, args))
        else:
            row_count = int(run_scalar_query(f"select count(*) from {object_name};", args.database, args))
        storage_mb = run_scalar_query(
            f"select round(pg_total_relation_size('{object_name}')::numeric / 1024 / 1024, 3);",
            args.database,
            args,
        )
        query = f"""
insert into ops.benchmark_snapshots (
    batch_id,
    pipeline_run_id,
    benchmark_name,
    layer_name,
    object_name,
    row_count,
    duration_seconds,
    rows_per_second,
    storage_mb,
    notes
)
values (
    {batch_id},
    null,
    'table_storage',
    '{sql_literal(layer_name)}',
    '{sql_literal(object_name)}',
    {row_count},
    null,
    null,
    {storage_mb},
    'Row count is batch-scoped; storage size is full-table size after a completed batch run.'
);
"""
        run_query(query, args.database, args)


def capture_file_benchmarks(batch_id: int, batch_name: str, source_file: str, source_path: Path | None, project_root: Path, args: argparse.Namespace) -> None:
    if source_path and source_path.exists():
        query = f"""
insert into ops.benchmark_snapshots (
    batch_id,
    pipeline_run_id,
    benchmark_name,
    layer_name,
    object_name,
    row_count,
    duration_seconds,
    rows_per_second,
    storage_mb,
    notes
)
values (
    {batch_id},
    null,
    'file_storage',
    'source',
    '{sql_literal(source_path.name)}',
    null,
    null,
    null,
    {round(source_path.stat().st_size / 1024 / 1024, 3)},
    'Source file size on disk.'
);
"""
        run_query(query, args.database, args)

    batch_dir = PROCESSED_DIR / batch_name
    if batch_dir.exists():
        total_size_mb = round(
            sum(file_path.stat().st_size for file_path in batch_dir.rglob("*") if file_path.is_file()) / 1024 / 1024,
            3,
        )
        query = f"""
insert into ops.benchmark_snapshots (
    batch_id,
    pipeline_run_id,
    benchmark_name,
    layer_name,
    object_name,
    row_count,
    duration_seconds,
    rows_per_second,
    storage_mb,
    notes
)
values (
    {batch_id},
    null,
    'file_storage',
    'processed',
    '{sql_literal(batch_name)}',
    null,
    null,
    null,
    {total_size_mb},
    'Processed Spark artifact directory size on disk.'
);
"""
        run_query(query, args.database, args)


def main() -> None:
    args = parse_args()
    project_root = PROJECT_ROOT
    sql_dir = SQL_DIR

    ensure_pipeline_metadata(sql_dir, args.database, args)
    run_query("delete from ops.benchmark_snapshots where recorded_at < now() - interval '90 days';", args.database, args)

    batch_id, source_file = resolve_batch_context(
        table_name="warehouse.fact_ad_events",
        batch_name=args.batch_name,
        database=args.database,
        args=args,
    )
    batch_details = fetch_batch_details(args.batch_name, args.database, args) if args.batch_name else None
    source_path = None
    batch_name = args.batch_name
    if batch_details:
        batch_name = str(batch_details["batch_name"]) if "batch_name" in batch_details else args.batch_name
        if batch_details["source_path"]:
            source_path = Path(str(batch_details["source_path"]))
    pipeline_run_id = create_pipeline_run(
        batch_id=batch_id,
        pipeline_name="benchmark_capture",
        layer_name="ops",
        source_file=source_file,
        triggered_by=args.triggered_by,
        database=args.database,
        args=args,
    )
    pipeline_step_id = create_pipeline_step(
        pipeline_run_id=pipeline_run_id,
        batch_id=batch_id,
        step_name="capture_benchmark_snapshots",
        layer_name="ops",
        target_table="ops.benchmark_snapshots",
        source_file=source_file,
        database=args.database,
        args=args,
    )

    try:
        capture_stage_benchmarks(batch_id, args)
        capture_storage_benchmarks(batch_id, args)
        capture_file_benchmarks(batch_id, batch_name or f"batch_{batch_id}", source_file, source_path, project_root, args)

        benchmark_rows = int(
            run_scalar_query(
                f"select count(*) from ops.pipeline_benchmark_summary where batch_id = {batch_id};",
                args.database,
                args,
            )
        )
        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="SUCCESS",
            rows_processed=benchmark_rows,
            step_message=f"Benchmark capture completed successfully for batch {batch_id}.",
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="SUCCESS",
            run_message=f"Benchmark capture completed successfully for batch {batch_id}.",
            database=args.database,
            args=args,
        )

        print(f"Batch ID: {batch_id}")
        print(f"Source file tag: {source_file}")
        print(f"Benchmark summary rows: {benchmark_rows}")
        print("Benchmark capture completed successfully.")
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
        raise


if __name__ == "__main__":
    main()
