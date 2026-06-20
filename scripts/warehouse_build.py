#!/usr/bin/env python3

"""Build the warehouse layer from staging.stg_criteo_events."""

from __future__ import annotations

import argparse
import subprocess

from pipeline_tracking import (
    build_connection_command,
    build_env,
    complete_pipeline_run,
    complete_pipeline_step,
    create_pipeline_run,
    create_pipeline_step,
    ensure_pipeline_metadata,
    resolve_batch_context,
    run_psql,
    run_scalar_query,
    sql_literal,
    update_batch_status,
)
from project_config import PROJECT_ROOT, SQL_DIR, add_db_connection_args

NUMERIC_FEATURES = [f"i{i}" for i in range(1, 14)]
CATEGORICAL_FEATURES = [f"c{i}" for i in range(1, 27)]
NUMERIC_BUCKETS = [
    ("negative", "negative values", 1, "null::bigint", "-1::bigint"),
    ("zero", "0", 2, "0", "0"),
    ("1_10", "1 to 10", 3, "1", "10"),
    ("11_100", "11 to 100", 4, "11", "100"),
    ("101_1000", "101 to 1000", 5, "101", "1000"),
    ("1001_10000", "1001 to 10000", 6, "1001", "10000"),
    ("10001_plus", "10001 and above", 7, "10001", "null::bigint"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build warehouse tables from staging.stg_criteo_events")
    parser.add_argument("--batch-name")
    parser.add_argument("--triggered-by", default="manual")
    add_db_connection_args(parser)
    return parser.parse_args()


def build_numeric_bucket_seed_sql() -> str:
    rows: list[str] = []
    for feature_name in NUMERIC_FEATURES:
        for bucket_code, bucket_label, bucket_order, lower_bound, upper_bound in NUMERIC_BUCKETS:
            rows.append(
                f"('{feature_name}', '{bucket_code}', '{bucket_label}', {bucket_order}, {lower_bound}, {upper_bound})"
            )
    return ",\n        ".join(rows)


def build_categorical_lateral_sql(source_alias: str) -> str:
    return ",\n                ".join(
        f"('{feature_name}', {source_alias}.{feature_name})" for feature_name in CATEGORICAL_FEATURES
    )


def build_core_warehouse_sql(batch_id: int) -> str:
    categorical_lateral = build_categorical_lateral_sql("s")
    numeric_bucket_seed = build_numeric_bucket_seed_sql()
    return f"""
delete from warehouse.fact_ad_events
where batch_id = {batch_id};

insert into warehouse.dim_event_day (
    event_batch,
    event_day_number,
    day_label
)
select distinct
    s.event_batch,
    s.event_day_number,
    'training_day_' || s.event_day_number
from staging.stg_criteo_events s
where s.batch_id = {batch_id}
on conflict (event_batch, event_day_number) do nothing;

insert into warehouse.dim_numeric_bucket (
    feature_name,
    bucket_code,
    bucket_label,
    bucket_order,
    lower_bound,
    upper_bound
)
select
    feature_name,
    bucket_code,
    bucket_label,
    bucket_order,
    lower_bound,
    upper_bound
from (
    values
        {numeric_bucket_seed}
) as bucket_seed (
    feature_name,
    bucket_code,
    bucket_label,
    bucket_order,
    lower_bound,
    upper_bound
)
on conflict (feature_name, bucket_code) do nothing;

insert into warehouse.dim_categorical_value (
    feature_name,
    feature_value,
    is_unknown
)
with categorical_seed as (
    select distinct
        categorical_values.feature_name,
        categorical_values.feature_value
    from staging.stg_criteo_events s
    cross join lateral (
        values
                {categorical_lateral}
    ) as categorical_values(feature_name, feature_value)
    where s.batch_id = {batch_id}
)
select
    feature_name,
    feature_value,
    feature_value = 'unknown' as is_unknown
from categorical_seed
on conflict (feature_name, feature_value) do nothing;

insert into warehouse.fact_ad_events (
    raw_event_id,
    batch_id,
    event_day_key,
    label,
    click_flag,
    impression_count,
    click_count,
    missing_numeric_count,
    missing_categorical_count,
    event_batch,
    source_file,
    ingested_at
)
select
    s.raw_event_id,
    s.batch_id,
    d.event_day_key,
    s.label,
    s.click_flag,
    s.impression_count,
    s.click_count,
    s.missing_numeric_count,
    s.missing_categorical_count,
    s.event_batch,
    s.source_file,
    s.ingested_at
from staging.stg_criteo_events s
join warehouse.dim_event_day d
  on d.event_batch = s.event_batch
 and d.event_day_number = s.event_day_number
where s.batch_id = {batch_id};

analyze warehouse.fact_ad_events;
"""


def build_audit_insert_sql(source_file: str, row_count: int, batch_id: int, pipeline_run_id: int) -> str:
    escaped_source = sql_literal(source_file)
    message = sql_literal(f"Warehouse layer build completed successfully for {source_file}.")
    return (
        "insert into quality.load_audit "
        "(batch_id, pipeline_run_id, layer_name, table_name, source_file, row_count, check_status, check_message) "
        f"values ({batch_id}, {pipeline_run_id}, 'warehouse', 'warehouse.fact_ad_events', '{escaped_source}', {row_count}, 'SUCCESS', '{message}') "
        "returning audit_id;"
    )


def main() -> None:
    args = parse_args()
    project_root = PROJECT_ROOT
    sql_dir = SQL_DIR

    ensure_pipeline_metadata(sql_dir, args.database, args)
    run_psql(sql_dir / "04_warehouse_tables.sql", args.database, args)
    run_psql(sql_dir / "06_quality_checks.sql", args.database, args)
    run_psql(sql_dir / "07_indexes.sql", args.database, args)

    batch_id, source_file = resolve_batch_context(
        table_name="staging.stg_criteo_events",
        batch_name=args.batch_name,
        database=args.database,
        args=args,
    )
    staging_row_count = int(
        run_scalar_query(
            f"select count(*) from staging.stg_criteo_events where batch_id = {batch_id};",
            args.database,
            args,
        )
    )
    if staging_row_count == 0:
        raise RuntimeError("staging.stg_criteo_events is empty. Build the staging layer before building the warehouse.")
    pipeline_run_id = create_pipeline_run(
        batch_id=batch_id,
        pipeline_name="warehouse_layer_build",
        layer_name="warehouse",
        source_file=source_file,
        triggered_by=args.triggered_by,
        database=args.database,
        args=args,
    )
    pipeline_step_id = create_pipeline_step(
        pipeline_run_id=pipeline_run_id,
        batch_id=batch_id,
        step_name="build_fact_and_dimensions",
        layer_name="warehouse",
        target_table="warehouse.fact_ad_events",
        source_file=source_file,
        database=args.database,
        args=args,
    )
    update_batch_status(
        batch_id=batch_id,
        batch_status="WAREHOUSE_BUILDING",
        last_successful_stage="staging",
        row_column=None,
        row_count=None,
        mark_started=False,
        mark_completed=False,
        database=args.database,
        args=args,
    )

    try:
        command = build_connection_command(["psql"], args.database, args)
        command.extend(["-v", "ON_ERROR_STOP=1", "-c", build_core_warehouse_sql(batch_id)])
        subprocess.run(command, check=True, env=build_env(args))

        fact_row_count = int(
            run_scalar_query(
                f"select count(*) from warehouse.fact_ad_events where batch_id = {batch_id};",
                args.database,
                args,
            )
        )
        if fact_row_count != staging_row_count:
            raise RuntimeError(
                f"Row count mismatch: staging has {staging_row_count:,}, fact has {fact_row_count:,}"
            )

        update_batch_status(
            batch_id=batch_id,
            batch_status="WAREHOUSE_READY",
            last_successful_stage="warehouse",
            row_column="actual_fact_row_count",
            row_count=fact_row_count,
            mark_started=False,
            mark_completed=False,
            database=args.database,
            args=args,
        )
        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="SUCCESS",
            rows_processed=fact_row_count,
            step_message=f"Warehouse build completed successfully for {source_file}.",
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="SUCCESS",
            run_message=f"Warehouse layer build completed successfully for {source_file}.",
            database=args.database,
            args=args,
        )

        audit_id = run_scalar_query(
            build_audit_insert_sql(source_file, fact_row_count, batch_id, pipeline_run_id),
            args.database,
            args,
        )

        print(f"Staging rows: {staging_row_count:,}")
        print(f"Fact rows: {fact_row_count:,}")
        print(f"Source file tag: {source_file}")
        print(f"Batch ID: {batch_id}")
        print(f"Pipeline run ID: {pipeline_run_id}")
        print("Warehouse-layer build validation passed.")
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
            batch_status="WAREHOUSE_FAILED",
            last_successful_stage="staging",
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
