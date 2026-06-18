#!/usr/bin/env python3

"""Build the staging layer from raw.criteo_events."""

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build staging.stg_criteo_events from raw.criteo_events")
    parser.add_argument("--batch-name")
    parser.add_argument("--triggered-by", default="manual")
    add_db_connection_args(parser)
    return parser.parse_args()


def build_staging_sql(batch_id: int) -> str:
    return f"""
delete from staging.stg_criteo_events
where batch_id = {batch_id};

insert into staging.stg_criteo_events (
    raw_event_id,
    batch_id,
    label,
    i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13,
    c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13,
    c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26,
    event_day_number,
    event_batch,
    click_flag,
    impression_count,
    click_count,
    missing_numeric_count,
    missing_categorical_count,
    source_file,
    ingested_at
)
with raw_prepared as (
    select
        event_id as raw_event_id,
        batch_id,
        case when coalesce(label, 0) = 1 then 1 else 0 end as label,
        coalesce(i1, 0) as i1,
        coalesce(i2, 0) as i2,
        coalesce(i3, 0) as i3,
        coalesce(i4, 0) as i4,
        coalesce(i5, 0) as i5,
        coalesce(i6, 0) as i6,
        coalesce(i7, 0) as i7,
        coalesce(i8, 0) as i8,
        coalesce(i9, 0) as i9,
        coalesce(i10, 0) as i10,
        coalesce(i11, 0) as i11,
        coalesce(i12, 0) as i12,
        coalesce(i13, 0) as i13,
        coalesce(nullif(c1, ''), 'unknown') as c1,
        coalesce(nullif(c2, ''), 'unknown') as c2,
        coalesce(nullif(c3, ''), 'unknown') as c3,
        coalesce(nullif(c4, ''), 'unknown') as c4,
        coalesce(nullif(c5, ''), 'unknown') as c5,
        coalesce(nullif(c6, ''), 'unknown') as c6,
        coalesce(nullif(c7, ''), 'unknown') as c7,
        coalesce(nullif(c8, ''), 'unknown') as c8,
        coalesce(nullif(c9, ''), 'unknown') as c9,
        coalesce(nullif(c10, ''), 'unknown') as c10,
        coalesce(nullif(c11, ''), 'unknown') as c11,
        coalesce(nullif(c12, ''), 'unknown') as c12,
        coalesce(nullif(c13, ''), 'unknown') as c13,
        coalesce(nullif(c14, ''), 'unknown') as c14,
        coalesce(nullif(c15, ''), 'unknown') as c15,
        coalesce(nullif(c16, ''), 'unknown') as c16,
        coalesce(nullif(c17, ''), 'unknown') as c17,
        coalesce(nullif(c18, ''), 'unknown') as c18,
        coalesce(nullif(c19, ''), 'unknown') as c19,
        coalesce(nullif(c20, ''), 'unknown') as c20,
        coalesce(nullif(c21, ''), 'unknown') as c21,
        coalesce(nullif(c22, ''), 'unknown') as c22,
        coalesce(nullif(c23, ''), 'unknown') as c23,
        coalesce(nullif(c24, ''), 'unknown') as c24,
        coalesce(nullif(c25, ''), 'unknown') as c25,
        coalesce(nullif(c26, ''), 'unknown') as c26,
        ntile(7) over (partition by source_file order by event_id) as event_day_number,
        case
            when source_file like '%100k%' then '100k'
            when source_file like '%1m%' then '1m'
            when source_file like '%5m%' then '5m'
            else 'full'
        end as event_batch,
        1 as impression_count,
        coalesce((i1 is null)::int, 0)
            + coalesce((i2 is null)::int, 0)
            + coalesce((i3 is null)::int, 0)
            + coalesce((i4 is null)::int, 0)
            + coalesce((i5 is null)::int, 0)
            + coalesce((i6 is null)::int, 0)
            + coalesce((i7 is null)::int, 0)
            + coalesce((i8 is null)::int, 0)
            + coalesce((i9 is null)::int, 0)
            + coalesce((i10 is null)::int, 0)
            + coalesce((i11 is null)::int, 0)
            + coalesce((i12 is null)::int, 0)
            + coalesce((i13 is null)::int, 0) as missing_numeric_count,
        coalesce((c1 is null or c1 = '')::int, 0)
            + coalesce((c2 is null or c2 = '')::int, 0)
            + coalesce((c3 is null or c3 = '')::int, 0)
            + coalesce((c4 is null or c4 = '')::int, 0)
            + coalesce((c5 is null or c5 = '')::int, 0)
            + coalesce((c6 is null or c6 = '')::int, 0)
            + coalesce((c7 is null or c7 = '')::int, 0)
            + coalesce((c8 is null or c8 = '')::int, 0)
            + coalesce((c9 is null or c9 = '')::int, 0)
            + coalesce((c10 is null or c10 = '')::int, 0)
            + coalesce((c11 is null or c11 = '')::int, 0)
            + coalesce((c12 is null or c12 = '')::int, 0)
            + coalesce((c13 is null or c13 = '')::int, 0)
            + coalesce((c14 is null or c14 = '')::int, 0)
            + coalesce((c15 is null or c15 = '')::int, 0)
            + coalesce((c16 is null or c16 = '')::int, 0)
            + coalesce((c17 is null or c17 = '')::int, 0)
            + coalesce((c18 is null or c18 = '')::int, 0)
            + coalesce((c19 is null or c19 = '')::int, 0)
            + coalesce((c20 is null or c20 = '')::int, 0)
            + coalesce((c21 is null or c21 = '')::int, 0)
            + coalesce((c22 is null or c22 = '')::int, 0)
            + coalesce((c23 is null or c23 = '')::int, 0)
            + coalesce((c24 is null or c24 = '')::int, 0)
            + coalesce((c25 is null or c25 = '')::int, 0)
            + coalesce((c26 is null or c26 = '')::int, 0) as missing_categorical_count,
        source_file,
        ingested_at
    from raw.criteo_events
    where batch_id = {batch_id}
)
select
    raw_event_id,
    batch_id,
    label,
    i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13,
    c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13,
    c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26,
    event_day_number,
    event_batch,
    label as click_flag,
    impression_count,
    label as click_count,
    missing_numeric_count,
    missing_categorical_count,
    source_file,
    ingested_at
from raw_prepared
order by raw_event_id;
"""


def build_audit_insert_sql(source_file: str, row_count: int, batch_id: int, pipeline_run_id: int) -> str:
    escaped_source = sql_literal(source_file)
    message = sql_literal(f"Staging layer build completed successfully for {source_file}.")
    return (
        "insert into quality.load_audit "
        "(batch_id, pipeline_run_id, layer_name, table_name, source_file, row_count, check_status, check_message) "
        f"values ({batch_id}, {pipeline_run_id}, 'staging', 'staging.stg_criteo_events', '{escaped_source}', {row_count}, 'SUCCESS', '{message}') "
        "returning audit_id;"
    )


def main() -> None:
    args = parse_args()
    project_root = PROJECT_ROOT
    sql_dir = SQL_DIR

    ensure_pipeline_metadata(sql_dir, args.database, args)
    run_psql(sql_dir / "03_staging_tables.sql", args.database, args)
    run_psql(sql_dir / "06_quality_checks.sql", args.database, args)

    batch_id, source_file = resolve_batch_context(
        table_name="raw.criteo_events",
        batch_name=args.batch_name,
        database=args.database,
        args=args,
    )
    raw_row_count = int(
        run_scalar_query(f"select count(*) from raw.criteo_events where batch_id = {batch_id};", args.database, args)
    )
    if raw_row_count == 0:
        raise RuntimeError("raw.criteo_events is empty. Load the raw layer before building staging.")
    pipeline_run_id = create_pipeline_run(
        batch_id=batch_id,
        pipeline_name="staging_layer_build",
        layer_name="staging",
        source_file=source_file,
        triggered_by=args.triggered_by,
        database=args.database,
        args=args,
    )
    pipeline_step_id = create_pipeline_step(
        pipeline_run_id=pipeline_run_id,
        batch_id=batch_id,
        step_name="build_staging_criteo_events",
        layer_name="staging",
        target_table="staging.stg_criteo_events",
        source_file=source_file,
        database=args.database,
        args=args,
    )
    update_batch_status(
        batch_id=batch_id,
        batch_status="STAGING_BUILDING",
        last_successful_stage="raw",
        row_column=None,
        row_count=None,
        mark_started=False,
        mark_completed=False,
        database=args.database,
        args=args,
    )

    try:
        command = build_connection_command(["psql"], args.database, args)
        command.extend(["-v", "ON_ERROR_STOP=1", "-c", build_staging_sql(batch_id)])
        subprocess.run(command, check=True, env=build_env(args))

        staging_row_count = int(
            run_scalar_query(
                f"select count(*) from staging.stg_criteo_events where batch_id = {batch_id};",
                args.database,
                args,
            )
        )
        if staging_row_count != raw_row_count:
            raise RuntimeError(
                f"Row count mismatch: raw has {raw_row_count:,}, staging has {staging_row_count:,}"
            )

        update_batch_status(
            batch_id=batch_id,
            batch_status="STAGING_READY",
            last_successful_stage="staging",
            row_column="actual_staging_row_count",
            row_count=staging_row_count,
            mark_started=False,
            mark_completed=False,
            database=args.database,
            args=args,
        )
        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="SUCCESS",
            rows_processed=staging_row_count,
            step_message=f"Staging build completed successfully for {source_file}.",
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="SUCCESS",
            run_message=f"Staging layer build completed successfully for {source_file}.",
            database=args.database,
            args=args,
        )

        audit_id = run_scalar_query(
            build_audit_insert_sql(source_file, staging_row_count, batch_id, pipeline_run_id),
            args.database,
            args,
        )

        print(f"Raw rows: {raw_row_count:,}")
        print(f"Staging rows: {staging_row_count:,}")
        print(f"Source file tag: {source_file}")
        print(f"Batch ID: {batch_id}")
        print(f"Pipeline run ID: {pipeline_run_id}")
        print("Staging-layer build validation passed.")
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
            batch_status="STAGING_FAILED",
            last_successful_stage="raw",
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
