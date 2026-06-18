#!/usr/bin/env python3

"""Build the marts layer from the warehouse and staging layers."""

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
    parser = argparse.ArgumentParser(description="Build marts from warehouse and staging tables")
    parser.add_argument("--batch-name")
    parser.add_argument("--triggered-by", default="manual")
    add_db_connection_args(parser)
    return parser.parse_args()


def build_batch_marts_sql(batch_id: int, source_file: str) -> str:
    escaped_source = sql_literal(source_file)
    return f"""
delete from marts.batch_feature_interaction_ctr where batch_id = {batch_id};
delete from marts.batch_missing_value_impact where batch_id = {batch_id};
delete from marts.batch_feature_ctr_summary where batch_id = {batch_id};
delete from marts.batch_numeric_bucket_ctr where batch_id = {batch_id};
delete from marts.batch_event_day_ctr_trend where batch_id = {batch_id};
delete from marts.batch_overall_ctr_summary where batch_id = {batch_id};

insert into marts.batch_overall_ctr_summary (
    batch_id,
    source_file,
    event_batch,
    impressions,
    clicks,
    ctr,
    avg_missing_numeric_count,
    avg_missing_categorical_count
)
select
    {batch_id},
    '{escaped_source}',
    max(event_batch),
    count(*),
    sum(click_count),
    round(sum(click_count)::numeric / nullif(sum(impression_count), 0), 6),
    round(avg(missing_numeric_count::numeric), 6),
    round(avg(missing_categorical_count::numeric), 6)
from warehouse.fact_ad_events
where batch_id = {batch_id};

insert into marts.batch_event_day_ctr_trend (
    batch_id,
    event_batch,
    event_day_number,
    day_label,
    impressions,
    clicks,
    ctr
)
select
    {batch_id},
    d.event_batch,
    d.event_day_number,
    d.day_label,
    count(*) as impressions,
    sum(f.click_count) as clicks,
    round(sum(f.click_count)::numeric / nullif(sum(f.impression_count), 0), 6) as ctr
from warehouse.fact_ad_events f
join warehouse.dim_event_day d
  on d.event_day_key = f.event_day_key
where f.batch_id = {batch_id}
group by d.event_batch, d.event_day_number, d.day_label;

insert into marts.batch_numeric_bucket_ctr (
    batch_id,
    feature_name,
    bucket_code,
    bucket_label,
    bucket_order,
    impressions,
    clicks,
    ctr
)
with numeric_source as (
    select click_count, 'i1' as feature_name, i1 as feature_value from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'i2', i2 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'i3', i3 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'i4', i4 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'i5', i5 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'i6', i6 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'i7', i7 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'i8', i8 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'i9', i9 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'i10', i10 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'i11', i11 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'i12', i12 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'i13', i13 from staging.stg_criteo_events where batch_id = {batch_id}
),
bucketed as (
    select
        feature_name,
        click_count,
        case
            when feature_value < 0 then 'negative'
            when feature_value = 0 then 'zero'
            when feature_value between 1 and 10 then '1_10'
            when feature_value between 11 and 100 then '11_100'
            when feature_value between 101 and 1000 then '101_1000'
            when feature_value between 1001 and 10000 then '1001_10000'
            else '10001_plus'
        end as bucket_code
    from numeric_source
)
select
    {batch_id},
    b.feature_name,
    d.bucket_code,
    d.bucket_label,
    d.bucket_order,
    count(*) as impressions,
    sum(b.click_count) as clicks,
    round(sum(b.click_count)::numeric / nullif(count(*), 0), 6) as ctr
from bucketed b
join warehouse.dim_numeric_bucket d
  on d.feature_name = b.feature_name
 and d.bucket_code = b.bucket_code
group by b.feature_name, d.bucket_code, d.bucket_label, d.bucket_order;

insert into marts.batch_feature_ctr_summary (
    batch_id,
    feature_name,
    feature_value,
    is_unknown,
    impressions,
    clicks,
    ctr
)
with categorical_source as (
    select click_count, 'c1' as feature_name, c1 as feature_value from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c2', c2 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c3', c3 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c4', c4 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c5', c5 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c6', c6 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c7', c7 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c8', c8 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c9', c9 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c10', c10 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c11', c11 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c12', c12 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c13', c13 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c14', c14 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c15', c15 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c16', c16 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c17', c17 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c18', c18 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c19', c19 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c20', c20 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c21', c21 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c22', c22 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c23', c23 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c24', c24 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c25', c25 from staging.stg_criteo_events where batch_id = {batch_id}
    union all select click_count, 'c26', c26 from staging.stg_criteo_events where batch_id = {batch_id}
)
select
    {batch_id},
    feature_name,
    feature_value,
    feature_value = 'unknown' as is_unknown,
    count(*) as impressions,
    sum(click_count) as clicks,
    round(sum(click_count)::numeric / nullif(count(*), 0), 6) as ctr
from categorical_source
group by feature_name, feature_value;

insert into marts.batch_missing_value_impact (
    batch_id,
    missing_type,
    missing_count,
    impressions,
    clicks,
    ctr
)
select
    {batch_id},
    'missing_numeric_count',
    missing_numeric_count,
    count(*),
    sum(click_count),
    round(sum(click_count)::numeric / nullif(count(*), 0), 6)
from warehouse.fact_ad_events
where batch_id = {batch_id}
group by missing_numeric_count
union all
select
    {batch_id},
    'missing_categorical_count',
    missing_categorical_count,
    count(*),
    sum(click_count),
    round(sum(click_count)::numeric / nullif(count(*), 0), 6)
from warehouse.fact_ad_events
where batch_id = {batch_id}
group by missing_categorical_count;

insert into marts.batch_feature_interaction_ctr (
    batch_id,
    interaction_name,
    left_feature_name,
    left_feature_value,
    right_feature_name,
    right_feature_value,
    impressions,
    clicks,
    ctr
)
with categorical_pair_c22_c19 as (
    select
        'categorical_pair_c22_c19'::text as interaction_name,
        'c22'::text as left_feature_name,
        c22::text as left_feature_value,
        'c19'::text as right_feature_name,
        c19::text as right_feature_value,
        count(*) as impressions,
        sum(click_count) as clicks,
        round(sum(click_count)::numeric / nullif(count(*), 0), 6) as ctr
    from staging.stg_criteo_events
    where batch_id = {batch_id}
    group by c22, c19
),
categorical_pair_c22_c6 as (
    select
        'categorical_pair_c22_c6'::text as interaction_name,
        'c22'::text as left_feature_name,
        c22::text as left_feature_value,
        'c6'::text as right_feature_name,
        c6::text as right_feature_value,
        count(*) as impressions,
        sum(click_count) as clicks,
        round(sum(click_count)::numeric / nullif(count(*), 0), 6) as ctr
    from staging.stg_criteo_events
    where batch_id = {batch_id}
    group by c22, c6
),
numeric_categorical_i1_c22 as (
    select
        'numeric_categorical_i1_c22'::text as interaction_name,
        'i1_bucket'::text as left_feature_name,
        case
            when i1 < 0 then 'negative'
            when i1 = 0 then 'zero'
            when i1 between 1 and 10 then '1_10'
            when i1 between 11 and 100 then '11_100'
            when i1 between 101 and 1000 then '101_1000'
            when i1 between 1001 and 10000 then '1001_10000'
            else '10001_plus'
        end::text as left_feature_value,
        'c22'::text as right_feature_name,
        c22::text as right_feature_value,
        count(*) as impressions,
        sum(click_count) as clicks,
        round(sum(click_count)::numeric / nullif(count(*), 0), 6) as ctr
    from staging.stg_criteo_events
    where batch_id = {batch_id}
    group by
        case
            when i1 < 0 then 'negative'
            when i1 = 0 then 'zero'
            when i1 between 1 and 10 then '1_10'
            when i1 between 11 and 100 then '11_100'
            when i1 between 101 and 1000 then '101_1000'
            when i1 between 1001 and 10000 then '1001_10000'
            else '10001_plus'
        end,
        c22
)
select
    {batch_id},
    interaction_name,
    left_feature_name,
    left_feature_value,
    right_feature_name,
    right_feature_value,
    impressions,
    clicks,
    ctr
from (
    select * from categorical_pair_c22_c19
    union all
    select * from categorical_pair_c22_c6
    union all
    select * from numeric_categorical_i1_c22
) interactions;
"""


def build_audit_insert_sql(source_file: str, row_count: int, batch_id: int, pipeline_run_id: int) -> str:
    escaped_source = sql_literal(source_file)
    message = sql_literal(f"Marts layer build completed successfully for {source_file}.")
    return (
        "insert into quality.load_audit "
        "(batch_id, pipeline_run_id, layer_name, table_name, source_file, row_count, check_status, check_message) "
        f"values ({batch_id}, {pipeline_run_id}, 'marts', 'marts layer', '{escaped_source}', {row_count}, 'SUCCESS', '{message}') "
        "returning audit_id;"
    )


def main() -> None:
    args = parse_args()
    project_root = PROJECT_ROOT
    sql_dir = SQL_DIR

    batch_id, source_file = resolve_batch_context(
        table_name="warehouse.fact_ad_events",
        batch_name=args.batch_name,
        database=args.database,
        args=args,
    )
    fact_row_count = int(
        run_scalar_query(
            f"select count(*) from warehouse.fact_ad_events where batch_id = {batch_id};",
            args.database,
            args,
        )
    )
    if fact_row_count == 0:
        raise RuntimeError("warehouse.fact_ad_events has no rows for this batch. Build the warehouse layer before building marts.")

    ensure_pipeline_metadata(sql_dir, args.database, args)
    run_psql(sql_dir / "05_marts.sql", args.database, args)
    run_psql(sql_dir / "06_quality_checks.sql", args.database, args)

    pipeline_run_id = create_pipeline_run(
        batch_id=batch_id,
        pipeline_name="marts_layer_build",
        layer_name="marts",
        source_file=source_file,
        triggered_by=args.triggered_by,
        database=args.database,
        args=args,
    )
    pipeline_step_id = create_pipeline_step(
        pipeline_run_id=pipeline_run_id,
        batch_id=batch_id,
        step_name="build_marts_assets",
        layer_name="marts",
        target_table="marts layer",
        source_file=source_file,
        database=args.database,
        args=args,
    )
    update_batch_status(
        batch_id=batch_id,
        batch_status="MARTS_BUILDING",
        last_successful_stage="warehouse",
        row_column=None,
        row_count=None,
        mark_started=False,
        mark_completed=False,
        database=args.database,
        args=args,
    )

    try:
        command = build_connection_command(["psql"], args.database, args)
        command.extend(["-v", "ON_ERROR_STOP=1", "-c", build_batch_marts_sql(batch_id, source_file)])
        subprocess.run(command, check=True, env=build_env(args))

        update_batch_status(
            batch_id=batch_id,
            batch_status="MARTS_READY",
            last_successful_stage="marts",
            row_column=None,
            row_count=None,
            mark_started=False,
            mark_completed=False,
            database=args.database,
            args=args,
        )
        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="SUCCESS",
            rows_processed=fact_row_count,
            step_message=f"Marts layer build completed successfully for {source_file}.",
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="SUCCESS",
            run_message=f"Marts layer build completed successfully for {source_file}.",
            database=args.database,
            args=args,
        )

        audit_id = run_scalar_query(
            build_audit_insert_sql(source_file, fact_row_count, batch_id, pipeline_run_id),
            args.database,
            args,
        )

        print(f"Warehouse fact rows: {fact_row_count:,}")
        print(f"Source file tag: {source_file}")
        print(f"Batch ID: {batch_id}")
        print(f"Pipeline run ID: {pipeline_run_id}")
        print("Marts-layer build validation passed.")
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
            batch_status="MARTS_FAILED",
            last_successful_stage="warehouse",
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
