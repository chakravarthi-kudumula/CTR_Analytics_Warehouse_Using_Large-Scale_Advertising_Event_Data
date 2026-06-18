#!/usr/bin/env python3

"""Run reusable quality validations across all pipeline layers."""

from __future__ import annotations

import argparse
from pathlib import Path
import subprocess

from pipeline_tracking import (
    complete_pipeline_run,
    complete_pipeline_step,
    create_pipeline_run,
    create_pipeline_step,
    ensure_pipeline_metadata,
    resolve_batch_context,
    update_batch_status,
)
from project_config import PROJECT_ROOT, SQL_DIR, add_db_connection_args

def sql_literal(value: str) -> str:
    return value.replace("'", "''")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run quality validations across all pipeline layers")
    parser.add_argument("--batch-name")
    parser.add_argument("--triggered-by", default="manual")
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


def run_psql(sql_file: Path, database: str, args: argparse.Namespace) -> None:
    env = os.environ.copy()
    if args.password:
        env["PGPASSWORD"] = args.password
    else:
        env.pop("PGPASSWORD", None)
    command = build_connection_command(["psql"], database, args)
    command.extend(["-v", "ON_ERROR_STOP=1", "-f", str(sql_file)])
    subprocess.run(command, check=True, env=env)


def run_scalar_query(query: str, database: str, args: argparse.Namespace) -> str:
    env = os.environ.copy()
    if args.password:
        env["PGPASSWORD"] = args.password
    else:
        env.pop("PGPASSWORD", None)
    command = build_connection_command(["psql"], database, args)
    command.extend(["-t", "-A", "-c", query])
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    return lines[0] if lines else ""

def build_quality_sql(run_id: int, batch_id: int, pipeline_run_id: int) -> str:
    return f"""
insert into quality.validation_results (
    run_id, batch_id, pipeline_run_id, layer_name, table_name, check_name, check_type, severity, check_status,
    actual_value, expected_value, threshold_value, check_message, source_file
)
with source_file_context as (
    select coalesce(string_agg(distinct source_file, ', '), 'unknown') as source_file
    from raw.criteo_events
),
raw_metrics as (
    select
        count(*) as row_count,
        count(*) filter (where label not in (0, 1) or label is null) as invalid_label_rows,
        count(*) filter (where source_file is null) as null_source_file_rows,
        count(*) filter (where ingested_at is null) as null_ingested_at_rows
    from raw.criteo_events
),
staging_metrics as (
    select
        (select count(*) from raw.criteo_events) as raw_row_count,
        count(*) as staging_row_count,
        count(*) filter (where raw_event_id is null) as null_raw_event_id_rows,
        count(*) filter (where click_flag not in (0, 1)) as invalid_click_flag_rows,
        count(*) filter (where click_count not in (0, 1)) as invalid_click_count_rows,
        count(*) filter (where impression_count <> 1) as invalid_impression_rows,
        count(*) filter (where event_batch is null) as null_event_batch_rows
    from staging.stg_criteo_events
),
staging_duplicates as (
    select count(*) as duplicate_raw_event_id_groups
    from (
        select raw_event_id
        from staging.stg_criteo_events
        group by raw_event_id
        having count(*) > 1
    ) duplicates
),
staging_null_rates as (
    select
        round(avg((c22 = 'unknown')::int)::numeric, 6) as c22_unknown_rate,
        round(avg((c19 = 'unknown')::int)::numeric, 6) as c19_unknown_rate,
        round(avg((c20 = 'unknown')::int)::numeric, 6) as c20_unknown_rate,
        round(avg((c25 = 'unknown')::int)::numeric, 6) as c25_unknown_rate,
        round(avg((c26 = 'unknown')::int)::numeric, 6) as c26_unknown_rate
    from staging.stg_criteo_events
),
staging_sparse_rates as (
    select 'c22'::text as column_name, c22_unknown_rate as unknown_rate from staging_null_rates
    union all
    select 'c19'::text as column_name, c19_unknown_rate as unknown_rate from staging_null_rates
    union all
    select 'c20'::text as column_name, c20_unknown_rate as unknown_rate from staging_null_rates
    union all
    select 'c25'::text as column_name, c25_unknown_rate as unknown_rate from staging_null_rates
    union all
    select 'c26'::text as column_name, c26_unknown_rate as unknown_rate from staging_null_rates
),
staging_thresholds as (
    select
        column_name,
        warning_threshold,
        error_threshold,
        comparison_operator
    from quality.validation_thresholds
    where layer_name = 'staging'
      and table_name = 'staging.stg_criteo_events'
      and check_name = 'unknown rate threshold'
      and is_active = true
),
warehouse_metrics as (
    select
        (select count(*) from staging.stg_criteo_events) as staging_row_count,
        count(*) as fact_row_count,
        count(*) filter (where raw_event_id is null) as null_raw_event_id_rows,
        count(*) filter (where event_day_key is null) as null_event_day_key_rows,
        count(*) filter (where click_count not in (0, 1)) as invalid_click_count_rows,
        count(*) filter (where click_flag not in (0, 1)) as invalid_click_flag_rows,
        count(*) filter (where impression_count <> 1) as invalid_impression_rows,
        round(sum(click_count)::numeric / nullif(sum(impression_count), 0), 6) as warehouse_ctr
    from warehouse.fact_ad_events
),
warehouse_duplicates as (
    select count(*) as duplicate_raw_event_id_groups
    from (
        select raw_event_id
        from warehouse.fact_ad_events
        group by raw_event_id
        having count(*) > 1
    ) duplicates
),
marts_metrics as (
    select
        (select count(*) from marts.overall_ctr_summary) as overall_ctr_summary_rows,
        (select count(*) from marts.event_day_ctr_trend) as event_day_ctr_trend_rows,
        (select count(*) from marts.numeric_bucket_ctr) as numeric_bucket_ctr_rows,
        (select count(*) from marts.feature_ctr_summary) as feature_ctr_summary_rows,
        (select count(*) from marts.missing_value_impact) as missing_value_impact_rows,
        (select count(*) from marts.feature_interaction_ctr) as feature_interaction_ctr_rows,
        (select max(ctr) from marts.overall_ctr_summary) as overall_ctr,
        (select max(ctr) from marts.event_day_ctr_trend) as max_event_day_ctr
),
advanced_metrics as (
    select
        (select count(*) from marts.feature_ctr_ranked) as feature_ctr_ranked_rows,
        (select count(*) from marts.event_day_ctr_rolling) as event_day_ctr_rolling_rows,
        (select count(*) from marts.high_value_segments) as high_value_segments_rows,
        (select count(*) from marts.low_performing_segments) as low_performing_segments_rows,
        (select count(*) from marts.feature_ctr_lift_ranked) as feature_ctr_lift_ranked_rows,
        (select count(*) from marts.feature_interaction_ranked) as feature_interaction_ranked_rows,
        (select max(rolling_3_day_weighted_ctr) from marts.event_day_ctr_rolling) as max_rolling_ctr,
        (select min(rolling_3_day_weighted_ctr) from marts.event_day_ctr_rolling) as min_rolling_ctr
),
feature_store_metrics as (
    select
        (select count(*) from warehouse.fact_ad_events) as fact_row_count,
        count(*) as feature_store_row_count,
        count(*) filter (where label not in (0, 1)) as invalid_label_rows,
        count(*) filter (where event_day_number not between 1 and 7) as invalid_event_day_rows
    from feature_store.ctr_training_features
),
feature_store_duplicates as (
    select count(*) as duplicate_raw_event_id_groups
    from (
        select raw_event_id
        from feature_store.ctr_training_features
        group by raw_event_id
        having count(*) > 1
    ) duplicates
)
select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'raw',
    'raw.criteo_events',
    'raw row count greater than zero',
    'row_count',
    'error',
    case when rm.row_count > 0 then 'PASS' else 'FAIL' end,
    rm.row_count::text,
    '> 0',
    null,
    'Raw layer should contain rows after ingestion.',
    sfc.source_file
from raw_metrics rm
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'raw',
    'raw.criteo_events',
    'label must be 0 or 1',
    'validity',
    'error',
    case when rm.invalid_label_rows = 0 then 'PASS' else 'FAIL' end,
    rm.invalid_label_rows::text,
    '0 invalid rows',
    null,
    'All raw labels must be binary.',
    sfc.source_file
from raw_metrics rm
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'raw',
    'raw.criteo_events',
    'source_file not null',
    'completeness',
    'error',
    case when rm.null_source_file_rows = 0 then 'PASS' else 'FAIL' end,
    rm.null_source_file_rows::text,
    '0 null rows',
    null,
    'source_file must be present on all raw rows.',
    sfc.source_file
from raw_metrics rm
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'raw',
    'raw.criteo_events',
    'ingested_at not null',
    'freshness',
    'error',
    case when rm.null_ingested_at_rows = 0 then 'PASS' else 'FAIL' end,
    rm.null_ingested_at_rows::text,
    '0 null rows',
    null,
    'ingested_at must be present on all raw rows.',
    sfc.source_file
from raw_metrics rm
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'staging',
    'staging.stg_criteo_events',
    'row count parity with raw',
    'consistency',
    'error',
    case when sm.staging_row_count = sm.raw_row_count then 'PASS' else 'FAIL' end,
    sm.staging_row_count::text,
    sm.raw_row_count::text,
    null,
    'Staging row count should match raw row count.',
    sfc.source_file
from staging_metrics sm
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'staging',
    'staging.stg_criteo_events',
    'raw_event_id uniqueness',
    'uniqueness',
    'error',
    case when sd.duplicate_raw_event_id_groups = 0 then 'PASS' else 'FAIL' end,
    sd.duplicate_raw_event_id_groups::text,
    '0 duplicate groups',
    null,
    'raw_event_id must uniquely identify staging rows.',
    sfc.source_file
from staging_duplicates sd
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'staging',
    'staging.stg_criteo_events',
    'click_flag binary validity',
    'validity',
    'error',
    case when sm.invalid_click_flag_rows = 0 then 'PASS' else 'FAIL' end,
    sm.invalid_click_flag_rows::text,
    '0 invalid rows',
    null,
    'click_flag must be 0 or 1.',
    sfc.source_file
from staging_metrics sm
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'staging',
    'staging.stg_criteo_events',
    'click_count binary validity',
    'validity',
    'error',
    case when sm.invalid_click_count_rows = 0 then 'PASS' else 'FAIL' end,
    sm.invalid_click_count_rows::text,
    '0 invalid rows',
    null,
    'click_count must be 0 or 1.',
    sfc.source_file
from staging_metrics sm
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'staging',
    'staging.stg_criteo_events',
    'impression_count must equal 1',
    'validity',
    'error',
    case when sm.invalid_impression_rows = 0 then 'PASS' else 'FAIL' end,
    sm.invalid_impression_rows::text,
    '0 invalid rows',
    null,
    'Every staging row should represent exactly one impression.',
    sfc.source_file
from staging_metrics sm
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'staging',
    'staging.stg_criteo_events',
    ssr.column_name || ' unknown rate threshold',
    'completeness',
    'warning',
    case
        when ssr.unknown_rate <= st.warning_threshold then 'PASS'
        when st.error_threshold is not null and ssr.unknown_rate <= st.error_threshold then 'WARN'
        else 'FAIL'
    end,
    ssr.unknown_rate::text,
    st.comparison_operator || ' ' || st.warning_threshold::text,
    coalesce(st.error_threshold::text, st.warning_threshold::text),
    'Configured unknown-rate threshold for sparse categorical columns.',
    sfc.source_file
from staging_sparse_rates ssr
inner join staging_thresholds st
    on ssr.column_name = st.column_name
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'warehouse',
    'warehouse.fact_ad_events',
    'row count parity with staging',
    'consistency',
    'error',
    case when wm.fact_row_count = wm.staging_row_count then 'PASS' else 'FAIL' end,
    wm.fact_row_count::text,
    wm.staging_row_count::text,
    null,
    'Warehouse fact row count should match staging row count.',
    sfc.source_file
from warehouse_metrics wm
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'warehouse',
    'warehouse.fact_ad_events',
    'raw_event_id uniqueness',
    'uniqueness',
    'error',
    case when wd.duplicate_raw_event_id_groups = 0 then 'PASS' else 'FAIL' end,
    wd.duplicate_raw_event_id_groups::text,
    '0 duplicate groups',
    null,
    'raw_event_id must uniquely identify fact rows.',
    sfc.source_file
from warehouse_duplicates wd
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'warehouse',
    'warehouse.fact_ad_events',
    'event_day_key not null',
    'completeness',
    'error',
    case when wm.null_event_day_key_rows = 0 then 'PASS' else 'FAIL' end,
    wm.null_event_day_key_rows::text,
    '0 null rows',
    null,
    'Every fact row should map to an event-day dimension key.',
    sfc.source_file
from warehouse_metrics wm
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'warehouse',
    'warehouse.fact_ad_events',
    'warehouse ctr between 0 and 1',
    'range',
    'error',
    case when wm.warehouse_ctr between 0 and 1 then 'PASS' else 'FAIL' end,
    wm.warehouse_ctr::text,
    '0 to 1',
    null,
    'Warehouse CTR must stay within valid probability bounds.',
    sfc.source_file
from warehouse_metrics wm
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'marts',
    'marts layer',
    'mart row counts greater than zero',
    'row_count',
    'error',
    case
        when mm.overall_ctr_summary_rows > 0
         and mm.event_day_ctr_trend_rows > 0
         and mm.numeric_bucket_ctr_rows > 0
         and mm.feature_ctr_summary_rows > 0
         and mm.missing_value_impact_rows > 0
         and mm.feature_interaction_ctr_rows > 0
        then 'PASS' else 'FAIL'
    end,
    (
        'overall=' || mm.overall_ctr_summary_rows ||
        ', event_day=' || mm.event_day_ctr_trend_rows ||
        ', numeric=' || mm.numeric_bucket_ctr_rows ||
        ', feature=' || mm.feature_ctr_summary_rows ||
        ', missing=' || mm.missing_value_impact_rows ||
        ', interaction=' || mm.feature_interaction_ctr_rows
    ),
    'all mart row counts > 0',
    null,
    'All primary mart objects should be populated.',
    sfc.source_file
from marts_metrics mm
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'marts',
    'marts layer',
    'mart ctr values between 0 and 1',
    'range',
    'error',
    case when mm.overall_ctr between 0 and 1 and mm.max_event_day_ctr between 0 and 1 then 'PASS' else 'FAIL' end,
    'overall=' || mm.overall_ctr || ', max_event_day=' || mm.max_event_day_ctr,
    '0 to 1',
    null,
    'Mart CTR outputs must stay within valid probability bounds.',
    sfc.source_file
from marts_metrics mm
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'advanced_sql',
    'advanced sql layer',
    'advanced outputs populated',
    'row_count',
    'error',
    case
        when am.feature_ctr_ranked_rows > 0
         and am.event_day_ctr_rolling_rows > 0
         and am.high_value_segments_rows > 0
         and am.low_performing_segments_rows > 0
         and am.feature_ctr_lift_ranked_rows > 0
         and am.feature_interaction_ranked_rows > 0
        then 'PASS' else 'FAIL'
    end,
    (
        'feature_ranked=' || am.feature_ctr_ranked_rows ||
        ', rolling=' || am.event_day_ctr_rolling_rows ||
        ', high_value=' || am.high_value_segments_rows ||
        ', low_value=' || am.low_performing_segments_rows ||
        ', lift_ranked=' || am.feature_ctr_lift_ranked_rows ||
        ', interaction_ranked=' || am.feature_interaction_ranked_rows
    ),
    'all advanced outputs > 0',
    null,
    'All advanced SQL assets should be populated.',
    sfc.source_file
from advanced_metrics am
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'advanced_sql',
    'marts.event_day_ctr_rolling',
    'rolling ctr between 0 and 1',
    'range',
    'error',
    case when am.min_rolling_ctr between 0 and 1 and am.max_rolling_ctr between 0 and 1 then 'PASS' else 'FAIL' end,
    'min=' || am.min_rolling_ctr || ', max=' || am.max_rolling_ctr,
    '0 to 1',
    null,
    'Rolling CTR outputs must stay within valid probability bounds.',
    sfc.source_file
from advanced_metrics am
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'feature_store',
    'feature_store.ctr_training_features',
    'feature store row count parity',
    'consistency',
    'error',
    case when fsm.feature_store_row_count = fsm.fact_row_count then 'PASS' else 'FAIL' end,
    fsm.feature_store_row_count::text,
    fsm.fact_row_count::text,
    null,
    'Feature store rows should match warehouse fact rows.',
    sfc.source_file
from feature_store_metrics fsm
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'feature_store',
    'feature_store.ctr_training_features',
    'feature store labels valid',
    'validity',
    'error',
    case when fsm.invalid_label_rows = 0 then 'PASS' else 'FAIL' end,
    fsm.invalid_label_rows::text,
    '0 invalid rows',
    null,
    'Feature store labels must remain binary.',
    sfc.source_file
from feature_store_metrics fsm
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'feature_store',
    'feature_store.ctr_training_features',
    'feature store raw_event_id uniqueness',
    'uniqueness',
    'error',
    case when fsd.duplicate_raw_event_id_groups = 0 then 'PASS' else 'FAIL' end,
    fsd.duplicate_raw_event_id_groups::text,
    '0 duplicate groups',
    null,
    'Feature store should keep one row per raw event.',
    sfc.source_file
from feature_store_duplicates fsd
cross join source_file_context sfc

union all

select
    {run_id},
    {batch_id},
    {pipeline_run_id},
    'feature_store',
    'feature_store.ctr_training_features',
    'feature store event_day_number validity',
    'validity',
    'error',
    case when fsm.invalid_event_day_rows = 0 then 'PASS' else 'FAIL' end,
    fsm.invalid_event_day_rows::text,
    '0 invalid rows',
    null,
    'Feature store event_day_number should stay within the 1 to 7 derived training window.',
    sfc.source_file
from feature_store_metrics fsm
cross join source_file_context sfc;
"""


def build_validation_run_insert_sql(source_file: str, batch_id: int, pipeline_run_id: int) -> str:
    escaped_source = sql_literal(source_file)
    message = sql_literal(f"Quality framework run started for {source_file}.")
    return (
        "insert into quality.validation_runs "
        "(batch_id, pipeline_run_id, pipeline_name, source_file, run_status, run_message) "
        f"values ({batch_id}, {pipeline_run_id}, 'quality_framework', '{escaped_source}', 'RUNNING', '{message}') "
        "returning run_id;"
    )


def build_validation_run_update_sql(run_id: int, status: str, message: str) -> str:
    escaped_status = sql_literal(status)
    escaped_message = sql_literal(message)
    return (
        "update quality.validation_runs "
        f"set run_status = '{escaped_status}', "
        f"run_message = '{escaped_message}', "
        "completed_at = now() "
        f"where run_id = {run_id};"
    )


def build_audit_insert_sql(source_file: str, row_count: int, batch_id: int, pipeline_run_id: int) -> str:
    escaped_source = sql_literal(source_file)
    message = sql_literal(f"Quality framework validation completed successfully for {source_file}.")
    return (
        "insert into quality.load_audit "
        "(batch_id, pipeline_run_id, layer_name, table_name, source_file, row_count, check_status, check_message) "
        f"values ({batch_id}, {pipeline_run_id}, 'quality', 'quality.validation_results', '{escaped_source}', {row_count}, 'SUCCESS', '{message}') "
        "returning audit_id;"
    )


def main() -> None:
    args = parse_args()
    project_root = PROJECT_ROOT
    sql_dir = SQL_DIR

    ensure_pipeline_metadata(sql_dir, args.database, args)
    run_psql(sql_dir / "06_quality_checks.sql", args.database, args)

    batch_id, source_file = resolve_batch_context(
        table_name="warehouse.fact_ad_events",
        batch_name=args.batch_name,
        database=args.database,
        args=args,
    )
    pipeline_run_id = create_pipeline_run(
        batch_id=batch_id,
        pipeline_name="quality_framework",
        layer_name="quality",
        source_file=source_file,
        triggered_by=args.triggered_by,
        database=args.database,
        args=args,
    )
    pipeline_step_id = create_pipeline_step(
        pipeline_run_id=pipeline_run_id,
        batch_id=batch_id,
        step_name="run_quality_framework",
        layer_name="quality",
        target_table="quality.validation_results",
        source_file=source_file,
        database=args.database,
        args=args,
    )
    update_batch_status(
        batch_id=batch_id,
        batch_status="QUALITY_CHECKING",
        last_successful_stage="advanced_sql",
        row_column=None,
        row_count=None,
        mark_started=False,
        mark_completed=False,
        database=args.database,
        args=args,
    )
    run_id = int(run_scalar_query(build_validation_run_insert_sql(source_file, batch_id, pipeline_run_id), args.database, args))

    env = os.environ.copy()
    if args.password:
        env["PGPASSWORD"] = args.password
    else:
        env.pop("PGPASSWORD", None)

    try:
        command = build_connection_command(["psql"], args.database, args)
        command.extend(["-v", "ON_ERROR_STOP=1", "-c", build_quality_sql(run_id, batch_id, pipeline_run_id)])
        subprocess.run(command, check=True, env=env)

        validation_row_count = int(
            run_scalar_query(
                f"select count(*) from quality.validation_results where run_id = {run_id};",
                args.database,
                args,
            )
        )
        run_scalar_query(
            build_validation_run_update_sql(
                run_id,
                "SUCCESS",
                f"Quality framework validation completed successfully for {source_file}.",
            ),
            args.database,
            args,
        )
        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="SUCCESS",
            rows_processed=validation_row_count,
            step_message=f"Quality framework validation completed successfully for {source_file}.",
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="SUCCESS",
            run_message=f"Quality framework validation completed successfully for {source_file}.",
            database=args.database,
            args=args,
        )
        update_batch_status(
            batch_id=batch_id,
            batch_status="QUALITY_CHECKED",
            last_successful_stage="quality",
            row_column=None,
            row_count=None,
            mark_started=False,
            mark_completed=True,
            database=args.database,
            args=args,
        )
        audit_id = run_scalar_query(
            build_audit_insert_sql(source_file, validation_row_count, batch_id, pipeline_run_id),
            args.database,
            args,
        )
    except subprocess.CalledProcessError:
        run_scalar_query(
            build_validation_run_update_sql(
                run_id,
                "FAILED",
                f"Quality framework validation failed for {source_file}.",
            ),
            args.database,
            args,
        )
        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="FAILED",
            rows_processed=None,
            step_message=f"Quality framework validation failed for {source_file}.",
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="FAILED",
            run_message=f"Quality framework validation failed for {source_file}.",
            database=args.database,
            args=args,
        )
        update_batch_status(
            batch_id=batch_id,
            batch_status="QUALITY_FAILED",
            last_successful_stage="advanced_sql",
            row_column=None,
            row_count=None,
            mark_started=False,
            mark_completed=False,
            database=args.database,
            args=args,
        )
        raise

    print(f"Validation results rows: {validation_row_count:,}")
    print(f"Source file tag: {source_file}")
    print(f"Batch ID: {batch_id}")
    print(f"Pipeline run ID: {pipeline_run_id}")
    print(f"Validation run ID: {run_id}")
    print("Quality framework validation passed.")
    print(f"Audit row created: {audit_id}")


if __name__ == "__main__":
    main()
