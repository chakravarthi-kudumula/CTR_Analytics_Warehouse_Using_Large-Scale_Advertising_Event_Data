#!/usr/bin/env python3

"""Build batch-aware advanced SQL analytics assets on top of the marts layer."""

from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path

from pipeline_tracking import (
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
    parser = argparse.ArgumentParser(description="Build advanced SQL assets from batch marts and warehouse tables")
    parser.add_argument("--batch-name")
    parser.add_argument("--triggered-by", default="manual")
    add_db_connection_args(parser)
    return parser.parse_args()


def build_env(args: argparse.Namespace) -> dict[str, str]:
    env = os.environ.copy()
    if args.host:
        env["PGHOST"] = args.host
    if args.port:
        env["PGPORT"] = str(args.port)
    if args.database:
        env["PGDATABASE"] = args.database
    if args.user:
        env["PGUSER"] = args.user
    if args.password:
        env["PGPASSWORD"] = args.password
    return env


def build_batch_advanced_sql(batch_id: int) -> str:
    return f"""
delete from marts.batch_feature_interaction_ranked where batch_id = {batch_id};
delete from marts.batch_feature_ctr_lift_ranked where batch_id = {batch_id};
delete from marts.batch_low_performing_segments where batch_id = {batch_id};
delete from marts.batch_high_value_segments where batch_id = {batch_id};
delete from marts.batch_event_day_ctr_rolling where batch_id = {batch_id};
delete from marts.batch_feature_ctr_ranked where batch_id = {batch_id};

insert into marts.batch_feature_ctr_ranked (
    batch_id,
    feature_name,
    feature_value,
    is_unknown,
    impressions,
    clicks,
    ctr,
    overall_ctr,
    ctr_lift_vs_overall,
    volume_band,
    row_number_in_feature,
    dense_rank_in_feature,
    percent_rank_in_feature,
    ctr_quartile_in_feature
)
with ranked_features as (
    select
        f.feature_name,
        f.feature_value,
        f.is_unknown,
        f.impressions,
        f.clicks,
        f.ctr,
        o.overall_ctr,
        round(f.ctr - o.overall_ctr, 6) as ctr_lift_vs_overall,
        case
            when f.impressions >= 10000 then 'high'
            when f.impressions >= 5000 then 'medium'
            else 'low'
        end as volume_band,
        row_number() over (
            partition by f.feature_name
            order by f.ctr desc, f.impressions desc, f.feature_value
        ) as row_number_in_feature,
        dense_rank() over (
            partition by f.feature_name
            order by f.ctr desc, f.impressions desc
        ) as dense_rank_in_feature,
        percent_rank() over (
            partition by f.feature_name
            order by f.ctr
        ) as percent_rank_in_feature,
        ntile(4) over (
            partition by f.feature_name
            order by f.ctr desc, f.impressions desc
        ) as ctr_quartile_in_feature
    from marts.batch_feature_ctr_summary f
    cross join (
        select ctr as overall_ctr
        from marts.batch_overall_ctr_summary
        where batch_id = {batch_id}
    ) o
    where f.batch_id = {batch_id}
)
select
    {batch_id},
    feature_name,
    feature_value,
    is_unknown,
    impressions,
    clicks,
    ctr,
    overall_ctr,
    ctr_lift_vs_overall,
    volume_band,
    row_number_in_feature,
    dense_rank_in_feature,
    round(percent_rank_in_feature::numeric, 6),
    ctr_quartile_in_feature
from ranked_features;

insert into marts.batch_event_day_ctr_rolling (
    batch_id,
    event_batch,
    event_day_number,
    day_label,
    impressions,
    clicks,
    ctr,
    previous_day_ctr,
    ctr_change_from_previous_day,
    cumulative_impressions,
    cumulative_clicks,
    cumulative_ctr,
    rolling_3_day_avg_ctr,
    rolling_3_day_weighted_ctr
)
with ordered_days as (
    select
        event_batch,
        event_day_number,
        day_label,
        impressions,
        clicks,
        ctr
    from marts.batch_event_day_ctr_trend
    where batch_id = {batch_id}
),
windowed as (
    select
        event_batch,
        event_day_number,
        day_label,
        impressions,
        clicks,
        ctr,
        lag(ctr) over (
            partition by event_batch
            order by event_day_number
        ) as previous_day_ctr,
        ctr - lag(ctr) over (
            partition by event_batch
            order by event_day_number
        ) as ctr_change_from_previous_day,
        sum(impressions) over (
            partition by event_batch
            order by event_day_number
            rows between unbounded preceding and current row
        ) as cumulative_impressions,
        sum(clicks) over (
            partition by event_batch
            order by event_day_number
            rows between unbounded preceding and current row
        ) as cumulative_clicks,
        avg(ctr) over (
            partition by event_batch
            order by event_day_number
            rows between 2 preceding and current row
        ) as rolling_3_day_avg_ctr,
        sum(clicks) over (
            partition by event_batch
            order by event_day_number
            rows between 2 preceding and current row
        )::numeric
        / nullif(
            sum(impressions) over (
                partition by event_batch
                order by event_day_number
                rows between 2 preceding and current row
            ),
            0
        ) as rolling_3_day_weighted_ctr
    from ordered_days
)
select
    {batch_id},
    event_batch,
    event_day_number,
    day_label,
    impressions,
    clicks,
    ctr,
    round(previous_day_ctr::numeric, 6),
    round(ctr_change_from_previous_day::numeric, 6),
    cumulative_impressions,
    cumulative_clicks,
    round(cumulative_clicks::numeric / nullif(cumulative_impressions, 0), 6),
    round(rolling_3_day_avg_ctr::numeric, 6),
    round(rolling_3_day_weighted_ctr::numeric, 6)
from windowed;

insert into marts.batch_high_value_segments (
    batch_id,
    segment_type,
    segment_name,
    segment_value,
    impressions,
    clicks,
    ctr,
    overall_ctr,
    ctr_lift_vs_overall,
    volume_band,
    segment_rank_by_lift,
    segment_rank_by_clicks
)
with overall as (
    select ctr as overall_ctr
    from marts.batch_overall_ctr_summary
    where batch_id = {batch_id}
),
candidate_segments as (
    select
        'categorical_feature_value'::text as segment_type,
        f.feature_name as segment_name,
        f.feature_value as segment_value,
        f.impressions,
        f.clicks,
        f.ctr,
        round(f.ctr - o.overall_ctr, 6) as ctr_lift_vs_overall,
        case
            when f.impressions >= 10000 then 'high'
            when f.impressions >= 5000 then 'medium'
            else 'low'
        end as volume_band
    from marts.batch_feature_ctr_summary f
    cross join overall o
    where f.batch_id = {batch_id}
      and f.impressions >= 10000
      and round(f.ctr - o.overall_ctr, 6) > 0.05

    union all

    select
        'numeric_feature_bucket'::text as segment_type,
        n.feature_name as segment_name,
        n.bucket_label as segment_value,
        n.impressions,
        n.clicks,
        n.ctr,
        round(n.ctr - o.overall_ctr, 6) as ctr_lift_vs_overall,
        case
            when n.impressions >= 10000 then 'high'
            when n.impressions >= 1000 then 'medium'
            else 'low'
        end as volume_band
    from marts.batch_numeric_bucket_ctr n
    cross join overall o
    where n.batch_id = {batch_id}
      and n.impressions >= 10000
      and round(n.ctr - o.overall_ctr, 6) > 0.05

    union all

    select
        'feature_interaction'::text as segment_type,
        i.interaction_name as segment_name,
        i.left_feature_value || ' x ' || i.right_feature_value as segment_value,
        i.impressions,
        i.clicks,
        i.ctr,
        round(i.ctr - o.overall_ctr, 6) as ctr_lift_vs_overall,
        case
            when i.impressions >= 10000 then 'high'
            when i.impressions >= 5000 then 'medium'
            else 'low'
        end as volume_band
    from marts.batch_feature_interaction_ctr i
    cross join overall o
    where i.batch_id = {batch_id}
      and i.impressions >= 5000
      and round(i.ctr - o.overall_ctr, 6) > 0.05
)
select
    {batch_id},
    segment_type,
    segment_name,
    segment_value,
    impressions,
    clicks,
    ctr,
    o.overall_ctr,
    ctr_lift_vs_overall,
    volume_band,
    dense_rank() over (
        partition by segment_type
        order by ctr_lift_vs_overall desc, impressions desc
    ),
    dense_rank() over (
        partition by segment_type
        order by clicks desc, impressions desc
    )
from candidate_segments
cross join overall o;

insert into marts.batch_low_performing_segments (
    batch_id,
    segment_type,
    segment_name,
    segment_value,
    impressions,
    clicks,
    ctr,
    overall_ctr,
    ctr_lift_vs_overall,
    volume_band,
    segment_rank_by_underperformance,
    segment_rank_by_volume
)
with overall as (
    select ctr as overall_ctr
    from marts.batch_overall_ctr_summary
    where batch_id = {batch_id}
),
candidate_segments as (
    select
        'categorical_feature_value'::text as segment_type,
        f.feature_name as segment_name,
        f.feature_value as segment_value,
        f.impressions,
        f.clicks,
        f.ctr,
        round(f.ctr - o.overall_ctr, 6) as ctr_lift_vs_overall,
        case
            when f.impressions >= 10000 then 'high'
            when f.impressions >= 5000 then 'medium'
            else 'low'
        end as volume_band
    from marts.batch_feature_ctr_summary f
    cross join overall o
    where f.batch_id = {batch_id}
      and f.impressions >= 10000
      and round(f.ctr - o.overall_ctr, 6) < -0.05

    union all

    select
        'numeric_feature_bucket'::text as segment_type,
        n.feature_name as segment_name,
        n.bucket_label as segment_value,
        n.impressions,
        n.clicks,
        n.ctr,
        round(n.ctr - o.overall_ctr, 6) as ctr_lift_vs_overall,
        case
            when n.impressions >= 10000 then 'high'
            when n.impressions >= 1000 then 'medium'
            else 'low'
        end as volume_band
    from marts.batch_numeric_bucket_ctr n
    cross join overall o
    where n.batch_id = {batch_id}
      and n.impressions >= 10000
      and round(n.ctr - o.overall_ctr, 6) < -0.05

    union all

    select
        'feature_interaction'::text as segment_type,
        i.interaction_name as segment_name,
        i.left_feature_value || ' x ' || i.right_feature_value as segment_value,
        i.impressions,
        i.clicks,
        i.ctr,
        round(i.ctr - o.overall_ctr, 6) as ctr_lift_vs_overall,
        case
            when i.impressions >= 10000 then 'high'
            when i.impressions >= 5000 then 'medium'
            else 'low'
        end as volume_band
    from marts.batch_feature_interaction_ctr i
    cross join overall o
    where i.batch_id = {batch_id}
      and i.impressions >= 5000
      and round(i.ctr - o.overall_ctr, 6) < -0.02
)
select
    {batch_id},
    segment_type,
    segment_name,
    segment_value,
    impressions,
    clicks,
    ctr,
    o.overall_ctr,
    ctr_lift_vs_overall,
    volume_band,
    dense_rank() over (
        partition by segment_type
        order by ctr_lift_vs_overall asc, impressions desc
    ),
    dense_rank() over (
        partition by segment_type
        order by impressions desc, clicks asc
    )
from candidate_segments
cross join overall o;

insert into marts.batch_feature_ctr_lift_ranked (
    batch_id,
    feature_family,
    feature_name,
    segment_value,
    impressions,
    clicks,
    ctr,
    overall_ctr,
    ctr_lift_vs_overall,
    volume_band,
    row_number_by_lift,
    dense_rank_by_lift,
    lift_quintile
)
with combined_feature_sets as (
    select
        'categorical_feature'::text as feature_family,
        f.feature_name,
        f.feature_value as segment_value,
        f.impressions,
        f.clicks,
        f.ctr,
        o.overall_ctr,
        round(f.ctr - o.overall_ctr, 6) as ctr_lift_vs_overall,
        case
            when f.impressions >= 10000 then 'high'
            when f.impressions >= 5000 then 'medium'
            else 'low'
        end as volume_band
    from marts.batch_feature_ctr_summary f
    cross join (
        select ctr as overall_ctr
        from marts.batch_overall_ctr_summary
        where batch_id = {batch_id}
    ) o
    where f.batch_id = {batch_id}

    union all

    select
        'numeric_bucket'::text as feature_family,
        n.feature_name,
        n.bucket_label as segment_value,
        n.impressions,
        n.clicks,
        n.ctr,
        o.overall_ctr,
        round(n.ctr - o.overall_ctr, 6) as ctr_lift_vs_overall,
        case
            when n.impressions >= 10000 then 'high'
            when n.impressions >= 1000 then 'medium'
            else 'low'
        end as volume_band
    from marts.batch_numeric_bucket_ctr n
    cross join (
        select ctr as overall_ctr
        from marts.batch_overall_ctr_summary
        where batch_id = {batch_id}
    ) o
    where n.batch_id = {batch_id}
)
select
    {batch_id},
    feature_family,
    feature_name,
    segment_value,
    impressions,
    clicks,
    ctr,
    overall_ctr,
    ctr_lift_vs_overall,
    volume_band,
    row_number() over (
        partition by feature_family, feature_name
        order by ctr_lift_vs_overall desc, impressions desc, segment_value
    ),
    dense_rank() over (
        partition by feature_family, feature_name
        order by ctr_lift_vs_overall desc, impressions desc
    ),
    ntile(5) over (
        partition by feature_family
        order by ctr_lift_vs_overall desc, impressions desc
    )
from combined_feature_sets;

insert into marts.batch_feature_interaction_ranked (
    batch_id,
    interaction_name,
    left_feature_name,
    left_feature_value,
    right_feature_name,
    right_feature_value,
    impressions,
    clicks,
    ctr,
    overall_ctr,
    ctr_lift_vs_overall,
    volume_band,
    row_number_in_interaction,
    dense_rank_in_interaction,
    percent_rank_in_interaction,
    cumulative_clicks_by_interaction
)
with ranked_interactions as (
    select
        i.interaction_name,
        i.left_feature_name,
        i.left_feature_value,
        i.right_feature_name,
        i.right_feature_value,
        i.impressions,
        i.clicks,
        i.ctr,
        o.overall_ctr,
        round(i.ctr - o.overall_ctr, 6) as ctr_lift_vs_overall,
        case
            when i.impressions >= 10000 then 'high'
            when i.impressions >= 5000 then 'medium'
            else 'low'
        end as volume_band,
        row_number() over (
            partition by i.interaction_name
            order by i.ctr desc, i.impressions desc, i.left_feature_value, i.right_feature_value
        ) as row_number_in_interaction,
        dense_rank() over (
            partition by i.interaction_name
            order by i.ctr desc, i.impressions desc
        ) as dense_rank_in_interaction,
        percent_rank() over (
            partition by i.interaction_name
            order by i.ctr
        ) as percent_rank_in_interaction,
        sum(clicks) over (
            partition by i.interaction_name
            order by i.clicks desc, i.impressions desc
            rows between unbounded preceding and current row
        ) as cumulative_clicks_by_interaction
    from marts.batch_feature_interaction_ctr i
    cross join (
        select ctr as overall_ctr
        from marts.batch_overall_ctr_summary
        where batch_id = {batch_id}
    ) o
    where i.batch_id = {batch_id}
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
    ctr,
    overall_ctr,
    ctr_lift_vs_overall,
    volume_band,
    row_number_in_interaction,
    dense_rank_in_interaction,
    round(percent_rank_in_interaction::numeric, 6),
    cumulative_clicks_by_interaction
from ranked_interactions;
"""


def build_audit_insert_sql(source_file: str, row_count: int, batch_id: int, pipeline_run_id: int) -> str:
    escaped_source = sql_literal(source_file)
    message = sql_literal(f"Advanced SQL layer build completed successfully for {source_file}.")
    return (
        "insert into quality.load_audit "
        "(batch_id, pipeline_run_id, layer_name, table_name, source_file, row_count, check_status, check_message) "
        f"values ({batch_id}, {pipeline_run_id}, 'advanced_sql', 'advanced sql layer', '{escaped_source}', {row_count}, 'SUCCESS', '{message}') "
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
    marts_row_count = int(
        run_scalar_query(
            f"select count(*) from marts.batch_feature_ctr_summary where batch_id = {batch_id};",
            args.database,
            args,
        )
    )
    if marts_row_count == 0:
        raise RuntimeError(
            f"marts.batch_feature_ctr_summary is empty for batch {batch_id}. Build the marts layer before building advanced SQL assets."
        )

    ensure_pipeline_metadata(sql_dir, args.database, args)
    run_psql(sql_dir / "12_advanced_sql_assets.sql", args.database, args)
    run_psql(sql_dir / "06_quality_checks.sql", args.database, args)

    fact_rows = int(
        run_scalar_query(
            f"select count(*) from warehouse.fact_ad_events where batch_id = {batch_id};",
            args.database,
            args,
        )
    )
    pipeline_run_id = create_pipeline_run(
        batch_id=batch_id,
        pipeline_name="advanced_sql_build",
        layer_name="advanced_sql",
        source_file=source_file,
        triggered_by=args.triggered_by,
        database=args.database,
        args=args,
    )
    pipeline_step_id = create_pipeline_step(
        pipeline_run_id=pipeline_run_id,
        batch_id=batch_id,
        step_name="build_advanced_sql_assets",
        layer_name="advanced_sql",
        target_table="advanced sql layer",
        source_file=source_file,
        database=args.database,
        args=args,
    )
    update_batch_status(
        batch_id=batch_id,
        batch_status="ADVANCED_SQL_BUILDING",
        last_successful_stage="marts",
        row_column=None,
        row_count=None,
        mark_started=False,
        mark_completed=False,
        database=args.database,
        args=args,
    )

    try:
        command = ["psql"]
        if args.host:
            command.extend(["-h", args.host])
        if args.port:
            command.extend(["-p", str(args.port)])
        if args.user:
            command.extend(["-U", args.user])
        command.extend(
            [
                "-d",
                args.database,
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                build_batch_advanced_sql(batch_id),
            ]
        )
        subprocess.run(command, check=True, env=build_env(args))

        advanced_rows = int(
            run_scalar_query(
                f"select count(*) from marts.batch_feature_ctr_ranked where batch_id = {batch_id};",
                args.database,
                args,
            )
        )
        if advanced_rows == 0:
            raise RuntimeError(f"Advanced SQL outputs were not populated for batch {batch_id}.")

        update_batch_status(
            batch_id=batch_id,
            batch_status="ADVANCED_SQL_READY",
            last_successful_stage="advanced_sql",
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
            rows_processed=fact_rows,
            step_message=f"Advanced SQL build completed successfully for {source_file}.",
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="SUCCESS",
            run_message=f"Advanced SQL layer build completed successfully for {source_file}.",
            database=args.database,
            args=args,
        )

        audit_id = run_scalar_query(
            build_audit_insert_sql(source_file, fact_rows, batch_id, pipeline_run_id),
            args.database,
            args,
        )

        print(f"Warehouse fact rows: {fact_rows:,}")
        print(f"Advanced SQL ranked feature rows: {advanced_rows:,}")
        print(f"Source file tag: {source_file}")
        print(f"Batch ID: {batch_id}")
        print(f"Pipeline run ID: {pipeline_run_id}")
        print("Advanced SQL layer build validation passed.")
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
            batch_status="ADVANCED_SQL_FAILED",
            last_successful_stage="marts",
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
