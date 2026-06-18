drop view if exists ops.batch_stage_runtime_drift;
drop view if exists ops.batch_drift_summary;
drop view if exists ops.batch_metric_baseline;

create or replace view ops.batch_metric_baseline as
with fact_metrics as (
    select
        batch_id,
        count(*) as fact_rows,
        sum(click_count) as clicks,
        round(sum(click_count)::numeric / nullif(count(*), 0), 6) as ctr,
        round(avg(missing_numeric_count::numeric), 6) as avg_missing_numeric_count,
        round(avg(missing_categorical_count::numeric), 6) as avg_missing_categorical_count
    from warehouse.fact_ad_events
    group by batch_id
),
sample_file_sizes as (
    select
        batch_id,
        max(storage_mb) as sample_file_size_mb
    from ops.pipeline_benchmark_summary
    where benchmark_name = 'file_storage'
      and layer_name = 'source'
    group by batch_id
),
processed_file_sizes as (
    select
        batch_id,
        sum(storage_mb) as processed_artifact_size_mb
    from ops.pipeline_benchmark_summary
    where benchmark_name = 'file_storage'
      and layer_name = 'processed'
    group by batch_id
),
table_storage as (
    select
        batch_id,
        max(storage_mb) filter (where layer_name = 'raw') as raw_table_size_mb,
        max(storage_mb) filter (where layer_name = 'staging') as staging_table_size_mb,
        max(storage_mb) filter (where layer_name = 'warehouse') as warehouse_table_size_mb,
        max(storage_mb) filter (where layer_name = 'feature_store') as feature_store_table_size_mb
    from ops.pipeline_benchmark_summary
    where benchmark_name = 'table_storage'
    group by batch_id
)
select
    b.batch_id,
    b.batch_name,
    b.source_file,
    b.source_type,
    b.sample_scale,
    b.batch_status,
    b.last_successful_stage,
    b.created_at,
    b.started_at,
    b.completed_at,
    b.expected_row_count,
    b.actual_raw_row_count,
    b.actual_staging_row_count,
    b.actual_fact_row_count,
    round(extract(epoch from (coalesce(b.completed_at, now()) - coalesce(b.started_at, b.created_at)))::numeric, 3) as batch_elapsed_seconds,
    coalesce(f.fact_rows, b.actual_fact_row_count, 0) as fact_rows,
    coalesce(f.clicks, 0) as clicks,
    f.ctr,
    f.avg_missing_numeric_count,
    f.avg_missing_categorical_count,
    coalesce(h.total_quality_checks, 0) as total_quality_checks,
    coalesce(h.passed_quality_checks, 0) as passed_quality_checks,
    coalesce(h.warning_quality_checks, 0) as warning_quality_checks,
    coalesce(h.failed_quality_checks, 0) as failed_quality_checks,
    coalesce(h.total_alerts, 0) as total_alerts,
    coalesce(h.error_alerts, 0) as error_alerts,
    coalesce(h.warning_alerts, 0) as warning_alerts,
    round(coalesce(h.total_stage_runtime_seconds, 0)::numeric, 3) as total_stage_runtime_seconds,
    round(coalesce(h.slowest_stage_runtime_seconds, 0)::numeric, 3) as slowest_stage_runtime_seconds,
    s.sample_file_size_mb,
    p.processed_artifact_size_mb,
    t.raw_table_size_mb,
    t.staging_table_size_mb,
    t.warehouse_table_size_mb,
    t.feature_store_table_size_mb,
    b.archive_path,
    b.failure_path
from ops.batch_registry b
left join fact_metrics f
  on f.batch_id = b.batch_id
left join ops.pipeline_health_dashboard h
  on h.batch_id = b.batch_id
left join sample_file_sizes s
  on s.batch_id = b.batch_id
left join processed_file_sizes p
  on p.batch_id = b.batch_id
left join table_storage t
  on t.batch_id = b.batch_id;

create or replace view ops.batch_drift_summary as
with ranked_batches as (
    select
        m.*,
        lag(batch_id) over (
            partition by source_type, sample_scale
            order by batch_id
        ) as previous_batch_id,
        lag(batch_name) over (
            partition by source_type, sample_scale
            order by batch_id
        ) as previous_batch_name,
        lag(fact_rows) over (
            partition by source_type, sample_scale
            order by batch_id
        ) as previous_fact_rows,
        lag(ctr) over (
            partition by source_type, sample_scale
            order by batch_id
        ) as previous_ctr,
        lag(avg_missing_numeric_count) over (
            partition by source_type, sample_scale
            order by batch_id
        ) as previous_avg_missing_numeric_count,
        lag(avg_missing_categorical_count) over (
            partition by source_type, sample_scale
            order by batch_id
        ) as previous_avg_missing_categorical_count,
        lag(total_stage_runtime_seconds) over (
            partition by source_type, sample_scale
            order by batch_id
        ) as previous_total_stage_runtime_seconds,
        lag(total_quality_checks) over (
            partition by source_type, sample_scale
            order by batch_id
        ) as previous_total_quality_checks,
        lag(failed_quality_checks) over (
            partition by source_type, sample_scale
            order by batch_id
        ) as previous_failed_quality_checks,
        lag(total_alerts) over (
            partition by source_type, sample_scale
            order by batch_id
        ) as previous_total_alerts,
        lag(sample_file_size_mb) over (
            partition by source_type, sample_scale
            order by batch_id
        ) as previous_sample_file_size_mb
    from ops.batch_metric_baseline m
)
select
    batch_id,
    batch_name,
    source_file,
    source_type,
    sample_scale,
    batch_status,
    last_successful_stage,
    previous_batch_id,
    previous_batch_name,
    fact_rows,
    previous_fact_rows,
    fact_rows - previous_fact_rows as fact_row_delta,
    case
        when previous_fact_rows is null or previous_fact_rows = 0 then null
        else round(((fact_rows - previous_fact_rows)::numeric / previous_fact_rows), 6)
    end as fact_row_delta_ratio,
    ctr,
    previous_ctr,
    round((ctr - previous_ctr)::numeric, 6) as ctr_delta,
    avg_missing_numeric_count,
    previous_avg_missing_numeric_count,
    round((avg_missing_numeric_count - previous_avg_missing_numeric_count)::numeric, 6) as avg_missing_numeric_delta,
    avg_missing_categorical_count,
    previous_avg_missing_categorical_count,
    round((avg_missing_categorical_count - previous_avg_missing_categorical_count)::numeric, 6) as avg_missing_categorical_delta,
    total_stage_runtime_seconds,
    previous_total_stage_runtime_seconds,
    round((total_stage_runtime_seconds - previous_total_stage_runtime_seconds)::numeric, 3) as runtime_delta_seconds,
    case
        when previous_total_stage_runtime_seconds is null or previous_total_stage_runtime_seconds = 0 then null
        else round(((total_stage_runtime_seconds - previous_total_stage_runtime_seconds)::numeric / previous_total_stage_runtime_seconds), 6)
    end as runtime_delta_ratio,
    total_quality_checks,
    previous_total_quality_checks,
    failed_quality_checks,
    previous_failed_quality_checks,
    failed_quality_checks - previous_failed_quality_checks as failed_quality_delta,
    total_alerts,
    previous_total_alerts,
    total_alerts - previous_total_alerts as alert_delta,
    sample_file_size_mb,
    previous_sample_file_size_mb,
    round((sample_file_size_mb - previous_sample_file_size_mb)::numeric, 3) as sample_file_size_delta_mb,
    case
        when previous_batch_id is null then 'baseline'
        when failed_quality_checks > coalesce(previous_failed_quality_checks, 0) then 'quality_regression'
        when total_alerts > coalesce(previous_total_alerts, 0) then 'alert_regression'
        when ctr is not null and previous_ctr is not null and abs(ctr - previous_ctr) >= 0.050000 then 'ctr_shift'
        when avg_missing_categorical_count is not null and previous_avg_missing_categorical_count is not null
             and abs(avg_missing_categorical_count - previous_avg_missing_categorical_count) >= 0.500000 then 'missingness_shift'
        when total_stage_runtime_seconds is not null and previous_total_stage_runtime_seconds is not null
             and previous_total_stage_runtime_seconds > 0
             and ((total_stage_runtime_seconds - previous_total_stage_runtime_seconds)::numeric / previous_total_stage_runtime_seconds) >= 0.500000 then 'runtime_regression'
        else 'stable'
    end as drift_status
from ranked_batches;

create or replace view ops.batch_stage_runtime_drift as
with stage_runtime as (
    select
        r.batch_id,
        r.batch_name,
        b.source_type,
        r.sample_scale,
        r.layer_name,
        r.object_name,
        r.duration_seconds,
        r.row_count,
        r.rows_per_second,
        r.recorded_at
    from ops.batch_runtime_trend r
    join ops.batch_registry b
      on b.batch_id = r.batch_id
),
ranked_runtime as (
    select
        s.*,
        lag(batch_id) over (
            partition by source_type, sample_scale, layer_name, object_name
            order by batch_id
        ) as previous_batch_id,
        lag(batch_name) over (
            partition by source_type, sample_scale, layer_name, object_name
            order by batch_id
        ) as previous_batch_name,
        lag(duration_seconds) over (
            partition by source_type, sample_scale, layer_name, object_name
            order by batch_id
        ) as previous_duration_seconds,
        lag(rows_per_second) over (
            partition by source_type, sample_scale, layer_name, object_name
            order by batch_id
        ) as previous_rows_per_second
    from stage_runtime s
)
select
    batch_id,
    batch_name,
    source_type,
    sample_scale,
    layer_name,
    object_name,
    row_count,
    duration_seconds,
    previous_batch_id,
    previous_batch_name,
    previous_duration_seconds,
    round((duration_seconds - previous_duration_seconds)::numeric, 3) as duration_delta_seconds,
    case
        when previous_duration_seconds is null or previous_duration_seconds = 0 then null
        else round(((duration_seconds - previous_duration_seconds)::numeric / previous_duration_seconds), 6)
    end as duration_delta_ratio,
    rows_per_second,
    previous_rows_per_second,
    round((rows_per_second - previous_rows_per_second)::numeric, 3) as rows_per_second_delta,
    recorded_at
from ranked_runtime;
