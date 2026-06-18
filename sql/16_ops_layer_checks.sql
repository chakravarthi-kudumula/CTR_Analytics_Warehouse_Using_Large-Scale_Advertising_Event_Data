select count(*) as batch_registry_rows from ops.batch_registry;
select count(*) as pipeline_runs_rows from ops.pipeline_runs;
select count(*) as pipeline_steps_rows from ops.pipeline_steps;
select count(*) as batch_artifacts_rows from ops.batch_artifacts;
select count(*) as pipeline_alerts_rows from ops.pipeline_alerts;
select count(*) as benchmark_snapshots_rows from ops.benchmark_snapshots;
select count(*) as latest_batch_status_rows from ops.latest_batch_status;
select count(*) as latest_batch_artifacts_rows from ops.latest_batch_artifacts;
select count(*) as pipeline_run_summary_rows from ops.pipeline_run_summary;
select count(*) as pipeline_benchmark_summary_rows from ops.pipeline_benchmark_summary;

select
    batch_id,
    batch_name,
    source_file,
    sample_scale,
    expected_row_count,
    actual_raw_row_count,
    actual_staging_row_count,
    actual_fact_row_count,
    batch_status,
    last_successful_stage,
    created_at,
    started_at,
    completed_at
from ops.batch_registry
order by batch_id desc
limit 10;

select
    pipeline_run_id,
    batch_id,
    pipeline_name,
    layer_name,
    run_status,
    started_at,
    completed_at
from ops.pipeline_runs
order by pipeline_run_id desc
limit 20;

select
    pipeline_step_id,
    pipeline_run_id,
    batch_id,
    step_name,
    layer_name,
    target_table,
    rows_processed,
    step_status,
    started_at,
    completed_at
from ops.pipeline_steps
order by pipeline_step_id desc
limit 30;

select
    artifact_id,
    batch_id,
    pipeline_run_id,
    artifact_name,
    artifact_type,
    artifact_format,
    artifact_path,
    row_count,
    artifact_status,
    created_at
from ops.batch_artifacts
order by artifact_id desc
limit 30;

select
    alert_id,
    batch_id,
    pipeline_name,
    task_name,
    alert_level,
    alert_type,
    created_at
from ops.pipeline_alerts
order by alert_id desc
limit 30;

select
    benchmark_id,
    batch_id,
    benchmark_name,
    layer_name,
    object_name,
    row_count,
    duration_seconds,
    rows_per_second,
    storage_mb,
    recorded_at
from ops.pipeline_benchmark_summary
order by benchmark_id desc
limit 50;
