-- Power BI import queries for the Pipeline Operations Overview page.

select
    batch_id,
    batch_name,
    source_file,
    source_type,
    sample_scale,
    batch_status,
    last_successful_stage,
    created_at,
    started_at,
    completed_at,
    batch_elapsed_seconds,
    orchestration_run_id,
    latest_quality_run_id,
    latest_quality_run_status,
    total_quality_checks,
    passed_quality_checks,
    warning_quality_checks,
    failed_quality_checks,
    total_alerts,
    error_alerts,
    warning_alerts,
    total_stage_runtime_seconds,
    slowest_stage_runtime_seconds,
    archive_path,
    failure_path
from ops.pipeline_health_dashboard
order by batch_id desc;


select
    batch_id,
    batch_name,
    sample_scale,
    layer_name,
    object_name,
    duration_seconds,
    rows_per_second,
    row_count,
    storage_mb,
    recorded_at
from ops.batch_runtime_trend
order by recorded_at desc, duration_seconds desc;
