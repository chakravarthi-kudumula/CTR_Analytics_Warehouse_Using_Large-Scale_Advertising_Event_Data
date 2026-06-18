select count(*) as batch_metric_baseline_rows from ops.batch_metric_baseline;
select count(*) as batch_drift_summary_rows from ops.batch_drift_summary;
select count(*) as batch_stage_runtime_drift_rows from ops.batch_stage_runtime_drift;

select
    batch_id,
    batch_name,
    source_type,
    sample_scale,
    fact_rows,
    ctr,
    total_stage_runtime_seconds,
    failed_quality_checks,
    total_alerts
from ops.batch_metric_baseline
order by batch_id desc
limit 10;

select
    batch_id,
    batch_name,
    previous_batch_name,
    fact_row_delta,
    ctr_delta,
    runtime_delta_seconds,
    failed_quality_delta,
    alert_delta,
    drift_status
from ops.batch_drift_summary
order by batch_id desc
limit 10;

select
    batch_id,
    batch_name,
    layer_name,
    object_name,
    duration_seconds,
    previous_duration_seconds,
    duration_delta_seconds,
    duration_delta_ratio
from ops.batch_stage_runtime_drift
order by batch_id desc, duration_seconds desc
limit 20;
