select count(*) as validation_runs_rows from quality.validation_runs;
select count(*) as validation_results_rows from quality.validation_results;
select count(*) as validation_threshold_rows from quality.validation_thresholds;
select count(*) as latest_validation_summary_rows from quality.latest_validation_summary;
select count(*) as validation_dashboard_summary_rows from quality.validation_dashboard_summary;

select
    run_id,
    pipeline_name,
    source_file,
    run_status,
    started_at,
    completed_at
from quality.validation_runs
order by run_id desc
limit 5;

select *
from quality.validation_dashboard_summary;

select
    layer_name,
    table_name,
    check_status,
    count(*) as checks
from quality.latest_validation_summary
group by layer_name, table_name, check_status
order by layer_name, table_name, check_status;

select
    layer_name,
    table_name,
    check_name,
    check_type,
    severity,
    check_status,
    actual_value,
    expected_value,
    threshold_value,
    checked_at
from quality.latest_validation_summary
order by layer_name, table_name, check_name;
