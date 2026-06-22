-- ML foundation validation checks

select 'model_registry_exists' as check_name, count(*) as row_count
from information_schema.tables
where table_schema = 'ml' and table_name = 'model_registry';

select 'training_runs_exists' as check_name, count(*) as row_count
from information_schema.tables
where table_schema = 'ml' and table_name = 'training_runs';

select 'model_metrics_exists' as check_name, count(*) as row_count
from information_schema.tables
where table_schema = 'ml' and table_name = 'model_metrics';

select 'prediction_scores_exists' as check_name, count(*) as row_count
from information_schema.tables
where table_schema = 'ml' and table_name = 'prediction_scores';

select 'latest_training_metrics_view_exists' as check_name, count(*) as row_count
from information_schema.views
where table_schema = 'ml' and table_name = 'latest_training_metrics';

select 'score_decile_performance_view_exists' as check_name, count(*) as row_count
from information_schema.views
where table_schema = 'ml' and table_name = 'score_decile_performance';

select 'top_decile_performance_view_exists' as check_name, count(*) as row_count
from information_schema.views
where table_schema = 'ml' and table_name = 'top_decile_performance';

select 'score_drift_summary_view_exists' as check_name, count(*) as row_count
from information_schema.views
where table_schema = 'ml' and table_name = 'score_drift_summary';

select 'model_comparison_summary_view_exists' as check_name, count(*) as row_count
from information_schema.views
where table_schema = 'ml' and table_name = 'model_comparison_summary';

select
    tablename as table_name,
    count(*) as indexed_columns
from pg_indexes
where schemaname = 'ml'
group by tablename
order by tablename;

select
    column_name,
    data_type,
    is_nullable
from information_schema.columns
where table_schema = 'ml'
  and table_name = 'prediction_scores'
order by ordinal_position;

select
    batch_id,
    model_name,
    model_version,
    rows_scored,
    top_decile_rows,
    overall_actual_ctr,
    top_decile_actual_ctr,
    top_decile_lift_vs_batch_ctr
from ml.top_decile_performance
order by batch_id desc, model_name, model_version;

select
    batch_id,
    model_name,
    model_version,
    previous_batch_id,
    avg_predicted_ctr_delta,
    actual_ctr_delta,
    top_decile_actual_ctr_delta,
    top_decile_lift_delta
from ml.score_drift_summary
order by batch_id desc, model_name, model_version;

select
    model_name,
    model_version,
    training_run_id,
    train_batch_name,
    rows_trained,
    training_scale_band,
    is_canonical_1m_run,
    comparison_note,
    validation_roc_auc,
    validation_pr_auc,
    validation_log_loss,
    top_decile_lift_vs_batch_ctr
from ml.model_comparison_summary
order by is_canonical_1m_run desc, rows_trained desc, model_name, training_run_id;
