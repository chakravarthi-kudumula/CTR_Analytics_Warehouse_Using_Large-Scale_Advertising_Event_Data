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

select 'latest_model_monitoring_dashboard_view_exists' as check_name, count(*) as row_count
from information_schema.views
where table_schema = 'ml' and table_name = 'latest_model_monitoring_dashboard';

select 'batch_model_rankings_view_exists' as check_name, count(*) as row_count
from information_schema.views
where table_schema = 'ml' and table_name = 'batch_model_rankings';

select 'model_drift_watchlist_view_exists' as check_name, count(*) as row_count
from information_schema.views
where table_schema = 'ml' and table_name = 'model_drift_watchlist';

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

select
    model_name,
    model_version,
    training_run_id,
    train_batch_name,
    validation_roc_auc,
    top_decile_lift_vs_batch_ctr,
    validation_quality_band,
    ranking_quality_band,
    drift_severity
from ml.latest_model_monitoring_dashboard
order by model_name, model_version;

select
    batch_id,
    batch_name,
    model_name,
    model_version,
    batch_rank_by_lift,
    batch_rank_by_auc,
    top_decile_lift_vs_batch_ctr,
    validation_roc_auc
from ml.batch_model_rankings
order by batch_id desc, batch_rank_by_lift, model_name, model_version;

select
    batch_id,
    batch_name,
    model_name,
    model_version,
    previous_batch_id,
    drift_status,
    drift_note,
    top_decile_lift_delta,
    actual_ctr_delta
from ml.model_drift_watchlist
order by batch_id desc, model_name, model_version;
