-- Power BI import queries for the ML Model Monitoring page.

select
    model_name,
    model_version,
    training_run_id,
    train_batch_name,
    training_scale_band,
    is_canonical_1m_run,
    is_active_canonical,
    canonical_promoted_at,
    canonical_promotion_note,
    rows_trained,
    rows_validated,
    rows_tested,
    validation_roc_auc,
    validation_pr_auc,
    validation_log_loss,
    validation_precision_at_10pct,
    validation_lift_at_10pct,
    test_roc_auc,
    test_pr_auc,
    test_log_loss,
    test_precision_at_10pct,
    test_lift_at_10pct,
    latest_scored_batch_id,
    latest_scored_batch_name,
    rows_scored,
    overall_avg_predicted_ctr,
    overall_actual_ctr,
    top_decile_rows,
    top_decile_avg_predicted_ctr,
    top_decile_actual_ctr,
    top_decile_lift_vs_batch_ctr,
    top_decile_predicted_ctr_gap,
    top_decile_actual_ctr_gap,
    previous_batch_id,
    previous_batch_name,
    avg_predicted_ctr_delta,
    actual_ctr_delta,
    top_decile_actual_ctr_delta,
    top_decile_lift_delta,
    validation_quality_band,
    ranking_quality_band,
    drift_severity,
    comparison_note,
    first_scored_at,
    last_scored_at
from ml.latest_model_monitoring_dashboard
order by validation_roc_auc desc, top_decile_lift_vs_batch_ctr desc;


select
    batch_id,
    batch_name,
    model_id,
    training_run_id,
    model_name,
    model_version,
    training_scale_band,
    is_canonical_1m_run,
    validation_roc_auc,
    validation_pr_auc,
    validation_precision_at_10pct,
    validation_lift_at_10pct,
    rows_scored,
    overall_actual_ctr,
    top_decile_rows,
    top_decile_actual_ctr,
    top_decile_lift_vs_batch_ctr,
    batch_rank_by_lift,
    batch_rank_by_auc
from ml.batch_model_rankings
order by batch_id desc, batch_rank_by_lift, validation_roc_auc desc;


select
    batch_id,
    batch_name,
    model_id,
    training_run_id,
    model_name,
    model_version,
    previous_batch_id,
    previous_batch_name,
    overall_avg_predicted_ctr,
    overall_actual_ctr,
    top_decile_actual_ctr,
    top_decile_lift_vs_batch_ctr,
    avg_predicted_ctr_delta,
    actual_ctr_delta,
    top_decile_actual_ctr_delta,
    top_decile_lift_delta,
    drift_status,
    drift_note,
    first_scored_at,
    last_scored_at
from ml.model_drift_watchlist
order by batch_id desc, drift_status desc, model_name, model_version;


select
    model_name,
    model_version,
    training_run_id,
    feature_name,
    feature_group,
    importance_value,
    abs_importance_value,
    relative_importance_pct,
    importance_direction,
    interpretation_note,
    importance_rank,
    recorded_at
from ml.latest_model_feature_importance
order by model_name, model_version, importance_rank;


select
    model_name,
    model_version,
    training_run_id,
    feature_group,
    features_in_group,
    total_abs_importance,
    avg_abs_importance,
    max_abs_importance,
    group_relative_importance_pct
from ml.latest_feature_group_importance
order by model_name, model_version, total_abs_importance desc;
