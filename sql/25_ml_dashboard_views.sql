drop view if exists ml.model_drift_watchlist;
drop view if exists ml.batch_model_rankings;
drop view if exists ml.latest_model_monitoring_dashboard;

create or replace view ml.latest_model_monitoring_dashboard as
with latest_training_run as (
    select distinct on (mcs.model_name, mcs.model_version)
        mcs.model_name,
        mcs.model_version,
        mcs.training_run_id,
        mcs.train_batch_name,
        mcs.validation_batch_name,
        mcs.test_batch_name,
        mcs.rows_trained,
        mcs.rows_validated,
        mcs.rows_tested,
        mcs.training_scale_band,
        mcs.is_canonical_1m_run,
        mcs.comparison_note,
        mcs.validation_roc_auc,
        mcs.validation_pr_auc,
        mcs.validation_log_loss,
        mcs.validation_precision_at_10pct,
        mcs.validation_lift_at_10pct,
        mcs.test_roc_auc,
        mcs.test_pr_auc,
        mcs.test_log_loss,
        mcs.test_precision_at_10pct,
        mcs.test_lift_at_10pct,
        mcs.scored_batch_name,
        mcs.rows_scored,
        mcs.overall_actual_ctr,
        mcs.top_decile_actual_ctr,
        mcs.top_decile_lift_vs_batch_ctr,
        mcs.top_decile_avg_predicted_ctr
    from ml.model_comparison_summary mcs
    order by mcs.model_name, mcs.model_version, mcs.training_run_id desc
),
latest_scoring_batch as (
    select distinct on (tdp.model_name, tdp.model_version)
        tdp.model_name,
        tdp.model_version,
        tdp.training_run_id,
        tdp.batch_id,
        tdp.batch_name,
        tdp.rows_scored,
        tdp.overall_avg_predicted_ctr,
        tdp.overall_actual_ctr,
        tdp.top_decile_rows,
        tdp.top_decile_avg_predicted_ctr,
        tdp.top_decile_actual_ctr,
        tdp.top_decile_lift_vs_batch_ctr,
        tdp.top_decile_predicted_ctr_gap,
        tdp.top_decile_actual_ctr_gap,
        tdp.first_scored_at,
        tdp.last_scored_at
    from ml.top_decile_performance tdp
    order by tdp.model_name, tdp.model_version, tdp.batch_id desc
),
latest_drift as (
    select distinct on (sds.model_name, sds.model_version)
        sds.model_name,
        sds.model_version,
        sds.batch_id,
        sds.batch_name,
        sds.previous_batch_id,
        sds.previous_batch_name,
        sds.avg_predicted_ctr_delta,
        sds.actual_ctr_delta,
        sds.top_decile_actual_ctr_delta,
        sds.top_decile_lift_delta
    from ml.score_drift_summary sds
    order by sds.model_name, sds.model_version, sds.batch_id desc
)
select
    ltr.model_name,
    ltr.model_version,
    ltr.training_run_id,
    ltr.train_batch_name,
    ltr.training_scale_band,
    ltr.is_canonical_1m_run,
    ltr.rows_trained,
    ltr.rows_validated,
    ltr.rows_tested,
    ltr.validation_roc_auc,
    ltr.validation_pr_auc,
    ltr.validation_log_loss,
    ltr.validation_precision_at_10pct,
    ltr.validation_lift_at_10pct,
    ltr.test_roc_auc,
    ltr.test_pr_auc,
    ltr.test_log_loss,
    ltr.test_precision_at_10pct,
    ltr.test_lift_at_10pct,
    coalesce(acm.model_id is not null, false) as is_active_canonical,
    acm.canonical_promoted_at,
    acm.canonical_promotion_note,
    lsb.batch_id as latest_scored_batch_id,
    lsb.batch_name as latest_scored_batch_name,
    lsb.rows_scored,
    lsb.overall_avg_predicted_ctr,
    lsb.overall_actual_ctr,
    lsb.top_decile_rows,
    lsb.top_decile_avg_predicted_ctr,
    lsb.top_decile_actual_ctr,
    lsb.top_decile_lift_vs_batch_ctr,
    lsb.top_decile_predicted_ctr_gap,
    lsb.top_decile_actual_ctr_gap,
    ld.previous_batch_id,
    ld.previous_batch_name,
    ld.avg_predicted_ctr_delta,
    ld.actual_ctr_delta,
    ld.top_decile_actual_ctr_delta,
    ld.top_decile_lift_delta,
    case
        when ltr.validation_roc_auc >= 0.70 then 'strong'
        when ltr.validation_roc_auc >= 0.60 then 'good'
        when ltr.validation_roc_auc >= 0.55 then 'watch'
        else 'weak'
    end as validation_quality_band,
    case
        when coalesce(lsb.top_decile_lift_vs_batch_ctr, 0) >= 2.0 then 'strong'
        when coalesce(lsb.top_decile_lift_vs_batch_ctr, 0) >= 1.5 then 'good'
        when coalesce(lsb.top_decile_lift_vs_batch_ctr, 0) >= 1.2 then 'watch'
        else 'weak'
    end as ranking_quality_band,
    case
        when abs(coalesce(ld.top_decile_lift_delta, 0)) >= 0.50 then 'high'
        when abs(coalesce(ld.top_decile_lift_delta, 0)) >= 0.20 then 'medium'
        else 'low'
    end as drift_severity,
    ltr.comparison_note,
    lsb.first_scored_at,
    lsb.last_scored_at
from latest_training_run ltr
left join ml.active_canonical_model acm
  on acm.model_name = ltr.model_name
 and acm.model_version = ltr.model_version
 and acm.training_run_id = ltr.training_run_id
left join latest_scoring_batch lsb
  on lsb.model_name = ltr.model_name
 and lsb.model_version = ltr.model_version
 and lsb.training_run_id = ltr.training_run_id
left join latest_drift ld
  on ld.model_name = ltr.model_name
 and ld.model_version = ltr.model_version
 and ld.batch_id = lsb.batch_id;


create or replace view ml.batch_model_rankings as
select
    tdp.batch_id,
    tdp.batch_name,
    tdp.model_id,
    tdp.training_run_id,
    tdp.model_name,
    tdp.model_version,
    mcs.training_scale_band,
    mcs.is_canonical_1m_run,
    mcs.validation_roc_auc,
    mcs.validation_pr_auc,
    mcs.validation_precision_at_10pct,
    mcs.validation_lift_at_10pct,
    tdp.rows_scored,
    tdp.overall_actual_ctr,
    tdp.top_decile_rows,
    tdp.top_decile_actual_ctr,
    tdp.top_decile_lift_vs_batch_ctr,
    dense_rank() over (
        partition by tdp.batch_id
        order by tdp.top_decile_lift_vs_batch_ctr desc, mcs.validation_roc_auc desc
    ) as batch_rank_by_lift,
    dense_rank() over (
        partition by tdp.batch_id
        order by mcs.validation_roc_auc desc, tdp.top_decile_lift_vs_batch_ctr desc
    ) as batch_rank_by_auc
from ml.top_decile_performance tdp
left join ml.model_comparison_summary mcs
  on mcs.model_name = tdp.model_name
 and mcs.model_version = tdp.model_version
 and mcs.training_run_id = tdp.training_run_id;


create or replace view ml.model_drift_watchlist as
select
    sds.batch_id,
    sds.batch_name,
    sds.model_id,
    sds.training_run_id,
    sds.model_name,
    sds.model_version,
    sds.previous_batch_id,
    sds.previous_batch_name,
    sds.overall_avg_predicted_ctr,
    sds.overall_actual_ctr,
    sds.top_decile_actual_ctr,
    sds.top_decile_lift_vs_batch_ctr,
    sds.avg_predicted_ctr_delta,
    sds.actual_ctr_delta,
    sds.top_decile_actual_ctr_delta,
    sds.top_decile_lift_delta,
    case
        when sds.previous_batch_id is null then 'baseline'
        when abs(coalesce(sds.top_decile_lift_delta, 0)) >= 0.50
          or abs(coalesce(sds.actual_ctr_delta, 0)) >= 0.05 then 'investigate'
        when abs(coalesce(sds.top_decile_lift_delta, 0)) >= 0.20
          or abs(coalesce(sds.actual_ctr_delta, 0)) >= 0.02 then 'watch'
        else 'stable'
    end as drift_status,
    case
        when sds.previous_batch_id is null then 'No previous scored batch for comparison.'
        when abs(coalesce(sds.top_decile_lift_delta, 0)) >= 0.50
            then 'Top-decile lift changed materially from the previous batch.'
        when abs(coalesce(sds.actual_ctr_delta, 0)) >= 0.05
            then 'Observed batch CTR shifted materially from the previous batch.'
        when abs(coalesce(sds.top_decile_lift_delta, 0)) >= 0.20
            then 'Top-decile lift moved enough to deserve review.'
        when abs(coalesce(sds.actual_ctr_delta, 0)) >= 0.02
            then 'Observed CTR moved enough to deserve review.'
        else 'No material drift detected.'
    end as drift_note,
    sds.first_scored_at,
    sds.last_scored_at
from ml.score_drift_summary sds;
