create or replace view ml.model_comparison_summary as
with metric_pivot as (
    select
        mr.model_name,
        mr.model_version,
        tr.training_run_id,
        tr.train_batch_name,
        tr.validation_batch_name,
        tr.test_batch_name,
        tr.rows_trained,
        tr.rows_validated,
        tr.rows_tested,
        max(case when mm.dataset_split = 'train' then mm.roc_auc end) as train_roc_auc,
        max(case when mm.dataset_split = 'train' then mm.pr_auc end) as train_pr_auc,
        max(case when mm.dataset_split = 'train' then mm.log_loss end) as train_log_loss,
        max(case when mm.dataset_split = 'validation' then mm.roc_auc end) as validation_roc_auc,
        max(case when mm.dataset_split = 'validation' then mm.pr_auc end) as validation_pr_auc,
        max(case when mm.dataset_split = 'validation' then mm.log_loss end) as validation_log_loss,
        max(case when mm.dataset_split = 'validation' then mm.precision_at_10pct end) as validation_precision_at_10pct,
        max(case when mm.dataset_split = 'validation' then mm.lift_at_10pct end) as validation_lift_at_10pct,
        max(case when mm.dataset_split = 'test' then mm.roc_auc end) as test_roc_auc,
        max(case when mm.dataset_split = 'test' then mm.pr_auc end) as test_pr_auc,
        max(case when mm.dataset_split = 'test' then mm.log_loss end) as test_log_loss,
        max(case when mm.dataset_split = 'test' then mm.precision_at_10pct end) as test_precision_at_10pct,
        max(case when mm.dataset_split = 'test' then mm.lift_at_10pct end) as test_lift_at_10pct
    from ml.model_registry mr
    join ml.training_runs tr
      on tr.model_id = mr.model_id
    join ml.model_metrics mm
      on mm.training_run_id = tr.training_run_id
    group by
        mr.model_name,
        mr.model_version,
        tr.training_run_id,
        tr.train_batch_name,
        tr.validation_batch_name,
        tr.test_batch_name,
        tr.rows_trained,
        tr.rows_validated,
        tr.rows_tested
),
scoring_summary as (
    select
        tdp.model_name,
        tdp.model_version,
        tdp.training_run_id,
        tdp.batch_name as scored_batch_name,
        tdp.rows_scored,
        tdp.overall_actual_ctr,
        tdp.top_decile_actual_ctr,
        tdp.top_decile_lift_vs_batch_ctr,
        tdp.top_decile_avg_predicted_ctr
    from ml.top_decile_performance tdp
)
select
    mp.model_name,
    mp.model_version,
    mp.training_run_id,
    mp.train_batch_name,
    mp.validation_batch_name,
    mp.test_batch_name,
    mp.rows_trained,
    mp.rows_validated,
    mp.rows_tested,
    case
        when mp.train_batch_name = 'criteo_1m_ml_canonical_batch' then 'large_scale'
        when mp.rows_trained >= 500000 then 'large_scale'
        when mp.rows_trained >= 100000 then 'medium_scale'
        else 'small_scale'
    end as training_scale_band,
    case
        when mp.train_batch_name = 'criteo_1m_ml_canonical_batch' then true
        else false
    end as is_canonical_1m_run,
    case
        when mp.train_batch_name = 'criteo_1m_ml_canonical_batch'
            and mp.model_name = 'ctr_logistic_regression'
            then 'Canonical 1M logistic baseline.'
        when mp.rows_trained < 1000
            then 'Small-batch prototype; useful for experimentation, not the main benchmark claim.'
        else 'Experimental comparison run.'
    end as comparison_note,
    mp.train_roc_auc,
    mp.train_pr_auc,
    mp.train_log_loss,
    mp.validation_roc_auc,
    mp.validation_pr_auc,
    mp.validation_log_loss,
    mp.validation_precision_at_10pct,
    mp.validation_lift_at_10pct,
    mp.test_roc_auc,
    mp.test_pr_auc,
    mp.test_log_loss,
    mp.test_precision_at_10pct,
    mp.test_lift_at_10pct,
    ss.scored_batch_name,
    ss.rows_scored,
    ss.overall_actual_ctr,
    ss.top_decile_actual_ctr,
    ss.top_decile_lift_vs_batch_ctr,
    ss.top_decile_avg_predicted_ctr
from metric_pivot mp
left join scoring_summary ss
  on ss.model_name = mp.model_name
 and ss.model_version = mp.model_version
 and ss.training_run_id = mp.training_run_id
order by
    is_canonical_1m_run desc,
    rows_trained desc,
    model_name,
    training_run_id;
