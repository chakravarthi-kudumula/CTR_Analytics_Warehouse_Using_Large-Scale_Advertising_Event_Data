alter table ml.model_registry
    add column if not exists is_active_canonical boolean not null default false;

alter table ml.model_registry
    add column if not exists canonical_promoted_at timestamptz;

alter table ml.model_registry
    add column if not exists canonical_promotion_note text;

create table if not exists ml.model_promotion_audit (
    promotion_audit_id bigint generated always as identity primary key,
    model_name text not null,
    candidate_model_id bigint references ml.model_registry (model_id),
    candidate_training_run_id bigint references ml.training_runs (training_run_id),
    previous_model_id bigint references ml.model_registry (model_id),
    previous_training_run_id bigint references ml.training_runs (training_run_id),
    promotion_decision text not null,
    decision_reason text not null,
    candidate_validation_roc_auc numeric(12, 6),
    candidate_validation_pr_auc numeric(12, 6),
    candidate_validation_lift_at_10pct numeric(12, 6),
    previous_validation_roc_auc numeric(12, 6),
    previous_validation_pr_auc numeric(12, 6),
    previous_validation_lift_at_10pct numeric(12, 6),
    created_at timestamptz not null default now()
);

create index if not exists idx_model_promotion_audit_model
    on ml.model_promotion_audit (model_name, created_at desc);

create unique index if not exists idx_model_registry_active_canonical_unique
    on ml.model_registry (model_name)
    where is_active_canonical = true;

create or replace view ml.active_canonical_model as
with active_model as (
    select
        mr.model_id,
        mr.model_name,
        mr.model_version,
        mr.is_active_canonical,
        mr.canonical_promoted_at,
        mr.canonical_promotion_note
    from ml.model_registry mr
    where mr.is_active_canonical = true
)
select
    am.model_id,
    am.model_name,
    am.model_version,
    mcs.training_run_id,
    mcs.train_batch_name,
    mcs.rows_trained,
    mcs.validation_roc_auc,
    mcs.validation_pr_auc,
    mcs.validation_log_loss,
    mcs.validation_precision_at_10pct,
    mcs.validation_lift_at_10pct,
    mcs.test_roc_auc,
    mcs.test_pr_auc,
    mcs.test_log_loss,
    mcs.top_decile_lift_vs_batch_ctr,
    am.canonical_promoted_at,
    am.canonical_promotion_note
from active_model am
left join ml.model_comparison_summary mcs
  on mcs.model_name = am.model_name
 and mcs.model_version = am.model_version
 and (
      mcs.training_run_id = (
          select max(training_run_id)
          from ml.training_runs tr
          where tr.model_id = am.model_id
            and tr.run_status = 'SUCCESS'
      )
 );
