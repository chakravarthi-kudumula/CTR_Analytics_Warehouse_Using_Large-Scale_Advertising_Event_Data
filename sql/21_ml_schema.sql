create schema if not exists ml;

create table if not exists ml.model_registry (
    model_id bigint generated always as identity primary key,
    model_name text not null,
    model_version text not null,
    model_type text not null,
    framework_name text not null,
    feature_source text not null,
    target_column text not null default 'label',
    artifact_path text,
    hyperparameters jsonb,
    feature_columns jsonb,
    training_start_date date,
    training_end_date date,
    model_status text not null default 'REGISTERED',
    notes text,
    registered_at timestamptz not null default now(),
    unique (model_name, model_version)
);

create index if not exists idx_model_registry_status
    on ml.model_registry (model_status, registered_at desc);

create table if not exists ml.training_runs (
    training_run_id bigint generated always as identity primary key,
    model_id bigint not null references ml.model_registry (model_id),
    train_batch_name text not null,
    validation_batch_name text,
    test_batch_name text,
    dataset_split_strategy text not null,
    rows_trained bigint,
    rows_validated bigint,
    rows_tested bigint,
    training_parameters jsonb,
    run_status text not null default 'REGISTERED',
    started_at timestamptz,
    completed_at timestamptz,
    notes text,
    created_at timestamptz not null default now()
);

create index if not exists idx_training_runs_model
    on ml.training_runs (model_id, created_at desc);

create index if not exists idx_training_runs_status
    on ml.training_runs (run_status, created_at desc);

create table if not exists ml.model_metrics (
    metric_id bigint generated always as identity primary key,
    training_run_id bigint not null references ml.training_runs (training_run_id),
    dataset_split text not null,
    roc_auc numeric(12, 6),
    pr_auc numeric(12, 6),
    log_loss numeric(12, 6),
    brier_score numeric(12, 6),
    precision_at_10pct numeric(12, 6),
    lift_at_10pct numeric(12, 6),
    recorded_at timestamptz not null default now(),
    notes text,
    unique (training_run_id, dataset_split),
    check (roc_auc is null or (roc_auc >= 0 and roc_auc <= 1)),
    check (pr_auc is null or (pr_auc >= 0 and pr_auc <= 1)),
    check (log_loss is null or log_loss >= 0),
    check (brier_score is null or (brier_score >= 0 and brier_score <= 1)),
    check (precision_at_10pct is null or (precision_at_10pct >= 0 and precision_at_10pct <= 1))
);

create index if not exists idx_model_metrics_run
    on ml.model_metrics (training_run_id, recorded_at desc);

create table if not exists ml.prediction_scores (
    prediction_id bigint generated always as identity primary key,
    raw_event_id bigint not null references warehouse.fact_ad_events (raw_event_id),
    batch_id bigint not null references ops.batch_registry (batch_id),
    model_id bigint not null references ml.model_registry (model_id),
    training_run_id bigint references ml.training_runs (training_run_id),
    model_name text not null,
    model_version text not null,
    predicted_ctr numeric(12, 6) not null,
    score_decile integer,
    is_top_decile boolean not null default false,
    actual_click integer,
    scored_at timestamptz not null default now(),
    notes text,
    unique (raw_event_id, batch_id, model_name, model_version),
    check (predicted_ctr >= 0 and predicted_ctr <= 1),
    check (score_decile is null or (score_decile >= 1 and score_decile <= 10)),
    check (actual_click is null or actual_click in (0, 1))
);

create index if not exists idx_prediction_scores_batch_model
    on ml.prediction_scores (batch_id, model_name, model_version, scored_at desc);

create index if not exists idx_prediction_scores_top_decile
    on ml.prediction_scores (batch_id, is_top_decile, scored_at desc);

create or replace view ml.latest_training_metrics as
with ranked_metrics as (
    select
        mr.model_name,
        mr.model_version,
        tr.training_run_id,
        tr.run_status,
        mm.dataset_split,
        mm.roc_auc,
        mm.pr_auc,
        mm.log_loss,
        mm.brier_score,
        mm.precision_at_10pct,
        mm.lift_at_10pct,
        mm.recorded_at,
        row_number() over (
            partition by mr.model_name, mr.model_version, mm.dataset_split
            order by mm.recorded_at desc, mm.metric_id desc
        ) as metric_rank
    from ml.model_registry mr
    join ml.training_runs tr
      on tr.model_id = mr.model_id
    join ml.model_metrics mm
      on mm.training_run_id = tr.training_run_id
)
select
    model_name,
    model_version,
    training_run_id,
    run_status,
    dataset_split,
    roc_auc,
    pr_auc,
    log_loss,
    brier_score,
    precision_at_10pct,
    lift_at_10pct,
    recorded_at
from ranked_metrics
where metric_rank = 1;
