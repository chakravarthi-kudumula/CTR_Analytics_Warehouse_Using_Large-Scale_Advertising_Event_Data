create table if not exists ml.model_feature_importance (
    importance_id bigint generated always as identity primary key,
    model_id bigint not null references ml.model_registry (model_id),
    training_run_id bigint not null references ml.training_runs (training_run_id),
    model_name text not null,
    model_version text not null,
    feature_name text not null,
    feature_group text not null,
    importance_value numeric(18, 8) not null,
    abs_importance_value numeric(18, 8) not null,
    relative_importance_pct numeric(12, 6),
    importance_direction text not null,
    importance_rank integer not null,
    recorded_at timestamptz not null default now(),
    notes text,
    unique (training_run_id, feature_name)
);

create index if not exists idx_model_feature_importance_run
    on ml.model_feature_importance (training_run_id, importance_rank);

create index if not exists idx_model_feature_importance_model
    on ml.model_feature_importance (model_name, model_version, abs_importance_value desc);


create or replace view ml.latest_model_feature_importance as
with latest_runs as (
    select distinct on (mfi.model_name, mfi.model_version)
        mfi.model_name,
        mfi.model_version,
        mfi.training_run_id
    from ml.model_feature_importance mfi
    order by mfi.model_name, mfi.model_version, mfi.training_run_id desc
)
select
    mfi.model_id,
    mfi.training_run_id,
    mfi.model_name,
    mfi.model_version,
    mfi.feature_name,
    mfi.feature_group,
    mfi.importance_value,
    mfi.abs_importance_value,
    mfi.relative_importance_pct,
    mfi.importance_direction,
    mfi.importance_rank,
    mfi.recorded_at,
    mfi.notes
from ml.model_feature_importance mfi
join latest_runs lr
  on lr.model_name = mfi.model_name
 and lr.model_version = mfi.model_version
 and lr.training_run_id = mfi.training_run_id;


create or replace view ml.latest_feature_group_importance as
select
    lmfi.model_name,
    lmfi.model_version,
    lmfi.training_run_id,
    lmfi.feature_group,
    count(*) as features_in_group,
    sum(lmfi.abs_importance_value)::numeric(18, 8) as total_abs_importance,
    avg(lmfi.abs_importance_value)::numeric(18, 8) as avg_abs_importance,
    max(lmfi.abs_importance_value)::numeric(18, 8) as max_abs_importance,
    sum(lmfi.relative_importance_pct)::numeric(12, 6) as group_relative_importance_pct
from ml.latest_model_feature_importance lmfi
group by
    lmfi.model_name,
    lmfi.model_version,
    lmfi.training_run_id,
    lmfi.feature_group;
