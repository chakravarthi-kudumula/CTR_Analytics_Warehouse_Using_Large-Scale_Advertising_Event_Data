do $$
declare
    object_name text;
begin
    foreach object_name in array array[
        'feature_interaction_ranked',
        'feature_ctr_lift_ranked',
        'low_performing_segments',
        'high_value_segments',
        'event_day_ctr_rolling',
        'feature_ctr_ranked'
    ]
    loop
        if exists (
            select 1
            from pg_matviews
            where schemaname = 'marts'
              and matviewname = object_name
        ) then
            execute format('drop materialized view if exists marts.%I', object_name);
        elsif exists (
            select 1
            from pg_views
            where schemaname = 'marts'
              and viewname = object_name
        ) then
            execute format('drop view if exists marts.%I', object_name);
        end if;
    end loop;
end $$;
drop view if exists marts.top_feature_interaction_ctr;
drop view if exists marts.top_feature_ctr_summary;
drop view if exists marts.top_numeric_bucket_ctr;
drop view if exists marts.feature_interaction_ctr;
drop view if exists marts.missing_value_impact;
drop view if exists marts.feature_ctr_summary;
drop view if exists marts.numeric_bucket_ctr;
drop view if exists marts.event_day_ctr_trend;
drop view if exists marts.overall_ctr_summary;

create table if not exists marts.batch_overall_ctr_summary (
    batch_id bigint primary key references ops.batch_registry (batch_id),
    source_file text not null,
    event_batch text not null,
    impressions bigint not null,
    clicks bigint not null,
    ctr numeric(18, 6) not null,
    avg_missing_numeric_count numeric(18, 6) not null,
    avg_missing_categorical_count numeric(18, 6) not null,
    recorded_at timestamptz not null default now()
);

create table if not exists marts.batch_event_day_ctr_trend (
    batch_id bigint not null references ops.batch_registry (batch_id),
    event_batch text not null,
    event_day_number integer not null,
    day_label text not null,
    impressions bigint not null,
    clicks bigint not null,
    ctr numeric(18, 6) not null,
    recorded_at timestamptz not null default now(),
    primary key (batch_id, event_batch, event_day_number)
);

create table if not exists marts.batch_numeric_bucket_ctr (
    batch_id bigint not null references ops.batch_registry (batch_id),
    feature_name text not null,
    bucket_code text not null,
    bucket_label text not null,
    bucket_order integer not null,
    impressions bigint not null,
    clicks bigint not null,
    ctr numeric(18, 6) not null,
    recorded_at timestamptz not null default now(),
    primary key (batch_id, feature_name, bucket_code)
);

create table if not exists marts.batch_feature_ctr_summary (
    batch_id bigint not null references ops.batch_registry (batch_id),
    feature_name text not null,
    feature_value text not null,
    is_unknown boolean not null,
    impressions bigint not null,
    clicks bigint not null,
    ctr numeric(18, 6) not null,
    recorded_at timestamptz not null default now(),
    primary key (batch_id, feature_name, feature_value)
);

create table if not exists marts.batch_missing_value_impact (
    batch_id bigint not null references ops.batch_registry (batch_id),
    missing_type text not null,
    missing_count integer not null,
    impressions bigint not null,
    clicks bigint not null,
    ctr numeric(18, 6) not null,
    recorded_at timestamptz not null default now(),
    primary key (batch_id, missing_type, missing_count)
);

create table if not exists marts.batch_feature_interaction_ctr (
    batch_id bigint not null references ops.batch_registry (batch_id),
    interaction_name text not null,
    left_feature_name text not null,
    left_feature_value text not null,
    right_feature_name text not null,
    right_feature_value text not null,
    impressions bigint not null,
    clicks bigint not null,
    ctr numeric(18, 6) not null,
    recorded_at timestamptz not null default now(),
    primary key (batch_id, interaction_name, left_feature_value, right_feature_value)
);

create index if not exists idx_batch_numeric_bucket_feature
    on marts.batch_numeric_bucket_ctr (feature_name, bucket_code);

create index if not exists idx_batch_feature_ctr_feature
    on marts.batch_feature_ctr_summary (feature_name, feature_value);

create index if not exists idx_batch_feature_interaction_name
    on marts.batch_feature_interaction_ctr (interaction_name);

create or replace view marts.overall_ctr_summary as
with overall as (
    select
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        round(sum(clicks)::numeric / nullif(sum(impressions), 0), 6) as ctr,
        round(sum(avg_missing_numeric_count * impressions)::numeric / nullif(sum(impressions), 0), 6) as avg_missing_numeric_count,
        round(sum(avg_missing_categorical_count * impressions)::numeric / nullif(sum(impressions), 0), 6) as avg_missing_categorical_count
    from marts.batch_overall_ctr_summary
)
select
    'all_loaded_batches'::text as scope_name,
    impressions,
    clicks,
    ctr,
    avg_missing_numeric_count,
    avg_missing_categorical_count
from overall;

create or replace view marts.event_day_ctr_trend as
with aggregated as (
    select
        event_batch,
        event_day_number,
        day_label,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        round(sum(clicks)::numeric / nullif(sum(impressions), 0), 6) as ctr
    from marts.batch_event_day_ctr_trend
    group by event_batch, event_day_number, day_label
)
select
    event_batch,
    event_day_number,
    day_label,
    impressions,
    clicks,
    ctr,
    dense_rank() over (partition by event_batch order by ctr desc) as ctr_rank_in_batch,
    dense_rank() over (partition by event_batch order by impressions desc) as impression_rank_in_batch
from aggregated;

create or replace view marts.numeric_bucket_ctr as
with overall as (
    select ctr from marts.overall_ctr_summary
),
aggregated as (
    select
        feature_name,
        bucket_code,
        bucket_label,
        bucket_order,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        round(sum(clicks)::numeric / nullif(sum(impressions), 0), 6) as ctr
    from marts.batch_numeric_bucket_ctr
    group by feature_name, bucket_code, bucket_label, bucket_order
)
select
    a.feature_name,
    a.bucket_code,
    a.bucket_label,
    a.bucket_order,
    a.impressions,
    a.clicks,
    a.ctr,
    o.ctr as overall_ctr,
    round(a.ctr - o.ctr, 6) as ctr_lift_vs_overall,
    case
        when a.impressions >= 10000 then 'high'
        when a.impressions >= 1000 then 'medium'
        else 'low'
    end as volume_band,
    dense_rank() over (partition by a.feature_name order by a.ctr desc) as ctr_rank_in_feature,
    dense_rank() over (partition by a.feature_name order by a.impressions desc) as impression_rank_in_feature
from aggregated a
cross join overall o;

create or replace view marts.feature_ctr_summary as
with overall as (
    select ctr from marts.overall_ctr_summary
),
aggregated as (
    select
        feature_name,
        feature_value,
        bool_or(is_unknown) as is_unknown,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        round(sum(clicks)::numeric / nullif(sum(impressions), 0), 6) as ctr
    from marts.batch_feature_ctr_summary
    group by feature_name, feature_value
)
select
    a.feature_name,
    a.feature_value,
    a.is_unknown,
    a.impressions,
    a.clicks,
    a.ctr,
    o.ctr as overall_ctr,
    round(a.ctr - o.ctr, 6) as ctr_lift_vs_overall,
    case
        when a.impressions >= 10000 then 'high'
        when a.impressions >= 5000 then 'medium'
        else 'low'
    end as volume_band,
    dense_rank() over (partition by a.feature_name order by a.ctr desc, a.impressions desc) as ctr_rank_in_feature,
    dense_rank() over (partition by a.feature_name order by a.impressions desc, a.ctr desc) as impression_rank_in_feature
from aggregated a
cross join overall o;

create or replace view marts.missing_value_impact as
with overall as (
    select ctr from marts.overall_ctr_summary
),
aggregated as (
    select
        missing_type,
        missing_count,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        round(sum(clicks)::numeric / nullif(sum(impressions), 0), 6) as ctr
    from marts.batch_missing_value_impact
    group by missing_type, missing_count
)
select
    a.missing_type,
    a.missing_count,
    a.impressions,
    a.clicks,
    a.ctr,
    o.ctr as overall_ctr,
    round(a.ctr - o.ctr, 6) as ctr_lift_vs_overall
from aggregated a
cross join overall o
order by a.missing_type, a.missing_count;

create or replace view marts.feature_interaction_ctr as
with overall as (
    select ctr from marts.overall_ctr_summary
),
aggregated as (
    select
        interaction_name,
        left_feature_name,
        left_feature_value,
        right_feature_name,
        right_feature_value,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        round(sum(clicks)::numeric / nullif(sum(impressions), 0), 6) as ctr
    from marts.batch_feature_interaction_ctr
    group by interaction_name, left_feature_name, left_feature_value, right_feature_name, right_feature_value
)
select
    a.interaction_name,
    a.left_feature_name,
    a.left_feature_value,
    a.right_feature_name,
    a.right_feature_value,
    a.impressions,
    a.clicks,
    a.ctr,
    o.ctr as overall_ctr,
    round(a.ctr - o.ctr, 6) as ctr_lift_vs_overall,
    case
        when a.impressions >= 10000 then 'high'
        when a.impressions >= 5000 then 'medium'
        else 'low'
    end as volume_band,
    dense_rank() over (partition by a.interaction_name order by a.ctr desc, a.impressions desc) as ctr_rank_in_interaction
from aggregated a
cross join overall o;

create or replace view marts.top_numeric_bucket_ctr as
select
    feature_name,
    bucket_label,
    impressions,
    clicks,
    ctr,
    ctr_lift_vs_overall,
    volume_band,
    ctr_rank_in_feature,
    impression_rank_in_feature
from marts.numeric_bucket_ctr
where impressions >= 1000
  and ctr_rank_in_feature <= 5;

create or replace view marts.top_feature_ctr_summary as
select
    feature_name,
    feature_value,
    impressions,
    clicks,
    ctr,
    ctr_lift_vs_overall,
    volume_band,
    ctr_rank_in_feature,
    impression_rank_in_feature
from marts.feature_ctr_summary
where impressions >= 5000
  and ctr_rank_in_feature <= 5;

create or replace view marts.top_feature_interaction_ctr as
select
    interaction_name,
    left_feature_name,
    left_feature_value,
    right_feature_name,
    right_feature_value,
    impressions,
    clicks,
    ctr,
    ctr_lift_vs_overall,
    volume_band,
    ctr_rank_in_interaction
from marts.feature_interaction_ctr
where impressions >= 5000
  and ctr_rank_in_interaction <= 5;
