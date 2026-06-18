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

create table if not exists marts.batch_feature_ctr_ranked (
    batch_id bigint not null references ops.batch_registry (batch_id),
    feature_name text not null,
    feature_value text not null,
    is_unknown boolean not null,
    impressions bigint not null,
    clicks bigint not null,
    ctr numeric(18, 6) not null,
    overall_ctr numeric(18, 6) not null,
    ctr_lift_vs_overall numeric(18, 6) not null,
    volume_band text not null,
    row_number_in_feature integer not null,
    dense_rank_in_feature integer not null,
    percent_rank_in_feature numeric(18, 6) not null,
    ctr_quartile_in_feature integer not null,
    recorded_at timestamptz not null default now(),
    primary key (batch_id, feature_name, feature_value)
);

create table if not exists marts.batch_event_day_ctr_rolling (
    batch_id bigint not null references ops.batch_registry (batch_id),
    event_batch text not null,
    event_day_number integer not null,
    day_label text not null,
    impressions bigint not null,
    clicks bigint not null,
    ctr numeric(18, 6) not null,
    previous_day_ctr numeric(18, 6),
    ctr_change_from_previous_day numeric(18, 6),
    cumulative_impressions bigint not null,
    cumulative_clicks bigint not null,
    cumulative_ctr numeric(18, 6) not null,
    rolling_3_day_avg_ctr numeric(18, 6) not null,
    rolling_3_day_weighted_ctr numeric(18, 6) not null,
    recorded_at timestamptz not null default now(),
    primary key (batch_id, event_batch, event_day_number)
);

create table if not exists marts.batch_high_value_segments (
    batch_id bigint not null references ops.batch_registry (batch_id),
    segment_type text not null,
    segment_name text not null,
    segment_value text not null,
    impressions bigint not null,
    clicks bigint not null,
    ctr numeric(18, 6) not null,
    overall_ctr numeric(18, 6) not null,
    ctr_lift_vs_overall numeric(18, 6) not null,
    volume_band text not null,
    segment_rank_by_lift integer not null,
    segment_rank_by_clicks integer not null,
    recorded_at timestamptz not null default now(),
    primary key (batch_id, segment_type, segment_name, segment_value)
);

create table if not exists marts.batch_low_performing_segments (
    batch_id bigint not null references ops.batch_registry (batch_id),
    segment_type text not null,
    segment_name text not null,
    segment_value text not null,
    impressions bigint not null,
    clicks bigint not null,
    ctr numeric(18, 6) not null,
    overall_ctr numeric(18, 6) not null,
    ctr_lift_vs_overall numeric(18, 6) not null,
    volume_band text not null,
    segment_rank_by_underperformance integer not null,
    segment_rank_by_volume integer not null,
    recorded_at timestamptz not null default now(),
    primary key (batch_id, segment_type, segment_name, segment_value)
);

create table if not exists marts.batch_feature_ctr_lift_ranked (
    batch_id bigint not null references ops.batch_registry (batch_id),
    feature_family text not null,
    feature_name text not null,
    segment_value text not null,
    impressions bigint not null,
    clicks bigint not null,
    ctr numeric(18, 6) not null,
    overall_ctr numeric(18, 6) not null,
    ctr_lift_vs_overall numeric(18, 6) not null,
    volume_band text not null,
    row_number_by_lift integer not null,
    dense_rank_by_lift integer not null,
    lift_quintile integer not null,
    recorded_at timestamptz not null default now(),
    primary key (batch_id, feature_family, feature_name, segment_value)
);

create table if not exists marts.batch_feature_interaction_ranked (
    batch_id bigint not null references ops.batch_registry (batch_id),
    interaction_name text not null,
    left_feature_name text not null,
    left_feature_value text not null,
    right_feature_name text not null,
    right_feature_value text not null,
    impressions bigint not null,
    clicks bigint not null,
    ctr numeric(18, 6) not null,
    overall_ctr numeric(18, 6) not null,
    ctr_lift_vs_overall numeric(18, 6) not null,
    volume_band text not null,
    row_number_in_interaction integer not null,
    dense_rank_in_interaction integer not null,
    percent_rank_in_interaction numeric(18, 6) not null,
    cumulative_clicks_by_interaction bigint not null,
    recorded_at timestamptz not null default now(),
    primary key (batch_id, interaction_name, left_feature_value, right_feature_value)
);

create index if not exists idx_batch_feature_ctr_ranked_feature
    on marts.batch_feature_ctr_ranked (feature_name, dense_rank_in_feature);

create index if not exists idx_batch_event_day_ctr_rolling_batch
    on marts.batch_event_day_ctr_rolling (event_batch, event_day_number);

create index if not exists idx_batch_high_value_segments_type
    on marts.batch_high_value_segments (segment_type, segment_rank_by_lift);

create index if not exists idx_batch_low_performing_segments_type
    on marts.batch_low_performing_segments (segment_type, segment_rank_by_underperformance);

create index if not exists idx_batch_feature_ctr_lift_ranked_feature
    on marts.batch_feature_ctr_lift_ranked (feature_family, feature_name, dense_rank_by_lift);

create index if not exists idx_batch_feature_interaction_ranked_name
    on marts.batch_feature_interaction_ranked (interaction_name, dense_rank_in_interaction);

create or replace view marts.feature_ctr_ranked as
select
    batch_id,
    feature_name,
    feature_value,
    is_unknown,
    impressions,
    clicks,
    ctr,
    overall_ctr,
    ctr_lift_vs_overall,
    volume_band,
    row_number_in_feature,
    dense_rank_in_feature,
    percent_rank_in_feature,
    ctr_quartile_in_feature
from marts.batch_feature_ctr_ranked;

create or replace view marts.event_day_ctr_rolling as
select
    batch_id,
    event_batch,
    event_day_number,
    day_label,
    impressions,
    clicks,
    ctr,
    previous_day_ctr,
    ctr_change_from_previous_day,
    cumulative_impressions,
    cumulative_clicks,
    cumulative_ctr,
    rolling_3_day_avg_ctr,
    rolling_3_day_weighted_ctr
from marts.batch_event_day_ctr_rolling;

create or replace view marts.high_value_segments as
select
    batch_id,
    segment_type,
    segment_name,
    segment_value,
    impressions,
    clicks,
    ctr,
    overall_ctr,
    ctr_lift_vs_overall,
    volume_band,
    segment_rank_by_lift,
    segment_rank_by_clicks
from marts.batch_high_value_segments;

create or replace view marts.low_performing_segments as
select
    batch_id,
    segment_type,
    segment_name,
    segment_value,
    impressions,
    clicks,
    ctr,
    overall_ctr,
    ctr_lift_vs_overall,
    volume_band,
    segment_rank_by_underperformance,
    segment_rank_by_volume
from marts.batch_low_performing_segments;

create or replace view marts.feature_ctr_lift_ranked as
select
    batch_id,
    feature_family,
    feature_name,
    segment_value,
    impressions,
    clicks,
    ctr,
    overall_ctr,
    ctr_lift_vs_overall,
    volume_band,
    row_number_by_lift,
    dense_rank_by_lift,
    lift_quintile
from marts.batch_feature_ctr_lift_ranked;

create or replace view marts.feature_interaction_ranked as
select
    batch_id,
    interaction_name,
    left_feature_name,
    left_feature_value,
    right_feature_name,
    right_feature_value,
    impressions,
    clicks,
    ctr,
    overall_ctr,
    ctr_lift_vs_overall,
    volume_band,
    row_number_in_interaction,
    dense_rank_in_interaction,
    percent_rank_in_interaction,
    cumulative_clicks_by_interaction
from marts.batch_feature_interaction_ranked;
