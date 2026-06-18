create table if not exists warehouse.dim_event_day (
    event_day_key bigint generated always as identity primary key,
    event_batch text not null,
    event_day_number integer not null,
    day_label text not null,
    unique (event_batch, event_day_number)
);

create table if not exists warehouse.dim_numeric_bucket (
    numeric_bucket_key bigint generated always as identity primary key,
    feature_name text not null,
    bucket_code text not null,
    bucket_label text not null,
    bucket_order integer not null,
    lower_bound bigint,
    upper_bound bigint,
    unique (feature_name, bucket_code)
);

create table if not exists warehouse.dim_categorical_value (
    categorical_value_key bigint generated always as identity primary key,
    feature_name text not null,
    feature_value text not null,
    is_unknown boolean not null,
    unique (feature_name, feature_value)
);

create table if not exists warehouse.fact_ad_events (
    fact_event_key bigint generated always as identity primary key,
    raw_event_id bigint not null unique,
    batch_id bigint references ops.batch_registry (batch_id),
    event_day_key bigint not null references warehouse.dim_event_day (event_day_key),
    label integer not null,
    click_flag integer not null,
    impression_count integer not null,
    click_count integer not null,
    missing_numeric_count integer not null,
    missing_categorical_count integer not null,
    event_batch text not null,
    source_file text not null,
    ingested_at timestamptz not null
);

alter table warehouse.fact_ad_events
    add column if not exists batch_id bigint;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'fact_ad_events_batch_id_fkey'
    ) then
        alter table warehouse.fact_ad_events
            add constraint fact_ad_events_batch_id_fkey
            foreign key (batch_id) references ops.batch_registry (batch_id);
    end if;
end $$;

create table if not exists warehouse.bridge_event_numeric_bucket (
    fact_event_key bigint not null references warehouse.fact_ad_events (fact_event_key),
    feature_name text not null,
    numeric_bucket_key bigint not null references warehouse.dim_numeric_bucket (numeric_bucket_key),
    feature_value integer not null,
    primary key (fact_event_key, feature_name)
);

create table if not exists warehouse.bridge_event_categorical_value (
    fact_event_key bigint not null references warehouse.fact_ad_events (fact_event_key),
    feature_name text not null,
    categorical_value_key bigint not null references warehouse.dim_categorical_value (categorical_value_key),
    feature_value text not null,
    primary key (fact_event_key, feature_name)
);

create or replace view warehouse.v_event_day_summary as
select
    d.event_day_key,
    d.event_batch,
    d.event_day_number,
    d.day_label,
    count(*) as impressions,
    sum(f.click_count) as clicks,
    round(sum(f.click_count)::numeric / nullif(sum(f.impression_count), 0), 6) as ctr,
    avg(f.missing_numeric_count::numeric) as avg_missing_numeric_count,
    avg(f.missing_categorical_count::numeric) as avg_missing_categorical_count
from warehouse.fact_ad_events f
join warehouse.dim_event_day d
  on d.event_day_key = f.event_day_key
group by
    d.event_day_key,
    d.event_batch,
    d.event_day_number,
    d.day_label;

create or replace view warehouse.v_data_quality_summary as
select
    event_batch,
    source_file,
    count(*) as impressions,
    sum(click_count) as clicks,
    round(sum(click_count)::numeric / nullif(sum(impression_count), 0), 6) as ctr,
    avg(missing_numeric_count::numeric) as avg_missing_numeric_count,
    avg(missing_categorical_count::numeric) as avg_missing_categorical_count,
    max(missing_numeric_count) as max_missing_numeric_count,
    max(missing_categorical_count) as max_missing_categorical_count
from warehouse.fact_ad_events
group by event_batch, source_file;

create index if not exists idx_fact_ad_events_batch_id
    on warehouse.fact_ad_events (batch_id);
