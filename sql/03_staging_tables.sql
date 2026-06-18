create table if not exists staging.stg_criteo_events (
    raw_event_id bigint primary key,
    batch_id bigint references ops.batch_registry (batch_id),
    label integer not null,
    i1 integer,
    i2 integer,
    i3 integer,
    i4 integer,
    i5 integer,
    i6 integer,
    i7 integer,
    i8 integer,
    i9 integer,
    i10 integer,
    i11 integer,
    i12 integer,
    i13 integer,
    c1 text,
    c2 text,
    c3 text,
    c4 text,
    c5 text,
    c6 text,
    c7 text,
    c8 text,
    c9 text,
    c10 text,
    c11 text,
    c12 text,
    c13 text,
    c14 text,
    c15 text,
    c16 text,
    c17 text,
    c18 text,
    c19 text,
    c20 text,
    c21 text,
    c22 text,
    c23 text,
    c24 text,
    c25 text,
    c26 text,
    event_day_number integer not null,
    event_batch text not null,
    click_flag integer not null,
    impression_count integer not null,
    click_count integer not null,
    missing_numeric_count integer not null,
    missing_categorical_count integer not null,
    source_file text not null,
    ingested_at timestamptz not null
);

alter table staging.stg_criteo_events
    add column if not exists batch_id bigint;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'stg_criteo_events_batch_id_fkey'
    ) then
        alter table staging.stg_criteo_events
            add constraint stg_criteo_events_batch_id_fkey
            foreign key (batch_id) references ops.batch_registry (batch_id);
    end if;
end $$;

create index if not exists idx_stg_criteo_events_batch
    on staging.stg_criteo_events (event_batch);

create index if not exists idx_stg_criteo_events_day_number
    on staging.stg_criteo_events (event_day_number);

create index if not exists idx_stg_criteo_events_click_flag
    on staging.stg_criteo_events (click_flag);

create index if not exists idx_stg_criteo_events_source_file
    on staging.stg_criteo_events (source_file);

create index if not exists idx_stg_criteo_events_batch_id
    on staging.stg_criteo_events (batch_id);
