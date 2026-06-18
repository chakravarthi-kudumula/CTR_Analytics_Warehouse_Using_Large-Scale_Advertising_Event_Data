create table if not exists raw.criteo_events (
    event_id bigint generated always as identity primary key,
    batch_id bigint references ops.batch_registry (batch_id),
    label integer,
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
    source_file text not null,
    ingested_at timestamptz not null default now()
);

alter table raw.criteo_events
    add column if not exists batch_id bigint;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'raw_criteo_events_batch_id_fkey'
    ) then
        alter table raw.criteo_events
            add constraint raw_criteo_events_batch_id_fkey
            foreign key (batch_id) references ops.batch_registry (batch_id);
    end if;
end $$;

create index if not exists idx_raw_criteo_events_source_file
    on raw.criteo_events (source_file);

create index if not exists idx_raw_criteo_events_batch_id
    on raw.criteo_events (batch_id);
