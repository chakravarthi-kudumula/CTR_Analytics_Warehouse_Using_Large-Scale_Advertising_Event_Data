select
    (select count(*) from raw.criteo_events) as raw_row_count,
    (select count(*) from staging.stg_criteo_events) as staging_row_count;

select
    count(*) as duplicate_raw_event_id_rows
from (
    select raw_event_id
    from staging.stg_criteo_events
    group by raw_event_id
    having count(*) > 1
) duplicates;

select
    event_batch,
    event_day_number,
    count(*) as row_count,
    sum(click_count) as click_count,
    round(sum(click_count)::numeric / nullif(sum(impression_count), 0), 6) as ctr
from staging.stg_criteo_events
group by event_batch, event_day_number
order by event_batch, event_day_number;

select
    sum(case when impression_count <> 1 then 1 else 0 end) as invalid_impression_rows,
    sum(case when click_flag not in (0, 1) then 1 else 0 end) as invalid_click_flag_rows,
    sum(case when click_count not in (0, 1) then 1 else 0 end) as invalid_click_count_rows,
    sum(case when raw_event_id is null then 1 else 0 end) as null_raw_event_id_rows,
    sum(case when label is null then 1 else 0 end) as null_label_rows,
    sum(case when event_day_number is null then 1 else 0 end) as null_event_day_number_rows,
    sum(case when event_batch is null then 1 else 0 end) as null_event_batch_rows,
    sum(case when source_file is null then 1 else 0 end) as null_source_file_rows,
    sum(case when ingested_at is null then 1 else 0 end) as null_ingested_at_rows
from staging.stg_criteo_events;

select
    sum(case when i1 is null then 1 else 0 end) as null_i1_rows,
    sum(case when i2 is null then 1 else 0 end) as null_i2_rows,
    sum(case when i3 is null then 1 else 0 end) as null_i3_rows,
    sum(case when i4 is null then 1 else 0 end) as null_i4_rows,
    sum(case when i5 is null then 1 else 0 end) as null_i5_rows,
    sum(case when i6 is null then 1 else 0 end) as null_i6_rows,
    sum(case when i7 is null then 1 else 0 end) as null_i7_rows,
    sum(case when i8 is null then 1 else 0 end) as null_i8_rows,
    sum(case when i9 is null then 1 else 0 end) as null_i9_rows,
    sum(case when i10 is null then 1 else 0 end) as null_i10_rows,
    sum(case when i11 is null then 1 else 0 end) as null_i11_rows,
    sum(case when i12 is null then 1 else 0 end) as null_i12_rows,
    sum(case when i13 is null then 1 else 0 end) as null_i13_rows
from staging.stg_criteo_events;

select
    sum(case when c1 is null then 1 else 0 end) as null_c1_rows,
    sum(case when c2 is null then 1 else 0 end) as null_c2_rows,
    sum(case when c3 is null then 1 else 0 end) as null_c3_rows,
    sum(case when c4 is null then 1 else 0 end) as null_c4_rows,
    sum(case when c5 is null then 1 else 0 end) as null_c5_rows,
    sum(case when c6 is null then 1 else 0 end) as null_c6_rows,
    sum(case when c7 is null then 1 else 0 end) as null_c7_rows,
    sum(case when c8 is null then 1 else 0 end) as null_c8_rows,
    sum(case when c9 is null then 1 else 0 end) as null_c9_rows,
    sum(case when c10 is null then 1 else 0 end) as null_c10_rows,
    sum(case when c11 is null then 1 else 0 end) as null_c11_rows,
    sum(case when c12 is null then 1 else 0 end) as null_c12_rows,
    sum(case when c13 is null then 1 else 0 end) as null_c13_rows,
    sum(case when c14 is null then 1 else 0 end) as null_c14_rows,
    sum(case when c15 is null then 1 else 0 end) as null_c15_rows,
    sum(case when c16 is null then 1 else 0 end) as null_c16_rows,
    sum(case when c17 is null then 1 else 0 end) as null_c17_rows,
    sum(case when c18 is null then 1 else 0 end) as null_c18_rows,
    sum(case when c19 is null then 1 else 0 end) as null_c19_rows,
    sum(case when c20 is null then 1 else 0 end) as null_c20_rows,
    sum(case when c21 is null then 1 else 0 end) as null_c21_rows,
    sum(case when c22 is null then 1 else 0 end) as null_c22_rows,
    sum(case when c23 is null then 1 else 0 end) as null_c23_rows,
    sum(case when c24 is null then 1 else 0 end) as null_c24_rows,
    sum(case when c25 is null then 1 else 0 end) as null_c25_rows,
    sum(case when c26 is null then 1 else 0 end) as null_c26_rows
from staging.stg_criteo_events;

select
    max(missing_numeric_count) as max_missing_numeric_count,
    max(missing_categorical_count) as max_missing_categorical_count,
    avg(missing_numeric_count::numeric) as avg_missing_numeric_count,
    avg(missing_categorical_count::numeric) as avg_missing_categorical_count
from staging.stg_criteo_events;

select
    categorical_column,
    unknown_rows,
    round(unknown_rows::numeric / nullif(total_rows, 0), 6) as unknown_rate
from (
    select 'c1' as categorical_column, count(*) filter (where c1 = 'unknown') as unknown_rows, count(*) as total_rows from staging.stg_criteo_events
    union all
    select 'c2', count(*) filter (where c2 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c3', count(*) filter (where c3 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c4', count(*) filter (where c4 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c5', count(*) filter (where c5 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c6', count(*) filter (where c6 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c7', count(*) filter (where c7 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c8', count(*) filter (where c8 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c9', count(*) filter (where c9 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c10', count(*) filter (where c10 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c11', count(*) filter (where c11 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c12', count(*) filter (where c12 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c13', count(*) filter (where c13 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c14', count(*) filter (where c14 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c15', count(*) filter (where c15 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c16', count(*) filter (where c16 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c17', count(*) filter (where c17 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c18', count(*) filter (where c18 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c19', count(*) filter (where c19 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c20', count(*) filter (where c20 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c21', count(*) filter (where c21 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c22', count(*) filter (where c22 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c23', count(*) filter (where c23 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c24', count(*) filter (where c24 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c25', count(*) filter (where c25 = 'unknown'), count(*) from staging.stg_criteo_events
    union all
    select 'c26', count(*) filter (where c26 = 'unknown'), count(*) from staging.stg_criteo_events
) unknown_profile
order by unknown_rate desc, categorical_column;

select
    audit_id,
    source_file,
    row_count,
    check_status,
    checked_at
from quality.load_audit
where layer_name = 'staging'
  and table_name = 'staging.stg_criteo_events'
order by audit_id desc
limit 10;
