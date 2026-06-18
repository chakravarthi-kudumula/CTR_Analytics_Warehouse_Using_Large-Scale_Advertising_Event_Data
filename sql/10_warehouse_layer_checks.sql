select
    (select count(*) from staging.stg_criteo_events) as staging_row_count,
    (select count(*) from warehouse.fact_ad_events) as fact_row_count;

select
    count(*) as event_day_dimension_rows
from warehouse.dim_event_day;

select
    count(*) as numeric_bucket_dimension_rows
from warehouse.dim_numeric_bucket;

select
    count(*) as categorical_value_dimension_rows
from warehouse.dim_categorical_value;

select
    count(*) as numeric_bridge_rows
from warehouse.bridge_event_numeric_bucket;

select
    count(*) as categorical_bridge_rows
from warehouse.bridge_event_categorical_value;

select
    d.event_batch,
    d.event_day_number,
    count(*) as impressions,
    sum(f.click_count) as clicks,
    round(sum(f.click_count)::numeric / nullif(sum(f.impression_count), 0), 6) as ctr
from warehouse.fact_ad_events f
join warehouse.dim_event_day d
  on d.event_day_key = f.event_day_key
group by d.event_batch, d.event_day_number
order by d.event_batch, d.event_day_number;

select
    sum(case when impression_count <> 1 then 1 else 0 end) as invalid_impression_rows,
    sum(case when click_flag not in (0, 1) then 1 else 0 end) as invalid_click_flag_rows,
    sum(case when click_count not in (0, 1) then 1 else 0 end) as invalid_click_count_rows
from warehouse.fact_ad_events;

select
    count(*) as unknown_categorical_dimension_rows
from warehouse.dim_categorical_value
where is_unknown = true;

select
    audit_id,
    source_file,
    row_count,
    check_status,
    checked_at
from quality.load_audit
where layer_name = 'warehouse'
  and table_name = 'warehouse.fact_ad_events'
order by audit_id desc
limit 10;
