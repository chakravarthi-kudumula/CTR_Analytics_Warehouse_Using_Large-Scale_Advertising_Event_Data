#!/usr/bin/env python3

"""Build the warehouse layer from staging.stg_criteo_events."""

from __future__ import annotations

import argparse
import subprocess

from pipeline_tracking import (
    build_connection_command,
    build_env,
    complete_pipeline_run,
    complete_pipeline_step,
    create_pipeline_run,
    create_pipeline_step,
    ensure_pipeline_metadata,
    resolve_batch_context,
    run_psql,
    run_scalar_query,
    sql_literal,
    update_batch_status,
)
from project_config import PROJECT_ROOT, SQL_DIR, add_db_connection_args


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build warehouse tables from staging.stg_criteo_events")
    parser.add_argument("--batch-name")
    parser.add_argument("--triggered-by", default="manual")
    add_db_connection_args(parser)
    return parser.parse_args()


def build_warehouse_sql() -> str:
    return """
truncate table
    warehouse.bridge_event_categorical_value,
    warehouse.bridge_event_numeric_bucket,
    warehouse.fact_ad_events,
    warehouse.dim_categorical_value,
    warehouse.dim_numeric_bucket,
    warehouse.dim_event_day
restart identity cascade;

insert into warehouse.dim_event_day (
    event_batch,
    event_day_number,
    day_label
)
select distinct
    event_batch,
    event_day_number,
    'training_day_' || event_day_number
from staging.stg_criteo_events
order by event_batch, event_day_number;

insert into warehouse.dim_numeric_bucket (
    feature_name,
    bucket_code,
    bucket_label,
    bucket_order,
    lower_bound,
    upper_bound
)
select
    feature_name,
    bucket_code,
    bucket_label,
    bucket_order,
    lower_bound,
    upper_bound
from (
    select 'i1' as feature_name, 'negative' as bucket_code, 'negative values' as bucket_label, 1 as bucket_order, null::bigint as lower_bound, -1::bigint as upper_bound
    union all select 'i1', 'zero', '0', 2, 0, 0
    union all select 'i1', '1_10', '1 to 10', 3, 1, 10
    union all select 'i1', '11_100', '11 to 100', 4, 11, 100
    union all select 'i1', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i1', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i1', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i2', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i2', 'zero', '0', 2, 0, 0
    union all select 'i2', '1_10', '1 to 10', 3, 1, 10
    union all select 'i2', '11_100', '11 to 100', 4, 11, 100
    union all select 'i2', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i2', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i2', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i3', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i3', 'zero', '0', 2, 0, 0
    union all select 'i3', '1_10', '1 to 10', 3, 1, 10
    union all select 'i3', '11_100', '11 to 100', 4, 11, 100
    union all select 'i3', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i3', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i3', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i4', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i4', 'zero', '0', 2, 0, 0
    union all select 'i4', '1_10', '1 to 10', 3, 1, 10
    union all select 'i4', '11_100', '11 to 100', 4, 11, 100
    union all select 'i4', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i4', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i4', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i5', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i5', 'zero', '0', 2, 0, 0
    union all select 'i5', '1_10', '1 to 10', 3, 1, 10
    union all select 'i5', '11_100', '11 to 100', 4, 11, 100
    union all select 'i5', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i5', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i5', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i6', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i6', 'zero', '0', 2, 0, 0
    union all select 'i6', '1_10', '1 to 10', 3, 1, 10
    union all select 'i6', '11_100', '11 to 100', 4, 11, 100
    union all select 'i6', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i6', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i6', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i7', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i7', 'zero', '0', 2, 0, 0
    union all select 'i7', '1_10', '1 to 10', 3, 1, 10
    union all select 'i7', '11_100', '11 to 100', 4, 11, 100
    union all select 'i7', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i7', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i7', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i8', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i8', 'zero', '0', 2, 0, 0
    union all select 'i8', '1_10', '1 to 10', 3, 1, 10
    union all select 'i8', '11_100', '11 to 100', 4, 11, 100
    union all select 'i8', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i8', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i8', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i9', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i9', 'zero', '0', 2, 0, 0
    union all select 'i9', '1_10', '1 to 10', 3, 1, 10
    union all select 'i9', '11_100', '11 to 100', 4, 11, 100
    union all select 'i9', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i9', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i9', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i10', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i10', 'zero', '0', 2, 0, 0
    union all select 'i10', '1_10', '1 to 10', 3, 1, 10
    union all select 'i10', '11_100', '11 to 100', 4, 11, 100
    union all select 'i10', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i10', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i10', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i11', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i11', 'zero', '0', 2, 0, 0
    union all select 'i11', '1_10', '1 to 10', 3, 1, 10
    union all select 'i11', '11_100', '11 to 100', 4, 11, 100
    union all select 'i11', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i11', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i11', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i12', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i12', 'zero', '0', 2, 0, 0
    union all select 'i12', '1_10', '1 to 10', 3, 1, 10
    union all select 'i12', '11_100', '11 to 100', 4, 11, 100
    union all select 'i12', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i12', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i12', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i13', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i13', 'zero', '0', 2, 0, 0
    union all select 'i13', '1_10', '1 to 10', 3, 1, 10
    union all select 'i13', '11_100', '11 to 100', 4, 11, 100
    union all select 'i13', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i13', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i13', '10001_plus', '10001 and above', 7, 10001, null
) bucket_seed
order by feature_name, bucket_order;

insert into warehouse.dim_categorical_value (
    feature_name,
    feature_value,
    is_unknown
)
select
    feature_name,
    feature_value,
    feature_value = 'unknown' as is_unknown
from (
    select 'c1' as feature_name, c1 as feature_value from staging.stg_criteo_events
    union
    select 'c2', c2 from staging.stg_criteo_events
    union
    select 'c3', c3 from staging.stg_criteo_events
    union
    select 'c4', c4 from staging.stg_criteo_events
    union
    select 'c5', c5 from staging.stg_criteo_events
    union
    select 'c6', c6 from staging.stg_criteo_events
    union
    select 'c7', c7 from staging.stg_criteo_events
    union
    select 'c8', c8 from staging.stg_criteo_events
    union
    select 'c9', c9 from staging.stg_criteo_events
    union
    select 'c10', c10 from staging.stg_criteo_events
    union
    select 'c11', c11 from staging.stg_criteo_events
    union
    select 'c12', c12 from staging.stg_criteo_events
    union
    select 'c13', c13 from staging.stg_criteo_events
    union
    select 'c14', c14 from staging.stg_criteo_events
    union
    select 'c15', c15 from staging.stg_criteo_events
    union
    select 'c16', c16 from staging.stg_criteo_events
    union
    select 'c17', c17 from staging.stg_criteo_events
    union
    select 'c18', c18 from staging.stg_criteo_events
    union
    select 'c19', c19 from staging.stg_criteo_events
    union
    select 'c20', c20 from staging.stg_criteo_events
    union
    select 'c21', c21 from staging.stg_criteo_events
    union
    select 'c22', c22 from staging.stg_criteo_events
    union
    select 'c23', c23 from staging.stg_criteo_events
    union
    select 'c24', c24 from staging.stg_criteo_events
    union
    select 'c25', c25 from staging.stg_criteo_events
    union
    select 'c26', c26 from staging.stg_criteo_events
) categorical_seed
order by feature_name, feature_value;

insert into warehouse.fact_ad_events (
    raw_event_id,
    batch_id,
    event_day_key,
    label,
    click_flag,
    impression_count,
    click_count,
    missing_numeric_count,
    missing_categorical_count,
    event_batch,
    source_file,
    ingested_at
)
select
    s.raw_event_id,
    s.batch_id,
    d.event_day_key,
    s.label,
    s.click_flag,
    s.impression_count,
    s.click_count,
    s.missing_numeric_count,
    s.missing_categorical_count,
    s.event_batch,
    s.source_file,
    s.ingested_at
from staging.stg_criteo_events s
join warehouse.dim_event_day d
  on d.event_batch = s.event_batch
 and d.event_day_number = s.event_day_number
order by s.raw_event_id;

insert into warehouse.bridge_event_numeric_bucket (
    fact_event_key,
    feature_name,
    numeric_bucket_key,
    feature_value
)
with numeric_features as (
    select fact_event_key, raw_event_id from warehouse.fact_ad_events
),
numeric_source as (
    select raw_event_id, 'i1' as feature_name, i1 as feature_value from staging.stg_criteo_events
    union all select raw_event_id, 'i2', i2 from staging.stg_criteo_events
    union all select raw_event_id, 'i3', i3 from staging.stg_criteo_events
    union all select raw_event_id, 'i4', i4 from staging.stg_criteo_events
    union all select raw_event_id, 'i5', i5 from staging.stg_criteo_events
    union all select raw_event_id, 'i6', i6 from staging.stg_criteo_events
    union all select raw_event_id, 'i7', i7 from staging.stg_criteo_events
    union all select raw_event_id, 'i8', i8 from staging.stg_criteo_events
    union all select raw_event_id, 'i9', i9 from staging.stg_criteo_events
    union all select raw_event_id, 'i10', i10 from staging.stg_criteo_events
    union all select raw_event_id, 'i11', i11 from staging.stg_criteo_events
    union all select raw_event_id, 'i12', i12 from staging.stg_criteo_events
    union all select raw_event_id, 'i13', i13 from staging.stg_criteo_events
),
numeric_bucketed as (
    select
        ns.raw_event_id,
        ns.feature_name,
        ns.feature_value,
        case
            when ns.feature_value < 0 then 'negative'
            when ns.feature_value = 0 then 'zero'
            when ns.feature_value between 1 and 10 then '1_10'
            when ns.feature_value between 11 and 100 then '11_100'
            when ns.feature_value between 101 and 1000 then '101_1000'
            when ns.feature_value between 1001 and 10000 then '1001_10000'
            else '10001_plus'
        end as bucket_code
    from numeric_source ns
)
select
    f.fact_event_key,
    n.feature_name,
    b.numeric_bucket_key,
    n.feature_value
from numeric_bucketed n
join numeric_features f
  on f.raw_event_id = n.raw_event_id
join warehouse.dim_numeric_bucket b
  on b.feature_name = n.feature_name
 and b.bucket_code = n.bucket_code;

insert into warehouse.bridge_event_categorical_value (
    fact_event_key,
    feature_name,
    categorical_value_key,
    feature_value
)
with categorical_features as (
    select fact_event_key, raw_event_id from warehouse.fact_ad_events
),
categorical_source as (
    select raw_event_id, 'c1' as feature_name, c1 as feature_value from staging.stg_criteo_events
    union all select raw_event_id, 'c2', c2 from staging.stg_criteo_events
    union all select raw_event_id, 'c3', c3 from staging.stg_criteo_events
    union all select raw_event_id, 'c4', c4 from staging.stg_criteo_events
    union all select raw_event_id, 'c5', c5 from staging.stg_criteo_events
    union all select raw_event_id, 'c6', c6 from staging.stg_criteo_events
    union all select raw_event_id, 'c7', c7 from staging.stg_criteo_events
    union all select raw_event_id, 'c8', c8 from staging.stg_criteo_events
    union all select raw_event_id, 'c9', c9 from staging.stg_criteo_events
    union all select raw_event_id, 'c10', c10 from staging.stg_criteo_events
    union all select raw_event_id, 'c11', c11 from staging.stg_criteo_events
    union all select raw_event_id, 'c12', c12 from staging.stg_criteo_events
    union all select raw_event_id, 'c13', c13 from staging.stg_criteo_events
    union all select raw_event_id, 'c14', c14 from staging.stg_criteo_events
    union all select raw_event_id, 'c15', c15 from staging.stg_criteo_events
    union all select raw_event_id, 'c16', c16 from staging.stg_criteo_events
    union all select raw_event_id, 'c17', c17 from staging.stg_criteo_events
    union all select raw_event_id, 'c18', c18 from staging.stg_criteo_events
    union all select raw_event_id, 'c19', c19 from staging.stg_criteo_events
    union all select raw_event_id, 'c20', c20 from staging.stg_criteo_events
    union all select raw_event_id, 'c21', c21 from staging.stg_criteo_events
    union all select raw_event_id, 'c22', c22 from staging.stg_criteo_events
    union all select raw_event_id, 'c23', c23 from staging.stg_criteo_events
    union all select raw_event_id, 'c24', c24 from staging.stg_criteo_events
    union all select raw_event_id, 'c25', c25 from staging.stg_criteo_events
    union all select raw_event_id, 'c26', c26 from staging.stg_criteo_events
)
select
    f.fact_event_key,
    c.feature_name,
    d.categorical_value_key,
    c.feature_value
from categorical_source c
join categorical_features f
  on f.raw_event_id = c.raw_event_id
join warehouse.dim_categorical_value d
  on d.feature_name = c.feature_name
 and d.feature_value = c.feature_value;
"""


def build_core_warehouse_sql(batch_id: int) -> str:
    return f"""
delete from warehouse.fact_ad_events
where batch_id = {batch_id};

insert into warehouse.dim_event_day (
    event_batch,
    event_day_number,
    day_label
)
select distinct
    event_batch,
    event_day_number,
    'training_day_' || event_day_number
from staging.stg_criteo_events
where batch_id = {batch_id}
order by event_batch, event_day_number
on conflict (event_batch, event_day_number) do nothing;

insert into warehouse.dim_numeric_bucket (
    feature_name,
    bucket_code,
    bucket_label,
    bucket_order,
    lower_bound,
    upper_bound
)
select
    feature_name,
    bucket_code,
    bucket_label,
    bucket_order,
    lower_bound,
    upper_bound
from (
    select 'i1' as feature_name, 'negative' as bucket_code, 'negative values' as bucket_label, 1 as bucket_order, null::bigint as lower_bound, -1::bigint as upper_bound
    union all select 'i1', 'zero', '0', 2, 0, 0
    union all select 'i1', '1_10', '1 to 10', 3, 1, 10
    union all select 'i1', '11_100', '11 to 100', 4, 11, 100
    union all select 'i1', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i1', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i1', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i2', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i2', 'zero', '0', 2, 0, 0
    union all select 'i2', '1_10', '1 to 10', 3, 1, 10
    union all select 'i2', '11_100', '11 to 100', 4, 11, 100
    union all select 'i2', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i2', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i2', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i3', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i3', 'zero', '0', 2, 0, 0
    union all select 'i3', '1_10', '1 to 10', 3, 1, 10
    union all select 'i3', '11_100', '11 to 100', 4, 11, 100
    union all select 'i3', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i3', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i3', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i4', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i4', 'zero', '0', 2, 0, 0
    union all select 'i4', '1_10', '1 to 10', 3, 1, 10
    union all select 'i4', '11_100', '11 to 100', 4, 11, 100
    union all select 'i4', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i4', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i4', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i5', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i5', 'zero', '0', 2, 0, 0
    union all select 'i5', '1_10', '1 to 10', 3, 1, 10
    union all select 'i5', '11_100', '11 to 100', 4, 11, 100
    union all select 'i5', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i5', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i5', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i6', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i6', 'zero', '0', 2, 0, 0
    union all select 'i6', '1_10', '1 to 10', 3, 1, 10
    union all select 'i6', '11_100', '11 to 100', 4, 11, 100
    union all select 'i6', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i6', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i6', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i7', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i7', 'zero', '0', 2, 0, 0
    union all select 'i7', '1_10', '1 to 10', 3, 1, 10
    union all select 'i7', '11_100', '11 to 100', 4, 11, 100
    union all select 'i7', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i7', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i7', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i8', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i8', 'zero', '0', 2, 0, 0
    union all select 'i8', '1_10', '1 to 10', 3, 1, 10
    union all select 'i8', '11_100', '11 to 100', 4, 11, 100
    union all select 'i8', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i8', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i8', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i9', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i9', 'zero', '0', 2, 0, 0
    union all select 'i9', '1_10', '1 to 10', 3, 1, 10
    union all select 'i9', '11_100', '11 to 100', 4, 11, 100
    union all select 'i9', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i9', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i9', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i10', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i10', 'zero', '0', 2, 0, 0
    union all select 'i10', '1_10', '1 to 10', 3, 1, 10
    union all select 'i10', '11_100', '11 to 100', 4, 11, 100
    union all select 'i10', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i10', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i10', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i11', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i11', 'zero', '0', 2, 0, 0
    union all select 'i11', '1_10', '1 to 10', 3, 1, 10
    union all select 'i11', '11_100', '11 to 100', 4, 11, 100
    union all select 'i11', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i11', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i11', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i12', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i12', 'zero', '0', 2, 0, 0
    union all select 'i12', '1_10', '1 to 10', 3, 1, 10
    union all select 'i12', '11_100', '11 to 100', 4, 11, 100
    union all select 'i12', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i12', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i12', '10001_plus', '10001 and above', 7, 10001, null
    union all select 'i13', 'negative', 'negative values', 1, null::bigint, -1::bigint
    union all select 'i13', 'zero', '0', 2, 0, 0
    union all select 'i13', '1_10', '1 to 10', 3, 1, 10
    union all select 'i13', '11_100', '11 to 100', 4, 11, 100
    union all select 'i13', '101_1000', '101 to 1000', 5, 101, 1000
    union all select 'i13', '1001_10000', '1001 to 10000', 6, 1001, 10000
    union all select 'i13', '10001_plus', '10001 and above', 7, 10001, null
) bucket_seed
order by feature_name, bucket_order
on conflict (feature_name, bucket_code) do nothing;

insert into warehouse.dim_categorical_value (
    feature_name,
    feature_value,
    is_unknown
)
select
    feature_name,
    feature_value,
    feature_value = 'unknown' as is_unknown
from (
    select 'c1' as feature_name, c1 as feature_value from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c2', c2 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c3', c3 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c4', c4 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c5', c5 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c6', c6 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c7', c7 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c8', c8 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c9', c9 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c10', c10 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c11', c11 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c12', c12 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c13', c13 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c14', c14 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c15', c15 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c16', c16 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c17', c17 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c18', c18 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c19', c19 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c20', c20 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c21', c21 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c22', c22 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c23', c23 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c24', c24 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c25', c25 from staging.stg_criteo_events where batch_id = {batch_id}
    union
    select 'c26', c26 from staging.stg_criteo_events where batch_id = {batch_id}
) categorical_seed
order by feature_name, feature_value
on conflict (feature_name, feature_value) do nothing;

insert into warehouse.fact_ad_events (
    raw_event_id,
    batch_id,
    event_day_key,
    label,
    click_flag,
    impression_count,
    click_count,
    missing_numeric_count,
    missing_categorical_count,
    event_batch,
    source_file,
    ingested_at
)
select
    s.raw_event_id,
    s.batch_id,
    d.event_day_key,
    s.label,
    s.click_flag,
    s.impression_count,
    s.click_count,
    s.missing_numeric_count,
    s.missing_categorical_count,
    s.event_batch,
    s.source_file,
    s.ingested_at
from staging.stg_criteo_events s
join warehouse.dim_event_day d
  on d.event_batch = s.event_batch
 and d.event_day_number = s.event_day_number
where s.batch_id = {batch_id}
order by s.raw_event_id;
"""


def build_audit_insert_sql(source_file: str, row_count: int, batch_id: int, pipeline_run_id: int) -> str:
    escaped_source = sql_literal(source_file)
    message = sql_literal(f"Warehouse layer build completed successfully for {source_file}.")
    return (
        "insert into quality.load_audit "
        "(batch_id, pipeline_run_id, layer_name, table_name, source_file, row_count, check_status, check_message) "
        f"values ({batch_id}, {pipeline_run_id}, 'warehouse', 'warehouse.fact_ad_events', '{escaped_source}', {row_count}, 'SUCCESS', '{message}') "
        "returning audit_id;"
    )


def main() -> None:
    args = parse_args()
    project_root = PROJECT_ROOT
    sql_dir = SQL_DIR

    ensure_pipeline_metadata(sql_dir, args.database, args)
    run_psql(sql_dir / "04_warehouse_tables.sql", args.database, args)
    run_psql(sql_dir / "06_quality_checks.sql", args.database, args)
    run_psql(sql_dir / "07_indexes.sql", args.database, args)

    batch_id, source_file = resolve_batch_context(
        table_name="staging.stg_criteo_events",
        batch_name=args.batch_name,
        database=args.database,
        args=args,
    )
    staging_row_count = int(
        run_scalar_query(
            f"select count(*) from staging.stg_criteo_events where batch_id = {batch_id};",
            args.database,
            args,
        )
    )
    if staging_row_count == 0:
        raise RuntimeError("staging.stg_criteo_events is empty. Build the staging layer before building the warehouse.")
    pipeline_run_id = create_pipeline_run(
        batch_id=batch_id,
        pipeline_name="warehouse_layer_build",
        layer_name="warehouse",
        source_file=source_file,
        triggered_by=args.triggered_by,
        database=args.database,
        args=args,
    )
    pipeline_step_id = create_pipeline_step(
        pipeline_run_id=pipeline_run_id,
        batch_id=batch_id,
        step_name="build_fact_and_dimensions",
        layer_name="warehouse",
        target_table="warehouse.fact_ad_events",
        source_file=source_file,
        database=args.database,
        args=args,
    )
    update_batch_status(
        batch_id=batch_id,
        batch_status="WAREHOUSE_BUILDING",
        last_successful_stage="staging",
        row_column=None,
        row_count=None,
        mark_started=False,
        mark_completed=False,
        database=args.database,
        args=args,
    )

    try:
        command = build_connection_command(["psql"], args.database, args)
        command.extend(["-v", "ON_ERROR_STOP=1", "-c", build_core_warehouse_sql(batch_id)])
        subprocess.run(command, check=True, env=build_env(args))

        fact_row_count = int(
            run_scalar_query(
                f"select count(*) from warehouse.fact_ad_events where batch_id = {batch_id};",
                args.database,
                args,
            )
        )
        if fact_row_count != staging_row_count:
            raise RuntimeError(
                f"Row count mismatch: staging has {staging_row_count:,}, fact has {fact_row_count:,}"
            )

        update_batch_status(
            batch_id=batch_id,
            batch_status="WAREHOUSE_READY",
            last_successful_stage="warehouse",
            row_column="actual_fact_row_count",
            row_count=fact_row_count,
            mark_started=False,
            mark_completed=False,
            database=args.database,
            args=args,
        )
        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="SUCCESS",
            rows_processed=fact_row_count,
            step_message=f"Warehouse build completed successfully for {source_file}.",
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="SUCCESS",
            run_message=f"Warehouse layer build completed successfully for {source_file}.",
            database=args.database,
            args=args,
        )

        audit_id = run_scalar_query(
            build_audit_insert_sql(source_file, fact_row_count, batch_id, pipeline_run_id),
            args.database,
            args,
        )

        print(f"Staging rows: {staging_row_count:,}")
        print(f"Fact rows: {fact_row_count:,}")
        print(f"Source file tag: {source_file}")
        print(f"Batch ID: {batch_id}")
        print(f"Pipeline run ID: {pipeline_run_id}")
        print("Warehouse-layer build validation passed.")
        print(f"Audit row created: {audit_id}")
    except Exception as exc:
        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="FAILED",
            rows_processed=None,
            step_message=str(exc),
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="FAILED",
            run_message=str(exc),
            database=args.database,
            args=args,
        )
        update_batch_status(
            batch_id=batch_id,
            batch_status="WAREHOUSE_FAILED",
            last_successful_stage="staging",
            row_column=None,
            row_count=None,
            mark_started=False,
            mark_completed=False,
            database=args.database,
            args=args,
        )
        raise


if __name__ == "__main__":
    main()
