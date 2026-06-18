create index if not exists idx_raw_criteo_events_ingested_at
    on raw.criteo_events (ingested_at);

create index if not exists idx_stg_criteo_events_label
    on staging.stg_criteo_events (label);

create index if not exists idx_fact_ad_events_event_day_key
    on warehouse.fact_ad_events (event_day_key);

create index if not exists idx_fact_ad_events_event_batch
    on warehouse.fact_ad_events (event_batch);

create index if not exists idx_fact_ad_events_click_count
    on warehouse.fact_ad_events (click_count);

create index if not exists idx_bridge_event_numeric_bucket_fact_key
    on warehouse.bridge_event_numeric_bucket (fact_event_key);

create index if not exists idx_bridge_event_numeric_bucket_dimension_key
    on warehouse.bridge_event_numeric_bucket (numeric_bucket_key);

create index if not exists idx_bridge_event_categorical_value_fact_key
    on warehouse.bridge_event_categorical_value (fact_event_key);

create index if not exists idx_bridge_event_categorical_value_dimension_key
    on warehouse.bridge_event_categorical_value (categorical_value_key);

