# Data Lineage

This document explains how data moves through the CTR analytics platform from raw file arrival to reporting and feature engineering outputs.

## End-To-End Lineage

```text
Incoming raw CSV / sampled file
-> raw.criteo_events
-> staging.stg_criteo_events
-> warehouse.fact_ad_events + warehouse dimensions
-> marts layer
-> advanced SQL assets
-> feature_store.ctr_training_features
-> Power BI / downstream analytics
```

## 1. Source Intake

### File-based sources

The platform accepts:
- sampled development files such as `criteo_100k.csv`, `criteo_1m.csv`, `criteo_5m.csv`
- incoming production-style CSV files dropped into `data/raw/incoming/`

### Batch registration

Every file becomes a batch in `ops.batch_registry`.

Important lineage fields:
- `batch_id`
- `batch_name`
- `source_file`
- `source_path`
- `source_checksum`
- `sample_scale`

## 2. Raw Layer Lineage

### `raw.criteo_events`

The raw layer preserves source fidelity and adds only technical metadata.

Lineage keys:
- `event_id` is the raw-row technical identifier.
- `batch_id` links raw rows to `ops.batch_registry`.
- `source_file` and `ingested_at` preserve origin and load timing.

## 3. Staging Layer Lineage

### `staging.stg_criteo_events`

The staging layer standardizes missing values and adds light derived fields.

Lineage mapping:
- `raw.criteo_events.event_id` -> `staging.stg_criteo_events.raw_event_id`
- `raw.criteo_events.batch_id` -> `staging.stg_criteo_events.batch_id`

Derived lineage-safe enrichments:
- `event_day_number`
- `event_batch`
- `click_flag`
- `missing_numeric_count`
- `missing_categorical_count`

## 4. Warehouse Layer Lineage

### `warehouse.fact_ad_events`

The warehouse fact table is loaded incrementally by `batch_id`.

Lineage mapping:
- `staging.stg_criteo_events.raw_event_id` -> `warehouse.fact_ad_events.raw_event_id`
- `staging.stg_criteo_events.batch_id` -> `warehouse.fact_ad_events.batch_id`
- `staging.stg_criteo_events.event_day_number` -> `warehouse.dim_event_day.event_day_key`

### Dimension lineage

- `warehouse.dim_event_day` is derived from distinct `event_batch + event_day_number` pairs in staging.
- `warehouse.dim_numeric_bucket` is a controlled seed dimension maintained from warehouse logic.
- `warehouse.dim_categorical_value` is incrementally extended from distinct categorical values found in each batch.

## 5. Marts Lineage

The marts layer is batch-aware.

Physical storage pattern:
- batch results are stored in `marts.batch_*` tables
- public reporting objects are exposed as cumulative views

Examples:
- `marts.batch_overall_ctr_summary` -> `marts.overall_ctr_summary`
- `marts.batch_feature_ctr_summary` -> `marts.feature_ctr_summary`
- `marts.batch_numeric_bucket_ctr` -> `marts.numeric_bucket_ctr`

This gives the platform two useful properties:
- only the new batch is recomputed
- the reporting layer still shows cumulative history across retained batches

## 6. Advanced SQL Lineage

Advanced analytical assets are also batch-aware.

Examples:
- `marts.batch_feature_ctr_ranked` -> `marts.feature_ctr_ranked`
- `marts.batch_event_day_ctr_rolling` -> `marts.event_day_ctr_rolling`
- `marts.batch_feature_interaction_ranked` -> `marts.feature_interaction_ranked`

These build on top of the mart layer rather than directly from raw files.

## 7. Feature Store Lineage

### `feature_store.ctr_training_features`

The feature store is built from:
- `staging.stg_criteo_events`
- `marts.numeric_bucket_ctr`
- `marts.feature_ctr_summary`
- `marts.overall_ctr_summary`

This means every training row can be traced back to:
- the original raw event (`raw_event_id`)
- the processing batch (`batch_id`)
- the reporting context used to derive CTR-lift features

## 8. Quality And Monitoring Lineage

### Quality

Quality lineage is stored in:
- `quality.validation_runs`
- `quality.validation_results`
- `quality.load_audit`

These objects trace:
- which batch was validated
- which layer/table was checked
- which pipeline run performed the validation

### Ops

Operational lineage is stored in:
- `ops.pipeline_runs`
- `ops.pipeline_steps`
- `ops.pipeline_alerts`
- `ops.benchmark_snapshots`

These objects trace:
- when a stage ran
- whether it succeeded or failed
- how long it took
- whether alerts were raised

## 9. File Lifecycle Lineage

Incoming files follow this path:

```text
data/raw/incoming/
-> processing batch
-> data/raw/archive/   on success
-> data/raw/failed/    on failure
```

The final file location is stored back in `ops.batch_registry` through:
- `archive_path`
- `failure_path`
- `source_moved_at`

## 10. Why This Lineage Matters

This lineage design makes the project stronger in three ways:
- it supports debugging and batch replay
- it makes operational monitoring trustworthy
- it allows analytics outputs and ML features to be traced back to raw source events
