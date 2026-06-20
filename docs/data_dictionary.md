# Data Dictionary

This document summarizes the most important business and operational fields across the CTR analytics platform.

## Raw Layer

### `raw.criteo_events`

| Column | Meaning |
|---|---|
| `event_id` | Technical ingestion identifier generated in PostgreSQL. |
| `batch_id` | Batch registry identifier used to trace the file and pipeline run. |
| `label` | Click target from the source dataset: `1` for clicked, `0` for not clicked. |
| `i1`-`i13` | Numerical features from the Criteo source file. |
| `c1`-`c26` | Categorical hashed/anonymized features from the Criteo source file. |
| `source_file` | Source file name used for the raw load. |
| `ingested_at` | Raw-layer ingestion timestamp. |

## Staging Layer

### `staging.stg_criteo_events`

| Column | Meaning |
|---|---|
| `raw_event_id` | Lineage link back to `raw.criteo_events.event_id`. |
| `batch_id` | Current batch identifier. |
| `label` | Clean binary click label. |
| `i1`-`i13` | Numeric features with missing values replaced by `0`. |
| `c1`-`c26` | Categorical features with missing/blank values replaced by `'unknown'`. |
| `event_day_number` | Derived training-window bucket from `1` to `7`. |
| `event_batch` | Processing scale label such as `100k`, `1m`, `5m`, or `incoming`. |
| `click_flag` | Binary click indicator derived from `label`. |
| `impression_count` | Always `1`, representing one ad impression per row. |
| `click_count` | `1` for clicked impressions, else `0`. |
| `missing_numeric_count` | Count of missing numeric source values before standardization. |
| `missing_categorical_count` | Count of missing categorical source values before standardization. |
| `source_file` | Source file carried from raw ingestion. |
| `ingested_at` | Upstream ingestion timestamp. |

## Warehouse Layer

### `warehouse.fact_ad_events`

| Column | Meaning |
|---|---|
| `fact_event_key` | Surrogate key for the warehouse fact row. |
| `raw_event_id` | Lineage key back to staging/raw. |
| `batch_id` | Batch identifier for incremental warehouse loading. |
| `event_day_key` | Foreign key to `warehouse.dim_event_day`. |
| `label` | Binary click label. |
| `click_flag` | Binary click flag. |
| `impression_count` | Ad impression measure. |
| `click_count` | Click measure. |
| `missing_numeric_count` | Numeric missingness count. |
| `missing_categorical_count` | Categorical missingness count. |
| `event_batch` | Scale label stored with the fact row. |
| `source_file` | Source file for the batch. |
| `ingested_at` | Source ingestion timestamp. |

### `warehouse.dim_event_day`

| Column | Meaning |
|---|---|
| `event_day_key` | Surrogate key for derived event-day buckets. |
| `event_batch` | Sample/incoming batch scale label. |
| `event_day_number` | Derived day bucket from `1` to `7`. |
| `day_label` | Human-readable label such as `training_day_3`. |

### `warehouse.dim_numeric_bucket`

| Column | Meaning |
|---|---|
| `numeric_bucket_key` | Surrogate key for reusable numeric bucket definitions. |
| `feature_name` | Numeric feature name such as `i1` or `i7`. |
| `bucket_code` | Compact bucket code such as `11_100`. |
| `bucket_label` | Display label such as `11 to 100`. |
| `bucket_order` | Sort order for reporting. |
| `lower_bound` / `upper_bound` | Bucket boundaries when applicable. |

### `warehouse.dim_categorical_value`

| Column | Meaning |
|---|---|
| `categorical_value_key` | Surrogate key for a distinct categorical feature value. |
| `feature_name` | Categorical feature name such as `c1` or `c22`. |
| `feature_value` | Anonymized hashed feature value or `'unknown'`. |
| `is_unknown` | Boolean flag for standardized missing categorical values. |

## Analytics Layer

### `marts.overall_ctr_summary`

| Column | Meaning |
|---|---|
| `event_batch` | Aggregate scale label. |
| `impressions` | Total impressions included in the mart. |
| `clicks` | Total clicks included in the mart. |
| `ctr` | Click-through rate: `clicks / impressions`. |
| `avg_missing_numeric_count` | Average numeric missingness across the batch. |
| `avg_missing_categorical_count` | Average categorical missingness across the batch. |

### `marts.feature_ctr_summary`

| Column | Meaning |
|---|---|
| `feature_name` | Categorical feature being profiled. |
| `feature_value` | Distinct hashed categorical value. |
| `impressions` | Impression count for the feature value. |
| `clicks` | Click count for the feature value. |
| `ctr` | Click-through rate for the feature value. |
| `ctr_lift_vs_overall` | Difference between feature CTR and overall CTR. |
| `volume_band` | Heuristic classification such as `high`, `medium`, `low`. |

### `marts.numeric_bucket_ctr`

| Column | Meaning |
|---|---|
| `feature_name` | Numeric feature name. |
| `bucket_code` / `bucket_label` | Numeric bucket definition. |
| `impressions` | Impression count inside the bucket. |
| `clicks` | Click count inside the bucket. |
| `ctr` | Bucket-level click-through rate. |
| `ctr_lift_vs_overall` | Difference between bucket CTR and overall CTR. |

## Feature Store

### `feature_store.ctr_training_features`

| Column Group | Meaning |
|---|---|
| core labels | `label`, `click_flag`, `click_count`, `impression_count` |
| batch lineage | `raw_event_id`, `batch_id`, `event_batch`, `event_day_number` |
| numeric features | `i1`-`i13` |
| transformed numeric features | log-scaled numeric columns and bucket-code columns |
| categorical features | selected categorical columns such as `c1`, `c6`, `c19`, `c20`, `c22`, `c25`, `c26` |
| missingness flags | `has_missing_numeric`, `has_missing_categorical`, `high_missingness_flag` |
| CTR-lift features | numeric-bucket and selected categorical CTR-lift enrichments |
| metadata | `overall_ctr`, `feature_recorded_at` |

## Ops And Quality

### `quality.validation_results`

Stores the result of each quality check with:
- layer and table name
- check type and status
- expected vs actual values
- thresholds
- timestamps and batch linkage

### `ops.batch_registry`

Tracks every batch through the platform with:
- batch identity and source file
- sample scale and checksum
- row-count expectations and actuals
- lifecycle status
- archive or failure file paths

### `ops.pipeline_runs`

Tracks pipeline-stage execution history with:
- `pipeline_name`
- `layer_name`
- run status and timing
- triggering method

### `ops.benchmark_snapshots`

Stores runtime and storage measurements for:
- pipeline stage runtimes
- table storage sizes
- source file sizes
- processed artifact sizes
