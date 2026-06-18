# Benchmarking Notes

## Purpose

This stage captures repeatable benchmark snapshots after a completed pipeline run so scale-up from `1M` to `5M` can be measured instead of guessed.

## Current Outputs

Benchmark data is stored in:

- `ops.benchmark_snapshots`
- `ops.pipeline_benchmark_summary`
- `ops.batch_runtime_trend`

## What Gets Measured

- stage runtime from `ops.pipeline_run_summary`
- estimated rows per second by stage
- table storage size for:
  - `raw.criteo_events`
  - `staging.stg_criteo_events`
  - `warehouse.fact_ad_events`
  - `feature_store.ctr_training_features`
- sample CSV size on disk
- processed Spark artifact directory size on disk

## Why This Matters

This makes the `1M -> 5M` scale-up story much stronger because the project can show:

- which stage is slowest
- how storage grows by layer
- whether Spark preprocessing is reducing pain in downstream stages
- whether warehouse and mart stages remain acceptable as batch size increases

## Current Usage

Run from the project root:

`python3 scripts/benchmark_capture.py`

In Airflow, benchmark capture is designed to run after quality checks succeed.

## Power BI Usage

For dashboarding, use:

- `ops.pipeline_health_dashboard` for batch-level pipeline health
- `ops.batch_runtime_trend` for stage runtime comparisons

This gives a clean split between:

- operational health at the batch level
- performance trends at the stage level
