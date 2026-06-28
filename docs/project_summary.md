# Project Summary

## One-Line Summary

Built a production-style CTR analytics platform on top of the Criteo Display Advertising dataset using PostgreSQL, PySpark, Airflow, layered SQL transformations, data quality monitoring, benchmark capture, and an ML-ready feature store.

## What This Project Does

This project takes large-scale advertising event data and turns it into:

- a raw ingestion layer
- a cleaned staging layer
- an analytics-ready warehouse
- business-facing marts
- advanced SQL reporting assets
- a data quality and ops monitoring layer
- a feature store for future CTR modeling

It also supports automated incoming-file processing through Airflow, with checksum-based idempotency, archive/failure lifecycle handling, benchmark capture, and drift monitoring across batches.

The platform now also includes a machine-learning extension for CTR prediction, batch scoring, calibration, feature importance, and canonical model governance.

## Core Stack

- `PostgreSQL`
  - raw, staging, warehouse, marts, quality, ops, feature store
- `PySpark`
  - scalable batch preprocessing and processed artifact generation
- `Apache Airflow`
  - orchestration, retries, sensor-based intake, and task sequencing
- `Python`
  - batch utilities, ingestion, quality checks, benchmark capture, and pipeline helpers
- `Power BI`
  - intended reporting layer for business and ops dashboards

## Data And Scale Story

Source dataset:

- Criteo Display Advertising Challenge
- `45,840,617` rows
- `40` columns
  - `1` label
  - `13` numeric features
  - `26` categorical features

Sampling strategy:

- `100K` for fast testing
- `1M` for development
- `5M` for final platform validation

Validated scale-up:

- `1M` batch processed end to end
- `5M` batch processed end to end through Airflow, PySpark, PostgreSQL, quality checks, and benchmark capture

## Architecture

High-level flow:

`incoming file -> Airflow sensor -> batch registration -> raw load -> PySpark preprocessing -> staging -> warehouse -> marts -> advanced SQL -> feature store -> quality checks -> benchmarks -> Power BI`

Primary production-style entrypoint:

- `ctr_incoming_file_sensor`

Processing DAG:

- `ctr_batch_pipeline`

## Important Platform Features

- checksum-based incoming batch registration
- `incoming / archive / failed` file lifecycle
- batch-aware raw, staging, warehouse, marts, advanced SQL, and feature-store processing
- data quality framework with historical validation runs and thresholds
- ops monitoring with:
  - `ops.pipeline_health_dashboard`
  - `ops.batch_runtime_trend`
- cross-batch drift monitoring with:
  - `ops.batch_metric_baseline`
  - `ops.batch_drift_summary`
  - `ops.batch_stage_runtime_drift`

## Proven Runtime Highlights

Development batch:

- `1M` batch status: `QUALITY_CHECKED`
- total recorded stage runtime: `172.000s`

Scaled validation batch:

- `5M` batch status: `QUALITY_CHECKED`
- total recorded stage runtime: `867.000s`

Recent live incoming batch:

- batch `10`
- status: `ARCHIVED`
- total recorded stage runtime: `53.731s`
- failed quality checks: `2`
- total alerts: `0`

## Benchmark Takeaways

`1M -> 5M`:

- raw load: `26s -> 114s`
- Spark preprocessing: `44s -> 220s`
- staging build: `34s -> 126s`
- warehouse build: `59s -> 348s`
- quality checks: `8s -> 47s`

The main bottleneck remains the warehouse layer, while Spark scaled predictably and the overall platform stayed stable at `5M`.

## Honest Tradeoffs

- warehouse build is proven at `5M`, but not fully optimized like a large enterprise deployment
- partitioning strategy is documented, not physically implemented
- Power BI handoff assets are in the repo, but no committed `.pbix` file is included
- optional bridge-table population remains intentionally deferred to avoid unnecessary storage and runtime cost

## Why This Project Is Strong

This is no longer just a SQL project or a set of warehouse scripts.

It demonstrates:

- analytics engineering
- data engineering
- batch orchestration
- scalable preprocessing
- production-style monitoring
- performance benchmarking
- ML-ready feature engineering
- large-batch CTR model training, scoring, and calibration
- canonical model governance and ML monitoring

## Best Supporting Docs

- `docs/architecture_overview.md`
- `docs/final_benchmark_report.md`
- `docs/final_polish_review.md`
- `docs/power_bi_ops_page_spec.md`
