# Ops Layer Notes

This layer turns the project from a collection of runnable scripts into a batch-aware pipeline with a real operational control plane.

## Core Objects

- `ops.batch_registry`
- `ops.pipeline_runs`
- `ops.pipeline_steps`
- `ops.latest_batch_status`
- `ops.pipeline_run_summary`
- `ops.pipeline_health_dashboard`
- `ops.batch_runtime_trend`

## Purpose

The ops layer gives us:

- a reusable batch registry
- pipeline run history by layer
- task-step execution tracking
- row-count and stage progress visibility
- a clean bridge into future Airflow orchestration

## Batch Model

Each sample or source file batch is registered once in `ops.batch_registry`.

Important fields:

- `batch_id`
- `batch_name`
- `source_file`
- `source_path`
- `sample_scale`
- `expected_row_count`
- `actual_raw_row_count`
- `actual_staging_row_count`
- `actual_fact_row_count`
- `batch_status`
- `last_successful_stage`

## Pipeline Run Model

Every script-driven layer execution creates one record in `ops.pipeline_runs`.

Examples:

- `raw_layer_load`
- `staging_layer_build`
- `warehouse_layer_build`
- `marts_layer_build`
- `advanced_sql_build`
- `quality_framework`

Each run is tied back to a batch and can later map directly to an Airflow DAG run.

## Pipeline Step Model

Every layer execution also writes one step record into `ops.pipeline_steps`.

This gives a clean foundation for:

- one-step script monitoring now
- multi-step Airflow task monitoring later

## Dashboard-Ready Ops Views

Two views are now intended specifically for reporting and Power BI:

- `ops.pipeline_health_dashboard`
- `ops.batch_runtime_trend`

`ops.pipeline_health_dashboard` gives one row per batch and is designed for:

- latest batch status
- latest quality-run summary per batch
- alert counts
- end-to-end batch elapsed time
- archive and failure-path visibility

`ops.batch_runtime_trend` gives stage-level benchmark rows and is designed for:

- runtime by layer
- row-count throughput by batch
- stage trend comparisons across `100k`, `1m`, `5m`, and future incoming batches

Important behavior:

- quality counts in `ops.pipeline_health_dashboard` now reflect the latest validation run for each batch instead of lifetime accumulated validation rows
- runtime rows in `ops.batch_runtime_trend` are sourced from `ops.pipeline_benchmark_summary`

The dashboard implementation assets that sit on top of these views now live under:

- `dashboards/power_bi/pipeline_operations_overview/`

That folder includes:

- import queries
- recommended DAX measures
- field-to-visual mapping

## Current Script Integration

The following scripts now write to the ops layer:

- `scripts/data_ingestion.py`
- `scripts/staging_load.py`
- `scripts/warehouse_build.py`
- `scripts/marts_build.py`
- `scripts/advanced_sql_build.py`
- `scripts/quality_checks.py`

## Validation Review

Check the ops layer from the project root:

```bash
psql -p 5432 -U chakri -d ctr_analytics -f sql/16_ops_layer_checks.sql
```

## Why This Matters

Before this layer, the project had good data products.

After this layer, the project has:

- batch identity
- pipeline identity
- layer-level execution history
- operational lineage
- a real starting point for Airflow DAG orchestration
- dashboard-ready operational reporting
