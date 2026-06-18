# CTR Analytics Warehouse Using Large-Scale Advertising Event Data

This project builds a production-grade PostgreSQL analytics warehouse on top of the Criteo Display Advertising Challenge dataset. The target output is a layered warehouse with raw ingestion, staging transformations, analytics marts, data quality checks, and machine-learning-ready CTR feature tables.

## Project Structure

```text
advertisement-analytics-warehouse/
├── data/
│   ├── raw/
│   │   ├── incoming/
│   │   ├── archive/
│   │   └── failed/
│   ├── sample/
│   └── processed/
├── sql/
├── scripts/
├── docs/
├── dashboards/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Dataset

- Source: Criteo Display Advertising Challenge
- Download link: [Kaggle Display Advertising Challenge dataset](https://figshare.com/articles/dataset/Kaggle_Display_Advertising_Challenge_dataset/5732310?utm_source=chatgpt.com&file=10082655)
- Training rows: 45,840,617
- Columns: 40
- Layout: 1 label, 13 numeric features, 26 categorical features

The raw dataset is stored locally under `data/raw/` and excluded from Git to avoid pushing large files.

## Local Setup

1. Start PostgreSQL with Docker if needed:
   `docker compose up -d`
2. Optionally create a local virtual environment named `adv`:
   `python3 -m venv adv`
3. Install Python dependencies:
   `pip install -r requirements.txt`

## Planned Layers

- `raw`: source-faithful ingestion tables
- `staging`: cleaned and typed transformation layer
- `warehouse`: fact and dimension model
- `marts`: business-facing analytics outputs
- `quality`: data quality auditing and monitoring
- `ops`: batch registry and pipeline execution monitoring

## Status

Stage 1 data sampling is implemented. The sampling pipeline creates representative `100K`, `1M`, and `5M` CSV extracts from `train.txt` for testing, development, and final validation.

## Stage 1 Sampling

Run the sampling pipeline from the project root:

`python3 scripts/data_sampling.py`

The script:

- reads `data/raw/dac/train.txt`
- assigns column names
- creates `criteo_100k.csv`, `criteo_1m.csv`, and `criteo_5m.csv`
- uses stratified random sampling on the `label` column to reduce click imbalance
- writes sampled rows in original row order

## Stage 2 Raw Layer

Run the raw-layer load from the project root:

`python3 scripts/data_ingestion.py --sample 1m --maintenance-database postgres`

Helpful references:

- raw-layer rerun notes: `docs/raw_layer_notes.md`
- raw validation queries: `sql/08_raw_layer_checks.sql`

## Stage 3 Staging Layer

Run the staging-layer build from the project root:

`python3 scripts/staging_load.py`

Helpful references:

- staging-layer notes: `docs/staging_layer_notes.md`
- staging validation queries: `sql/09_staging_layer_checks.sql`

## Stage 4 Warehouse Layer

Run the warehouse-layer build from the project root:

`python3 scripts/warehouse_build.py`

Helpful references:

- warehouse-layer notes: `docs/warehouse_layer_notes.md`
- warehouse validation queries: `sql/10_warehouse_layer_checks.sql`

Stage 4 also includes lightweight warehouse summary views to support the mart layer without requiring full bridge-table population.

## Stage 5 Analytics Marts

Run the marts-layer build from the project root:

`python3 scripts/marts_build.py`

Helpful references:

- marts-layer notes: `docs/marts_layer_notes.md`
- marts validation queries: `sql/11_marts_layer_checks.sql`

## Stage 6 Advanced SQL

Run the advanced SQL build from the project root:

`python3 scripts/advanced_sql_build.py`

Refresh only the analytical materialized views:

`python3 scripts/analytics_refresh.py`

Helpful references:

- advanced SQL notes: `docs/advanced_sql_notes.md`
- advanced SQL validation queries: `sql/13_advanced_sql_checks.sql`

## Stage 7 Data Quality Framework

Run the quality framework from the project root:

`python3 scripts/quality_checks.py`

Helpful references:

- quality framework notes: `docs/quality_framework_notes.md`
- quality validation queries: `sql/14_quality_framework_checks.sql`

Stage 7 now includes:

- historical validation runs in `quality.validation_runs`
- configurable sparse-column thresholds in `quality.validation_thresholds`
- latest-check monitoring in `quality.latest_validation_summary`
- dashboard-ready run monitoring in `quality.validation_dashboard_summary`

## Stage 8 Ops Layer

The project now includes an operational metadata layer for future Airflow orchestration:

- `ops.batch_registry`
- `ops.pipeline_runs`
- `ops.pipeline_steps`
- `ops.latest_batch_status`
- `ops.pipeline_run_summary`
- `ops.pipeline_health_dashboard`
- `ops.batch_runtime_trend`

Helpful references:

- ops-layer notes: `docs/ops_layer_notes.md`
- ops validation queries: `sql/16_ops_layer_checks.sql`

The two main reporting views for ops monitoring are:

- `ops.pipeline_health_dashboard`
- `ops.batch_runtime_trend`

## Stage 9 Airflow Setup

Airflow is now prepared as a Docker-based orchestration layer.

Core files:

- `airflow/Dockerfile`
- `airflow/dags/ctr_batch_pipeline.py`
- `airflow/dags/ctr_incoming_file_sensor.py`
- `docs/airflow_setup_notes.md`

Start Airflow:

`docker compose up airflow-init`

`docker compose up -d airflow-webserver airflow-scheduler`

Airflow UI:

- `http://localhost:8080`

Recommended entrypoint:

- `ctr_incoming_file_sensor` is the primary production-style entrypoint
- `ctr_batch_pipeline` is now trigger-only and intended for:
  - sensor handoff
  - manual sample runs
  - controlled backfills or recovery runs

The Airflow setup now supports:

- manual sample runs through DAG config
- sensor-driven incoming-file pickup from `data/raw/incoming/`
- trigger-based handoff into `ctr_batch_pipeline`
- clean skip behavior when no incoming file is present

## Stage 10 Spark Batch Processing

The pipeline now includes a PySpark batch-processing step that runs before the SQL staging layer in Airflow.

Core files:

- `spark_jobs/spark_batch_processing.py`
- `docs/spark_processing_notes.md`

What this stage adds:

- local-mode Spark preprocessing through PySpark
- cleaned Parquet batch artifacts in `data/processed/<batch_name>/`
- batch metrics in JSON format
- column-level missingness profiles in CSV format
- ops-tracked artifact metadata in:
  - `ops.batch_artifacts`
  - `ops.latest_batch_artifacts`

You can also run the Spark step manually from the project root after a raw batch has been registered:

`python3 spark_jobs/spark_batch_processing.py --sample 1m --batch-name criteo_1m_batch`


## Stage 11 Feature Store

The pipeline now includes a dedicated ML-ready feature store layer.

Core files:

- `sql/17_feature_store.sql`
- `sql/18_feature_store_checks.sql`
- `scripts/feature_store_build.py`
- `docs/feature_store_notes.md`

Main output:

- `feature_store.ctr_training_features`

What this stage adds:

- log-scaled numeric features
- missingness flags
- numeric bucket-code features
- numeric bucket CTR-lift signals
- selected categorical CTR-lift signals
- a reusable training table for future CTR modeling

## Stage 12 Benchmark Capture

The pipeline now captures benchmark snapshots after completed runs.

Core files:

- `scripts/benchmark_capture.py`
- `docs/benchmarking_notes.md`

Main outputs:

- `ops.benchmark_snapshots`
- `ops.pipeline_benchmark_summary`

What this stage adds:

- stage runtime capture
- rows-per-second estimates by layer
- table storage tracking
- sample and processed artifact size tracking
- scale-up evidence for the move from `1M` to `5M`

Benchmark references:

- `docs/final_benchmark_report.md`
- `docs/batch_drift_notes.md`

## Stage 13 Incoming Batch Automation

The project now supports real incoming-file lifecycle management for production-style batch processing.

Core behavior:

- watch `data/raw/incoming/` for new CSV files
- auto-register a batch from the discovered file
- use checksum-based idempotency to avoid duplicate processing
- process only the current batch in raw, staging, and warehouse layers
- move successful files to `data/raw/archive/`
- move failed incoming files to `data/raw/failed/`

Core files:

- `scripts/incoming_batch.py`
- `airflow/dags/ctr_incoming_file_sensor.py`
- `airflow/dags/ctr_batch_pipeline.py`

Manual incoming-file discovery:

`python3 scripts/incoming_batch.py --action discover`

Manual file archival after a successful batch:

`python3 scripts/incoming_batch.py --action archive --batch-name <batch_name>`

Power BI handoff assets for the ops page now live under:

- `dashboards/power_bi/pipeline_operations_overview/`

## Stage 14 Batch Drift Monitoring

The project now includes cross-batch drift monitoring on top of the ops and quality layers.

Core files:

- `sql/19_batch_drift_views.sql`
- `sql/20_batch_drift_checks.sql`
- `docs/batch_drift_notes.md`

Main outputs:

- `ops.batch_metric_baseline`
- `ops.batch_drift_summary`
- `ops.batch_stage_runtime_drift`

What this stage adds:

- batch-to-batch CTR comparison
- missingness drift tracking
- row-count drift tracking
- runtime regression tracking by batch and by stage
- drift labels for operational review

## Stage 15 Config and Repo Cleanup

The project now includes a shared configuration layer and a cleaner Spark job structure.

Core files:

- `scripts/project_config.py`
- `spark_jobs/spark_batch_processing.py`
- `scripts/spark_batch_processing.py`
- `docs/config_and_repo_cleanup_notes.md`

What this stage adds:

- one shared place for project paths and database defaults
- reusable database-argument helpers for scripts
- a dedicated `spark_jobs/` area for Spark implementation code
- a compatibility wrapper so older Spark entry commands do not break immediately

## Architecture And Final Review

Helpful references:

- `docs/project_summary.md`
- `docs/architecture_overview.md`
- `docs/final_polish_review.md`

## Portfolio Positioning

This project now demonstrates:

- end-to-end SQL warehouse design
- PySpark batch preprocessing integrated into an Airflow pipeline
- layered raw, staging, warehouse, and marts architecture
- production-style audit logging and validation
- advanced analytical SQL with ranking, rolling metrics, cumulative metrics, and segment qualification
- performance-minded use of materialized views, indexes, and refreshable analytical assets
