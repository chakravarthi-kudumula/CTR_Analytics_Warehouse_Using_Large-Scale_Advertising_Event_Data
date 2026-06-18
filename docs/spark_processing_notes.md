# Spark Processing Notes

## Purpose

This project now includes a PySpark batch-processing step that runs before the SQL staging build in Airflow. The Spark step does not replace the PostgreSQL raw or staging layers. Instead, it adds a scalable preprocessing and profiling branch that prepares reusable batch artifacts.

The canonical Spark job now lives at:

- `spark_jobs/spark_batch_processing.py`

A compatibility wrapper still exists at:

- `scripts/spark_batch_processing.py`

## Current Spark Outputs

For each batch, the Spark step writes artifacts under:

`data/processed/<batch_name>/`

Artifacts created:

- `stg_criteo_events_parquet/`
  - cleaned row-level batch output in Parquet format
- `batch_metrics.json`
  - batch-level CTR and missingness summary
- `missing_value_profile.csv`
  - column-level missingness profile

## Current Design Choice

The raw PostgreSQL layer still stores the source file exactly as received.

The Spark step is used for:

- scalable preprocessing
- reusable processed artifacts
- fast batch-level profiling
- future feature engineering expansion

This keeps the warehouse honest to the raw data while also making the platform more realistic for larger-scale processing.

## Airflow Placement

Current batch flow:

- raw load
- spark batch processing
- SQL staging build
- warehouse build
- marts build
- advanced SQL build
- quality checks

## Ops Metadata

Spark artifacts are tracked in:

- `ops.batch_artifacts`
- `ops.latest_batch_artifacts`

This gives each batch a traceable artifact history rather than leaving processed files unmanaged on disk.

## Current Runtime Mode

Spark runs in local mode through PySpark inside the Airflow image.

That means:

- no separate Spark cluster is required
- the project remains laptop-friendly
- the architecture can later move to a larger Spark runtime if needed

## Next Logical Expansion

Possible future upgrades:

- read raw data through Spark from larger external storage
- write curated Spark outputs to a dedicated feature-store schema
- push Spark metrics into dashboard-ready reporting tables
- move from local Spark mode to a containerized Spark service if the project grows beyond local limits
