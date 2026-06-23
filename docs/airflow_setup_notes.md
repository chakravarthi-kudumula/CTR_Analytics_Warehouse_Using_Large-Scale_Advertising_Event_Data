# Airflow Setup Notes

This phase adds Airflow as the orchestration layer for the CTR analytics platform.

## What Was Added

- `airflow/Dockerfile`
- `airflow/dags/ctr_batch_pipeline.py`
- `airflow/logs/`
- `airflow/plugins/`
- updated `docker-compose.yml`

## Services

- `postgres`
- `airflow-init`
- `airflow-webserver`
- `airflow-scheduler`

## Why Docker-Based Airflow

Airflow is better treated as a service than as a casual local `pip install`.

This setup:

- keeps Airflow isolated
- matches production-style orchestration patterns
- avoids local dependency drift
- lets the project run DAGs against the same repository code

## Dependency Note

Airflow provider packages are more stable with `pandas 2.1.x` than with `pandas 3.x`.

The project requirements are pinned accordingly:

- `pandas>=2.1.4,<2.2.0`

If you rebuild the Airflow image later, it will pick up that safer version automatically.

## First DAG

Current DAG:

- `ctr_batch_pipeline`
- `ctr_incoming_file_sensor`

Current task flow:

1. raw load
2. Spark preprocessing
3. staging build
4. warehouse build
5. marts build
6. advanced SQL build
7. feature store build
8. quality checks
9. optional ML dataset build, training, and scoring
10. benchmark capture and archive lifecycle

Sensor DAG flow:

1. wait for a CSV in `data/raw/incoming/`
2. confirm `ctr_batch_pipeline` is not already running
3. trigger `ctr_batch_pipeline`

## Start Commands

From the project root:

```bash
docker compose up airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

Airflow UI:

- `http://localhost:8080`

Default admin user created by init:

- username: `admin`
- password: `admin`

## DAG Run Parameters

`ctr_batch_pipeline` supports:

- `sample`
- `batch_name`

Example Airflow run config:

```json
{
  "sample": "1m",
  "batch_name": "criteo_1m_airflow_batch"
}
```

## Dedicated ML DAG

The project now uses a separate ML DAG:

- `ctr_ml_pipeline`

It expects:

- `batch_name`
- optional `ml_dataset_name`
- optional `ml_model_name`
- optional `ml_model_version`
- optional `ml_chunksize`
- optional `ml_epochs`

Default scheduled retraining strategy:

- schedule: `30 3 * * 0`
- target batch: `criteo_1m_ml_canonical_batch`
- default model family: `ctr_logistic_regression`
- default manual model version: `v3`
- default scheduled model version pattern: `v3_YYYYMMDD`

Example ML DAG config:

```json
{
  "batch_name": "criteo_1m_ml_canonical_batch",
  "ml_model_name": "ctr_logistic_regression",
  "ml_model_version": "v3",
  "ml_chunksize": 10000
}
```

This DAG runs:

1. `scripts/ml_setup.py`
2. `scripts/ml_training_dataset.py`
3. `scripts/train_ctr_sgd.py`
4. `scripts/extract_model_feature_importance.py`
5. `scripts/score_ctr_batch_chunked.py`
6. `scripts/benchmark_capture.py`

Why it is separate:

- incoming production-style batches should not retrain models automatically
- the main pipeline stays focused on ingestion and analytics serving
- ML refreshes can be scheduled or triggered independently on canonical batches and controlled experiments

## Sensor-Based Intake

The project now has a separate Airflow sensor DAG:

- `ctr_incoming_file_sensor`

This DAG is meant for event-style local batch pickup:

- it watches `data/raw/incoming/`
- it waits for a `.csv` file
- it triggers `ctr_batch_pipeline` only when the main pipeline is idle

Recommended usage:

- keep `ctr_batch_pipeline` as the main processing DAG
- use `ctr_incoming_file_sensor` as the primary production-style entrypoint
- `ctr_batch_pipeline` is now trigger-only and is meant for:
  - sensor handoff
  - manual sample runs
  - recovery or backfill runs

## Current Recommendation

Use this orchestration pattern:

1. `ctr_incoming_file_sensor` watches `data/raw/incoming/`
2. the sensor DAG triggers `ctr_batch_pipeline` only when a CSV is present
3. `ctr_batch_pipeline` processes the discovered batch and archives the file on success

This avoids overlapping schedules and makes the entrypoint behavior much easier to explain in interviews and demos.

## Next Improvements

- add batch validation and registration as separate DAG tasks
- add ops-summary checks as post-run validation
- move heavy preprocessing into PySpark tasks
- add failure callbacks and alerting
- add a 5M batch DAG run after PySpark integration
