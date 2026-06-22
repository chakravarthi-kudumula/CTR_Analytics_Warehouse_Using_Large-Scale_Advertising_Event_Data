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

The first DAG supports:

- `sample`
- `batch_name`
- `run_ml`
- `ml_model_name`
- `ml_model_version`
- `ml_dataset_name`

Example Airflow run config:

```json
{
  "sample": "1m",
  "batch_name": "criteo_1m_airflow_batch",
  "run_ml": true,
  "ml_model_name": "ctr_logistic_regression",
  "ml_model_version": "v1"
}
```

### Optional ML Branch

The main processing DAG now includes an opt-in ML branch.

When `run_ml` is `true`, the DAG also runs:

1. `scripts/ml_setup.py`
2. `scripts/ml_training_dataset.py`
3. `scripts/train_ctr_sgd.py`
4. `scripts/score_ctr_batch_chunked.py`

This is intentionally opt-in so small incoming production-style batches do not retrain models automatically.

Recommended use:

- keep `run_ml` off for normal incoming-file ingestion
- turn `run_ml` on for canonical sample runs, controlled backfills, or explicit ML refreshes
- use `sample = 1m` plus `run_ml = true` when you want the full canonical ML path through Airflow

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
