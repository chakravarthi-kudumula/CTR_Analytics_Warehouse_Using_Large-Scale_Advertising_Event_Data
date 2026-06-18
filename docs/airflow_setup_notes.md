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
2. staging build
3. warehouse build
4. marts build
5. advanced SQL build
6. quality checks

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

Example Airflow run config:

```json
{
  "sample": "1m",
  "batch_name": "criteo_1m_airflow_batch"
}
```

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
