from __future__ import annotations

import sys
from pathlib import Path

import psycopg
from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta


PROJECT_ROOT = "/opt/project"
SCRIPTS_DIR = f"{PROJECT_ROOT}/scripts"
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from project_config import DEFAULT_DB_HOST, DEFAULT_DB_NAME, DEFAULT_DB_PASSWORD, DEFAULT_DB_PORT, DEFAULT_DB_USER, INCOMING_DIR

PGHOST = DEFAULT_DB_HOST or "postgres"
PGPORT = DEFAULT_DB_PORT
PGDATABASE = DEFAULT_DB_NAME
PGUSER = DEFAULT_DB_USER if DEFAULT_DB_USER != "root" else "postgres"
PGPASSWORD = DEFAULT_DB_PASSWORD or "postgres"


def _pg_connect():
    return psycopg.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
    )


def incoming_csv_exists() -> bool:
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    return any(path.is_file() and path.suffix.lower() == ".csv" for path in INCOMING_DIR.iterdir())


def no_active_pipeline_run() -> bool:
    try:
        with _pg_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    select count(*)
                    from dag_run
                    where dag_id = 'ctr_batch_pipeline'
                      and state in ('queued', 'running');
                    """
                )
                active_runs = cursor.fetchone()[0]
                return active_runs == 0
    except Exception:
        return False


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}


with DAG(
    dag_id="ctr_incoming_file_sensor",
    description="Sensor-based DAG that watches the incoming folder and triggers the main CTR batch pipeline.",
    start_date=datetime(2026, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ctr", "airflow", "sensor", "incoming"],
) as dag:
    wait_for_incoming_file = PythonSensor(
        task_id="wait_for_incoming_file",
        python_callable=incoming_csv_exists,
        mode="reschedule",
        poke_interval=30,
        timeout=55,
        soft_fail=True,
    )

    ensure_main_pipeline_is_idle = ShortCircuitOperator(
        task_id="ensure_main_pipeline_is_idle",
        python_callable=no_active_pipeline_run,
    )

    trigger_batch_pipeline = TriggerDagRunOperator(
        task_id="trigger_batch_pipeline",
        trigger_dag_id="ctr_batch_pipeline",
        conf={"trigger_mode": "incoming_sensor"},
        reset_dag_run=False,
        wait_for_completion=False,
    )

    wait_for_incoming_file >> ensure_main_pipeline_is_idle >> trigger_batch_pipeline
