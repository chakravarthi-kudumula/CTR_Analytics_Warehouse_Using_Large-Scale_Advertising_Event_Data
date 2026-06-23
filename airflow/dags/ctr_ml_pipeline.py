from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/opt/project"
PYTHON_BIN = sys.executable or "python"
SCRIPTS_DIR = f"{PROJECT_ROOT}/scripts"
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)


def validate_ml_run_config(**context):
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    batch_name = conf.get("batch_name")
    if not batch_name:
        raise ValueError("batch_name is required for ctr_ml_pipeline runs.")
    return {
        "batch_name": batch_name,
        "dataset_name": conf.get("ml_dataset_name") or f"ml_training_dataset_{batch_name}",
        "model_name": conf.get("ml_model_name", "ctr_logistic_regression"),
        "model_version": conf.get("ml_model_version", "v3"),
        "chunksize": int(conf.get("ml_chunksize", 10000)),
        "epochs": int(conf.get("ml_epochs", 2)),
    }


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
}


with DAG(
    dag_id="ctr_ml_pipeline",
    description="Dedicated ML orchestration for CTR training datasets, training, scoring, and monitoring.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ctr", "ml", "airflow", "training", "scoring"],
) as dag:
    prepare_ml_context = PythonOperator(
        task_id="prepare_ml_context",
        python_callable=validate_ml_run_config,
    )

    setup_ml_foundation = BashOperator(
        task_id="setup_ml_foundation",
        execution_timeout=timedelta(minutes=10),
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{PYTHON_BIN} scripts/ml_setup.py"
        ),
    )

    build_ml_training_dataset = BashOperator(
        task_id="build_ml_training_dataset",
        execution_timeout=timedelta(minutes=30),
        bash_command=(
            "{% set ctx = ti.xcom_pull(task_ids='prepare_ml_context') %}"
            f"cd {PROJECT_ROOT} && "
            f"{PYTHON_BIN} scripts/ml_training_dataset.py "
            "--batch-name '{{ ctx['batch_name'] }}' "
            "--dataset-name '{{ ctx['dataset_name'] }}' "
            "--triggered-by airflow_ml"
        ),
    )

    train_ml_baseline = BashOperator(
        task_id="train_ml_baseline",
        execution_timeout=timedelta(minutes=60),
        bash_command=(
            "{% set ctx = ti.xcom_pull(task_ids='prepare_ml_context') %}"
            f"cd {PROJECT_ROOT} && "
            f"{PYTHON_BIN} scripts/train_ctr_sgd.py "
            "--batch-name '{{ ctx['batch_name'] }}' "
            "--dataset-name '{{ ctx['dataset_name'] }}' "
            "--model-name '{{ ctx['model_name'] }}' "
            "--model-version '{{ ctx['model_version'] }}' "
            "--chunksize '{{ ctx['chunksize'] }}' "
            "--epochs '{{ ctx['epochs'] }}' "
            "--triggered-by airflow_ml"
        ),
    )

    score_ml_batch = BashOperator(
        task_id="score_ml_batch",
        execution_timeout=timedelta(minutes=60),
        bash_command=(
            "{% set ctx = ti.xcom_pull(task_ids='prepare_ml_context') %}"
            f"cd {PROJECT_ROOT} && "
            f"{PYTHON_BIN} scripts/score_ctr_batch_chunked.py "
            "--batch-name '{{ ctx['batch_name'] }}' "
            "--model-name '{{ ctx['model_name'] }}' "
            "--model-version '{{ ctx['model_version'] }}' "
            "--chunksize '{{ ctx['chunksize'] }}' "
            "--triggered-by airflow_ml"
        ),
    )

    capture_ml_benchmarks = BashOperator(
        task_id="capture_ml_benchmarks",
        execution_timeout=timedelta(minutes=15),
        bash_command=(
            "{% set ctx = ti.xcom_pull(task_ids='prepare_ml_context') %}"
            f"cd {PROJECT_ROOT} && "
            f"{PYTHON_BIN} scripts/benchmark_capture.py "
            "--batch-name '{{ ctx['batch_name'] }}' "
            "--triggered-by airflow_ml"
        ),
    )

    prepare_ml_context >> setup_ml_foundation >> build_ml_training_dataset >> train_ml_baseline >> score_ml_batch >> capture_ml_benchmarks
