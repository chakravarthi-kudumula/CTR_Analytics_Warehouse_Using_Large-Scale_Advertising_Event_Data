from __future__ import annotations

import json
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import psycopg
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup


PROJECT_ROOT = "/opt/project"
PYTHON_BIN = sys.executable or "python"
SCRIPTS_DIR = f"{PROJECT_ROOT}/scripts"
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from project_config import DEFAULT_DB_HOST, DEFAULT_DB_NAME, DEFAULT_DB_PASSWORD, DEFAULT_DB_PORT, DEFAULT_DB_USER

PGHOST = DEFAULT_DB_HOST or "postgres"
PGPORT = DEFAULT_DB_PORT
PGDATABASE = DEFAULT_DB_NAME
PGUSER = DEFAULT_DB_USER if DEFAULT_DB_USER != "root" else "postgres"
PGPASSWORD = DEFAULT_DB_PASSWORD or "postgres"
DISCOVERY_TASK_ID = "ops_layer.prepare_batch_context"


def _pg_connect():
    return psycopg.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
    )


def _safe_batch_lookup(batch_name: str | None) -> int | None:
    if not batch_name:
        return None
    try:
        with _pg_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("create schema if not exists ops;")
                cursor.execute(
                    "select batch_id from ops.batch_registry where batch_name = %s order by batch_id desc limit 1;",
                    (batch_name,),
                )
                row = cursor.fetchone()
                return row[0] if row else None
    except Exception:
        return None


def _fetch_batch_file_details(batch_name: str | None) -> dict[str, str | int | None] | None:
    if not batch_name:
        return None


def _update_batch_orchestration_run(batch_name: str | None, airflow_run_id: str | None) -> None:
    if not batch_name or not airflow_run_id:
        return
    try:
        with _pg_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    update ops.batch_registry
                    set orchestration_run_id = %s
                    where batch_name = %s;
                    """,
                    (airflow_run_id, batch_name),
                )
            connection.commit()
    except Exception as exc:
        print(f"Failed to update orchestration run id: {exc}")
    try:
        with _pg_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    select
                        batch_id,
                        source_path,
                        coalesce(source_type, 'sample'),
                        coalesce(batch_status, ''),
                        coalesce(archive_path, ''),
                        coalesce(failure_path, '')
                    from ops.batch_registry
                    where batch_name = %s
                    order by batch_id desc
                    limit 1;
                    """,
                    (batch_name,),
                )
                row = cursor.fetchone()
                if not row:
                    return None
                return {
                    "batch_id": row[0],
                    "source_path": row[1],
                    "source_type": row[2],
                    "batch_status": row[3],
                    "archive_path": row[4] or None,
                    "failure_path": row[5] or None,
                }
    except Exception:
        return None


def register_pipeline_alert(context, alert_level: str, alert_type: str, message: str) -> None:
    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance")
    batch_name = None
    if dag_run and dag_run.conf:
        batch_name = dag_run.conf.get("batch_name")
    if not batch_name and task_instance:
        try:
            batch_context = task_instance.xcom_pull(task_ids=DISCOVERY_TASK_ID)
            if batch_context:
                batch_name = batch_context.get("batch_name")
        except Exception:
            batch_name = None
    batch_id = _safe_batch_lookup(batch_name)
    layer_name = None
    if task_instance and getattr(task_instance, "task", None):
        task_group = getattr(task_instance.task, "task_group", None)
        layer_name = getattr(task_group, "group_id", None)

    payload = {
        "dag_id": context.get("dag").dag_id if context.get("dag") else None,
        "run_id": dag_run.run_id if dag_run else None,
        "task_id": task_instance.task_id if task_instance else None,
        "try_number": task_instance.try_number if task_instance else None,
        "execution_date": str(context.get("execution_date")),
        "batch_name": batch_name,
        "exception": str(context.get("exception")) if context.get("exception") else None,
    }

    try:
        with _pg_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("create schema if not exists ops;")
                cursor.execute(
                    """
                    create table if not exists ops.pipeline_alerts (
                        alert_id bigint generated always as identity primary key,
                        batch_id bigint,
                        pipeline_run_id bigint,
                        pipeline_name text,
                        task_name text,
                        layer_name text,
                        alert_level text not null,
                        alert_type text not null,
                        alert_message text not null,
                        alert_context jsonb,
                        created_at timestamptz not null default now(),
                        acknowledged_at timestamptz
                    );
                    """
                )
                cursor.execute(
                    """
                    insert into ops.pipeline_alerts (
                        batch_id,
                        alert_run_id,
                        pipeline_name,
                        task_name,
                        layer_name,
                        alert_level,
                        alert_type,
                        alert_message,
                        alert_context
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb);
                    """,
                    (
                        batch_id,
                        dag_run.run_id if dag_run else None,
                        context.get("dag").dag_id if context.get("dag") else None,
                        task_instance.task_id if task_instance else None,
                        layer_name,
                        alert_level,
                        alert_type,
                        message,
                        json.dumps(payload),
                    ),
                )
            connection.commit()
    except Exception as exc:
        print(f"Pipeline alert registration failed: {exc}")


def move_incoming_file_to_failed(context) -> None:
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    batch_name = None
    if dag_run and dag_run.conf:
        batch_name = dag_run.conf.get("batch_name")
    if not batch_name and task_instance:
        try:
            batch_context = task_instance.xcom_pull(task_ids=DISCOVERY_TASK_ID)
            if batch_context:
                batch_name = batch_context.get("batch_name")
        except Exception:
            batch_name = None
    batch_details = _fetch_batch_file_details(batch_name)
    if not batch_details:
        return
    if batch_details.get("source_type") != "incoming":
        return
    source_path = batch_details.get("source_path")
    if not source_path:
        return
    source = Path(str(source_path))
    if not source.exists():
        return
    failed_dir = Path(PROJECT_ROOT) / "data" / "raw" / "failed"
    failed_dir.mkdir(parents=True, exist_ok=True)
    target = failed_dir / source.name
    if target.exists():
        target = failed_dir / f"{source.stem}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}{source.suffix}"
    try:
        shutil.move(str(source), str(target))
        with _pg_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    update ops.batch_registry
                    set batch_status = 'FAILED_FILE_MOVED',
                        failure_path = %s,
                        source_moved_at = now()
                    where batch_name = %s;
                    """,
                    (str(target), batch_name),
                )
            connection.commit()
    except Exception as exc:
        print(f"Failed to move incoming file to failed: {exc}")


def failure_callback(context) -> None:
    register_pipeline_alert(
        context,
        alert_level="error",
        alert_type="task_failure",
        message=f"Task {context['task_instance'].task_id} failed in DAG {context['dag'].dag_id}.",
    )
    move_incoming_file_to_failed(context)


def retry_callback(context) -> None:
    register_pipeline_alert(
        context,
        alert_level="warning",
        alert_type="task_retry",
        message=f"Task {context['task_instance'].task_id} is retrying in DAG {context['dag'].dag_id}.",
    )


def prepare_batch_context(**context):
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    airflow_run_id = dag_run.run_id if dag_run else None
    sample = conf.get("sample")
    batch_name = conf.get("batch_name")
    source_path = conf.get("source_path")

    if sample:
        payload = {
            "mode": "sample",
            "sample": sample,
            "batch_name": batch_name or f"criteo_{sample}_airflow_batch",
            "sample_scale": sample,
            "source_path": None,
            "source_file": f"criteo_{sample}.csv",
        }
        return payload

    command = [PYTHON_BIN, "scripts/incoming_batch.py", "--action", "discover"]
    if batch_name:
        command.extend(["--batch-name", batch_name])
    if source_path:
        command.extend(["--source-path", source_path])

    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    payload = result.stdout.strip().splitlines()[-1] if result.stdout.strip() else "NO_BATCH_FOUND"
    if payload == "NO_BATCH_FOUND":
        return None

    batch_id, discovered_batch_name, source_file, discovered_path, sample_scale, should_process, archive_path, failed_path = payload.split("|", 7)
    payload = {
        "mode": "incoming",
        "batch_id": int(batch_id),
        "batch_name": discovered_batch_name,
        "sample": None,
        "sample_scale": sample_scale,
        "source_path": discovered_path,
        "source_file": source_file,
        "should_process": should_process == "true",
        "archive_path": archive_path,
        "failed_path": failed_path,
    }
    _update_batch_orchestration_run(discovered_batch_name, airflow_run_id)
    return payload


def should_run_pipeline(**context) -> bool:
    batch_context = context["ti"].xcom_pull(task_ids=DISCOVERY_TASK_ID)
    if not batch_context:
        return False
    return bool(batch_context.get("should_process", True) if batch_context.get("mode") == "incoming" else True)


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "on_failure_callback": failure_callback,
    "on_retry_callback": retry_callback,
}


with DAG(
    dag_id="ctr_batch_pipeline",
    description="Batch orchestration for the CTR analytics warehouse pipeline.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ctr", "warehouse", "batch", "airflow", "spark", "feature_store", "incoming"],
) as dag:
    with TaskGroup(group_id="ops_layer") as ops_layer:
        prepare_batch_context_task = PythonOperator(
            task_id="prepare_batch_context",
            python_callable=prepare_batch_context,
        )

        has_batch_to_process = ShortCircuitOperator(
            task_id="has_batch_to_process",
            python_callable=should_run_pipeline,
        )

        capture_benchmarks = BashOperator(
            task_id="capture_benchmarks",
            execution_timeout=timedelta(minutes=10),
            bash_command=(
                f"cd {PROJECT_ROOT} && "
                f"{PYTHON_BIN} scripts/benchmark_capture.py "
                "--batch-name '{{ ti.xcom_pull(task_ids=\"ops_layer.prepare_batch_context\")[\"batch_name\"] }}' "
                "--triggered-by airflow"
            ),
        )

        archive_incoming_file = BashOperator(
            task_id="archive_incoming_file",
            execution_timeout=timedelta(minutes=5),
            bash_command=(
                "{% set ctx = ti.xcom_pull(task_ids='ops_layer.prepare_batch_context') %}"
                f"cd {PROJECT_ROOT} && "
                "if [ \"{{ ctx['mode'] }}\" = \"incoming\" ]; then "
                f"{PYTHON_BIN} scripts/incoming_batch.py --action archive --batch-name '{{{{ ctx['batch_name'] }}}}'; "
                "else echo 'SKIP_ARCHIVE_SAMPLE_BATCH'; fi"
            ),
        )

        prepare_batch_context_task >> has_batch_to_process

    with TaskGroup(group_id="raw_layer") as raw_layer:
        load_raw = BashOperator(
            task_id="load_raw",
            execution_timeout=timedelta(minutes=20),
            bash_command=(
                "{% set ctx = ti.xcom_pull(task_ids='ops_layer.prepare_batch_context') %}"
                f"cd {PROJECT_ROOT} && "
                f"{PYTHON_BIN} scripts/data_ingestion.py "
                "{% if ctx['sample'] %}--sample '{{ ctx['sample'] }}' {% else %}--source-path '{{ ctx['source_path'] }}' --sample-scale '{{ ctx['sample_scale'] }}' {% endif %}"
                "--batch-name '{{ ctx['batch_name'] }}' "
                "--triggered-by airflow"
            ),
        )

    with TaskGroup(group_id="spark_layer") as spark_layer:
        process_batch = BashOperator(
            task_id="process_batch",
            execution_timeout=timedelta(minutes=25),
            bash_command=(
                "{% set ctx = ti.xcom_pull(task_ids='ops_layer.prepare_batch_context') %}"
                f"cd {PROJECT_ROOT} && "
                f"{PYTHON_BIN} spark_jobs/spark_batch_processing.py "
                "{% if ctx['sample'] %}--sample '{{ ctx['sample'] }}' {% endif %}"
                "--batch-name '{{ ctx['batch_name'] }}' "
                "--triggered-by airflow"
            ),
        )

    with TaskGroup(group_id="transformation_layers") as transformation_layers:
        build_staging = BashOperator(
            task_id="build_staging",
            execution_timeout=timedelta(minutes=20),
            bash_command=(
                "{% set ctx = ti.xcom_pull(task_ids='ops_layer.prepare_batch_context') %}"
                f"cd {PROJECT_ROOT} && "
                f"{PYTHON_BIN} scripts/staging_load.py --batch-name '{{{{ ctx['batch_name'] }}}}' --triggered-by airflow"
            ),
        )

        build_warehouse = BashOperator(
            task_id="build_warehouse",
            execution_timeout=timedelta(minutes=25),
            bash_command=(
                "{% set ctx = ti.xcom_pull(task_ids='ops_layer.prepare_batch_context') %}"
                f"cd {PROJECT_ROOT} && "
                f"{PYTHON_BIN} scripts/warehouse_build.py --batch-name '{{{{ ctx['batch_name'] }}}}' --triggered-by airflow"
            ),
        )

        build_staging >> build_warehouse

    with TaskGroup(group_id="analytics_layers") as analytics_layers:
        build_marts = BashOperator(
            task_id="build_marts",
            execution_timeout=timedelta(minutes=15),
            bash_command=(
                "{% set ctx = ti.xcom_pull(task_ids='ops_layer.prepare_batch_context') %}"
                f"cd {PROJECT_ROOT} && "
                f"{PYTHON_BIN} scripts/marts_build.py --batch-name '{{{{ ctx['batch_name'] }}}}' --triggered-by airflow"
            ),
        )

        build_advanced_sql = BashOperator(
            task_id="build_advanced_sql",
            execution_timeout=timedelta(minutes=10),
            bash_command=(
                "{% set ctx = ti.xcom_pull(task_ids='ops_layer.prepare_batch_context') %}"
                f"cd {PROJECT_ROOT} && "
                f"{PYTHON_BIN} scripts/advanced_sql_build.py --batch-name '{{{{ ctx['batch_name'] }}}}' --triggered-by airflow"
            ),
        )

        build_feature_store = BashOperator(
            task_id="build_feature_store",
            execution_timeout=timedelta(minutes=15),
            bash_command=(
                "{% set ctx = ti.xcom_pull(task_ids='ops_layer.prepare_batch_context') %}"
                f"cd {PROJECT_ROOT} && "
                f"{PYTHON_BIN} scripts/feature_store_build.py --batch-name '{{{{ ctx['batch_name'] }}}}' --triggered-by airflow"
            ),
        )

        build_marts >> build_advanced_sql >> build_feature_store

    with TaskGroup(group_id="quality_layer") as quality_layer:
        run_quality_checks = BashOperator(
            task_id="run_quality_checks",
            execution_timeout=timedelta(minutes=10),
            bash_command=(
                "{% set ctx = ti.xcom_pull(task_ids='ops_layer.prepare_batch_context') %}"
                f"cd {PROJECT_ROOT} && "
                f"{PYTHON_BIN} scripts/quality_checks.py --batch-name '{{{{ ctx['batch_name'] }}}}' --triggered-by airflow"
            ),
        )

    with TaskGroup(group_id="ml_layer") as ml_layer:
        setup_ml_foundation = BashOperator(
            task_id="setup_ml_foundation",
            execution_timeout=timedelta(minutes=10),
            bash_command=(
                "{% set run_ml = dag_run.conf.get('run_ml', false) if dag_run and dag_run.conf else false %}"
                f"cd {PROJECT_ROOT} && "
                "if [ \"{{ run_ml }}\" = \"True\" ] || [ \"{{ run_ml }}\" = \"true\" ]; then "
                f"{PYTHON_BIN} scripts/ml_setup.py; "
                "else echo 'SKIP_ML_SETUP'; fi"
            ),
        )

        build_ml_training_dataset = BashOperator(
            task_id="build_ml_training_dataset",
            execution_timeout=timedelta(minutes=20),
            bash_command=(
                "{% set ctx = ti.xcom_pull(task_ids='ops_layer.prepare_batch_context') %}"
                "{% set run_ml = dag_run.conf.get('run_ml', false) if dag_run and dag_run.conf else false %}"
                "{% set dataset_name = dag_run.conf.get('ml_dataset_name') if dag_run and dag_run.conf else none %}"
                f"cd {PROJECT_ROOT} && "
                "if [ \"{{ run_ml }}\" = \"True\" ] || [ \"{{ run_ml }}\" = \"true\" ]; then "
                f"{PYTHON_BIN} scripts/ml_training_dataset.py "
                "--batch-name '{{ ctx['batch_name'] }}' "
                "{% if dataset_name %}--dataset-name '{{ dataset_name }}' {% endif %}"
                "--triggered-by airflow; "
                "else echo 'SKIP_ML_TRAINING_DATASET'; fi"
            ),
        )

        train_ml_baseline = BashOperator(
            task_id="train_ml_baseline",
            execution_timeout=timedelta(minutes=45),
            bash_command=(
                "{% set ctx = ti.xcom_pull(task_ids='ops_layer.prepare_batch_context') %}"
                "{% set run_ml = dag_run.conf.get('run_ml', false) if dag_run and dag_run.conf else false %}"
                "{% set model_name = dag_run.conf.get('ml_model_name', 'ctr_logistic_regression') if dag_run and dag_run.conf else 'ctr_logistic_regression' %}"
                "{% set model_version = dag_run.conf.get('ml_model_version', 'v1') if dag_run and dag_run.conf else 'v1' %}"
                "{% set dataset_name = dag_run.conf.get('ml_dataset_name') if dag_run and dag_run.conf else none %}"
                f"cd {PROJECT_ROOT} && "
                "if [ \"{{ run_ml }}\" = \"True\" ] || [ \"{{ run_ml }}\" = \"true\" ]; then "
                f"{PYTHON_BIN} scripts/train_ctr_sgd.py "
                "--batch-name '{{ ctx['batch_name'] }}' "
                "--model-name '{{ model_name }}' "
                "--model-version '{{ model_version }}' "
                "{% if dataset_name %}--dataset-name '{{ dataset_name }}' {% endif %}"
                "--triggered-by airflow; "
                "else echo 'SKIP_ML_TRAINING'; fi"
            ),
        )

        score_ml_batch = BashOperator(
            task_id="score_ml_batch",
            execution_timeout=timedelta(minutes=45),
            bash_command=(
                "{% set ctx = ti.xcom_pull(task_ids='ops_layer.prepare_batch_context') %}"
                "{% set run_ml = dag_run.conf.get('run_ml', false) if dag_run and dag_run.conf else false %}"
                "{% set model_name = dag_run.conf.get('ml_model_name', 'ctr_logistic_regression') if dag_run and dag_run.conf else 'ctr_logistic_regression' %}"
                "{% set model_version = dag_run.conf.get('ml_model_version', 'v1') if dag_run and dag_run.conf else 'v1' %}"
                f"cd {PROJECT_ROOT} && "
                "if [ \"{{ run_ml }}\" = \"True\" ] || [ \"{{ run_ml }}\" = \"true\" ]; then "
                f"{PYTHON_BIN} scripts/score_ctr_batch_chunked.py "
                "--batch-name '{{ ctx['batch_name'] }}' "
                "--model-name '{{ model_name }}' "
                "--model-version '{{ model_version }}' "
                "--triggered-by airflow; "
                "else echo 'SKIP_ML_SCORING'; fi"
            ),
        )

        setup_ml_foundation >> build_ml_training_dataset >> train_ml_baseline >> score_ml_batch

    has_batch_to_process >> raw_layer >> spark_layer >> transformation_layers >> analytics_layers >> quality_layer >> ml_layer >> capture_benchmarks >> archive_incoming_file
