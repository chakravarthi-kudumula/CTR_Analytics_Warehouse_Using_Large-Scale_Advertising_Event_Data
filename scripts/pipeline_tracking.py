#!/usr/bin/env python3

"""Shared database and metadata helpers for batch-aware pipeline runs."""

from __future__ import annotations

import os
import json
import hashlib
from pathlib import Path
import subprocess


def sql_literal(value: str) -> str:
    return value.replace("'", "''")


def build_connection_command(base_command: list[str], database: str, args) -> list[str]:
    command = base_command.copy()
    if args.host:
        command.extend(["-h", args.host])
    if args.port:
        command.extend(["-p", str(args.port)])
    if args.user:
        command.extend(["-U", args.user])
    command.extend(["-d", database])
    return command


def build_env(args) -> dict[str, str]:
    env = os.environ.copy()
    if getattr(args, "password", None):
        env["PGPASSWORD"] = args.password
    else:
        env.pop("PGPASSWORD", None)
    return env


def run_psql(sql_file: Path, database: str, args) -> None:
    command = build_connection_command(["psql"], database, args)
    command.extend(["-v", "ON_ERROR_STOP=1", "-f", str(sql_file)])
    subprocess.run(command, check=True, env=build_env(args))


def run_query(query: str, database: str, args) -> None:
    command = build_connection_command(["psql"], database, args)
    command.extend(["-v", "ON_ERROR_STOP=1", "-c", query])
    subprocess.run(command, check=True, env=build_env(args))


def run_scalar_query(query: str, database: str, args) -> str:
    command = build_connection_command(["psql"], database, args)
    command.extend(["-t", "-A", "-c", query])
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        env=build_env(args),
    )
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    return lines[0] if lines else ""


def ensure_pipeline_metadata(sql_dir: Path, database: str, args) -> None:
    run_psql(sql_dir / "01_create_schemas.sql", database, args)
    run_psql(sql_dir / "15_pipeline_metadata.sql", database, args)


def compute_file_sha256(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def register_batch(
    *,
    batch_name: str,
    source_file: str,
    source_path: str,
    sample_scale: str,
    expected_row_count: int | None,
    notes: str | None,
    database: str,
    args,
) -> int:
    expected_value = "null" if expected_row_count is None else str(expected_row_count)
    notes_value = "null" if not notes else f"'{sql_literal(notes)}'"
    query = f"""
insert into ops.batch_registry (
    batch_name,
    source_file,
    source_path,
    sample_scale,
    expected_row_count,
    notes
)
values (
    '{sql_literal(batch_name)}',
    '{sql_literal(source_file)}',
    '{sql_literal(source_path)}',
    '{sql_literal(sample_scale)}',
    {expected_value},
    {notes_value}
)
on conflict (batch_name)
do update
set
    source_file = excluded.source_file,
    source_path = excluded.source_path,
    sample_scale = excluded.sample_scale,
    expected_row_count = excluded.expected_row_count,
    notes = coalesce(excluded.notes, ops.batch_registry.notes)
returning batch_id;
"""
    return int(run_scalar_query(query, database, args))


def register_incoming_batch(
    *,
    batch_name: str,
    source_file: str,
    source_path: str,
    source_checksum: str,
    sample_scale: str,
    expected_row_count: int | None,
    notes: str | None,
    database: str,
    args,
) -> int:
    expected_value = "null" if expected_row_count is None else str(expected_row_count)
    notes_value = "null" if not notes else f"'{sql_literal(notes)}'"
    query = f"""
with updated_batch as (
    update ops.batch_registry
    set
        source_file = '{sql_literal(source_file)}',
        source_path = '{sql_literal(source_path)}',
        expected_row_count = coalesce({expected_value}, expected_row_count),
        notes = coalesce({notes_value}, notes)
    where source_checksum = '{sql_literal(source_checksum)}'
    returning batch_id
),
inserted_batch as (
    insert into ops.batch_registry (
        batch_name,
        source_file,
        source_path,
        source_checksum,
        source_type,
        sample_scale,
        expected_row_count,
        notes
    )
    select
        '{sql_literal(batch_name)}',
        '{sql_literal(source_file)}',
        '{sql_literal(source_path)}',
        '{sql_literal(source_checksum)}',
        'incoming',
        '{sql_literal(sample_scale)}',
        {expected_value},
        {notes_value}
    where not exists (select 1 from updated_batch)
    returning batch_id
)
select batch_id from inserted_batch
union all
select batch_id from updated_batch
limit 1;
"""
    return int(run_scalar_query(query, database, args))


def create_pipeline_run(
    *,
    batch_id: int,
    pipeline_name: str,
    layer_name: str,
    source_file: str,
    triggered_by: str,
    database: str,
    args,
) -> int:
    query = f"""
insert into ops.pipeline_runs (
    batch_id,
    pipeline_name,
    layer_name,
    source_file,
    triggered_by
)
values (
    {batch_id},
    '{sql_literal(pipeline_name)}',
    '{sql_literal(layer_name)}',
    '{sql_literal(source_file)}',
    '{sql_literal(triggered_by)}'
)
returning pipeline_run_id;
"""
    return int(run_scalar_query(query, database, args))


def complete_pipeline_run(
    *,
    pipeline_run_id: int,
    run_status: str,
    run_message: str,
    database: str,
    args,
) -> None:
    query = f"""
update ops.pipeline_runs
set
    run_status = '{sql_literal(run_status)}',
    run_message = '{sql_literal(run_message)}',
    completed_at = now()
where pipeline_run_id = {pipeline_run_id};
"""
    run_query(query, database, args)


def create_pipeline_step(
    *,
    pipeline_run_id: int,
    batch_id: int,
    step_name: str,
    layer_name: str,
    target_table: str,
    source_file: str,
    database: str,
    args,
) -> int:
    query = f"""
insert into ops.pipeline_steps (
    pipeline_run_id,
    batch_id,
    step_name,
    layer_name,
    target_table,
    source_file
)
values (
    {pipeline_run_id},
    {batch_id},
    '{sql_literal(step_name)}',
    '{sql_literal(layer_name)}',
    '{sql_literal(target_table)}',
    '{sql_literal(source_file)}'
)
returning pipeline_step_id;
"""
    return int(run_scalar_query(query, database, args))


def complete_pipeline_step(
    *,
    pipeline_step_id: int,
    step_status: str,
    rows_processed: int | None,
    step_message: str,
    database: str,
    args,
) -> None:
    rows_value = "null" if rows_processed is None else str(rows_processed)
    query = f"""
update ops.pipeline_steps
set
    step_status = '{sql_literal(step_status)}',
    rows_processed = {rows_value},
    step_message = '{sql_literal(step_message)}',
    completed_at = now()
where pipeline_step_id = {pipeline_step_id};
"""
    run_query(query, database, args)


def update_batch_status(
    *,
    batch_id: int,
    batch_status: str,
    last_successful_stage: str | None,
    row_column: str | None,
    row_count: int | None,
    mark_started: bool,
    mark_completed: bool,
    database: str,
    args,
) -> None:
    updates = [f"batch_status = '{sql_literal(batch_status)}'"]
    if last_successful_stage is not None:
        updates.append(f"last_successful_stage = '{sql_literal(last_successful_stage)}'")
    if row_column and row_count is not None:
        updates.append(f"{row_column} = {row_count}")
    if mark_started:
        updates.append("started_at = coalesce(started_at, now())")
    if mark_completed:
        updates.append("completed_at = now()")

    query = f"""
update ops.batch_registry
set
    {', '.join(updates)}
where batch_id = {batch_id};
"""
    run_query(query, database, args)


def fetch_batch_context(table_name: str, database: str, args) -> tuple[int, str]:
    query = f"""
select batch_id, source_file
from {table_name}
where batch_id is not null
group by batch_id, source_file
order by max(ingested_at) desc
limit 1;
"""
    row = run_scalar_query(query, database, args)
    if not row:
        raise RuntimeError(f"No batch-aware rows found in {table_name}. Load the upstream layer first.")
    batch_id_text, source_file = row.split("|", 1)
    return int(batch_id_text), source_file


def fetch_batch_by_name(batch_name: str, database: str, args) -> tuple[int, str, str]:
    query = f"""
select batch_id, source_file, sample_scale
from ops.batch_registry
where batch_name = '{sql_literal(batch_name)}'
order by batch_id desc
limit 1;
"""
    row = run_scalar_query(query, database, args)
    if not row:
        raise RuntimeError(f"No batch found for batch_name={batch_name}. Run the raw layer first.")
    batch_id_text, source_file, sample_scale = row.split("|", 2)
    return int(batch_id_text), source_file, sample_scale


def fetch_batch_details(batch_name: str, database: str, args) -> dict[str, str | int | None]:
    query = f"""
select
    batch_id,
    batch_name,
    source_file,
    sample_scale,
    coalesce(source_path, ''),
    coalesce(source_type, 'sample'),
    coalesce(source_checksum, ''),
    coalesce(batch_status, ''),
    coalesce(last_successful_stage, ''),
    coalesce(archive_path, ''),
    coalesce(failure_path, '')
from ops.batch_registry
where batch_name = '{sql_literal(batch_name)}'
order by batch_id desc
limit 1;
"""
    row = run_scalar_query(query, database, args)
    if not row:
        raise RuntimeError(f"No batch found for batch_name={batch_name}.")
    (
        batch_id_text,
        batch_name,
        source_file,
        sample_scale,
        source_path,
        source_type,
        source_checksum,
        batch_status,
        last_successful_stage,
        archive_path,
        failure_path,
    ) = row.split("|", 10)
    return {
        "batch_id": int(batch_id_text),
        "batch_name": batch_name,
        "source_file": source_file,
        "sample_scale": sample_scale,
        "source_path": source_path or None,
        "source_type": source_type,
        "source_checksum": source_checksum or None,
        "batch_status": batch_status or None,
        "last_successful_stage": last_successful_stage or None,
        "archive_path": archive_path or None,
        "failure_path": failure_path or None,
    }


def resolve_batch_context(
    *,
    table_name: str,
    batch_name: str | None,
    database: str,
    args,
) -> tuple[int, str]:
    if batch_name:
        details = fetch_batch_details(batch_name, database, args)
        return int(details["batch_id"]), str(details["source_file"])
    return fetch_batch_context(table_name, database, args)


def mark_batch_file_location(
    *,
    batch_id: int,
    archive_path: str | None,
    failure_path: str | None,
    database: str,
    args,
) -> None:
    archive_value = "null" if not archive_path else f"'{sql_literal(archive_path)}'"
    failure_value = "null" if not failure_path else f"'{sql_literal(failure_path)}'"
    query = f"""
update ops.batch_registry
set
    archive_path = {archive_value},
    failure_path = {failure_value},
    source_moved_at = now()
where batch_id = {batch_id};
"""
    run_query(query, database, args)


def register_batch_artifact(
    *,
    batch_id: int,
    pipeline_run_id: int | None,
    artifact_name: str,
    artifact_type: str,
    artifact_format: str,
    artifact_path: str,
    row_count: int | None,
    artifact_status: str,
    notes: str | None,
    database: str,
    args,
) -> int:
    pipeline_run_value = "null" if pipeline_run_id is None else str(pipeline_run_id)
    row_count_value = "null" if row_count is None else str(row_count)
    notes_value = "null" if not notes else f"'{sql_literal(notes)}'"
    query = f"""
insert into ops.batch_artifacts (
    batch_id,
    pipeline_run_id,
    artifact_name,
    artifact_type,
    artifact_format,
    artifact_path,
    row_count,
    artifact_status,
    notes
)
values (
    {batch_id},
    {pipeline_run_value},
    '{sql_literal(artifact_name)}',
    '{sql_literal(artifact_type)}',
    '{sql_literal(artifact_format)}',
    '{sql_literal(artifact_path)}',
    {row_count_value},
    '{sql_literal(artifact_status)}',
    {notes_value}
)
returning artifact_id;
"""
    return int(run_scalar_query(query, database, args))


def register_pipeline_alert(
    *,
    batch_id: int | None,
    pipeline_run_id: int | None,
    pipeline_name: str | None,
    task_name: str | None,
    layer_name: str | None,
    alert_level: str,
    alert_type: str,
    alert_message: str,
    alert_context: dict | None,
    database: str,
    args,
) -> int:
    batch_id_value = "null" if batch_id is None else str(batch_id)
    pipeline_run_value = "null" if pipeline_run_id is None else str(pipeline_run_id)
    pipeline_name_value = "null" if not pipeline_name else f"'{sql_literal(pipeline_name)}'"
    task_name_value = "null" if not task_name else f"'{sql_literal(task_name)}'"
    layer_name_value = "null" if not layer_name else f"'{sql_literal(layer_name)}'"
    context_value = "null"
    if alert_context:
        context_value = f"'{sql_literal(json.dumps(alert_context))}'::jsonb"

    query = f"""
insert into ops.pipeline_alerts (
    batch_id,
    pipeline_run_id,
    pipeline_name,
    task_name,
    layer_name,
    alert_level,
    alert_type,
    alert_message,
    alert_context
)
values (
    {batch_id_value},
    {pipeline_run_value},
    {pipeline_name_value},
    {task_name_value},
    {layer_name_value},
    '{sql_literal(alert_level)}',
    '{sql_literal(alert_type)}',
    '{sql_literal(alert_message)}',
    {context_value}
)
returning alert_id;
"""
    return int(run_scalar_query(query, database, args))
