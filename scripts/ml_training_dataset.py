#!/usr/bin/env python3

"""Extract batch-aware ML training datasets from feature_store.ctr_training_features."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from pipeline_tracking import (
    complete_pipeline_run,
    complete_pipeline_step,
    create_pipeline_run,
    create_pipeline_step,
    ensure_pipeline_metadata,
    fetch_batch_details,
    register_batch_artifact,
    resolve_batch_context,
    run_scalar_query,
)
from project_config import ML_TRAINING_DATASET_DIR, PROJECT_ROOT, SQL_DIR, add_db_connection_args, ensure_ml_directories

TARGET_COLUMN = "label"
ID_COLUMNS = {"prediction_id", "feature_recorded_at"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract train/validation/test datasets from the CTR feature store")
    parser.add_argument("--batch-name")
    parser.add_argument("--dataset-name")
    parser.add_argument("--train-days", default="1,2,3,4,5")
    parser.add_argument("--validation-days", default="6")
    parser.add_argument("--test-days", default="7")
    parser.add_argument("--bootstrap-metadata", action="store_true")
    parser.add_argument("--triggered-by", default="manual")
    add_db_connection_args(parser)
    return parser.parse_args()


def connect(args: argparse.Namespace):
    import psycopg

    return psycopg.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=args.password,
    )


def parse_day_set(day_string: str) -> set[int]:
    parsed = {int(value.strip()) for value in day_string.split(",") if value.strip()}
    if not parsed:
        raise ValueError("At least one day bucket is required for each split.")
    invalid = {value for value in parsed if value < 1 or value > 7}
    if invalid:
        raise ValueError(f"Event day buckets must stay between 1 and 7. Invalid values: {sorted(invalid)}")
    return parsed


def validate_splits(train_days: set[int], validation_days: set[int], test_days: set[int]) -> None:
    overlaps = [
        ("train/validation", train_days & validation_days),
        ("train/test", train_days & test_days),
        ("validation/test", validation_days & test_days),
    ]
    conflicts = {name: sorted(values) for name, values in overlaps if values}
    if conflicts:
        raise ValueError(f"Split day buckets must be disjoint. Conflicts: {conflicts}")


def build_split_lookup(train_days: set[int], validation_days: set[int], test_days: set[int]) -> dict[int, str]:
    lookup: dict[int, str] = {}
    for day in train_days:
        lookup[day] = "train"
    for day in validation_days:
        lookup[day] = "validation"
    for day in test_days:
        lookup[day] = "test"
    return lookup


def fetch_feature_store_columns(connection) -> list[str]:
    query = """
        select column_name
        from information_schema.columns
        where table_schema = 'feature_store'
          and table_name = 'ctr_training_features'
        order by ordinal_position;
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        return [row[0] for row in cursor.fetchall()]


def stream_split_files(
    connection,
    *,
    batch_id: int,
    split_lookup: dict[int, str],
    output_dir: Path,
    column_names: list[str],
) -> dict[str, int]:
    split_files = {
        "train": output_dir / "train.csv",
        "validation": output_dir / "validation.csv",
        "test": output_dir / "test.csv",
    }
    counts = {name: 0 for name in split_files}

    handles = {name: path.open("w", newline="") for name, path in split_files.items()}
    try:
        writers = {
            name: csv.writer(handle)
            for name, handle in handles.items()
        }
        output_columns = [*column_names, "dataset_split"]
        for writer in writers.values():
            writer.writerow(output_columns)

        select_columns = ", ".join(column_names)
        query = f"""
            select {select_columns}
            from feature_store.ctr_training_features
            where batch_id = %s
            order by raw_event_id;
        """
        with connection.cursor(name="ml_training_dataset_cursor") as cursor:
            cursor.itersize = 10_000
            cursor.execute(query, (batch_id,))
            event_day_index = column_names.index("event_day_number")
            for row in cursor:
                split_name = split_lookup.get(row[event_day_index])
                if not split_name:
                    continue
                writers[split_name].writerow([*row, split_name])
                counts[split_name] += 1
    finally:
        for handle in handles.values():
            handle.close()

    return counts


def build_manifest(
    *,
    batch_name: str,
    batch_id: int,
    source_file: str,
    dataset_name: str,
    output_dir: Path,
    row_counts: dict[str, int],
    split_lookup: dict[int, str],
    column_names: list[str],
) -> dict[str, object]:
    feature_columns = [
        column_name
        for column_name in column_names
        if column_name not in {TARGET_COLUMN, "raw_event_id", "batch_id", "click_flag", "impression_count", "click_count"}
        and column_name not in ID_COLUMNS
    ]
    split_days: dict[str, list[int]] = {"train": [], "validation": [], "test": []}
    for day_number, split_name in sorted(split_lookup.items()):
        split_days[split_name].append(day_number)

    return {
        "dataset_name": dataset_name,
        "batch_id": batch_id,
        "batch_name": batch_name,
        "source_file": source_file,
        "feature_source": "feature_store.ctr_training_features",
        "target_column": TARGET_COLUMN,
        "split_strategy": "event_day_number",
        "split_days": split_days,
        "row_counts": row_counts,
        "feature_column_count": len(feature_columns),
        "feature_columns": feature_columns,
        "output_directory": str(output_dir),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    args = parse_args()
    train_days = parse_day_set(args.train_days)
    validation_days = parse_day_set(args.validation_days)
    test_days = parse_day_set(args.test_days)
    validate_splits(train_days, validation_days, test_days)
    split_lookup = build_split_lookup(train_days, validation_days, test_days)

    if args.bootstrap_metadata:
        ensure_pipeline_metadata(SQL_DIR, args.database, args)
    training_root = ensure_ml_directories()

    batch_id, source_file = resolve_batch_context(
        table_name="feature_store.ctr_training_features",
        batch_name=args.batch_name,
        database=args.database,
        args=args,
    )
    batch_details = fetch_batch_details(args.batch_name, args.database, args) if args.batch_name else None
    batch_name = args.batch_name or (str(batch_details["batch_name"]) if batch_details else f"batch_{batch_id}")
    dataset_name = args.dataset_name or f"ml_training_dataset_{batch_name}"
    output_dir = training_root / dataset_name
    output_dir.mkdir(parents=True, exist_ok=True)

    feature_store_rows = int(
        run_scalar_query(
            f"select count(*) from feature_store.ctr_training_features where batch_id = {batch_id};",
            args.database,
            args,
        )
    )
    if feature_store_rows == 0:
        raise RuntimeError("feature_store.ctr_training_features is empty for the selected batch.")

    pipeline_run_id = create_pipeline_run(
        batch_id=batch_id,
        pipeline_name="ml_training_dataset_build",
        layer_name="ml",
        source_file=source_file,
        triggered_by=args.triggered_by,
        database=args.database,
        args=args,
    )
    pipeline_step_id = create_pipeline_step(
        pipeline_run_id=pipeline_run_id,
        batch_id=batch_id,
        step_name="extract_training_dataset_splits",
        layer_name="ml",
        target_table="feature_store.ctr_training_features",
        source_file=source_file,
        database=args.database,
        args=args,
    )

    try:
        with connect(args) as connection:
            column_names = fetch_feature_store_columns(connection)
            row_counts = stream_split_files(
                connection,
                batch_id=batch_id,
                split_lookup=split_lookup,
                output_dir=output_dir,
                column_names=column_names,
            )

        manifest = build_manifest(
            batch_name=batch_name,
            batch_id=batch_id,
            source_file=source_file,
            dataset_name=dataset_name,
            output_dir=output_dir,
            row_counts=row_counts,
            split_lookup=split_lookup,
            column_names=column_names,
        )
        manifest_path = output_dir / "dataset_manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))

        for split_name, row_count in row_counts.items():
            register_batch_artifact(
                batch_id=batch_id,
                pipeline_run_id=pipeline_run_id,
                artifact_name=f"{dataset_name}_{split_name}",
                artifact_type="ml_training_dataset",
                artifact_format="csv",
                artifact_path=str(output_dir / f"{split_name}.csv"),
                row_count=row_count,
                artifact_status="READY",
                notes=f"ML {split_name} split extracted from feature_store.ctr_training_features.",
                database=args.database,
                args=args,
            )
        register_batch_artifact(
            batch_id=batch_id,
            pipeline_run_id=pipeline_run_id,
            artifact_name=f"{dataset_name}_manifest",
            artifact_type="ml_training_dataset_manifest",
            artifact_format="json",
            artifact_path=str(manifest_path),
            row_count=feature_store_rows,
            artifact_status="READY",
            notes="ML training dataset manifest with split metadata and feature columns.",
            database=args.database,
            args=args,
        )

        total_exported_rows = sum(row_counts.values())
        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="SUCCESS",
            rows_processed=total_exported_rows,
            step_message=f"ML training dataset extraction completed for {batch_name}.",
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="SUCCESS",
            run_message=f"ML training dataset extraction completed for {batch_name}.",
            database=args.database,
            args=args,
        )

        print(f"Project root: {PROJECT_ROOT}")
        print(f"Batch name: {batch_name}")
        print(f"Batch ID: {batch_id}")
        print(f"Output directory: {output_dir}")
        print(f"Train rows: {row_counts['train']:,}")
        print(f"Validation rows: {row_counts['validation']:,}")
        print(f"Test rows: {row_counts['test']:,}")
        print(f"Manifest: {manifest_path}")
        print("ML training dataset extraction completed successfully.")
    except Exception as exc:
        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="FAILED",
            rows_processed=None,
            step_message=str(exc),
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="FAILED",
            run_message=str(exc),
            database=args.database,
            args=args,
        )
        raise


if __name__ == "__main__":
    main()
