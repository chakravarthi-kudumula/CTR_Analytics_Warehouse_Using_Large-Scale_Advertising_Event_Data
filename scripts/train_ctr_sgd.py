#!/usr/bin/env python3

"""Train a scalable SGD-based logistic CTR model from chunked ML dataset splits."""

from __future__ import annotations

import argparse
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
)
from project_config import ML_MODEL_DIR, ML_TRAINING_DATASET_DIR, PROJECT_ROOT, SQL_DIR, add_db_connection_args, ensure_ml_directories
from train_ctr_baseline import (
    TARGET_COLUMN,
    connect,
    evaluate_split,
    feature_columns_from_manifest,
    load_dataset_artifacts,
    resolve_dataset_name,
    select_training_feature_columns,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train a chunked SGD logistic CTR model")
    parser.add_argument("--batch-name")
    parser.add_argument("--dataset-name")
    parser.add_argument("--model-name", default="ctr_sgd_logistic")
    parser.add_argument("--model-version", default="v1")
    parser.add_argument("--epochs", type=int, default=2)
    parser.add_argument("--chunksize", type=int, default=50000)
    parser.add_argument("--alpha", type=float, default=1e-5)
    parser.add_argument("--triggered-by", default="manual")
    add_db_connection_args(parser)
    return parser.parse_args()


def build_model(alpha: float):
    from sklearn.linear_model import SGDClassifier

    return SGDClassifier(
        loss="log_loss",
        penalty="l2",
        alpha=alpha,
        learning_rate="optimal",
        random_state=42,
    )


def train_chunked_model(model, train_csv: Path, feature_columns: list[str], *, epochs: int, chunksize: int) -> int:
    import pandas as pd

    total_rows = 0
    use_columns = [*feature_columns, TARGET_COLUMN]
    classes = [0, 1]
    for epoch in range(epochs):
        for chunk in pd.read_csv(train_csv, usecols=use_columns, chunksize=chunksize):
            y_chunk = chunk[TARGET_COLUMN].astype(int)
            x_chunk = chunk[feature_columns].fillna(0)
            if total_rows == 0 and epoch == 0:
                model.partial_fit(x_chunk, y_chunk, classes=classes)
            else:
                model.partial_fit(x_chunk, y_chunk)
            total_rows += len(chunk) if epoch == 0 else 0
    return total_rows


def upsert_model_registry(
    connection,
    *,
    model_name: str,
    model_version: str,
    artifact_path: str,
    hyperparameters: dict[str, object],
    feature_columns: list[str],
    notes: str,
) -> int:
    query = """
        insert into ml.model_registry (
            model_name,
            model_version,
            model_type,
            framework_name,
            feature_source,
            artifact_path,
            hyperparameters,
            feature_columns,
            training_start_date,
            training_end_date,
            model_status,
            notes
        )
        values (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, 'READY', %s)
        on conflict (model_name, model_version)
        do update set
            model_type = excluded.model_type,
            framework_name = excluded.framework_name,
            artifact_path = excluded.artifact_path,
            hyperparameters = excluded.hyperparameters,
            feature_columns = excluded.feature_columns,
            training_start_date = excluded.training_start_date,
            training_end_date = excluded.training_end_date,
            model_status = 'READY',
            notes = excluded.notes,
            registered_at = now()
        returning model_id;
    """
    with connection.cursor() as cursor:
        cursor.execute(
            query,
            (
                model_name,
                model_version,
                "sgd_logistic_regression",
                "scikit-learn",
                "feature_store.ctr_training_features",
                artifact_path,
                json.dumps(hyperparameters),
                json.dumps(feature_columns),
                None,
                None,
                notes,
            ),
        )
        return int(cursor.fetchone()[0])


def insert_training_run(
    connection,
    *,
    model_id: int,
    batch_name: str,
    rows_trained: int,
    rows_validated: int,
    rows_tested: int,
    training_parameters: dict[str, object],
) -> int:
    query = """
        insert into ml.training_runs (
            model_id,
            train_batch_name,
            validation_batch_name,
            test_batch_name,
            dataset_split_strategy,
            rows_trained,
            rows_validated,
            rows_tested,
            training_parameters,
            run_status,
            started_at,
            completed_at,
            notes
        )
        values (
            %s,
            %s,
            %s,
            %s,
            'event_day_number',
            %s,
            %s,
            %s,
            %s::jsonb,
            'SUCCESS',
            now(),
            now(),
            'Chunked SGD CTR model training run.'
        )
        returning training_run_id;
    """
    with connection.cursor() as cursor:
        cursor.execute(
            query,
            (
                model_id,
                batch_name,
                batch_name,
                batch_name,
                rows_trained,
                rows_validated,
                rows_tested,
                json.dumps(training_parameters),
            ),
        )
        return int(cursor.fetchone()[0])


def upsert_model_metrics(connection, training_run_id: int, split_name: str, metrics: dict[str, float]) -> None:
    query = """
        insert into ml.model_metrics (
            training_run_id,
            dataset_split,
            roc_auc,
            pr_auc,
            log_loss,
            brier_score,
            precision_at_10pct,
            lift_at_10pct,
            notes
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, 'Chunked SGD CTR metrics.')
        on conflict (training_run_id, dataset_split)
        do update set
            roc_auc = excluded.roc_auc,
            pr_auc = excluded.pr_auc,
            log_loss = excluded.log_loss,
            brier_score = excluded.brier_score,
            precision_at_10pct = excluded.precision_at_10pct,
            lift_at_10pct = excluded.lift_at_10pct,
            recorded_at = now(),
            notes = excluded.notes;
    """
    with connection.cursor() as cursor:
        cursor.execute(
            query,
            (
                training_run_id,
                split_name,
                metrics["roc_auc"],
                metrics["pr_auc"],
                metrics["log_loss"],
                metrics["brier_score"],
                metrics["precision_at_10pct"],
                metrics["lift_at_10pct"],
            ),
        )


def main() -> None:
    args = parse_args()
    ensure_pipeline_metadata(SQL_DIR, args.database, args)
    ensure_ml_directories()

    batch_id, source_file = resolve_batch_context(
        table_name="feature_store.ctr_training_features",
        batch_name=args.batch_name,
        database=args.database,
        args=args,
    )
    batch_details = fetch_batch_details(args.batch_name, args.database, args) if args.batch_name else None
    batch_name = args.batch_name or (str(batch_details["batch_name"]) if batch_details else f"batch_{batch_id}")
    dataset_name = resolve_dataset_name(batch_name, args.dataset_name)
    dataset_dir = ML_TRAINING_DATASET_DIR / dataset_name
    if not dataset_dir.exists():
        raise FileNotFoundError(f"Dataset directory not found: {dataset_dir}")

    manifest, _, validation_df, test_df = load_dataset_artifacts(dataset_dir)
    feature_columns = select_training_feature_columns(feature_columns_from_manifest(manifest))

    pipeline_run_id = create_pipeline_run(
        batch_id=batch_id,
        pipeline_name="ml_sgd_training",
        layer_name="ml",
        source_file=source_file,
        triggered_by=args.triggered_by,
        database=args.database,
        args=args,
    )
    pipeline_step_id = create_pipeline_step(
        pipeline_run_id=pipeline_run_id,
        batch_id=batch_id,
        step_name="train_sgd_ctr_model",
        layer_name="ml",
        target_table="ml.model_registry",
        source_file=source_file,
        database=args.database,
        args=args,
    )

    try:
        import joblib

        model_root = ML_MODEL_DIR / args.model_name / args.model_version
        model_root.mkdir(parents=True, exist_ok=True)
        model_path = model_root / "model.joblib"
        metrics_path = model_root / "metrics.json"

        model = build_model(args.alpha)
        train_rows = train_chunked_model(
            model,
            dataset_dir / "train.csv",
            feature_columns,
            epochs=args.epochs,
            chunksize=args.chunksize,
        )

        validation_y = validation_df[TARGET_COLUMN].astype(int)
        validation_x = validation_df[feature_columns].fillna(0)
        validation_scores = model.predict_proba(validation_x)[:, 1]

        test_y = test_df[TARGET_COLUMN].astype(int)
        test_x = test_df[feature_columns].fillna(0)
        test_scores = model.predict_proba(test_x)[:, 1]

        metrics_payload = {
            "validation": evaluate_split(validation_y, validation_scores),
            "test": evaluate_split(test_y, test_scores),
            "metadata": {
                "batch_id": batch_id,
                "batch_name": batch_name,
                "dataset_name": dataset_name,
                "feature_count": len(feature_columns),
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            },
        }

        joblib.dump(model, model_path)
        metrics_path.write_text(json.dumps(metrics_payload, indent=2))

        hyperparameters = {"epochs": args.epochs, "chunksize": args.chunksize, "alpha": args.alpha}
        with connect(args) as connection:
            model_id = upsert_model_registry(
                connection,
                model_name=args.model_name,
                model_version=args.model_version,
                artifact_path=str(model_path),
                hyperparameters=hyperparameters,
                feature_columns=feature_columns,
                notes=f"Chunked SGD model trained from dataset {dataset_name}.",
            )
            training_run_id = insert_training_run(
                connection,
                model_id=model_id,
                batch_name=batch_name,
                rows_trained=train_rows,
                rows_validated=len(validation_df),
                rows_tested=len(test_df),
                training_parameters=hyperparameters,
            )
            for split_name in ("validation", "test"):
                upsert_model_metrics(connection, training_run_id, split_name, metrics_payload[split_name])
            connection.commit()

        register_batch_artifact(
            batch_id=batch_id,
            pipeline_run_id=pipeline_run_id,
            artifact_name=f"{args.model_name}_{args.model_version}_model",
            artifact_type="ml_model_artifact",
            artifact_format="joblib",
            artifact_path=str(model_path),
            row_count=len(feature_columns),
            artifact_status="READY",
            notes="Chunked SGD model artifact.",
            database=args.database,
            args=args,
        )
        register_batch_artifact(
            batch_id=batch_id,
            pipeline_run_id=pipeline_run_id,
            artifact_name=f"{args.model_name}_{args.model_version}_metrics",
            artifact_type="ml_model_metrics",
            artifact_format="json",
            artifact_path=str(metrics_path),
            row_count=2,
            artifact_status="READY",
            notes="Chunked SGD metric summary.",
            database=args.database,
            args=args,
        )

        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="SUCCESS",
            rows_processed=train_rows,
            step_message=f"Chunked SGD training completed for {batch_name}.",
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="SUCCESS",
            run_message=f"Chunked SGD training completed for {batch_name}.",
            database=args.database,
            args=args,
        )

        print(f"Project root: {PROJECT_ROOT}")
        print(f"Batch name: {batch_name}")
        print(f"Dataset name: {dataset_name}")
        print(f"Model artifact: {model_path}")
        print(f"Metrics artifact: {metrics_path}")
        print(f"Validation ROC-AUC: {metrics_payload['validation']['roc_auc']:.6f}")
        print(f"Validation Log Loss: {metrics_payload['validation']['log_loss']:.6f}")
        print("Chunked SGD CTR training completed successfully.")
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
