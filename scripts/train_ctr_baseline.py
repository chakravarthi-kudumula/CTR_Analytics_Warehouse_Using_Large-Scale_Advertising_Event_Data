#!/usr/bin/env python3

"""Train the baseline Logistic Regression CTR model from extracted ML dataset splits."""

from __future__ import annotations

import argparse
import json
import math
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
from project_config import (
    ML_MODEL_DIR,
    ML_TRAINING_DATASET_DIR,
    PROJECT_ROOT,
    SQL_DIR,
    add_db_connection_args,
    ensure_ml_directories,
)
from ml_feature_engineering import DROP_COLUMNS, RAW_CATEGORICAL_COLUMNS, select_source_feature_columns

TARGET_COLUMN = "label"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train the baseline Logistic Regression CTR model")
    parser.add_argument("--batch-name")
    parser.add_argument("--dataset-name")
    parser.add_argument("--model-name", default="ctr_logistic_regression")
    parser.add_argument("--model-version", default="v1")
    parser.add_argument("--max-iter", type=int, default=500)
    parser.add_argument("--class-weight", default="balanced")
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


def resolve_dataset_name(batch_name: str, explicit_dataset_name: str | None) -> str:
    return explicit_dataset_name or f"ml_training_dataset_{batch_name}"


def load_dataset_artifacts(dataset_dir: Path) -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    import pandas as pd

    manifest = json.loads((dataset_dir / "dataset_manifest.json").read_text())
    train_df = pd.read_csv(dataset_dir / "train.csv")
    validation_df = pd.read_csv(dataset_dir / "validation.csv")
    test_df = pd.read_csv(dataset_dir / "test.csv")
    return manifest, train_df, validation_df, test_df


def feature_columns_from_manifest(manifest: dict[str, object]) -> list[str]:
    feature_columns = manifest.get("feature_columns", [])
    return [str(column) for column in feature_columns]


def select_training_feature_columns(feature_columns: list[str]) -> list[str]:
    return select_source_feature_columns(feature_columns)


def _non_null_values(values) -> list[object]:
    if hasattr(values, "dropna"):
        return [value for value in values.dropna().tolist() if value is not None]
    return [value for value in values if value is not None]


def split_feature_types(frame, feature_columns: list[str]) -> tuple[list[str], list[str]]:
    numeric_columns: list[str] = []
    categorical_columns: list[str] = []
    for column_name in feature_columns:
        has_column = column_name in getattr(frame, "columns", frame)
        if not has_column:
            continue
        if column_name in RAW_CATEGORICAL_COLUMNS or column_name.endswith("_bucket_code"):
            categorical_columns.append(column_name)
            continue
        values = _non_null_values(frame[column_name])
        sample_value = values[0] if values else None
        if isinstance(sample_value, (int, float, bool)):
            numeric_columns.append(column_name)
        else:
            categorical_columns.append(column_name)
    return numeric_columns, categorical_columns


def compute_ranking_metrics(y_true, y_score, top_fraction: float = 0.10) -> tuple[float, float]:
    targets = list(y_true)
    scores = list(y_score)
    if not targets:
        return 0.0, 0.0
    scored_rows = sorted(zip(scores, targets), key=lambda item: item[0], reverse=True)
    top_n = max(1, math.ceil(len(scored_rows) * top_fraction))
    top_targets = [float(target) for _, target in scored_rows[:top_n]]
    precision_at_top = sum(top_targets) / len(top_targets)
    baseline_ctr = sum(float(target) for target in targets) / len(targets)
    if baseline_ctr == 0:
        return precision_at_top, 0.0
    return precision_at_top, precision_at_top / baseline_ctr


def build_pipeline(max_iter: int, class_weight: str | None, numeric_columns: list[str], categorical_columns: list[str]):
    from sklearn.compose import ColumnTransformer
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    transformers = []
    if numeric_columns:
        numeric_pipeline = Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="constant", fill_value=0)),
                ("scaler", StandardScaler()),
            ]
        )
        transformers.append(("numeric", numeric_pipeline, numeric_columns))
    if categorical_columns:
        categorical_pipeline = Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="constant", fill_value="unknown")),
                ("encoder", OneHotEncoder(handle_unknown="ignore")),
            ]
        )
        transformers.append(("categorical", categorical_pipeline, categorical_columns))

    preprocessor = ColumnTransformer(transformers=transformers)
    model = LogisticRegression(
        max_iter=max_iter,
        class_weight=None if class_weight in {"none", "null", "None", ""} else class_weight,
        solver="liblinear",
    )
    return Pipeline(steps=[("preprocessor", preprocessor), ("model", model)])


def evaluate_split(y_true: pd.Series, y_score: pd.Series) -> dict[str, float]:
    from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score

    metrics: dict[str, float] = {}
    metrics["roc_auc"] = float(roc_auc_score(y_true, y_score))
    metrics["pr_auc"] = float(average_precision_score(y_true, y_score))
    metrics["log_loss"] = float(log_loss(y_true, y_score, labels=[0, 1]))
    metrics["brier_score"] = float(brier_score_loss(y_true, y_score))
    precision_at_10pct, lift_at_10pct = compute_ranking_metrics(y_true, y_score)
    metrics["precision_at_10pct"] = precision_at_10pct
    metrics["lift_at_10pct"] = lift_at_10pct
    return metrics


def upsert_model_registry(
    connection,
    *,
    model_name: str,
    model_version: str,
    model_type: str,
    feature_source: str,
    artifact_path: str,
    hyperparameters: dict[str, object],
    feature_columns: list[str],
    training_start_date: str | None,
    training_end_date: str | None,
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
                model_type,
                "scikit-learn",
                feature_source,
                artifact_path,
                json.dumps(hyperparameters),
                json.dumps(feature_columns),
                training_start_date,
                training_end_date,
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
    max_iter: int,
    class_weight: str,
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
            'Baseline Logistic Regression CTR model training run.'
        )
        returning training_run_id;
    """
    payload = {
        "model_type": "logistic_regression",
        "max_iter": max_iter,
        "class_weight": class_weight,
    }
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
                json.dumps(payload),
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
        values (%s, %s, %s, %s, %s, %s, %s, %s, 'Baseline Logistic Regression metrics.')
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
    if args.bootstrap_metadata:
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

    manifest, train_df, validation_df, test_df = load_dataset_artifacts(dataset_dir)
    feature_columns = select_training_feature_columns(feature_columns_from_manifest(manifest))
    numeric_columns, categorical_columns = split_feature_types(train_df, feature_columns)

    pipeline_run_id = create_pipeline_run(
        batch_id=batch_id,
        pipeline_name="ml_baseline_training",
        layer_name="ml",
        source_file=source_file,
        triggered_by=args.triggered_by,
        database=args.database,
        args=args,
    )
    pipeline_step_id = create_pipeline_step(
        pipeline_run_id=pipeline_run_id,
        batch_id=batch_id,
        step_name="train_logistic_regression_baseline",
        layer_name="ml",
        target_table="ml.model_registry",
        source_file=source_file,
        database=args.database,
        args=args,
    )

    try:
        import joblib
        import pandas as pd

        model_root = ML_MODEL_DIR / args.model_name / args.model_version
        model_root.mkdir(parents=True, exist_ok=True)
        model_path = model_root / "model.joblib"
        metrics_path = model_root / "metrics.json"

        model = build_pipeline(args.max_iter, args.class_weight, numeric_columns, categorical_columns)
        model.fit(train_df[feature_columns], train_df[TARGET_COLUMN])

        validation_scores = pd.Series(model.predict_proba(validation_df[feature_columns])[:, 1])
        test_scores = pd.Series(model.predict_proba(test_df[feature_columns])[:, 1])
        train_scores = pd.Series(model.predict_proba(train_df[feature_columns])[:, 1])

        metrics_payload = {
            "train": evaluate_split(train_df[TARGET_COLUMN], train_scores),
            "validation": evaluate_split(validation_df[TARGET_COLUMN], validation_scores),
            "test": evaluate_split(test_df[TARGET_COLUMN], test_scores),
        }
        metrics_payload["metadata"] = {
            "batch_id": batch_id,
            "batch_name": batch_name,
            "dataset_name": dataset_name,
            "feature_count": len(feature_columns),
            "numeric_feature_count": len(numeric_columns),
            "categorical_feature_count": len(categorical_columns),
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        }

        joblib.dump(model, model_path)
        metrics_path.write_text(json.dumps(metrics_payload, indent=2))

        with connect(args) as connection:
            model_id = upsert_model_registry(
                connection,
                model_name=args.model_name,
                model_version=args.model_version,
                model_type="logistic_regression",
                feature_source="feature_store.ctr_training_features",
                artifact_path=str(model_path),
                hyperparameters={"max_iter": args.max_iter, "class_weight": args.class_weight},
                feature_columns=feature_columns,
                training_start_date=None,
                training_end_date=None,
                notes=f"Baseline Logistic Regression trained from dataset {dataset_name}.",
            )
            training_run_id = insert_training_run(
                connection,
                model_id=model_id,
                batch_name=batch_name,
                rows_trained=len(train_df),
                rows_validated=len(validation_df),
                rows_tested=len(test_df),
                max_iter=args.max_iter,
                class_weight=args.class_weight,
            )
            for split_name in ("train", "validation", "test"):
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
            notes="Baseline Logistic Regression model artifact.",
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
            row_count=3,
            artifact_status="READY",
            notes="Baseline Logistic Regression metric summary.",
            database=args.database,
            args=args,
        )

        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="SUCCESS",
            rows_processed=len(train_df),
            step_message=f"Baseline Logistic Regression completed for {batch_name}.",
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="SUCCESS",
            run_message=f"Baseline Logistic Regression completed for {batch_name}.",
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
        print("Baseline Logistic Regression training completed successfully.")
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
