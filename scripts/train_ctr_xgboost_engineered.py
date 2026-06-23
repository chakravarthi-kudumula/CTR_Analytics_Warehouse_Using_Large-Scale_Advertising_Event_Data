#!/usr/bin/env python3

"""Train an engineered-feature XGBoost CTR model on the canonical ML dataset."""

from __future__ import annotations

import argparse
import gc
import json
import pickle
from datetime import datetime, timezone

from ml_feature_engineering import apply_feature_engineering, build_encoding_bundle
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
from train_ctr_xgboost import compute_scale_pos_weight, insert_training_run, upsert_model_metrics, upsert_model_registry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train an engineered XGBoost CTR model")
    parser.add_argument("--batch-name")
    parser.add_argument("--dataset-name")
    parser.add_argument("--model-name", default="ctr_xgboost_engineered")
    parser.add_argument("--model-version", default="v1")
    parser.add_argument("--n-estimators", type=int, default=350)
    parser.add_argument("--max-depth", type=int, default=6)
    parser.add_argument("--learning-rate", type=float, default=0.08)
    parser.add_argument("--subsample", type=float, default=0.80)
    parser.add_argument("--colsample-bytree", type=float, default=0.80)
    parser.add_argument("--min-child-weight", type=float, default=10.0)
    parser.add_argument("--reg-lambda", type=float, default=2.0)
    parser.add_argument("--reg-alpha", type=float, default=0.0)
    parser.add_argument("--early-stopping-rounds", type=int, default=30)
    parser.add_argument("--bootstrap-metadata", action="store_true")
    parser.add_argument("--triggered-by", default="manual")
    add_db_connection_args(parser)
    return parser.parse_args()


def build_model(args: argparse.Namespace, *, scale_pos_weight: float):
    from xgboost import XGBClassifier

    return XGBClassifier(
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        learning_rate=args.learning_rate,
        subsample=args.subsample,
        colsample_bytree=args.colsample_bytree,
        min_child_weight=args.min_child_weight,
        reg_lambda=args.reg_lambda,
        reg_alpha=args.reg_alpha,
        objective="binary:logistic",
        eval_metric=["auc", "aucpr", "logloss"],
        tree_method="hist",
        random_state=42,
        n_jobs=2,
        scale_pos_weight=scale_pos_weight,
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
    source_feature_columns = select_training_feature_columns(feature_columns_from_manifest(manifest))
    encoding_bundle = build_encoding_bundle(train_df[source_feature_columns + [TARGET_COLUMN]])

    train_x, engineered_feature_columns = apply_feature_engineering(train_df[source_feature_columns], encoding_bundle=encoding_bundle)
    validation_x, _ = apply_feature_engineering(validation_df[source_feature_columns], encoding_bundle=encoding_bundle)
    test_x, _ = apply_feature_engineering(test_df[source_feature_columns], encoding_bundle=encoding_bundle)
    train_y = train_df[TARGET_COLUMN].astype(int)
    validation_y = validation_df[TARGET_COLUMN].astype(int)
    test_y = test_df[TARGET_COLUMN].astype(int)

    pipeline_run_id = create_pipeline_run(
        batch_id=batch_id,
        pipeline_name="ml_xgboost_engineered_training",
        layer_name="ml",
        source_file=source_file,
        triggered_by=args.triggered_by,
        database=args.database,
        args=args,
    )
    pipeline_step_id = create_pipeline_step(
        pipeline_run_id=pipeline_run_id,
        batch_id=batch_id,
        step_name="train_xgboost_engineered_ctr_model",
        layer_name="ml",
        target_table="ml.model_registry",
        source_file=source_file,
        database=args.database,
        args=args,
    )

    try:
        scale_pos_weight = compute_scale_pos_weight(train_y)
        hyperparameters = {
            "n_estimators": args.n_estimators,
            "max_depth": args.max_depth,
            "learning_rate": args.learning_rate,
            "subsample": args.subsample,
            "colsample_bytree": args.colsample_bytree,
            "min_child_weight": args.min_child_weight,
            "reg_lambda": args.reg_lambda,
            "reg_alpha": args.reg_alpha,
            "early_stopping_rounds": args.early_stopping_rounds,
            "scale_pos_weight": scale_pos_weight,
            "feature_engineering_version": encoding_bundle["version"],
        }

        model_root = ML_MODEL_DIR / args.model_name / args.model_version
        model_root.mkdir(parents=True, exist_ok=True)
        model_path = model_root / "model.pkl"
        metrics_path = model_root / "metrics.json"

        model = build_model(args, scale_pos_weight=scale_pos_weight)
        model.fit(
            train_x,
            train_y,
            eval_set=[(validation_x, validation_y)],
            verbose=False,
        )

        validation_scores = model.predict_proba(validation_x)[:, 1]
        test_scores = model.predict_proba(test_x)[:, 1]

        metrics_payload = {
            "validation": evaluate_split(validation_y, validation_scores),
            "test": evaluate_split(test_y, test_scores),
            "metadata": {
                "batch_id": batch_id,
                "batch_name": batch_name,
                "dataset_name": dataset_name,
                "feature_count": len(engineered_feature_columns),
                "source_feature_count": len(source_feature_columns),
                "feature_engineering_version": encoding_bundle["version"],
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            },
        }

        model_bundle = {
            "model": model,
            "encoding_bundle": encoding_bundle,
            "scaler_bundle": None,
            "source_feature_columns": source_feature_columns,
            "engineered_feature_columns": engineered_feature_columns,
        }
        with model_path.open("wb") as handle:
            pickle.dump(model_bundle, handle)
        metrics_path.write_text(json.dumps(metrics_payload, indent=2))

        with connect(args) as connection:
            model_id = upsert_model_registry(
                connection,
                model_name=args.model_name,
                model_version=args.model_version,
                feature_source="feature_store.ctr_training_features",
                artifact_path=str(model_path),
                hyperparameters=hyperparameters,
                feature_columns=source_feature_columns,
                notes=f"Engineered XGBoost model trained from dataset {dataset_name}.",
            )
            training_run_id = insert_training_run(
                connection,
                model_id=model_id,
                batch_name=batch_name,
                rows_trained=len(train_df),
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
            artifact_format="pickle",
            artifact_path=str(model_path),
            row_count=len(engineered_feature_columns),
            artifact_status="READY",
            notes="Engineered XGBoost model artifact.",
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
            notes="Engineered XGBoost metric summary.",
            database=args.database,
            args=args,
        )

        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="SUCCESS",
            rows_processed=len(train_df),
            step_message=f"Engineered XGBoost training completed for {batch_name}.",
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="SUCCESS",
            run_message=f"Engineered XGBoost training completed for {batch_name}.",
            database=args.database,
            args=args,
        )

        print(f"Project root: {PROJECT_ROOT}")
        print(f"Batch name: {batch_name}")
        print(f"Dataset name: {dataset_name}")
        print(f"Model artifact: {model_path}")
        print(f"Metrics artifact: {metrics_path}")
        print(f"Validation ROC-AUC: {metrics_payload['validation']['roc_auc']:.6f}")
        print(f"Validation PR-AUC: {metrics_payload['validation']['pr_auc']:.6f}")
        print(f"Validation lift@10%: {metrics_payload['validation']['lift_at_10pct']:.6f}")
        print("Engineered XGBoost CTR training completed successfully.")
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
    finally:
        del train_df, validation_df, test_df
        gc.collect()


if __name__ == "__main__":
    main()
