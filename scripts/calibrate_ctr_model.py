#!/usr/bin/env python3

"""Calibrate a trained CTR model using validation-split probabilities and register a new calibrated version."""

from __future__ import annotations

import argparse
import json
import pickle
from datetime import datetime, timezone
from pathlib import Path

from ml_feature_engineering import apply_feature_engineering, apply_scaler
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
from score_ctr_batch import predict_ctr_scores
from train_ctr_baseline import (
    TARGET_COLUMN,
    connect,
    evaluate_split,
    feature_columns_from_manifest,
    resolve_dataset_name,
    select_training_feature_columns,
)
from train_ctr_sgd import insert_training_run, upsert_model_metrics, upsert_model_registry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Calibrate a trained CTR model")
    parser.add_argument("--batch-name", default="criteo_1m_ml_canonical_batch")
    parser.add_argument("--dataset-name")
    parser.add_argument("--source-model-name", default="ctr_logistic_regression")
    parser.add_argument("--source-model-version", default="v4")
    parser.add_argument("--model-name", default="ctr_logistic_regression")
    parser.add_argument("--model-version", default="v4_calibrated")
    parser.add_argument("--method", default="sigmoid", choices=["sigmoid", "isotonic"])
    parser.add_argument("--bootstrap-metadata", action="store_true")
    parser.add_argument("--triggered-by", default="manual")
    add_db_connection_args(parser)
    return parser.parse_args()


def split_validation_indices(total_rows: int) -> tuple[list[int], list[int]]:
    midpoint = total_rows // 2
    calibration_fit = list(range(0, midpoint))
    calibration_eval = list(range(midpoint, total_rows))
    return calibration_fit, calibration_eval


def build_calibrator(method: str, fit_scores, fit_targets):
    import numpy as np

    scores = np.asarray(fit_scores, dtype="float64")
    targets = np.asarray(fit_targets, dtype="int32")
    if method == "sigmoid":
        from sklearn.linear_model import LogisticRegression

        calibrator = LogisticRegression(solver="lbfgs")
        calibrator.fit(scores.reshape(-1, 1), targets)
        return calibrator
    from sklearn.isotonic import IsotonicRegression

    calibrator = IsotonicRegression(out_of_bounds="clip")
    calibrator.fit(scores, targets)
    return calibrator


def apply_calibrator(calibrator, method: str, scores):
    import numpy as np

    score_array = np.asarray(scores, dtype="float64")
    if method == "sigmoid":
        return calibrator.predict_proba(score_array.reshape(-1, 1))[:, 1]
    return calibrator.predict(score_array)


def main() -> None:
    import pandas as pd

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

    manifest = json.loads((dataset_dir / "dataset_manifest.json").read_text())
    validation_df = pd.read_csv(dataset_dir / "validation.csv")
    test_df = pd.read_csv(dataset_dir / "test.csv")
    source_feature_columns = select_training_feature_columns(feature_columns_from_manifest(manifest))

    pipeline_run_id = create_pipeline_run(
        batch_id=batch_id,
        pipeline_name="ml_model_calibration",
        layer_name="ml",
        source_file=source_file,
        triggered_by=args.triggered_by,
        database=args.database,
        args=args,
    )
    pipeline_step_id = create_pipeline_step(
        pipeline_run_id=pipeline_run_id,
        batch_id=batch_id,
        step_name="calibrate_ctr_model",
        layer_name="ml",
        target_table="ml.model_registry",
        source_file=source_file,
        database=args.database,
        args=args,
    )

    try:
        with connect(args) as connection:
            from score_ctr_batch import fetch_model_metadata

            source_model_metadata = fetch_model_metadata(connection, args.source_model_name, args.source_model_version)

        with open(source_model_metadata["artifact_path"], "rb") as handle:
            source_bundle = pickle.load(handle)

        encoding_bundle = source_bundle.get("encoding_bundle")
        scaler_bundle = source_bundle.get("scaler_bundle")
        source_columns = source_bundle.get("source_feature_columns", source_feature_columns)

        validation_x, _ = apply_feature_engineering(validation_df[source_columns], encoding_bundle=encoding_bundle)
        test_x, _ = apply_feature_engineering(test_df[source_columns], encoding_bundle=encoding_bundle)
        if scaler_bundle:
            validation_x = apply_scaler(validation_x, scaler_bundle)
            test_x = apply_scaler(test_x, scaler_bundle)

        validation_y = validation_df[TARGET_COLUMN].astype(int).reset_index(drop=True)
        test_y = test_df[TARGET_COLUMN].astype(int).reset_index(drop=True)
        raw_validation_scores = predict_ctr_scores({k: v for k, v in source_bundle.items() if k != "calibrator"}, validation_x)
        raw_test_scores = predict_ctr_scores({k: v for k, v in source_bundle.items() if k != "calibrator"}, test_x)

        calibration_fit_idx, calibration_eval_idx = split_validation_indices(len(validation_y))
        fit_scores = [raw_validation_scores[index] for index in calibration_fit_idx]
        fit_targets = validation_y.iloc[calibration_fit_idx].to_numpy()
        eval_scores = [raw_validation_scores[index] for index in calibration_eval_idx]
        eval_targets = validation_y.iloc[calibration_eval_idx]

        calibrator = build_calibrator(args.method, fit_scores, fit_targets)
        calibrated_eval_scores = apply_calibrator(calibrator, args.method, eval_scores)
        calibrated_test_scores = apply_calibrator(calibrator, args.method, raw_test_scores)

        metrics_payload = {
            "validation": evaluate_split(eval_targets, calibrated_eval_scores),
            "test": evaluate_split(test_y, calibrated_test_scores),
            "metadata": {
                "batch_id": batch_id,
                "batch_name": batch_name,
                "dataset_name": dataset_name,
                "source_model_name": args.source_model_name,
                "source_model_version": args.source_model_version,
                "calibration_method": args.method,
                "validation_fit_rows": len(calibration_fit_idx),
                "validation_eval_rows": len(calibration_eval_idx),
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            },
        }

        model_root = ML_MODEL_DIR / args.model_name / args.model_version
        model_root.mkdir(parents=True, exist_ok=True)
        model_path = model_root / "model.pkl"
        metrics_path = model_root / "metrics.json"

        calibrated_bundle = {
            **source_bundle,
            "calibrator": calibrator,
            "calibration_method": args.method,
            "source_model_name": args.source_model_name,
            "source_model_version": args.source_model_version,
        }
        with model_path.open("wb") as handle:
            pickle.dump(calibrated_bundle, handle)
        metrics_path.write_text(json.dumps(metrics_payload, indent=2))

        hyperparameters = {
            "calibration_method": args.method,
            "source_model_name": args.source_model_name,
            "source_model_version": args.source_model_version,
            "feature_engineering_version": source_bundle.get("encoding_bundle", {}).get("version"),
        }
        with connect(args) as connection:
            model_id = upsert_model_registry(
                connection,
                model_name=args.model_name,
                model_version=args.model_version,
                artifact_path=str(model_path),
                hyperparameters=hyperparameters,
                feature_columns=source_columns,
                notes=f"Calibrated model derived from {args.source_model_name} {args.source_model_version}.",
            )
            training_run_id = insert_training_run(
                connection,
                model_id=model_id,
                batch_name=batch_name,
                rows_trained=int(manifest["row_counts"]["train"]),
                rows_validated=len(calibration_eval_idx),
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
            row_count=1,
            artifact_status="READY",
            notes="Calibrated CTR model artifact.",
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
            notes="Calibrated CTR metric summary.",
            database=args.database,
            args=args,
        )

        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="SUCCESS",
            rows_processed=int(manifest["row_counts"]["train"]),
            step_message=f"Calibrated model {args.model_name} {args.model_version} created.",
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="SUCCESS",
            run_message=f"Calibrated model {args.model_name} {args.model_version} created.",
            database=args.database,
            args=args,
        )

        print(f"Project root: {PROJECT_ROOT}")
        print(f"Batch name: {batch_name}")
        print(f"Calibrated model: {args.model_name} {args.model_version}")
        print(f"Source model: {args.source_model_name} {args.source_model_version}")
        print(f"Calibration method: {args.method}")
        print(f"Model artifact: {model_path}")
        print(f"Metrics artifact: {metrics_path}")
        print(f"Validation ROC-AUC: {metrics_payload['validation']['roc_auc']:.6f}")
        print(f"Validation PR-AUC: {metrics_payload['validation']['pr_auc']:.6f}")
        print(f"Validation Log Loss: {metrics_payload['validation']['log_loss']:.6f}")
        print("CTR model calibration completed successfully.")
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
