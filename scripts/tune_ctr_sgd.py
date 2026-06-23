#!/usr/bin/env python3

"""Run a focused hyperparameter sweep for the chunked SGD CTR model on the canonical batch."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from ml_feature_engineering import (
    apply_feature_engineering,
    apply_scaler,
    build_encoding_bundle,
)
from project_config import ML_REPORT_DIR, ML_TRAINING_DATASET_DIR, ensure_ml_directories
from train_ctr_baseline import (
    TARGET_COLUMN,
    evaluate_split,
    feature_columns_from_manifest,
    load_dataset_artifacts,
    resolve_dataset_name,
    select_training_feature_columns,
)
from train_ctr_sgd import build_model, build_scaler_bundle, compute_balanced_class_weight_map, train_chunked_model


DEFAULT_CONFIGS = [
    {
        "config_name": "baseline_like_v3",
        "epochs": 2,
        "chunksize": 10000,
        "alpha": 1e-5,
        "penalty": "l2",
        "l1_ratio": 0.15,
        "learning_rate": "optimal",
        "eta0": 0.0,
        "power_t": 0.5,
        "class_weight": "none",
        "average": True,
    },
    {
        "config_name": "lower_alpha_more_epochs",
        "epochs": 3,
        "chunksize": 10000,
        "alpha": 5e-6,
        "penalty": "l2",
        "l1_ratio": 0.15,
        "learning_rate": "optimal",
        "eta0": 0.0,
        "power_t": 0.5,
        "class_weight": "none",
        "average": True,
    },
    {
        "config_name": "elasticnet_sparse",
        "epochs": 3,
        "chunksize": 10000,
        "alpha": 8e-6,
        "penalty": "elasticnet",
        "l1_ratio": 0.08,
        "learning_rate": "optimal",
        "eta0": 0.0,
        "power_t": 0.5,
        "class_weight": "none",
        "average": True,
    },
    {
        "config_name": "balanced_l2",
        "epochs": 3,
        "chunksize": 10000,
        "alpha": 1.5e-5,
        "penalty": "l2",
        "l1_ratio": 0.15,
        "learning_rate": "optimal",
        "eta0": 0.0,
        "power_t": 0.5,
        "class_weight": "balanced",
        "average": True,
    },
    {
        "config_name": "adaptive_eta",
        "epochs": 4,
        "chunksize": 10000,
        "alpha": 1e-5,
        "penalty": "l2",
        "l1_ratio": 0.15,
        "learning_rate": "adaptive",
        "eta0": 0.01,
        "power_t": 0.5,
        "class_weight": "none",
        "average": True,
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tune the chunked SGD CTR model on a canonical dataset")
    parser.add_argument("--batch-name", default="criteo_1m_ml_canonical_batch")
    parser.add_argument("--dataset-name")
    parser.add_argument("--report-name", default="ctr_sgd_tuning_report")
    return parser.parse_args()


def sort_results(results: list[dict[str, object]]) -> list[dict[str, object]]:
    return sorted(
        results,
        key=lambda row: (
            float(row["validation_pr_auc"]),
            float(row["validation_roc_auc"]),
            float(row["validation_lift_at_10pct"]),
            -float(row["validation_log_loss"]),
        ),
        reverse=True,
    )


def main() -> None:
    import pandas as pd

    args = parse_args()
    ensure_ml_directories()

    dataset_name = resolve_dataset_name(args.batch_name, args.dataset_name)
    dataset_dir = ML_TRAINING_DATASET_DIR / dataset_name
    if not dataset_dir.exists():
        raise FileNotFoundError(f"Dataset directory not found: {dataset_dir}")

    manifest, train_df, validation_df, test_df = load_dataset_artifacts(dataset_dir)
    source_feature_columns = select_training_feature_columns(feature_columns_from_manifest(manifest))
    encoding_columns = [column for column in source_feature_columns if column in train_df.columns]
    encoding_bundle = build_encoding_bundle(train_df[encoding_columns + [TARGET_COLUMN]])
    balanced_class_weight_map = compute_balanced_class_weight_map(train_df[TARGET_COLUMN].astype(int))
    scaler_bundle, engineered_feature_columns = build_scaler_bundle(
        dataset_dir / "train.csv",
        source_feature_columns,
        encoding_bundle=encoding_bundle,
        chunksize=10000,
    )

    validation_x, _ = apply_feature_engineering(validation_df[source_feature_columns], encoding_bundle=encoding_bundle)
    test_x, _ = apply_feature_engineering(test_df[source_feature_columns], encoding_bundle=encoding_bundle)
    validation_x = apply_scaler(validation_x, scaler_bundle)
    test_x = apply_scaler(test_x, scaler_bundle)
    validation_y = validation_df[TARGET_COLUMN].astype(int)
    test_y = test_df[TARGET_COLUMN].astype(int)

    report_root = ML_REPORT_DIR / "tuning" / args.batch_name
    report_root.mkdir(parents=True, exist_ok=True)
    report_json_path = report_root / f"{args.report_name}.json"
    report_csv_path = report_root / f"{args.report_name}.csv"

    results: list[dict[str, object]] = []
    for config in DEFAULT_CONFIGS:
        model = build_model(
            alpha=float(config["alpha"]),
            penalty=str(config["penalty"]),
            l1_ratio=float(config["l1_ratio"]),
            learning_rate=str(config["learning_rate"]),
            eta0=float(config["eta0"]),
            power_t=float(config["power_t"]),
            average=bool(config["average"]),
        )
        train_rows = train_chunked_model(
            model,
            dataset_dir / "train.csv",
            source_feature_columns,
            encoding_bundle=encoding_bundle,
            scaler_bundle=scaler_bundle,
            epochs=int(config["epochs"]),
            chunksize=int(config["chunksize"]),
            class_weight_map=balanced_class_weight_map if config["class_weight"] == "balanced" else None,
        )
        validation_scores = model.predict_proba(validation_x)[:, 1]
        test_scores = model.predict_proba(test_x)[:, 1]
        validation_metrics = evaluate_split(validation_y, validation_scores)
        test_metrics = evaluate_split(test_y, test_scores)
        results.append(
            {
                **config,
                "rows_trained": train_rows,
                "feature_count": len(engineered_feature_columns),
                "validation_roc_auc": validation_metrics["roc_auc"],
                "validation_pr_auc": validation_metrics["pr_auc"],
                "validation_log_loss": validation_metrics["log_loss"],
                "validation_precision_at_10pct": validation_metrics["precision_at_10pct"],
                "validation_lift_at_10pct": validation_metrics["lift_at_10pct"],
                "test_roc_auc": test_metrics["roc_auc"],
                "test_pr_auc": test_metrics["pr_auc"],
                "test_log_loss": test_metrics["log_loss"],
                "test_precision_at_10pct": test_metrics["precision_at_10pct"],
                "test_lift_at_10pct": test_metrics["lift_at_10pct"],
            }
        )

    ranked_results = sort_results(results)
    report_payload = {
        "batch_name": args.batch_name,
        "dataset_name": dataset_name,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "best_config_name": ranked_results[0]["config_name"] if ranked_results else None,
        "results": ranked_results,
    }
    report_json_path.write_text(json.dumps(report_payload, indent=2))
    pd.DataFrame(ranked_results).to_csv(report_csv_path, index=False)

    print(f"Batch name: {args.batch_name}")
    print(f"Dataset name: {dataset_name}")
    print(f"Best config: {report_payload['best_config_name']}")
    print(f"Tuning report JSON: {report_json_path}")
    print(f"Tuning report CSV: {report_csv_path}")
    if ranked_results:
        best = ranked_results[0]
        print(f"Best validation ROC-AUC: {best['validation_roc_auc']:.6f}")
        print(f"Best validation PR-AUC: {best['validation_pr_auc']:.6f}")
        print(f"Best validation lift@10%: {best['validation_lift_at_10pct']:.6f}")


if __name__ == "__main__":
    main()
