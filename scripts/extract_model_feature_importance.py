#!/usr/bin/env python3

"""Extract model-specific feature importance or coefficient summaries into the ML schema."""

from __future__ import annotations

import argparse
import csv
import json
import math
import pickle
from datetime import datetime, timezone

from pipeline_tracking import (
    complete_pipeline_run,
    complete_pipeline_step,
    create_pipeline_run,
    create_pipeline_step,
    ensure_pipeline_metadata,
    fetch_batch_details,
    register_batch_artifact,
)
from project_config import (
    ML_DEFAULT_MODEL_BASE_VERSION,
    ML_DEFAULT_MODEL_NAME,
    ML_REPORT_DIR,
    PROJECT_ROOT,
    SQL_DIR,
    add_db_connection_args,
    ensure_ml_directories,
)
from train_ctr_baseline import connect


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract model feature importance into ML monitoring tables")
    parser.add_argument("--model-name", default=ML_DEFAULT_MODEL_NAME)
    parser.add_argument("--model-version", default=ML_DEFAULT_MODEL_BASE_VERSION)
    parser.add_argument("--training-run-id", type=int)
    parser.add_argument("--top-n", type=int, default=25)
    parser.add_argument("--bootstrap-metadata", action="store_true")
    parser.add_argument("--triggered-by", default="manual")
    add_db_connection_args(parser)
    return parser.parse_args()


def fetch_model_context(connection, *, model_name: str, model_version: str, training_run_id: int | None) -> dict[str, object]:
    base_query = """
        select
            mr.model_id,
            mr.model_name,
            mr.model_version,
            mr.artifact_path,
            tr.training_run_id,
            tr.train_batch_name
        from ml.model_registry mr
        join ml.training_runs tr
          on tr.model_id = mr.model_id
         and tr.run_status = 'SUCCESS'
        where mr.model_name = %s
          and mr.model_version = %s
          and mr.model_status = 'READY'
    """
    params: tuple[object, ...]
    if training_run_id is None:
        query = base_query + """
        order by tr.training_run_id desc
        limit 1;
        """
        params = (model_name, model_version)
    else:
        query = base_query + """
          and tr.training_run_id = %s
        order by tr.training_run_id desc
        limit 1;
        """
        params = (model_name, model_version, training_run_id)
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        row = cursor.fetchone()
    if not row:
        raise ValueError(f"No READY successful training run found for {model_name} {model_version}")
    return {
        "model_id": int(row[0]),
        "model_name": str(row[1]),
        "model_version": str(row[2]),
        "artifact_path": str(row[3]),
        "training_run_id": int(row[4]),
        "train_batch_name": str(row[5]),
    }


def classify_feature_group(feature_name: str) -> str:
    if feature_name.startswith("missing_") or "missing" in feature_name:
        return "missingness"
    if feature_name in {"event_day_number", "overall_ctr"}:
        return "context"
    if feature_name.startswith("i") and feature_name.endswith("_log_scale"):
        return "numeric_log"
    if feature_name.startswith("i") and feature_name[1:].isdigit():
        return "numeric_raw"
    if "_bucket_code_enc_" in feature_name or "bucket_lift" in feature_name or feature_name.endswith("_bucket_ctr_lift"):
        return "bucket_signal"
    if feature_name.endswith("_ctr_lift") and feature_name.startswith("c"):
        return "categorical_lift"
    if "support" in feature_name or feature_name.endswith("_log1p"):
        return "support_signal"
    if "_enc_" in feature_name:
        return "target_encoding"
    if "_x_" in feature_name:
        return "interaction"
    if feature_name.startswith("numeric_"):
        return "numeric_aggregate"
    if feature_name.startswith("categorical_"):
        return "categorical_aggregate"
    if feature_name.startswith("total_") or feature_name.endswith("_ratio"):
        return "aggregate"
    return "other"


def load_importance_frame(model_context: dict[str, object]):
    import pandas as pd

    with open(model_context["artifact_path"], "rb") as handle:
        model_bundle = pickle.load(handle)

    if isinstance(model_bundle, dict) and "model" in model_bundle:
        model = model_bundle["model"]
        feature_names = model_bundle.get("engineered_feature_columns") or model_bundle.get("source_feature_columns") or []
    else:
        model = model_bundle
        feature_names = []

    if hasattr(model, "coef_"):
        coefficients = model.coef_[0]
        if not feature_names or len(feature_names) != len(coefficients):
            raise ValueError("Model coefficient vector does not align with stored feature names.")
        importance_values = [float(value) for value in coefficients]
    elif hasattr(model, "feature_importances_"):
        importance_values = [float(value) for value in model.feature_importances_]
        if not feature_names or len(feature_names) != len(importance_values):
            feature_names = [f"feature_{index}" for index in range(len(importance_values))]
    else:
        raise ValueError("This model artifact does not expose coefficients or feature importances.")

    total_abs = sum(abs(value) for value in importance_values) or 1.0
    frame = pd.DataFrame(
        {
            "feature_name": feature_names,
            "importance_value": importance_values,
        }
    )
    frame["abs_importance_value"] = frame["importance_value"].abs()
    frame["relative_importance_pct"] = frame["abs_importance_value"] / total_abs
    frame["importance_direction"] = frame["importance_value"].map(
        lambda value: "positive" if value > 0 else ("negative" if value < 0 else "neutral")
    )
    frame["feature_group"] = frame["feature_name"].map(classify_feature_group)
    frame = frame.sort_values(["abs_importance_value", "feature_name"], ascending=[False, True]).reset_index(drop=True)
    frame["importance_rank"] = frame.index + 1
    return frame


def upsert_feature_importance(connection, model_context: dict[str, object], importance_frame) -> None:
    delete_query = "delete from ml.model_feature_importance where training_run_id = %s;"
    insert_query = """
        insert into ml.model_feature_importance (
            model_id,
            training_run_id,
            model_name,
            model_version,
            feature_name,
            feature_group,
            importance_value,
            abs_importance_value,
            relative_importance_pct,
            importance_direction,
            importance_rank,
            notes
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    note = f"Feature importance extracted from {model_context['model_name']} {model_context['model_version']}."
    with connection.cursor() as cursor:
        cursor.execute(delete_query, (model_context["training_run_id"],))
        rows = [
            (
                model_context["model_id"],
                model_context["training_run_id"],
                model_context["model_name"],
                model_context["model_version"],
                row.feature_name,
                row.feature_group,
                float(row.importance_value),
                float(row.abs_importance_value),
                float(row.relative_importance_pct),
                row.importance_direction,
                int(row.importance_rank),
                note,
            )
            for row in importance_frame.itertuples(index=False)
        ]
        cursor.executemany(insert_query, rows)


def write_artifacts(model_context: dict[str, object], importance_frame, *, top_n: int) -> tuple[str, str]:
    ensure_ml_directories()
    report_dir = ML_REPORT_DIR / model_context["model_name"] / model_context["model_version"] / f"training_run_{model_context['training_run_id']}"
    report_dir.mkdir(parents=True, exist_ok=True)
    csv_path = report_dir / "feature_importance.csv"
    json_path = report_dir / "feature_importance_summary.json"

    top_features = importance_frame.head(top_n)
    group_summary = (
        importance_frame.groupby("feature_group", as_index=False)
        .agg(
            features_in_group=("feature_name", "count"),
            total_abs_importance=("abs_importance_value", "sum"),
            avg_abs_importance=("abs_importance_value", "mean"),
            group_relative_importance_pct=("relative_importance_pct", "sum"),
        )
        .sort_values("total_abs_importance", ascending=False)
    )

    with csv_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "feature_name",
                "feature_group",
                "importance_value",
                "abs_importance_value",
                "relative_importance_pct",
                "importance_direction",
                "importance_rank",
            ]
        )
        for row in importance_frame.itertuples(index=False):
            writer.writerow(
                [
                    row.feature_name,
                    row.feature_group,
                    f"{row.importance_value:.8f}",
                    f"{row.abs_importance_value:.8f}",
                    f"{row.relative_importance_pct:.8f}",
                    row.importance_direction,
                    row.importance_rank,
                ]
            )

    summary_payload = {
        "model_name": model_context["model_name"],
        "model_version": model_context["model_version"],
        "training_run_id": model_context["training_run_id"],
        "train_batch_name": model_context["train_batch_name"],
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "top_features": top_features.to_dict(orient="records"),
        "feature_group_summary": group_summary.to_dict(orient="records"),
    }
    json_path.write_text(json.dumps(summary_payload, indent=2))
    return str(csv_path), str(json_path)


def main() -> None:
    args = parse_args()
    if args.bootstrap_metadata:
        ensure_pipeline_metadata(SQL_DIR, args.database, args)
    ensure_ml_directories()

    with connect(args) as connection:
        model_context = fetch_model_context(
            connection,
            model_name=args.model_name,
            model_version=args.model_version,
            training_run_id=args.training_run_id,
        )

    batch_details = fetch_batch_details(model_context["train_batch_name"], args.database, args)
    batch_id = int(batch_details["batch_id"]) if batch_details else None
    source_file = str(batch_details["source_file"]) if batch_details else None

    pipeline_run_id = create_pipeline_run(
        batch_id=batch_id,
        pipeline_name="ml_feature_importance",
        layer_name="ml",
        source_file=source_file,
        triggered_by=args.triggered_by,
        database=args.database,
        args=args,
    )
    pipeline_step_id = create_pipeline_step(
        pipeline_run_id=pipeline_run_id,
        batch_id=batch_id,
        step_name="extract_model_feature_importance",
        layer_name="ml",
        target_table="ml.model_feature_importance",
        source_file=source_file,
        database=args.database,
        args=args,
    )

    try:
        importance_frame = load_importance_frame(model_context)
        csv_path, json_path = write_artifacts(model_context, importance_frame, top_n=args.top_n)

        with connect(args) as connection:
            upsert_feature_importance(connection, model_context, importance_frame)
            connection.commit()

        register_batch_artifact(
            batch_id=batch_id,
            pipeline_run_id=pipeline_run_id,
            artifact_name=f"{model_context['model_name']}_{model_context['model_version']}_feature_importance",
            artifact_type="ml_feature_importance",
            artifact_format="csv",
            artifact_path=csv_path,
            row_count=len(importance_frame),
            artifact_status="READY",
            notes="Model feature importance export.",
            database=args.database,
            args=args,
        )
        register_batch_artifact(
            batch_id=batch_id,
            pipeline_run_id=pipeline_run_id,
            artifact_name=f"{model_context['model_name']}_{model_context['model_version']}_feature_importance_summary",
            artifact_type="ml_feature_importance_summary",
            artifact_format="json",
            artifact_path=json_path,
            row_count=min(args.top_n, len(importance_frame)),
            artifact_status="READY",
            notes="Model feature importance summary.",
            database=args.database,
            args=args,
        )

        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="SUCCESS",
            rows_processed=len(importance_frame),
            step_message=f"Feature importance extracted for {model_context['model_name']} {model_context['model_version']}.",
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="SUCCESS",
            run_message=f"Feature importance extracted for {model_context['model_name']} {model_context['model_version']}.",
            database=args.database,
            args=args,
        )

        print(f"Project root: {PROJECT_ROOT}")
        print(f"Model: {model_context['model_name']} {model_context['model_version']}")
        print(f"Training run id: {model_context['training_run_id']}")
        print(f"Feature rows written: {len(importance_frame)}")
        print(f"CSV artifact: {csv_path}")
        print(f"Summary artifact: {json_path}")
        print("Model feature importance extraction completed successfully.")
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
