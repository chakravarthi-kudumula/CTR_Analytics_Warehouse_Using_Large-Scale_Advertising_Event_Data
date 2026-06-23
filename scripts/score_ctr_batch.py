#!/usr/bin/env python3

"""Score a batch of CTR feature rows with a trained baseline model."""

from __future__ import annotations

import argparse
import json
import math
import pickle
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
    ML_SCORING_DIR,
    PROJECT_ROOT,
    SQL_DIR,
    add_db_connection_args,
    ensure_ml_directories,
)

TARGET_COLUMN = "label"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score a batch with a trained CTR model")
    parser.add_argument("--batch-name")
    parser.add_argument("--model-name", default="ctr_logistic_regression")
    parser.add_argument("--model-version", default="v1")
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


def assign_score_deciles(scores) -> list[int]:
    score_list = [float(score) for score in scores]
    total = len(score_list)
    if total == 0:
        return []
    ranked_indices = sorted(range(total), key=lambda idx: score_list[idx], reverse=True)
    deciles = [0] * total
    for rank_position, original_index in enumerate(ranked_indices):
        band = min(9, math.floor((rank_position * 10) / total))
        deciles[original_index] = 10 - band
    return deciles


def build_score_summary(prediction_frame) -> dict[str, object]:
    if prediction_frame.empty:
        return {
            "row_count": 0,
            "avg_predicted_ctr": 0.0,
            "actual_ctr": 0.0,
            "top_decile_row_count": 0,
            "top_decile_actual_ctr": 0.0,
            "top_decile_avg_predicted_ctr": 0.0,
        }

    top_decile = prediction_frame[prediction_frame["is_top_decile"] == True]
    return {
        "row_count": int(len(prediction_frame)),
        "avg_predicted_ctr": float(prediction_frame["predicted_ctr"].mean()),
        "actual_ctr": float(prediction_frame["actual_click"].mean()),
        "top_decile_row_count": int(len(top_decile)),
        "top_decile_actual_ctr": float(top_decile["actual_click"].mean()) if not top_decile.empty else 0.0,
        "top_decile_avg_predicted_ctr": (
            float(top_decile["predicted_ctr"].mean()) if not top_decile.empty else 0.0
        ),
    }


def fetch_model_metadata(connection, model_name: str, model_version: str) -> dict[str, object]:
    query = """
        select
            mr.model_id,
            mr.model_name,
            mr.model_version,
            mr.artifact_path,
            mr.feature_columns,
            tr.training_run_id
        from ml.model_registry mr
        left join ml.training_runs tr
          on tr.model_id = mr.model_id
         and tr.run_status = 'SUCCESS'
        where mr.model_name = %s
          and mr.model_version = %s
          and mr.model_status = 'READY'
        order by tr.completed_at desc nulls last, tr.training_run_id desc nulls last
        limit 1;
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (model_name, model_version))
        row = cursor.fetchone()
    if not row:
        raise ValueError(f"No READY model found for {model_name} {model_version}")
    feature_columns = row[4]
    if not feature_columns:
        raise ValueError(f"Model {model_name} {model_version} does not have feature columns recorded.")
    return {
        "model_id": int(row[0]),
        "model_name": str(row[1]),
        "model_version": str(row[2]),
        "artifact_path": str(row[3]),
        "feature_columns": [str(column) for column in feature_columns],
        "training_run_id": int(row[5]) if row[5] is not None else None,
    }


def load_feature_frame(connection, batch_id: int, feature_columns: list[str]):
    import pandas as pd

    selected_columns = ["raw_event_id", "batch_id", TARGET_COLUMN, *feature_columns]
    projection = ", ".join(selected_columns)
    query = f"""
        select {projection}
        from feature_store.ctr_training_features
        where batch_id = %s
        order by raw_event_id;
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (batch_id,))
        rows = cursor.fetchall()
        columns = [desc.name for desc in cursor.description]
    return pd.DataFrame(rows, columns=columns)


def upsert_prediction_scores(connection, prediction_frame, model_id: int, training_run_id: int | None, model_name: str, model_version: str) -> None:
    query = """
        insert into ml.prediction_scores (
            raw_event_id,
            batch_id,
            model_id,
            training_run_id,
            model_name,
            model_version,
            predicted_ctr,
            score_decile,
            is_top_decile,
            actual_click,
            notes
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (raw_event_id, batch_id, model_name, model_version)
        do update set
            model_id = excluded.model_id,
            training_run_id = excluded.training_run_id,
            predicted_ctr = excluded.predicted_ctr,
            score_decile = excluded.score_decile,
            is_top_decile = excluded.is_top_decile,
            actual_click = excluded.actual_click,
            scored_at = now(),
            notes = excluded.notes;
    """
    notes = f"Batch CTR scoring output for {model_name} {model_version}."
    payload_rows = [
        (
            int(row.raw_event_id),
            int(row.batch_id),
            model_id,
            training_run_id,
            model_name,
            model_version,
            float(row.predicted_ctr),
            int(row.score_decile),
            bool(row.is_top_decile),
            int(row.actual_click),
            notes,
        )
        for row in prediction_frame.itertuples(index=False)
    ]
    with connection.cursor() as cursor:
        cursor.executemany(query, payload_rows)


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

    pipeline_run_id = create_pipeline_run(
        batch_id=batch_id,
        pipeline_name="ml_batch_scoring",
        layer_name="ml",
        source_file=source_file,
        triggered_by=args.triggered_by,
        database=args.database,
        args=args,
    )
    pipeline_step_id = create_pipeline_step(
        pipeline_run_id=pipeline_run_id,
        batch_id=batch_id,
        step_name="score_ctr_batch",
        layer_name="ml",
        target_table="ml.prediction_scores",
        source_file=source_file,
        database=args.database,
        args=args,
    )

    try:
        import pandas as pd

        with connect(args) as connection:
            model_metadata = fetch_model_metadata(connection, args.model_name, args.model_version)
            feature_frame = load_feature_frame(connection, batch_id, model_metadata["feature_columns"])

        if feature_frame.empty:
            raise ValueError(f"No feature store rows found for batch {batch_name}")

        artifact_path = Path(model_metadata["artifact_path"])
        if artifact_path.suffix == ".pkl":
            with artifact_path.open("rb") as handle:
                bundle = pickle.load(handle)
            model = bundle["model"] if isinstance(bundle, dict) and "model" in bundle else bundle
        else:
            import joblib

            model = joblib.load(artifact_path)
        scored_probabilities = model.predict_proba(feature_frame[model_metadata["feature_columns"]])[:, 1]
        deciles = assign_score_deciles(scored_probabilities)

        prediction_frame = pd.DataFrame(
            {
                "raw_event_id": feature_frame["raw_event_id"].astype(int),
                "batch_id": feature_frame["batch_id"].astype(int),
                "predicted_ctr": scored_probabilities,
                "score_decile": deciles,
                "is_top_decile": [decile == 10 for decile in deciles],
                "actual_click": feature_frame[TARGET_COLUMN].astype(int),
            }
        )

        summary_payload = build_score_summary(prediction_frame)
        summary_payload["metadata"] = {
            "batch_id": batch_id,
            "batch_name": batch_name,
            "model_name": model_metadata["model_name"],
            "model_version": model_metadata["model_version"],
            "training_run_id": model_metadata["training_run_id"],
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        }

        scoring_root = ML_SCORING_DIR / args.model_name / args.model_version / batch_name
        scoring_root.mkdir(parents=True, exist_ok=True)
        predictions_path = scoring_root / "prediction_scores.csv"
        summary_path = scoring_root / "score_summary.json"
        prediction_frame.to_csv(predictions_path, index=False)
        summary_path.write_text(json.dumps(summary_payload, indent=2))

        with connect(args) as connection:
            upsert_prediction_scores(
                connection,
                prediction_frame,
                model_id=model_metadata["model_id"],
                training_run_id=model_metadata["training_run_id"],
                model_name=model_metadata["model_name"],
                model_version=model_metadata["model_version"],
            )
            connection.commit()

        register_batch_artifact(
            batch_id=batch_id,
            pipeline_run_id=pipeline_run_id,
            artifact_name=f"{args.model_name}_{args.model_version}_prediction_scores",
            artifact_type="ml_prediction_scores",
            artifact_format="csv",
            artifact_path=str(predictions_path),
            row_count=len(prediction_frame),
            artifact_status="READY",
            notes="Batch CTR prediction scores.",
            database=args.database,
            args=args,
        )
        register_batch_artifact(
            batch_id=batch_id,
            pipeline_run_id=pipeline_run_id,
            artifact_name=f"{args.model_name}_{args.model_version}_score_summary",
            artifact_type="ml_scoring_summary",
            artifact_format="json",
            artifact_path=str(summary_path),
            row_count=1,
            artifact_status="READY",
            notes="Batch CTR scoring summary.",
            database=args.database,
            args=args,
        )

        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="SUCCESS",
            rows_processed=len(prediction_frame),
            step_message=f"Scored {len(prediction_frame)} rows for {batch_name}.",
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="SUCCESS",
            run_message=f"CTR batch scoring completed for {batch_name}.",
            database=args.database,
            args=args,
        )

        print(f"Project root: {PROJECT_ROOT}")
        print(f"Batch name: {batch_name}")
        print(f"Scored rows: {len(prediction_frame)}")
        print(f"Predictions artifact: {predictions_path}")
        print(f"Summary artifact: {summary_path}")
        print(f"Average predicted CTR: {summary_payload['avg_predicted_ctr']:.6f}")
        print(f"Top decile actual CTR: {summary_payload['top_decile_actual_ctr']:.6f}")
        print("CTR batch scoring completed successfully.")
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
