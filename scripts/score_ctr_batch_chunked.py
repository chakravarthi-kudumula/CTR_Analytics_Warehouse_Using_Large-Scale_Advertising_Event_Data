#!/usr/bin/env python3

"""Score large batches in chunks and write results into ml.prediction_scores."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

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
from project_config import ML_SCORING_DIR, PROJECT_ROOT, SQL_DIR, add_db_connection_args, ensure_ml_directories
from score_ctr_batch import assign_score_deciles, build_score_summary, connect, fetch_model_metadata

TARGET_COLUMN = "label"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score a large batch with chunked prediction processing")
    parser.add_argument("--batch-name")
    parser.add_argument("--model-name", default="ctr_sgd_logistic")
    parser.add_argument("--model-version", default="v1")
    parser.add_argument("--chunksize", type=int, default=50000)
    parser.add_argument("--triggered-by", default="manual")
    add_db_connection_args(parser)
    return parser.parse_args()


def iter_feature_chunks(connection, batch_id: int, feature_columns: list[str], *, chunksize: int):
    import pandas as pd

    select_columns = ["raw_event_id", "batch_id", TARGET_COLUMN, *feature_columns]
    projection = ", ".join(select_columns)
    query = f"""
        select {projection}
        from feature_store.ctr_training_features
        where batch_id = %s
        order by raw_event_id;
    """
    with connection.cursor(name="ml_chunked_scoring_cursor") as cursor:
        cursor.itersize = chunksize
        cursor.execute(query, (batch_id,))
        columns = [desc.name for desc in cursor.description]
        while True:
            rows = cursor.fetchmany(chunksize)
            if not rows:
                break
            yield pd.DataFrame(rows, columns=columns)


def upsert_prediction_scores_chunked(
    connection,
    prediction_frame,
    *,
    model_id: int,
    training_run_id: int | None,
    model_name: str,
    model_version: str,
    chunksize: int,
) -> None:
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
    note = f"Chunked batch CTR scoring output for {model_name} {model_version}."
    rows = list(
        zip(
            prediction_frame["raw_event_id"].astype(int),
            prediction_frame["batch_id"].astype(int),
            [model_id] * len(prediction_frame),
            [training_run_id] * len(prediction_frame),
            [model_name] * len(prediction_frame),
            [model_version] * len(prediction_frame),
            prediction_frame["predicted_ctr"].astype(float),
            prediction_frame["score_decile"].astype(int),
            prediction_frame["is_top_decile"].astype(bool),
            prediction_frame["actual_click"].astype(int),
            [note] * len(prediction_frame),
        )
    )
    with connection.cursor() as cursor:
        for start in range(0, len(rows), chunksize):
            cursor.executemany(query, rows[start : start + chunksize])


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

    pipeline_run_id = create_pipeline_run(
        batch_id=batch_id,
        pipeline_name="ml_chunked_batch_scoring",
        layer_name="ml",
        source_file=source_file,
        triggered_by=args.triggered_by,
        database=args.database,
        args=args,
    )
    pipeline_step_id = create_pipeline_step(
        pipeline_run_id=pipeline_run_id,
        batch_id=batch_id,
        step_name="score_ctr_batch_chunked",
        layer_name="ml",
        target_table="ml.prediction_scores",
        source_file=source_file,
        database=args.database,
        args=args,
    )

    try:
        import joblib
        import pandas as pd

        with connect(args) as connection:
            model_metadata = fetch_model_metadata(connection, args.model_name, args.model_version)

        model = joblib.load(model_metadata["artifact_path"])

        raw_event_ids: list[int] = []
        batch_ids: list[int] = []
        scores: list[float] = []
        actual_clicks: list[int] = []

        with connect(args) as connection:
            for feature_chunk in iter_feature_chunks(
                connection,
                batch_id,
                model_metadata["feature_columns"],
                chunksize=args.chunksize,
            ):
                x_chunk = feature_chunk[model_metadata["feature_columns"]].fillna(0)
                score_chunk = model.predict_proba(x_chunk)[:, 1]
                raw_event_ids.extend(feature_chunk["raw_event_id"].astype(int).tolist())
                batch_ids.extend(feature_chunk["batch_id"].astype(int).tolist())
                actual_clicks.extend(feature_chunk[TARGET_COLUMN].astype(int).tolist())
                scores.extend(score_chunk.tolist())

        prediction_frame = pd.DataFrame(
            {
                "raw_event_id": raw_event_ids,
                "batch_id": batch_ids,
                "predicted_ctr": scores,
                "actual_click": actual_clicks,
            }
        )
        prediction_frame["score_decile"] = assign_score_deciles(prediction_frame["predicted_ctr"])
        prediction_frame["is_top_decile"] = prediction_frame["score_decile"] == 10

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
            upsert_prediction_scores_chunked(
                connection,
                prediction_frame,
                model_id=model_metadata["model_id"],
                training_run_id=model_metadata["training_run_id"],
                model_name=model_metadata["model_name"],
                model_version=model_metadata["model_version"],
                chunksize=args.chunksize,
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
            notes="Chunked batch CTR prediction scores.",
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
            notes="Chunked batch CTR scoring summary.",
            database=args.database,
            args=args,
        )

        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="SUCCESS",
            rows_processed=len(prediction_frame),
            step_message=f"Chunked scoring completed for {batch_name}.",
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="SUCCESS",
            run_message=f"Chunked scoring completed for {batch_name}.",
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
        print("Chunked CTR batch scoring completed successfully.")
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
