#!/usr/bin/env python3

"""Run PySpark batch processing for a sampled Criteo batch."""

from __future__ import annotations

import argparse
import csv
import os
from datetime import datetime, timezone
from pathlib import Path
from functools import reduce
import json
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from pipeline_tracking import (
    complete_pipeline_run,
    complete_pipeline_step,
    create_pipeline_run,
    create_pipeline_step,
    ensure_pipeline_metadata,
    fetch_batch_details,
    register_batch_artifact,
    run_psql,
    sql_literal,
    update_batch_status,
)
from project_config import SAMPLE_FILES, SQL_DIR, add_db_connection_args

NUMERIC_COLUMNS = [f"I{i}" for i in range(1, 14)]
CATEGORICAL_COLUMNS = [f"C{i}" for i in range(1, 27)]
COLUMN_NAMES = ["label", *NUMERIC_COLUMNS, *CATEGORICAL_COLUMNS]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run PySpark batch processing for a Criteo sample batch")
    parser.add_argument("--sample", choices=SAMPLE_FILES)
    parser.add_argument("--batch-name", required=True)
    parser.add_argument("--triggered-by", default="manual")
    add_db_connection_args(parser)
    return parser.parse_args()


def build_schema():
    from pyspark.sql import types as T

    fields = [T.StructField("label", T.IntegerType(), True)]
    fields.extend(T.StructField(name, T.IntegerType(), True) for name in NUMERIC_COLUMNS)
    fields.extend(T.StructField(name, T.StringType(), True) for name in CATEGORICAL_COLUMNS)
    return T.StructType(fields)


def build_spark_session(batch_name: str):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(f"ctr_batch_processing_{batch_name}")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )


def write_missing_profile(output_path: Path, summary_rows: list[dict[str, object]]) -> None:
    with output_path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["column_name", "column_group", "missing_count", "missing_rate"],
        )
        writer.writeheader()
        writer.writerows(summary_rows)


def main() -> None:
    args = parse_args()
    project_root = PROJECT_ROOT
    sql_dir = SQL_DIR

    ensure_pipeline_metadata(sql_dir, args.database, args)
    run_psql(sql_dir / "15_pipeline_metadata.sql", args.database, args)

    batch_details = fetch_batch_details(args.batch_name, args.database, args)
    batch_id = int(batch_details["batch_id"])
    source_file = str(batch_details["source_file"])
    sample_scale = str(batch_details["sample_scale"])
    source_path = Path(str(batch_details["source_path"])).expanduser().resolve()
    if not source_path.exists():
        if args.sample:
            source_path = project_root / "data" / "sample" / SAMPLE_FILES[args.sample]
        else:
            raise FileNotFoundError(f"Source file not found for batch {args.batch_name}: {source_path}")
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    pipeline_run_id = create_pipeline_run(
        batch_id=batch_id,
        pipeline_name="spark_batch_processing",
        layer_name="spark",
        source_file=source_file,
        triggered_by=args.triggered_by,
        database=args.database,
        args=args,
    )
    pipeline_step_id = create_pipeline_step(
        pipeline_run_id=pipeline_run_id,
        batch_id=batch_id,
        step_name="build_processed_batch_artifacts",
        layer_name="spark",
        target_table="data/processed",
        source_file=source_file,
        database=args.database,
        args=args,
    )
    update_batch_status(
        batch_id=batch_id,
        batch_status="SPARK_PROCESSING",
        last_successful_stage="raw",
        row_column=None,
        row_count=None,
        mark_started=False,
        mark_completed=False,
        database=args.database,
        args=args,
    )

    spark: SparkSession | None = None
    batch_output_dir = project_root / "data" / "processed" / args.batch_name
    parquet_path = batch_output_dir / "stg_criteo_events_parquet"
    metrics_path = batch_output_dir / "batch_metrics.json"
    missing_profile_path = batch_output_dir / "missing_value_profile.csv"
    batch_output_dir.mkdir(parents=True, exist_ok=True)

    try:
        from pyspark.sql import functions as F

        spark = build_spark_session(args.batch_name)
        raw_df = (
            spark.read.format("csv")
            .option("header", True)
            .schema(build_schema())
            .load(str(source_path))
        )

        total_rows = raw_df.count()
        if total_rows == 0:
            raise RuntimeError(f"No rows found in {source_path}")

        numeric_missing_exprs = [F.when(F.col(name).isNull(), 1).otherwise(0) for name in NUMERIC_COLUMNS]
        categorical_missing_exprs = [
            F.when(F.col(name).isNull() | (F.trim(F.col(name)) == ""), 1).otherwise(0)
            for name in CATEGORICAL_COLUMNS
        ]

        day_bucket_seed = F.concat_ws(
            "|",
            F.coalesce(F.col("label").cast("string"), F.lit("0")),
            *[F.coalesce(F.col(name).cast("string"), F.lit("0")) for name in NUMERIC_COLUMNS],
            *[F.coalesce(F.col(name), F.lit("unknown")) for name in CATEGORICAL_COLUMNS],
        )

        cleaned_df = (
            raw_df.withColumn("label", F.when(F.coalesce(F.col("label"), F.lit(0)) == 1, 1).otherwise(0))
            .select(
                "label",
                *[F.coalesce(F.col(name), F.lit(0)).cast("int").alias(name) for name in NUMERIC_COLUMNS],
                *[
                    F.when(F.col(name).isNull() | (F.trim(F.col(name)) == ""), F.lit("unknown"))
                    .otherwise(F.col(name))
                    .alias(name)
                    for name in CATEGORICAL_COLUMNS
                ],
                *[expr.alias(f"missing_numeric_flag_{index + 1}") for index, expr in enumerate(numeric_missing_exprs)],
                *[
                    expr.alias(f"missing_categorical_flag_{index + 1}")
                    for index, expr in enumerate(categorical_missing_exprs)
                ],
            )
            .withColumn(
                "event_day_number",
                F.pmod(F.xxhash64(day_bucket_seed), F.lit(7)).cast("int") + F.lit(1),
            )
            .withColumn("event_batch", F.lit(sample_scale))
            .withColumn("click_flag", F.col("label"))
            .withColumn("impression_count", F.lit(1))
            .withColumn("click_count", F.col("label"))
            .withColumn(
                "missing_numeric_count",
                reduce(
                    lambda left, right: left + right,
                    (F.col(f"missing_numeric_flag_{index}") for index in range(1, len(NUMERIC_COLUMNS) + 1)),
                ),
            )
            .withColumn(
                "missing_categorical_count",
                reduce(
                    lambda left, right: left + right,
                    (
                        F.col(f"missing_categorical_flag_{index}")
                        for index in range(1, len(CATEGORICAL_COLUMNS) + 1)
                    ),
                ),
            )
            .withColumn("source_file", F.lit(source_file))
            .withColumn("batch_name", F.lit(args.batch_name))
            .withColumn("processed_at", F.current_timestamp())
            .drop(
                *[f"missing_numeric_flag_{index}" for index in range(1, len(NUMERIC_COLUMNS) + 1)],
                *[f"missing_categorical_flag_{index}" for index in range(1, len(CATEGORICAL_COLUMNS) + 1)],
            )
        )

        cleaned_df.repartition(4).write.mode("overwrite").parquet(str(parquet_path))

        processed_df = spark.read.parquet(str(parquet_path))

        metrics_row = processed_df.agg(
            F.count("*").alias("total_rows"),
            F.sum("click_count").alias("total_clicks"),
            F.avg("click_flag").alias("overall_ctr"),
            F.avg("missing_numeric_count").alias("avg_missing_numeric_count"),
            F.avg("missing_categorical_count").alias("avg_missing_categorical_count"),
        ).collect()[0]
        metrics = {
            "batch_id": batch_id,
            "batch_name": args.batch_name,
            "source_file": source_file,
            "sample_scale": sample_scale,
            "total_rows": int(metrics_row["total_rows"]),
            "total_clicks": int(metrics_row["total_clicks"] or 0),
            "overall_ctr": round(float(metrics_row["overall_ctr"] or 0.0), 6),
            "avg_missing_numeric_count": round(float(metrics_row["avg_missing_numeric_count"] or 0.0), 6),
            "avg_missing_categorical_count": round(float(metrics_row["avg_missing_categorical_count"] or 0.0), 6),
            "processed_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        metrics_path.write_text(json.dumps(metrics, indent=2))

        missing_profile_frame = raw_df.agg(
            *[
                F.sum(
                    F.when(
                        F.col(column_name).isNull() | (F.trim(F.col(column_name).cast("string")) == ""),
                        1,
                    ).otherwise(0)
                ).alias(column_name)
                for column_name in COLUMN_NAMES
            ]
        )
        missing_profile_counts = missing_profile_frame.collect()[0].asDict()

        missing_profile_rows: list[dict[str, object]] = []
        for column_name in COLUMN_NAMES:
            missing_count = int(missing_profile_counts[column_name])
            column_group = (
                "numeric"
                if column_name in NUMERIC_COLUMNS
                else "categorical" if column_name in CATEGORICAL_COLUMNS else "label"
            )
            missing_profile_rows.append(
                {
                    "column_name": column_name,
                    "column_group": column_group,
                    "missing_count": int(missing_count),
                    "missing_rate": round(missing_count / total_rows, 6),
                }
            )
        write_missing_profile(missing_profile_path, missing_profile_rows)

        register_batch_artifact(
            batch_id=batch_id,
            pipeline_run_id=pipeline_run_id,
            artifact_name="spark_staging_parquet",
            artifact_type="spark_output",
            artifact_format="parquet",
            artifact_path=str(parquet_path),
            row_count=total_rows,
            artifact_status="READY",
            notes="PySpark cleaned batch output in local Spark mode.",
            database=args.database,
            args=args,
        )
        register_batch_artifact(
            batch_id=batch_id,
            pipeline_run_id=pipeline_run_id,
            artifact_name="spark_batch_metrics",
            artifact_type="spark_summary",
            artifact_format="json",
            artifact_path=str(metrics_path),
            row_count=1,
            artifact_status="READY",
            notes="PySpark batch-level CTR and missingness summary.",
            database=args.database,
            args=args,
        )
        register_batch_artifact(
            batch_id=batch_id,
            pipeline_run_id=pipeline_run_id,
            artifact_name="spark_missing_value_profile",
            artifact_type="spark_profile",
            artifact_format="csv",
            artifact_path=str(missing_profile_path),
            row_count=len(missing_profile_rows),
            artifact_status="READY",
            notes="Column-level missingness profile generated by PySpark.",
            database=args.database,
            args=args,
        )

        update_batch_status(
            batch_id=batch_id,
            batch_status="SPARK_READY",
            last_successful_stage="spark",
            row_column=None,
            row_count=None,
            mark_started=False,
            mark_completed=False,
            database=args.database,
            args=args,
        )
        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="SUCCESS",
            rows_processed=total_rows,
            step_message=f"PySpark batch processing completed successfully for {args.batch_name}.",
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="SUCCESS",
            run_message=f"PySpark batch processing completed successfully for {args.batch_name}.",
            database=args.database,
            args=args,
        )

        print(f"Processed rows: {total_rows:,}")
        print(f"Parquet output: {parquet_path}")
        print(f"Metrics output: {metrics_path}")
        print(f"Missingness output: {missing_profile_path}")
    except Exception as exc:
        update_batch_status(
            batch_id=batch_id,
            batch_status="SPARK_FAILED",
            last_successful_stage="raw",
            row_column=None,
            row_count=None,
            mark_started=False,
            mark_completed=False,
            database=args.database,
            args=args,
        )
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
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                pass


if __name__ == "__main__":
    main()
