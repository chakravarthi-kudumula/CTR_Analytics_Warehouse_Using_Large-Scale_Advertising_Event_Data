#!/usr/bin/env python3

"""Run the local CTR pipeline end to end from a single command."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
from typing import Iterable

from project_config import PROJECT_ROOT, SAMPLE_FILES, add_db_connection_args

TRIGGERED_BY = "local_pipeline_runner"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the CTR pipeline end to end from a single command")
    parser.add_argument("--sample", choices=SAMPLE_FILES, help="Sample scale to process for a local run.")
    parser.add_argument("--source-path", help="Explicit incoming CSV path to process through the pipeline.")
    parser.add_argument("--batch-name", help="Explicit batch name. Required with --source-path.")
    parser.add_argument("--sample-scale", default="incoming", help="Sample scale label to use with --source-path.")
    parser.add_argument("--use-spark", action="store_true", help="Run the PySpark preprocessing stage as part of the pipeline.")
    parser.add_argument("--skip-quality", action="store_true", help="Skip the quality framework step.")
    parser.add_argument("--skip-benchmarks", action="store_true", help="Skip the benchmark capture step.")
    add_db_connection_args(parser, include_maintenance_database=True)
    args = parser.parse_args()
    if not args.sample and not args.source_path:
        parser.error("Either --sample or --source-path is required.")
    if args.source_path and not args.batch_name:
        parser.error("--batch-name is required when using --source-path.")
    return args


def determine_batch_name(args: argparse.Namespace) -> str:
    if args.batch_name:
        return args.batch_name
    if args.sample:
        return f"criteo_{args.sample}_runner_batch"
    raise ValueError("Unable to determine batch name.")


def extend_db_args(command: list[str], args: argparse.Namespace, *, include_maintenance_database: bool = False) -> list[str]:
    if args.host:
        command.extend(["--host", str(args.host)])
    if args.port:
        command.extend(["--port", str(args.port)])
    if args.database:
        command.extend(["--database", str(args.database)])
    if include_maintenance_database and args.maintenance_database:
        command.extend(["--maintenance-database", str(args.maintenance_database)])
    if args.user:
        command.extend(["--user", str(args.user)])
    if args.password:
        command.extend(["--password", str(args.password)])
    return command


def build_step_plan(args: argparse.Namespace) -> list[tuple[str, list[str]]]:
    batch_name = determine_batch_name(args)

    if args.sample:
        raw_command = [
            "python3",
            "scripts/data_ingestion.py",
            "--sample",
            str(args.sample),
            "--batch-name",
            batch_name,
            "--triggered-by",
            TRIGGERED_BY,
        ]
    else:
        raw_command = [
            "python3",
            "scripts/data_ingestion.py",
            "--source-path",
            str(args.source_path),
            "--sample-scale",
            str(args.sample_scale),
            "--batch-name",
            batch_name,
            "--triggered-by",
            TRIGGERED_BY,
        ]
    extend_db_args(raw_command, args, include_maintenance_database=True)

    plan: list[tuple[str, list[str]]] = [("raw", raw_command)]

    if args.use_spark:
        spark_command = [
            "python3",
            "spark_jobs/spark_batch_processing.py",
            "--batch-name",
            batch_name,
            "--triggered-by",
            TRIGGERED_BY,
        ]
        if args.sample:
            spark_command.extend(["--sample", str(args.sample)])
        extend_db_args(spark_command, args)
        plan.append(("spark", spark_command))

    for step_name, script_path in [
        ("staging", "scripts/staging_load.py"),
        ("warehouse", "scripts/warehouse_build.py"),
        ("marts", "scripts/marts_build.py"),
        ("advanced_sql", "scripts/advanced_sql_build.py"),
        ("feature_store", "scripts/feature_store_build.py"),
    ]:
        command = [
            "python3",
            script_path,
            "--batch-name",
            batch_name,
            "--triggered-by",
            TRIGGERED_BY,
        ]
        extend_db_args(command, args)
        plan.append((step_name, command))

    if not args.skip_quality:
        quality_command = [
            "python3",
            "scripts/quality_checks.py",
            "--batch-name",
            batch_name,
            "--triggered-by",
            TRIGGERED_BY,
        ]
        extend_db_args(quality_command, args)
        plan.append(("quality", quality_command))

    if not args.skip_benchmarks:
        benchmark_command = [
            "python3",
            "scripts/benchmark_capture.py",
            "--batch-name",
            batch_name,
            "--triggered-by",
            TRIGGERED_BY,
        ]
        extend_db_args(benchmark_command, args)
        plan.append(("benchmarks", benchmark_command))

    return plan


def run_step(label: str, command: Iterable[str], cwd: Path) -> None:
    command_list = list(command)
    print(f"\n[{label}] {' '.join(command_list)}")
    subprocess.run(command_list, cwd=cwd, check=True)


def main() -> None:
    args = parse_args()
    plan = build_step_plan(args)
    batch_name = determine_batch_name(args)

    print(f"Pipeline batch: {batch_name}")
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Planned steps: {', '.join(label for label, _ in plan)}")

    for label, command in plan:
        run_step(label, command, PROJECT_ROOT)

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()
