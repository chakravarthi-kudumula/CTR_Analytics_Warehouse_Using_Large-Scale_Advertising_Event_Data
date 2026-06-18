#!/usr/bin/env python3

"""Manage incoming raw files for automated batch intake."""

from __future__ import annotations

import argparse
import csv
import shutil
from datetime import datetime, timezone
from pathlib import Path

from pipeline_tracking import (
    compute_file_sha256,
    ensure_pipeline_metadata,
    fetch_batch_details,
    mark_batch_file_location,
    register_batch_artifact,
    register_incoming_batch,
    update_batch_status,
)
from project_config import (
    INCOMING_DIR,
    PROJECT_ROOT,
    SQL_DIR,
    SUPPORTED_INCOMING_SUFFIXES,
    add_db_connection_args,
    ensure_batch_directories,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage incoming raw batch files.")
    parser.add_argument("--action", choices=["discover", "archive", "fail"], required=True)
    parser.add_argument("--batch-name")
    parser.add_argument("--source-path")
    add_db_connection_args(parser)
    return parser.parse_args()


def count_csv_rows(csv_file: Path) -> int:
    with csv_file.open("r", newline="") as source:
        return max(sum(1 for _ in source) - 1, 0)


def infer_sample_scale(file_path: Path) -> str:
    stem = file_path.stem.lower()
    if "100k" in stem:
        return "100k"
    if "1m" in stem:
        return "1m"
    if "5m" in stem:
        return "5m"
    return "incoming"


def build_batch_name(file_path: Path, checksum: str) -> str:
    safe_stem = file_path.stem.lower().replace(" ", "_").replace("-", "_")
    return f"{safe_stem}_{checksum[:12]}"


def locate_candidate_file(project_root: Path, explicit_path: str | None) -> Path | None:
    if explicit_path:
        candidate = Path(explicit_path).expanduser().resolve()
        return candidate if candidate.exists() else None

    candidates = sorted(
        [
            path
            for path in INCOMING_DIR.iterdir()
            if path.is_file() and path.suffix.lower() in SUPPORTED_INCOMING_SUFFIXES
        ],
        key=lambda path: (path.stat().st_mtime, path.name),
    )
    return candidates[0] if candidates else None


def discover_batch(args: argparse.Namespace, project_root: Path) -> None:
    sql_dir = SQL_DIR
    ensure_pipeline_metadata(sql_dir, args.database, args)
    _, archive_dir, failed_dir = ensure_batch_directories()

    candidate_file = locate_candidate_file(project_root, args.source_path)
    if candidate_file is None:
        print("NO_BATCH_FOUND")
        return

    checksum = compute_file_sha256(candidate_file)
    batch_name = build_batch_name(candidate_file, checksum)
    expected_rows = count_csv_rows(candidate_file)
    batch_id = register_incoming_batch(
        batch_name=batch_name,
        source_file=candidate_file.name,
        source_path=str(candidate_file),
        source_checksum=checksum,
        sample_scale=infer_sample_scale(candidate_file),
        expected_row_count=expected_rows,
        notes="Registered by incoming_batch.py",
        database=args.database,
        args=args,
    )
    details = fetch_batch_details(batch_name, args.database, args)
    status = details["batch_status"] or "REGISTERED"
    should_process = status not in {"QUALITY_CHECKED", "ARCHIVED"}

    if details["archive_path"]:
        should_process = False

    register_batch_artifact(
        batch_id=batch_id,
        pipeline_run_id=None,
        artifact_name=candidate_file.name,
        artifact_type="incoming_file",
        artifact_format=candidate_file.suffix.lower().lstrip(".") or "csv",
        artifact_path=str(candidate_file),
        row_count=expected_rows,
        artifact_status="DISCOVERED",
        notes="Detected in incoming raw folder.",
        database=args.database,
        args=args,
    )

    print(
        "|".join(
            [
                str(batch_id),
                batch_name,
                candidate_file.name,
                str(candidate_file),
                str(details["sample_scale"]),
                "true" if should_process else "false",
                str(archive_dir / candidate_file.name),
                str(failed_dir / candidate_file.name),
            ]
        )
    )


def move_batch_file(args: argparse.Namespace, project_root: Path, target_kind: str) -> None:
    if not args.batch_name:
        raise ValueError("--batch-name is required for archive/fail actions.")

    sql_dir = SQL_DIR
    ensure_pipeline_metadata(sql_dir, args.database, args)
    _, archive_dir, failed_dir = ensure_batch_directories()
    details = fetch_batch_details(args.batch_name, args.database, args)
    batch_id = int(details["batch_id"])
    source_type = str(details["source_type"])
    source_path_text = details["source_path"]
    if source_type != "incoming" or not source_path_text:
        print(f"SKIP_{target_kind.upper()}|{args.batch_name}|not_incoming")
        return

    source_path = Path(str(source_path_text))
    target_dir = archive_dir if target_kind == "archive" else failed_dir
    target_path = target_dir / source_path.name

    if source_path.exists():
        if target_path.exists():
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            target_path = target_dir / f"{source_path.stem}_{timestamp}{source_path.suffix}"
        shutil.move(str(source_path), str(target_path))

    if target_kind == "archive":
        update_batch_status(
            batch_id=batch_id,
            batch_status="ARCHIVED",
            last_successful_stage=str(details["batch_status"] or "quality"),
            row_column=None,
            row_count=None,
            mark_started=False,
            mark_completed=True,
            database=args.database,
            args=args,
        )
        mark_batch_file_location(
            batch_id=batch_id,
            archive_path=str(target_path),
            failure_path=None,
            database=args.database,
            args=args,
        )
    else:
        update_batch_status(
            batch_id=batch_id,
            batch_status="FAILED_FILE_MOVED",
            last_successful_stage=str(details["last_successful_stage"]) if details["last_successful_stage"] else None,
            row_column=None,
            row_count=None,
            mark_started=False,
            mark_completed=False,
            database=args.database,
            args=args,
        )
        mark_batch_file_location(
            batch_id=batch_id,
            archive_path=None,
            failure_path=str(target_path),
            database=args.database,
            args=args,
        )

    register_batch_artifact(
        batch_id=batch_id,
        pipeline_run_id=None,
        artifact_name=target_path.name,
        artifact_type=f"{target_kind}_file",
        artifact_format=target_path.suffix.lower().lstrip(".") or "csv",
        artifact_path=str(target_path),
        row_count=None,
        artifact_status=target_kind.upper(),
        notes=f"Moved to {target_kind} folder by incoming_batch.py.",
        database=args.database,
        args=args,
    )
    print(f"{target_kind.upper()}D|{args.batch_name}|{target_path}")


def main() -> None:
    args = parse_args()
    project_root = PROJECT_ROOT

    if args.action == "discover":
        discover_batch(args, project_root)
        return
    if args.action == "archive":
        move_batch_file(args, project_root, "archive")
        return
    move_batch_file(args, project_root, "fail")


if __name__ == "__main__":
    main()
