#!/usr/bin/env python3

"""Shared project configuration for scripts and Airflow DAGs."""

from __future__ import annotations

import argparse
import getpass
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
INCOMING_DIR = RAW_DIR / "incoming"
ARCHIVE_DIR = RAW_DIR / "archive"
FAILED_DIR = RAW_DIR / "failed"
SAMPLE_DIR = DATA_DIR / "sample"
PROCESSED_DIR = DATA_DIR / "processed"
ML_DIR = DATA_DIR / "ml"
ML_TRAINING_DATASET_DIR = ML_DIR / "training_datasets"
ML_MODEL_DIR = ML_DIR / "models"
ML_SCORING_DIR = ML_DIR / "scoring"
ML_REPORT_DIR = ML_DIR / "reports"
SQL_DIR = PROJECT_ROOT / "sql"
SPARK_JOBS_DIR = PROJECT_ROOT / "spark_jobs"
ML_CANONICAL_BATCH_NAME = os.getenv("ML_CANONICAL_BATCH_NAME", "criteo_1m_ml_canonical_batch")
ML_DEFAULT_MODEL_NAME = os.getenv("ML_DEFAULT_MODEL_NAME", "ctr_logistic_regression")
ML_DEFAULT_MODEL_BASE_VERSION = os.getenv("ML_DEFAULT_MODEL_BASE_VERSION", "v3")
ML_RETRAIN_SCHEDULE = os.getenv("ML_RETRAIN_SCHEDULE", "30 3 * * 0")

DEFAULT_DB_HOST = os.getenv("PGHOST")
DEFAULT_DB_PORT = os.getenv("PGPORT", "5432")
DEFAULT_DB_NAME = os.getenv("PGDATABASE", "ctr_analytics")
DEFAULT_DB_USER = os.getenv("PGUSER", getpass.getuser())
DEFAULT_DB_PASSWORD = os.getenv("PGPASSWORD")
DEFAULT_MAINTENANCE_DB = os.getenv("PGMAINTENANCE_DB", "postgres")

SAMPLE_FILES = {
    "100k": "criteo_100k.csv",
    "1m": "criteo_1m.csv",
    "5m": "criteo_5m.csv",
}

SUPPORTED_INCOMING_SUFFIXES = {".csv"}


def add_db_connection_args(
    parser: argparse.ArgumentParser,
    *,
    include_maintenance_database: bool = False,
) -> argparse.ArgumentParser:
    parser.add_argument("--host", default=DEFAULT_DB_HOST)
    parser.add_argument("--port", default=DEFAULT_DB_PORT)
    parser.add_argument("--database", default=DEFAULT_DB_NAME)
    if include_maintenance_database:
        parser.add_argument("--maintenance-database", default=DEFAULT_MAINTENANCE_DB)
    parser.add_argument("--user", default=DEFAULT_DB_USER)
    parser.add_argument("--password", default=DEFAULT_DB_PASSWORD)
    return parser


def ensure_batch_directories() -> tuple[Path, Path, Path]:
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    FAILED_DIR.mkdir(parents=True, exist_ok=True)
    return INCOMING_DIR, ARCHIVE_DIR, FAILED_DIR


def ensure_ml_directories() -> Path:
    ML_TRAINING_DATASET_DIR.mkdir(parents=True, exist_ok=True)
    ML_MODEL_DIR.mkdir(parents=True, exist_ok=True)
    ML_SCORING_DIR.mkdir(parents=True, exist_ok=True)
    ML_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    return ML_TRAINING_DATASET_DIR
