# Config And Repo Cleanup Notes

## Purpose

This cleanup step makes the project easier to maintain as the platform grows.

Two improvements were added:

- a shared configuration layer for common paths and database defaults
- a dedicated `spark_jobs/` area for Spark-specific implementation code

## Shared Config Layer

Main file:

- `scripts/project_config.py`

This module now centralizes:

- project-root paths
- raw, sample, processed, and batch-lifecycle directories
- PostgreSQL default connection values
- sample file names
- reusable CLI argument helpers for database parameters

This reduces repeated hardcoded values across scripts and makes future refactors safer.

## Spark Repo Cleanup

Canonical Spark job:

- `spark_jobs/spark_batch_processing.py`

Compatibility wrapper:

- `scripts/spark_batch_processing.py`

Why both exist:

- `spark_jobs/` makes the repo structure more honest and scalable
- the wrapper prevents older commands and references from breaking immediately

## Current Design Outcome

The repo now has a cleaner split between:

- `scripts/`
  - orchestration helpers
  - ingestion utilities
  - quality and benchmark runners
  - compatibility entry points
- `spark_jobs/`
  - Spark-specific transformation implementation

## Why This Matters

This closes another roadmap gap by introducing:

- a central config pattern
- a more maintainable repo structure for Spark-based batch processing
