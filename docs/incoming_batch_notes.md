# Incoming Batch Notes

This project now supports a production-style incoming raw file flow.

## Folder Convention

- `data/raw/incoming/`
  - new files waiting to be processed
- `data/raw/archive/`
  - files that completed successfully
- `data/raw/failed/`
  - files moved aside after a failed batch run

## Batch Discovery

Use:

`python3 scripts/incoming_batch.py --action discover`

Behavior:

- scans the incoming folder for the oldest CSV file
- computes a SHA-256 checksum
- creates or reuses a batch in `ops.batch_registry`
- returns a stable `batch_name`

## Idempotency

Incoming files are tracked by checksum.

If the same file is discovered again:

- the existing batch is reused
- duplicate raw inserts are avoided by batch-scoped reloads
- already archived files are not processed again

## Incremental Processing

The following layers now process the current batch instead of truncating the whole table:

- `raw.criteo_events`
- `staging.stg_criteo_events`
- `warehouse.fact_ad_events`

Warehouse dimensions now use incremental insert patterns where practical:

- event-day dimension uses `on conflict do nothing`
- numeric bucket dimension uses `on conflict do nothing`
- categorical value dimension uses `on conflict do nothing`

## Airflow Behavior

The Airflow DAG now:

- checks for incoming files on schedule
- skips the run cleanly if nothing is waiting
- carries one explicit `batch_name` through the pipeline
- archives a successful incoming file
- moves a failed incoming file to the failed folder through the failure callback
