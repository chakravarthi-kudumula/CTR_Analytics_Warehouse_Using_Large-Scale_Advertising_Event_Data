# Staging Layer Notes

The staging layer standardizes the raw Criteo data and adds lightweight derived fields for downstream warehouse development.

## Key Design Choices

- numerical nulls are replaced with `0`
- categorical nulls and empty strings are replaced with `'unknown'`
- `raw_event_id` preserves lineage back to `raw.criteo_events`
- `event_day_number` is a derived 1-to-7 bucket from source row order
- `event_batch` is inferred from `source_file`
- `click_flag` and `click_count` are normalized to binary values
- `impression_count` is always `1`

## Build Command

Run the staging build from the project root:

```bash
python3 scripts/staging_load.py
```

## Validation

Run the staging validation queries:

```bash
psql -p 5432 -U chakri -d ctr_analytics -f sql/09_staging_layer_checks.sql
```

The checks cover:

- raw-to-staging row count parity
- duplicate detection on `raw_event_id`
- event-day distribution
- CTR by day bucket
- derived field validity
- column-level null checks after standardization
- missing-value summary
- unknown-rate profiling by categorical column
- recent staging audit history
