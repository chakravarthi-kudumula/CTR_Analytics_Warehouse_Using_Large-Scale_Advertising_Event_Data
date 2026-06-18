# Raw Layer Notes

The raw layer keeps the source data as close to the file as possible. The only added fields are technical metadata used for warehouse operations:

- `event_id`
- `source_file`
- `ingested_at`

## Raw Load Rerun

Run a raw-layer load from the project root:

```bash
python3 scripts/data_ingestion.py --sample 100k --maintenance-database postgres
python3 scripts/data_ingestion.py --sample 1m --maintenance-database postgres
python3 scripts/data_ingestion.py --sample 5m --maintenance-database postgres
```

Each run:

- creates the database if needed
- creates the required schemas
- creates the raw table if it does not exist
- truncates `raw.criteo_events`
- loads the selected sample file
- validates final row count
- writes a success record into `quality.load_audit`

## Raw Validation Queries

Use the dedicated validation file:

```bash
psql -p 5432 -U chakri -d ctr_analytics -f sql/08_raw_layer_checks.sql
```

This checks:

- raw row count
- source file tagging
- ingestion timestamps
- recent raw audit history

