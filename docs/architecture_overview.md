# Architecture Overview

## Platform Flow

```mermaid
flowchart TD
    A["Incoming CSV files<br/>data/raw/incoming"] --> B["Airflow Sensor DAG<br/>ctr_incoming_file_sensor"]
    B --> C["Processing DAG<br/>ctr_batch_pipeline"]
    C --> D["Batch Registration<br/>ops.batch_registry"]
    C --> E["Raw Load<br/>raw.criteo_events"]
    E --> F["PySpark Processing<br/>spark_jobs/spark_batch_processing.py"]
    F --> G["Processed Artifacts<br/>data/processed/<batch_name>"]
    E --> H["Staging Layer<br/>staging.stg_criteo_events"]
    H --> I["Warehouse Layer<br/>warehouse.fact_ad_events + dimensions"]
    I --> J["Analytics Marts<br/>marts schema"]
    J --> K["Advanced SQL Assets<br/>ranked and rolling views"]
    J --> L["Feature Store<br/>feature_store.ctr_training_features"]
    C --> M["Quality Framework<br/>quality schema"]
    C --> N["Benchmark Capture<br/>ops.benchmark_snapshots"]
    D --> O["Ops Monitoring<br/>ops.pipeline_health_dashboard"]
    N --> O
    M --> O
    O --> P["Power BI<br/>Business + Ops Pages"]
```

## Main Design Choices

- PostgreSQL remains the serving warehouse and reporting source
- PySpark handles heavier batch preprocessing and profiling
- Airflow orchestrates lifecycle, retries, and batch progression
- `ops` stores metadata, runtime, and health visibility
- `quality` stores validation history and latest-run checks
- incoming batches are idempotent through checksum-based registration

## Current Entry Points

- production-style intake:
  - `ctr_incoming_file_sensor`
- processing DAG:
  - `ctr_batch_pipeline`
- manual Spark run:
  - `python3 spark_jobs/spark_batch_processing.py --sample 1m --batch-name <batch_name>`
