# Repository Guide

## Best Starting Points

If you are new to the project, read these in order:

1. `README.md`
2. `docs/project_summary.md`
3. `docs/architecture_overview.md`
4. `docs/final_benchmark_report.md`

## Key Folders

- `sql/`: schema, warehouse, marts, quality, drift, and ops SQL assets
- `scripts/`: Python runners and orchestration helpers
- `spark_jobs/`: canonical Spark processing code
- `airflow/dags/`: Airflow orchestration entrypoints
- `docs/`: architecture, benchmarking, lineage, and operational notes
- `dashboards/`: Power BI handoff assets

## Best Demo Commands

Sample end-to-end run:

```bash
python3 scripts/run_pipeline.py --sample 1m --batch-name criteo_1m_demo --use-spark
```

Incoming-style discovery:

```bash
python3 scripts/incoming_batch.py --action discover
```

## Best Tables And Views To Inspect

- `warehouse.fact_ad_events`
- `marts.overall_ctr_summary`
- `marts.feature_ctr_summary`
- `feature_store.ctr_training_features`
- `ops.pipeline_health_dashboard`
- `ops.batch_runtime_trend`
- `ops.sample_scale_benchmark_comparison`
