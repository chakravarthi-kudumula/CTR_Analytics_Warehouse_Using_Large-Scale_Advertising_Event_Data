# Final Benchmark Report

## Purpose

This report summarizes the current scale-up evidence for the CTR analytics platform after moving from the `1M` development batch to the `5M` validation batch.

Compared batches:

- `batch_id = 5`
  - `criteo_1m_airflow_feature_store_batch_2`
- `batch_id = 6`
  - `criteo_5m_airflow_scale_batch_1`

## Source Summary

| Metric | 1M Batch | 5M Batch |
|---|---:|---:|
| Sample scale | `1m` | `5m` |
| Raw rows loaded | 1,000,000 | 5,000,000 |
| Staging rows built | 1,000,000 | 5,000,000 |
| Warehouse fact rows built | 1,000,000 | 5,000,000 |
| Final batch status | `QUALITY_CHECKED` | `QUALITY_CHECKED` |

## Stage Runtime Comparison

| Stage | 1M Runtime (s) | 5M Runtime (s) | Scale Factor |
|---|---:|---:|---:|
| `raw_layer_load` | 26 | 114 | 4.38x |
| `spark_batch_processing` | 44 | 220 | 5.00x |
| `staging_layer_build` | 34 | 126 | 3.71x |
| `warehouse_layer_build` | 59 | 348 | 5.90x |
| `quality_framework` | 8 | 47 | 5.88x |
| `feature_store_build` | 1 | 12 | 12.00x |

Notes:

- `marts_layer_build` and `advanced_sql_build` were recorded as `0.000s` in the earlier 1M and 5M benchmark snapshots because those captures happened before runtime precision was tightened.
- This does not mean the stages were skipped. The Airflow task history confirms they ran successfully.

## Throughput Comparison

| Stage | 1M Rows/Sec | 5M Rows/Sec |
|---|---:|---:|
| `raw_layer_load` | 38,461.538 | 43,859.649 |
| `spark_batch_processing` | 22,727.273 | 22,727.273 |
| `staging_layer_build` | 29,411.765 | 39,682.540 |
| `warehouse_layer_build` | 16,949.153 | 14,367.816 |
| `quality_framework` | 125,000.000 | 106,382.979 |
| `feature_store_build` | 1,000,000.000 | 416,666.667 |

Interpretation:

- the warehouse layer remains the slowest SQL-heavy stage and the clearest tuning target
- Spark scaled predictably between `1M` and `5M`
- raw and staging throughput remained healthy at the larger batch size
- feature store build still completed quickly, but the larger batch reduced rows-per-second efficiency

## Storage Comparison

### Source and processed files

| Object | 1M Size (MB) | 5M Size (MB) |
|---|---:|---:|
| sample CSV | 232.836 | 1164.354 |
| processed Spark artifacts | 62.808 | 337.455 |

### PostgreSQL table sizes

| Table | 1M Size (MB) | 5M Size (MB) |
|---|---:|---:|
| `raw.criteo_events` | 361.844 | 1808.828 |
| `staging.stg_criteo_events` | 430.641 | 2152.492 |
| `warehouse.fact_ad_events` | 179.227 | 895.453 |
| `feature_store.ctr_training_features` | 641.859 | 3208.492 |

Interpretation:

- storage growth was broadly consistent with the 5x row increase
- the feature store is the largest downstream table and should stay in focus for future storage tuning
- raw and staging size growth is predictable enough to support larger future batches with capacity planning

## Platform Findings

### What scaled well

- Airflow orchestration remained stable at `5M`
- batch registry, quality tracking, and benchmark capture all held up
- Spark preprocessing scaled without requiring a redesign
- incoming batch automation and archive lifecycle remained intact

### Main bottlenecks

- `warehouse_layer_build` is the slowest SQL stage
- `quality_framework` becomes materially heavier at `5M`
- feature-store storage footprint is the largest downstream footprint

### Operational conclusion

The platform has now moved from:

- “designed for `5M`”

to:

- “validated on `5M` with measured runtime and storage evidence”

## Roadmap Gap Closed

This report closes the roadmap item that called for a benchmarked, portfolio-ready comparison between the `1M` baseline and the `5M` scaled platform.
