# Batch Drift Notes

This layer adds cross-batch monitoring on top of the existing ops and quality framework.

## Purpose

The earlier ops layer answered:

- did the latest batch succeed?
- how long did each stage take?
- were there quality failures or alerts?

The drift layer adds the next production-style questions:

- how did this batch change versus the previous one?
- did CTR shift unexpectedly?
- did missingness increase?
- did runtime regress for a particular stage?
- are row counts moving in ways we should investigate?

## Main Views

- `ops.batch_metric_baseline`
- `ops.batch_drift_summary`
- `ops.batch_stage_runtime_drift`

## `ops.batch_metric_baseline`

One row per batch with:

- batch identity and lifecycle fields
- fact row count
- click count
- CTR
- average missing numeric count
- average missing categorical count
- latest quality counts
- latest alert counts
- total stage runtime
- sample file size
- processed artifact size
- table storage sizes

This is the clean base view for ops comparisons and benchmark reporting.

## `ops.batch_drift_summary`

One row per batch with lag-based comparisons against the prior batch in the same `source_type` and `sample_scale`.

Main deltas:

- row-count delta
- CTR delta
- missing numeric delta
- missing categorical delta
- runtime delta
- failed-quality delta
- alert delta
- sample-file-size delta

Drift labels:

- `baseline`
- `stable`
- `quality_regression`
- `alert_regression`
- `ctr_shift`
- `missingness_shift`
- `runtime_regression`

This is designed for Power BI tables, ops monitoring, and batch review.

## `ops.batch_stage_runtime_drift`

Stage-level lag view that compares runtime and throughput by:

- `layer_name`
- `object_name`

This helps answer:

- which stage got slower?
- how much slower?
- did throughput fall even when row counts were similar?

## Validation

Run:

```bash
psql -p 5432 -U chakri -d ctr_analytics -f sql/20_batch_drift_checks.sql
```

## Why This Matters

This closes one of the main roadmap gaps: the platform now has cross-batch drift monitoring instead of only latest-batch health reporting.
