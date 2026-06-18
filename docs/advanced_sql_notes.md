# Advanced SQL Notes

Stage 6 is designed to showcase production-style analytical SQL rather than add another storage layer.

## Advanced SQL Assets Included

- `marts.feature_ctr_ranked`
- `marts.event_day_ctr_rolling`
- `marts.high_value_segments`
- `marts.low_performing_segments`
- `marts.feature_ctr_lift_ranked`
- `marts.feature_interaction_ranked`

## SQL Techniques Demonstrated

- chained CTEs
- window functions
- `row_number`
- `dense_rank`
- `percent_rank`
- `ntile`
- rolling metrics
- cumulative metrics
- conditional segmentation
- materialized views
- index creation on analytical outputs

## Real-World Framing

This stage is intentionally closer to how industry analytical engineering work is presented:

- reusable materialized outputs instead of one-off notebook queries
- segment qualification using volume thresholds
- cumulative and rolling metrics for trend interpretation
- explicit ranking logic for feature comparison
- optimization-minded indexing on analytical assets

## Materialized Views vs Plain Views

This stage intentionally uses materialized views for heavier analytical outputs instead of plain views.

Why:

- ranking, rolling windows, and segment qualification can be expensive to recompute repeatedly
- dashboard tools benefit from precomputed analytical outputs
- refresh-based analytical assets are closer to how real reporting systems are operated

When a plain view is still better:

- when logic is lightweight
- when fresh results are more important than query speed
- when the output is mostly a thin semantic layer over an already optimized object

In this project:

- heavy analytical outputs are materialized
- lightweight helper outputs in Stage 5 remain plain views

## Partitioning Strategy

True date partitioning is not appropriate here because the source dataset does not provide a real event timestamp.

Instead, the industry-grade partitioning strategy for future scale would be:

- partition large fact-like tables by `event_batch`
- optionally sub-partition by derived `event_day_number`
- keep staging and raw loads as full refreshes for development scale
- move to incremental partition refreshes only at `5M+` or full `45M+` scale

This keeps the project honest to the dataset while still showing sound partitioning design judgment.

## Build Command

Run the advanced SQL build from the project root:

```bash
python3 scripts/advanced_sql_build.py
```

Refresh only the analytical materialized views:

```bash
python3 scripts/analytics_refresh.py
```

## Validation

Run the advanced SQL validation queries:

```bash
psql -p 5432 -U chakri -d ctr_analytics -f sql/13_advanced_sql_checks.sql
```
