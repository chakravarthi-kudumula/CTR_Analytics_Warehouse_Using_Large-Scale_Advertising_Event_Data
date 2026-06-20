# Warehouse Layer Notes

The warehouse layer turns staging data into analytics-ready fact and dimension tables without pretending that the anonymized Criteo fields represent campaign, user, device, or browser entities.

## Warehouse Design

- `warehouse.fact_ad_events`
  - one row per ad impression
  - measures: `impression_count`, `click_count`
- `warehouse.dim_event_day`
  - derived training-window day buckets from `event_day_number`
- `warehouse.dim_numeric_bucket`
  - reusable bucket definitions for `I1` to `I13`
- `warehouse.dim_categorical_value`
  - hashed categorical values for `C1` to `C26`
- `warehouse.bridge_event_numeric_bucket`
  - optional extension for feature-level warehouse analysis
- `warehouse.bridge_event_categorical_value`
  - optional extension for feature-level warehouse analysis

## Why Bridge Tables Are Not Loaded By Default

The bridge tables are part of the warehouse design, but they are not populated in the default Stage 4 build.

Reason:

- the full `1M` development build would create about `13M` numeric bridge rows and `26M` categorical bridge rows
- that made the warehouse build too heavy for a normal laptop iteration loop
- the core warehouse is more useful for continuing into marts without blocking progress

So the default Stage 4 build focuses on:

- `warehouse.fact_ad_events`
- `warehouse.dim_event_day`
- `warehouse.dim_numeric_bucket`
- `warehouse.dim_categorical_value`

The bridge tables remain available as a later extension when we specifically need feature-level warehouse joins.

## Performance Improvements

The warehouse loader was tightened to behave better at larger batch sizes:

- the fact load is batch-scoped instead of full-refresh
- categorical dimension extraction now uses one batch scan with `cross join lateral (values ...)`
- unnecessary `order by` clauses were removed from the batch inserts
- staging and fact indexes were expanded to support batch-local joins and lookups

These changes reduced unnecessary sorting and repeated table scans, which matters most when the project scales from `1M` to `5M`.

## Build Command

Run the warehouse build from the project root:

```bash
python3 scripts/warehouse_build.py
```

## Validation

Run the warehouse validation queries:

```bash
psql -p 5432 -U chakri -d ctr_analytics -f sql/10_warehouse_layer_checks.sql
```

The checks cover:

- staging-to-fact row count parity
- warehouse dimension row counts
- bridge row counts
- CTR by derived event day
- derived measure validity
- unknown value coverage in the categorical dimension
- recent warehouse audit history

## Warehouse Summary Views

Two lightweight warehouse views are available for easier Stage 5 mart development:

- `warehouse.v_event_day_summary`
  - CTR and volume summary by derived event day
- `warehouse.v_data_quality_summary`
  - batch-level CTR and missing-value summary
