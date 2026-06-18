# Pipeline Operations Overview

This folder contains the dashboard-ready assets for the Power BI operations page built on top of:

- `ops.pipeline_health_dashboard`
- `ops.batch_runtime_trend`

Use these files as the implementation handoff when creating the actual Power BI page.

## Files

- `ops_page_queries.sql`
  - import queries for the two main ops views
- `ops_page_measures.dax`
  - recommended DAX measures for KPI cards and drill-down visuals
- `ops_page_fields.csv`
  - field mapping by visual

## Recommended Build Order

1. Connect Power BI to the PostgreSQL database.
2. Import the queries from `ops_page_queries.sql`.
3. Add the measures from `ops_page_measures.dax`.
4. Build the visuals using `ops_page_fields.csv` and `docs/power_bi_ops_page_spec.md`.

## Page Name

`Pipeline Operations Overview`

## Main Sources

- `ops.pipeline_health_dashboard`
- `ops.batch_runtime_trend`
