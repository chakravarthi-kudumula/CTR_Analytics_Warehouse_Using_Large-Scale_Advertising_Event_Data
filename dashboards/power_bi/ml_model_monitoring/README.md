# ML Model Monitoring

This folder contains dashboard-ready assets for a Power BI page built on top of:

- `ml.latest_model_monitoring_dashboard`
- `ml.batch_model_rankings`
- `ml.model_drift_watchlist`
- `ml.latest_model_feature_importance`
- `ml.latest_feature_group_importance`

## Files

- `ml_page_queries.sql`
  - import queries for the main ML monitoring views
- `ml_page_measures.dax`
  - recommended DAX measures for KPI cards
- `ml_page_fields.csv`
  - field mapping by visual

## Recommended Build Order

1. Connect Power BI to PostgreSQL.
2. Import the queries from `ml_page_queries.sql`.
3. Add the measures from `ml_page_measures.dax`.
4. Build the visuals using `ml_page_fields.csv` and [power_bi_ml_page_spec.md](/Users/chakri/Documents/SQL%20Project/advertisement-analytics-warehouse/docs/power_bi_ml_page_spec.md).

## Page Name

`ML Model Monitoring`
