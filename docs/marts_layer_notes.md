# Marts Layer Notes

The marts layer creates business-facing CTR analysis outputs on top of the warehouse and staging layers.

## Marts Included

- `marts.overall_ctr_summary`
- `marts.event_day_ctr_trend`
- `marts.numeric_bucket_ctr`
- `marts.feature_ctr_summary`
- `marts.missing_value_impact`
- `marts.feature_interaction_ctr`

## Design Choices

- derived event-day buckets are used instead of real dates
- feature-level marts use impression thresholds to reduce noise
- CTR lift vs overall CTR is included where useful
- interaction analysis is limited to selected 2-way combinations
- feature-level marts use `staging.stg_criteo_events` directly where that is lighter than full bridge-table population

## Interpreting Low-Volume High-CTR Groups

Some feature values or feature interactions can show very high CTR simply because they occur in a small number of impressions.

Rule of thumb:

- `high` volume means the group has enough impressions to treat the CTR as relatively stable
- `medium` volume means the group is useful, but should be compared carefully
- `low` volume means the group may be noisy even if CTR looks strong

The marts include a `volume_band` column to make this easier to interpret in SQL and dashboards.

For dashboard-friendly filtering, use the helper views:

- `marts.top_numeric_bucket_ctr`
- `marts.top_feature_ctr_summary`
- `marts.top_feature_interaction_ctr`

## Build Command

Run the marts build from the project root:

```bash
python3 scripts/marts_build.py
```

## Validation

Run the marts validation queries:

```bash
psql -p 5432 -U chakri -d ctr_analytics -f sql/11_marts_layer_checks.sql
```

## Selected Interaction Scope

The interaction mart is intentionally controlled rather than exhaustive. It currently includes:

- `c22 x c19`
- `c22 x c6`
- `i1 bucket x c22`

This keeps the mart interpretable and avoids a combinatorial explosion of low-signal feature pairs.
