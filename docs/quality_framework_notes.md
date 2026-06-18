# Data Quality Framework Notes

Stage 7 turns the project into a reusable monitoring-oriented data pipeline instead of a one-time SQL build.

## Core Objects

- `quality.validation_runs`
- `quality.validation_results`
- `quality.validation_thresholds`
- `quality.latest_validation_summary`
- `quality.validation_dashboard_summary`
- `quality.load_audit`

## Framework Design

The framework records validation outcomes across:

- raw
- staging
- warehouse
- marts
- advanced SQL

Each execution creates a validation run first and then writes all check results under that run, so the framework keeps historical quality history instead of overwriting the previous run.

Each validation stores:

- run id
- layer
- table
- check name
- check type
- severity
- status
- actual value
- expected value
- threshold
- message
- source file
- timestamp

## Validation Types Covered

- row count validation
- cross-layer parity checks
- binary label validity
- uniqueness checks
- duplicate detection
- completeness checks
- configurable null-equivalent threshold checks
- CTR range validation
- timestamp/freshness checks
- advanced output population checks

## Status Model

- `PASS`
- `FAIL`
- `WARN`

## Severity Model

- `error`
- `warning`
- `info`

## Build Command

Run the quality framework from the project root:

```bash
python3 scripts/quality_checks.py
```

## Validation Review

Check the latest summarized results:

```bash
psql -p 5432 -U chakri -d ctr_analytics -f sql/14_quality_framework_checks.sql
```

## Why This Feels Production-Grade

- reusable validation storage instead of console-only checks
- layered quality checks rather than isolated table checks
- historical run tracking for trend analysis
- threshold-aware warnings driven by configuration
- latest-status summary view for dashboarding or monitoring
- dashboard-friendly run summary view
- audit trail for each quality run
