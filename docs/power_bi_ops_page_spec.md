# Power BI Ops Page Spec

## Purpose

This page is meant to monitor pipeline health, batch lifecycle, validation quality, alerting, and runtime behavior for the CTR analytics platform.

Primary sources:

- `ops.pipeline_health_dashboard`
- `ops.batch_runtime_trend`

Secondary supporting source if needed:

- `quality.validation_dashboard_summary`

## Page Name

`Pipeline Operations Overview`

## Filters

Recommended report-level filters:

- `sample_scale`
- `source_type`
- `batch_status`

Recommended page slicers:

- `batch_name`
- `source_file`
- `created_at`

## KPI Cards

### Card 1

Title:

`Latest Batch Status`

Source:

- `ops.pipeline_health_dashboard`

Fields:

- `batch_name`
- `batch_status`

Suggested behavior:

- show the latest selected batch
- use color formatting by `batch_status`

### Card 2

Title:

`Total Stage Runtime (s)`

Source:

- `ops.pipeline_health_dashboard`

Field:

- `total_stage_runtime_seconds`

### Card 3

Title:

`Slowest Stage Runtime (s)`

Source:

- `ops.pipeline_health_dashboard`

Field:

- `slowest_stage_runtime_seconds`

### Card 4

Title:

`Quality Pass Rate`

Source:

- `ops.pipeline_health_dashboard`

Fields:

- `passed_quality_checks`
- `total_quality_checks`

Formula:

- `passed_quality_checks / total_quality_checks`

### Card 5

Title:

`Latest Alert Count`

Source:

- `ops.pipeline_health_dashboard`

Field:

- `total_alerts`

### Card 6

Title:

`Error Alerts`

Source:

- `ops.pipeline_health_dashboard`

Field:

- `error_alerts`

## Visual 1: Batch Health Table

Type:

- table or matrix

Source:

- `ops.pipeline_health_dashboard`

Columns:

- `batch_id`
- `batch_name`
- `source_file`
- `source_type`
- `sample_scale`
- `batch_status`
- `last_successful_stage`
- `latest_quality_run_status`
- `total_quality_checks`
- `failed_quality_checks`
- `total_alerts`
- `error_alerts`
- `batch_elapsed_seconds`
- `archive_path`
- `failure_path`

Purpose:

- show the operational status of each batch in one place

## Visual 2: Stage Runtime by Batch

Type:

- clustered bar chart

Source:

- `ops.batch_runtime_trend`

Axis:

- `object_name`

Legend:

- `batch_name`

Value:

- `duration_seconds`

Purpose:

- compare stage runtimes across batches

## Visual 3: Rows Processed by Stage

Type:

- clustered column chart

Source:

- `ops.batch_runtime_trend`

Axis:

- `layer_name`

Value:

- `row_count`

Tooltips:

- `object_name`
- `duration_seconds`
- `rows_per_second`

Purpose:

- show processing volume and throughput by layer

## Visual 4: Runtime Trend Over Time

Type:

- line chart

Source:

- `ops.batch_runtime_trend`

Axis:

- `recorded_at`

Legend:

- `object_name`

Value:

- `duration_seconds`

Purpose:

- track whether stage runtime is getting worse over time

## Visual 5: Quality Summary

Type:

- stacked bar or donut

Source:

- `ops.pipeline_health_dashboard`

Fields:

- `passed_quality_checks`
- `warning_quality_checks`
- `failed_quality_checks`

Purpose:

- show latest-run quality distribution for the selected batch

## Visual 6: Alert Summary

Type:

- stacked column chart

Source:

- `ops.pipeline_health_dashboard`

Fields:

- `error_alerts`
- `warning_alerts`

Axis:

- `batch_name`

Purpose:

- show latest-run alert concentration by batch

## Visual 7: Batch Lifecycle Timeline

Type:

- table

Source:

- `ops.pipeline_health_dashboard`

Columns:

- `batch_name`
- `created_at`
- `started_at`
- `completed_at`
- `batch_elapsed_seconds`
- `batch_status`
- `archive_path`

Purpose:

- make the batch journey easy to audit

## Field Mapping

### `ops.pipeline_health_dashboard`

Key fields:

- `batch_id`
- `batch_name`
- `source_file`
- `source_type`
- `sample_scale`
- `batch_status`
- `last_successful_stage`
- `batch_elapsed_seconds`
- `orchestration_run_id`
- `latest_quality_run_id`
- `latest_quality_run_status`
- `total_quality_checks`
- `passed_quality_checks`
- `warning_quality_checks`
- `failed_quality_checks`
- `total_alerts`
- `error_alerts`
- `warning_alerts`
- `total_stage_runtime_seconds`
- `slowest_stage_runtime_seconds`
- `archive_path`
- `failure_path`

### `ops.batch_runtime_trend`

Key fields:

- `batch_id`
- `batch_name`
- `sample_scale`
- `layer_name`
- `object_name`
- `duration_seconds`
- `rows_per_second`
- `row_count`
- `storage_mb`
- `recorded_at`

## Recommended Layout

Top row:

- KPI cards

Middle row:

- batch health table
- quality summary
- alert summary

Bottom row:

- stage runtime by batch
- runtime trend over time
- rows processed by stage

## Design Notes

- use conditional formatting for `batch_status`
- use red for `error_alerts` and failed quality checks
- use amber for warnings
- use green for successful batches
- keep archive/failure paths in detail tables, not KPI cards

## Expected Outcome

This page should let someone answer these questions quickly:

- Did the latest batch succeed?
- Which stage was slowest?
- Were there validation failures?
- Were there Airflow retry or failure alerts?
- Is runtime getting worse as batches grow?
