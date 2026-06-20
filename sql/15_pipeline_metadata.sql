create table if not exists ops.batch_registry (
    batch_id bigint generated always as identity primary key,
    batch_name text not null unique,
    source_file text not null,
    source_path text,
    source_checksum text,
    source_type text not null default 'sample',
    sample_scale text not null,
    expected_row_count bigint,
    actual_raw_row_count bigint,
    actual_staging_row_count bigint,
    actual_fact_row_count bigint,
    batch_status text not null default 'REGISTERED',
    last_successful_stage text,
    created_at timestamptz not null default now(),
    started_at timestamptz,
    completed_at timestamptz,
    source_moved_at timestamptz,
    archive_path text,
    failure_path text,
    orchestration_run_id text,
    notes text
);

alter table ops.batch_registry
    add column if not exists source_checksum text;

alter table ops.batch_registry
    add column if not exists source_type text not null default 'sample';

alter table ops.batch_registry
    add column if not exists source_moved_at timestamptz;

alter table ops.batch_registry
    add column if not exists archive_path text;

alter table ops.batch_registry
    add column if not exists failure_path text;

alter table ops.batch_registry
    add column if not exists orchestration_run_id text;

create index if not exists idx_batch_registry_status
    on ops.batch_registry (batch_status);

create index if not exists idx_batch_registry_source_file
    on ops.batch_registry (source_file);

create unique index if not exists uq_batch_registry_source_checksum
    on ops.batch_registry (source_checksum)
    where source_checksum is not null;

create index if not exists idx_batch_registry_source_type
    on ops.batch_registry (source_type);

create table if not exists ops.pipeline_runs (
    pipeline_run_id bigint generated always as identity primary key,
    batch_id bigint not null references ops.batch_registry (batch_id),
    pipeline_name text not null,
    layer_name text not null,
    source_file text,
    triggered_by text not null default 'manual',
    run_status text not null default 'RUNNING',
    run_message text,
    started_at timestamptz not null default now(),
    completed_at timestamptz
);

create index if not exists idx_pipeline_runs_batch
    on ops.pipeline_runs (batch_id, started_at desc);

create index if not exists idx_pipeline_runs_status
    on ops.pipeline_runs (run_status);

create table if not exists ops.pipeline_steps (
    pipeline_step_id bigint generated always as identity primary key,
    pipeline_run_id bigint not null references ops.pipeline_runs (pipeline_run_id),
    batch_id bigint not null references ops.batch_registry (batch_id),
    step_name text not null,
    layer_name text not null,
    target_table text not null,
    source_file text,
    rows_processed bigint,
    step_status text not null default 'RUNNING',
    step_message text,
    started_at timestamptz not null default now(),
    completed_at timestamptz
);

create index if not exists idx_pipeline_steps_run
    on ops.pipeline_steps (pipeline_run_id, started_at desc);

create index if not exists idx_pipeline_steps_status
    on ops.pipeline_steps (step_status);

create table if not exists ops.batch_artifacts (
    artifact_id bigint generated always as identity primary key,
    batch_id bigint not null references ops.batch_registry (batch_id),
    pipeline_run_id bigint references ops.pipeline_runs (pipeline_run_id),
    artifact_name text not null,
    artifact_type text not null,
    artifact_format text not null,
    artifact_path text not null,
    row_count bigint,
    artifact_status text not null default 'READY',
    notes text,
    created_at timestamptz not null default now()
);

create index if not exists idx_batch_artifacts_batch
    on ops.batch_artifacts (batch_id, created_at desc);

create index if not exists idx_batch_artifacts_type
    on ops.batch_artifacts (artifact_type);

create table if not exists ops.pipeline_alerts (
    alert_id bigint generated always as identity primary key,
    batch_id bigint references ops.batch_registry (batch_id),
    pipeline_run_id bigint references ops.pipeline_runs (pipeline_run_id),
    alert_run_id text,
    pipeline_name text,
    task_name text,
    layer_name text,
    alert_level text not null,
    alert_type text not null,
    alert_message text not null,
    alert_context jsonb,
    created_at timestamptz not null default now(),
    acknowledged_at timestamptz
);

alter table ops.pipeline_alerts
    add column if not exists alert_run_id text;

create index if not exists idx_pipeline_alerts_created
    on ops.pipeline_alerts (created_at desc);

create index if not exists idx_pipeline_alerts_level
    on ops.pipeline_alerts (alert_level);

create index if not exists idx_pipeline_alerts_run
    on ops.pipeline_alerts (batch_id, alert_run_id, created_at desc);

create table if not exists ops.benchmark_snapshots (
    benchmark_id bigint generated always as identity primary key,
    batch_id bigint references ops.batch_registry (batch_id),
    pipeline_run_id bigint references ops.pipeline_runs (pipeline_run_id),
    benchmark_name text not null,
    layer_name text not null,
    object_name text not null,
    row_count bigint,
    duration_seconds numeric(18, 3),
    rows_per_second numeric(18, 3),
    storage_mb numeric(18, 3),
    notes text,
    recorded_at timestamptz not null default now()
);

create index if not exists idx_benchmark_snapshots_batch
    on ops.benchmark_snapshots (batch_id, recorded_at desc);

create index if not exists idx_benchmark_snapshots_layer
    on ops.benchmark_snapshots (layer_name, recorded_at desc);

create or replace view ops.latest_batch_status as
with ranked_batches as (
    select
        batch_id,
        batch_name,
        source_file,
        source_path,
        sample_scale,
        expected_row_count,
        actual_raw_row_count,
        actual_staging_row_count,
        actual_fact_row_count,
        batch_status,
        last_successful_stage,
        created_at,
        started_at,
        completed_at,
        notes,
        row_number() over (
            partition by source_file, sample_scale
            order by created_at desc, batch_id desc
        ) as batch_rank
    from ops.batch_registry
)
select
    batch_id,
    batch_name,
    source_file,
    source_path,
    sample_scale,
    expected_row_count,
    actual_raw_row_count,
    actual_staging_row_count,
    actual_fact_row_count,
    batch_status,
    last_successful_stage,
    created_at,
    started_at,
    completed_at,
    notes
from ranked_batches
where batch_rank = 1;

drop view if exists ops.pipeline_run_summary;

create view ops.pipeline_run_summary as
select
    runs.pipeline_run_id,
    runs.batch_id,
    batches.batch_name,
    runs.pipeline_name,
    runs.layer_name,
    runs.source_file,
    runs.triggered_by,
    runs.run_status,
    runs.run_message,
    runs.started_at,
    runs.completed_at,
    round(extract(epoch from (coalesce(runs.completed_at, now()) - runs.started_at))::numeric, 3) as duration_seconds,
    count(steps.pipeline_step_id) as total_steps,
    count(*) filter (where steps.step_status = 'SUCCESS') as successful_steps,
    count(*) filter (where steps.step_status = 'FAILED') as failed_steps
from ops.pipeline_runs runs
join ops.batch_registry batches
  on batches.batch_id = runs.batch_id
left join ops.pipeline_steps steps
  on steps.pipeline_run_id = runs.pipeline_run_id
group by
    runs.pipeline_run_id,
    runs.batch_id,
    batches.batch_name,
    runs.pipeline_name,
    runs.layer_name,
    runs.source_file,
    runs.triggered_by,
    runs.run_status,
    runs.run_message,
    runs.started_at,
    runs.completed_at;

create or replace view ops.latest_batch_artifacts as
with ranked_artifacts as (
    select
        artifact_id,
        batch_id,
        pipeline_run_id,
        artifact_name,
        artifact_type,
        artifact_format,
        artifact_path,
        row_count,
        artifact_status,
        notes,
        created_at,
        row_number() over (
            partition by batch_id, artifact_name
            order by created_at desc, artifact_id desc
        ) as artifact_rank
    from ops.batch_artifacts
)
select
    artifact_id,
    batch_id,
    pipeline_run_id,
    artifact_name,
    artifact_type,
    artifact_format,
    artifact_path,
    row_count,
    artifact_status,
    notes,
    created_at
from ranked_artifacts
where artifact_rank = 1;

create or replace view ops.pipeline_benchmark_summary as
with latest_snapshots as (
    select
        benchmark_id,
        batch_id,
        pipeline_run_id,
        benchmark_name,
        layer_name,
        object_name,
        row_count,
        duration_seconds,
        rows_per_second,
        storage_mb,
        notes,
        recorded_at,
        row_number() over (
            partition by batch_id, benchmark_name, layer_name, object_name
            order by recorded_at desc, benchmark_id desc
        ) as benchmark_rank
    from ops.benchmark_snapshots
)
select
    s.benchmark_id,
    s.batch_id,
    b.batch_name,
    b.sample_scale,
    s.pipeline_run_id,
    s.benchmark_name,
    s.layer_name,
    s.object_name,
    s.row_count,
    s.duration_seconds,
    s.rows_per_second,
    s.storage_mb,
    s.notes,
    s.recorded_at
from latest_snapshots s
join ops.batch_registry b
  on b.batch_id = s.batch_id
where s.benchmark_rank = 1;

create or replace view ops.pipeline_health_dashboard as
with latest_quality_runs as (
    select
        run_id,
        batch_id,
        pipeline_run_id,
        run_status,
        started_at,
        completed_at,
        row_number() over (
            partition by batch_id
            order by started_at desc, run_id desc
        ) as quality_run_rank
    from quality.validation_runs
),
latest_quality as (
    select
        v.batch_id,
        v.run_id as latest_quality_run_id,
        v.pipeline_run_id as latest_quality_pipeline_run_id,
        v.run_status as latest_quality_run_status,
        v.started_at as latest_quality_started_at,
        v.completed_at as latest_quality_completed_at,
        count(*) as total_quality_checks,
        count(*) filter (where check_status = 'PASS') as passed_quality_checks,
        count(*) filter (where check_status = 'WARN') as warning_quality_checks,
        count(*) filter (where check_status = 'FAIL') as failed_quality_checks
    from quality.validation_results r
    join latest_quality_runs v
      on r.run_id = v.run_id
    where v.quality_run_rank = 1
    group by
        v.batch_id,
        v.run_id,
        v.pipeline_run_id,
        v.run_status,
        v.started_at,
        v.completed_at
),
latest_alerts as (
    select
        a.batch_id,
        a.alert_run_id,
        count(*) as total_alerts,
        count(*) filter (where a.alert_level = 'error') as error_alerts,
        count(*) filter (where a.alert_level = 'warning') as warning_alerts
    from ops.pipeline_alerts a
    join ops.batch_registry b
      on b.batch_id = a.batch_id
    where a.alert_run_id is not null
      and b.orchestration_run_id is not null
      and a.alert_run_id = b.orchestration_run_id
    group by a.batch_id, a.alert_run_id
),
latest_runtime as (
    select
        batch_id,
        sum(duration_seconds) filter (where benchmark_name = 'pipeline_stage_runtime') as total_stage_runtime_seconds,
        max(duration_seconds) filter (where benchmark_name = 'pipeline_stage_runtime') as slowest_stage_runtime_seconds
    from ops.pipeline_benchmark_summary
    group by batch_id
)
select
    b.batch_id,
    b.batch_name,
    b.source_file,
    b.source_type,
    b.sample_scale,
    b.batch_status,
    b.last_successful_stage,
    b.expected_row_count,
    b.actual_raw_row_count,
    b.actual_staging_row_count,
    b.actual_fact_row_count,
    b.created_at,
    b.started_at,
    b.completed_at,
    round(extract(epoch from (coalesce(b.completed_at, now()) - coalesce(b.started_at, b.created_at)))::numeric, 3) as batch_elapsed_seconds,
    b.orchestration_run_id,
    q.latest_quality_run_id,
    q.latest_quality_pipeline_run_id,
    q.latest_quality_run_status,
    q.latest_quality_started_at,
    q.latest_quality_completed_at,
    coalesce(q.total_quality_checks, 0) as total_quality_checks,
    coalesce(q.passed_quality_checks, 0) as passed_quality_checks,
    coalesce(q.warning_quality_checks, 0) as warning_quality_checks,
    coalesce(q.failed_quality_checks, 0) as failed_quality_checks,
    coalesce(a.total_alerts, 0) as total_alerts,
    coalesce(a.error_alerts, 0) as error_alerts,
    coalesce(a.warning_alerts, 0) as warning_alerts,
    round(coalesce(r.total_stage_runtime_seconds, 0)::numeric, 3) as total_stage_runtime_seconds,
    round(coalesce(r.slowest_stage_runtime_seconds, 0)::numeric, 3) as slowest_stage_runtime_seconds,
    b.archive_path,
    b.failure_path
from ops.batch_registry b
left join latest_quality q
  on q.batch_id = b.batch_id
left join latest_alerts a
  on a.batch_id = b.batch_id
left join latest_runtime r
  on r.batch_id = b.batch_id;

create or replace view ops.batch_runtime_trend as
select
    b.batch_id,
    b.batch_name,
    b.sample_scale,
    s.layer_name,
    s.object_name,
    s.duration_seconds,
    s.rows_per_second,
    s.row_count,
    s.storage_mb,
    s.recorded_at
from ops.pipeline_benchmark_summary s
join ops.batch_registry b
  on b.batch_id = s.batch_id
where s.benchmark_name = 'pipeline_stage_runtime';

create or replace view ops.sample_scale_benchmark_comparison as
with ranked_scale_batches as (
    select
        batch_id,
        batch_name,
        sample_scale,
        source_type,
        completed_at,
        row_number() over (
            partition by sample_scale
            order by completed_at desc nulls last, batch_id desc
        ) as scale_rank
    from ops.batch_registry
    where sample_scale in ('100k', '1m', '5m')
),
latest_scale_batches as (
    select
        batch_id,
        batch_name,
        sample_scale,
        source_type,
        completed_at
    from ranked_scale_batches
    where scale_rank = 1
)
select
    b.batch_id,
    b.batch_name,
    b.sample_scale,
    b.source_type,
    s.benchmark_name,
    s.layer_name,
    s.object_name,
    s.row_count,
    round(s.duration_seconds::numeric, 3) as duration_seconds,
    round(s.rows_per_second::numeric, 3) as rows_per_second,
    round(s.storage_mb::numeric, 3) as storage_mb,
    s.recorded_at,
    b.completed_at
from ops.pipeline_benchmark_summary s
join latest_scale_batches b
  on b.batch_id = s.batch_id;
