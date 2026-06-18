create table if not exists quality.load_audit (
    audit_id bigint generated always as identity primary key,
    batch_id bigint references ops.batch_registry (batch_id),
    pipeline_run_id bigint references ops.pipeline_runs (pipeline_run_id),
    layer_name text not null,
    table_name text not null,
    source_file text,
    row_count bigint,
    check_status text not null,
    check_message text,
    checked_at timestamptz not null default now()
);

alter table quality.load_audit
    add column if not exists batch_id bigint;

alter table quality.load_audit
    add column if not exists pipeline_run_id bigint;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'load_audit_batch_id_fkey'
    ) then
        alter table quality.load_audit
            add constraint load_audit_batch_id_fkey
            foreign key (batch_id) references ops.batch_registry (batch_id);
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conname = 'load_audit_pipeline_run_id_fkey'
    ) then
        alter table quality.load_audit
            add constraint load_audit_pipeline_run_id_fkey
            foreign key (pipeline_run_id) references ops.pipeline_runs (pipeline_run_id);
    end if;
end $$;

create table if not exists quality.validation_runs (
    run_id bigint generated always as identity primary key,
    batch_id bigint references ops.batch_registry (batch_id),
    pipeline_run_id bigint references ops.pipeline_runs (pipeline_run_id),
    pipeline_name text not null,
    source_file text,
    run_status text not null default 'RUNNING',
    run_message text,
    started_at timestamptz not null default now(),
    completed_at timestamptz
);

alter table quality.validation_runs
    add column if not exists batch_id bigint;

alter table quality.validation_runs
    add column if not exists pipeline_run_id bigint;

create table if not exists quality.validation_results (
    validation_id bigint generated always as identity primary key,
    batch_id bigint references ops.batch_registry (batch_id),
    pipeline_run_id bigint references ops.pipeline_runs (pipeline_run_id),
    layer_name text not null,
    table_name text not null,
    check_name text not null,
    check_type text not null,
    severity text not null,
    check_status text not null,
    actual_value text,
    expected_value text,
    threshold_value text,
    check_message text,
    source_file text,
    checked_at timestamptz not null default now()
);

alter table quality.validation_results
    add column if not exists run_id bigint;

alter table quality.validation_results
    add column if not exists source_file text;

alter table quality.validation_results
    add column if not exists batch_id bigint;

alter table quality.validation_results
    add column if not exists pipeline_run_id bigint;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'fk_validation_results_run'
    ) then
        alter table quality.validation_results
            add constraint fk_validation_results_run
            foreign key (run_id) references quality.validation_runs (run_id);
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conname = 'validation_runs_batch_id_fkey'
    ) then
        alter table quality.validation_runs
            add constraint validation_runs_batch_id_fkey
            foreign key (batch_id) references ops.batch_registry (batch_id);
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conname = 'validation_runs_pipeline_run_id_fkey'
    ) then
        alter table quality.validation_runs
            add constraint validation_runs_pipeline_run_id_fkey
            foreign key (pipeline_run_id) references ops.pipeline_runs (pipeline_run_id);
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conname = 'validation_results_batch_id_fkey'
    ) then
        alter table quality.validation_results
            add constraint validation_results_batch_id_fkey
            foreign key (batch_id) references ops.batch_registry (batch_id);
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conname = 'validation_results_pipeline_run_id_fkey'
    ) then
        alter table quality.validation_results
            add constraint validation_results_pipeline_run_id_fkey
            foreign key (pipeline_run_id) references ops.pipeline_runs (pipeline_run_id);
    end if;
end $$;

create index if not exists idx_validation_results_layer_table
    on quality.validation_results (layer_name, table_name);

create index if not exists idx_validation_results_checked_at
    on quality.validation_results (checked_at desc);

create index if not exists idx_validation_results_run_id
    on quality.validation_results (run_id);

create table if not exists quality.validation_thresholds (
    threshold_id bigint generated always as identity primary key,
    layer_name text not null,
    table_name text not null,
    column_name text not null default '',
    check_name text not null,
    warning_threshold numeric(12, 6),
    error_threshold numeric(12, 6),
    comparison_operator text not null default '<=',
    is_active boolean not null default true,
    notes text,
    updated_at timestamptz not null default now()
);

create unique index if not exists uq_validation_thresholds_check
    on quality.validation_thresholds (
        layer_name,
        table_name,
        column_name,
        check_name
    );

insert into quality.validation_thresholds (
    layer_name,
    table_name,
    column_name,
    check_name,
    warning_threshold,
    error_threshold,
    comparison_operator,
    notes
)
values
    (
        'staging',
        'staging.stg_criteo_events',
        'c22',
        'unknown rate threshold',
        0.80,
        0.90,
        '<=',
        'Monitor the highest-missing categorical feature and escalate only if sparsity becomes extreme.'
    ),
    (
        'staging',
        'staging.stg_criteo_events',
        'c19',
        'unknown rate threshold',
        0.50,
        0.65,
        '<=',
        'Track large jumps in sparsity for high-volume categorical columns.'
    ),
    (
        'staging',
        'staging.stg_criteo_events',
        'c20',
        'unknown rate threshold',
        0.50,
        0.65,
        '<=',
        'Track large jumps in sparsity for high-volume categorical columns.'
    ),
    (
        'staging',
        'staging.stg_criteo_events',
        'c25',
        'unknown rate threshold',
        0.50,
        0.65,
        '<=',
        'Track large jumps in sparsity for high-volume categorical columns.'
    ),
    (
        'staging',
        'staging.stg_criteo_events',
        'c26',
        'unknown rate threshold',
        0.50,
        0.65,
        '<=',
        'Track large jumps in sparsity for high-volume categorical columns.'
    )
on conflict (
    layer_name,
    table_name,
    column_name,
    check_name
)
do update
set
    warning_threshold = excluded.warning_threshold,
    error_threshold = excluded.error_threshold,
    comparison_operator = excluded.comparison_operator,
    is_active = true,
    notes = excluded.notes,
    updated_at = now();

drop view if exists quality.validation_dashboard_summary;
drop view if exists quality.latest_validation_summary;

create or replace view quality.latest_validation_summary as
with latest_checks as (
    select
        validation_id,
        run_id,
        layer_name,
        table_name,
        check_name,
        check_type,
        severity,
        check_status,
        actual_value,
        expected_value,
        threshold_value,
        check_message,
        source_file,
        checked_at,
        row_number() over (
            partition by layer_name, table_name, check_name
            order by checked_at desc, validation_id desc
        ) as row_number_in_check
    from quality.validation_results
)
select
    validation_id,
    run_id,
    layer_name,
    table_name,
    check_name,
    check_type,
    severity,
    check_status,
    actual_value,
    expected_value,
    threshold_value,
    check_message,
    source_file,
    checked_at
from latest_checks
where row_number_in_check = 1;

create or replace view quality.validation_dashboard_summary as
with latest_run as (
    select max(run_id) as run_id
    from quality.validation_runs
),
latest_run_checks as (
    select
        vr.run_id,
        vr.layer_name,
        vr.table_name,
        vr.check_name,
        vr.severity,
        vr.check_status
    from quality.validation_results vr
    inner join latest_run lr
        on vr.run_id = lr.run_id
)
select
    runs.run_id,
    runs.pipeline_name,
    runs.source_file,
    runs.run_status,
    runs.started_at,
    runs.completed_at,
    count(*) as total_checks,
    count(*) filter (where checks.check_status = 'PASS') as pass_checks,
    count(*) filter (where checks.check_status = 'WARN') as warn_checks,
    count(*) filter (where checks.check_status = 'FAIL') as fail_checks,
    count(*) filter (where checks.severity = 'error') as error_severity_checks,
    count(*) filter (where checks.severity = 'warning') as warning_severity_checks
from quality.validation_runs runs
left join latest_run_checks checks
    on runs.run_id = checks.run_id
where runs.run_id = (select run_id from latest_run)
group by
    runs.run_id,
    runs.pipeline_name,
    runs.source_file,
    runs.run_status,
    runs.started_at,
    runs.completed_at;
