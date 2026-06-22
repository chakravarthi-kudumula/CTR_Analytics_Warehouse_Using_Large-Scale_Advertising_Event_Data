create or replace view ml.score_decile_performance as
select
    ps.batch_id,
    br.batch_name,
    ps.model_id,
    ps.training_run_id,
    ps.model_name,
    ps.model_version,
    ps.score_decile,
    count(*) as rows_scored,
    avg(ps.predicted_ctr)::numeric(12, 6) as avg_predicted_ctr,
    avg(ps.actual_click)::numeric(12, 6) as actual_ctr,
    (avg(ps.predicted_ctr) - avg(ps.actual_click))::numeric(12, 6) as calibration_gap,
    min(ps.scored_at) as first_scored_at,
    max(ps.scored_at) as last_scored_at
from ml.prediction_scores ps
join ops.batch_registry br
  on br.batch_id = ps.batch_id
group by
    ps.batch_id,
    br.batch_name,
    ps.model_id,
    ps.training_run_id,
    ps.model_name,
    ps.model_version,
    ps.score_decile;


create or replace view ml.top_decile_performance as
with overall_batch_performance as (
    select
        ps.batch_id,
        br.batch_name,
        ps.model_id,
        ps.training_run_id,
        ps.model_name,
        ps.model_version,
        count(*) as rows_scored,
        avg(ps.predicted_ctr)::numeric(12, 6) as overall_avg_predicted_ctr,
        avg(ps.actual_click)::numeric(12, 6) as overall_actual_ctr,
        min(ps.scored_at) as first_scored_at,
        max(ps.scored_at) as last_scored_at
    from ml.prediction_scores ps
    join ops.batch_registry br
      on br.batch_id = ps.batch_id
    group by
        ps.batch_id,
        br.batch_name,
        ps.model_id,
        ps.training_run_id,
        ps.model_name,
        ps.model_version
),
top_decile_batch_performance as (
    select
        ps.batch_id,
        ps.model_id,
        ps.training_run_id,
        ps.model_name,
        ps.model_version,
        count(*) as top_decile_rows,
        avg(ps.predicted_ctr)::numeric(12, 6) as top_decile_avg_predicted_ctr,
        avg(ps.actual_click)::numeric(12, 6) as top_decile_actual_ctr
    from ml.prediction_scores ps
    where ps.is_top_decile = true
    group by
        ps.batch_id,
        ps.model_id,
        ps.training_run_id,
        ps.model_name,
        ps.model_version
)
select
    overall.batch_id,
    overall.batch_name,
    overall.model_id,
    overall.training_run_id,
    overall.model_name,
    overall.model_version,
    overall.rows_scored,
    overall.overall_avg_predicted_ctr,
    overall.overall_actual_ctr,
    coalesce(top_decile.top_decile_rows, 0) as top_decile_rows,
    coalesce(top_decile.top_decile_avg_predicted_ctr, 0)::numeric(12, 6) as top_decile_avg_predicted_ctr,
    coalesce(top_decile.top_decile_actual_ctr, 0)::numeric(12, 6) as top_decile_actual_ctr,
    case
        when overall.overall_actual_ctr = 0 then null
        else (coalesce(top_decile.top_decile_actual_ctr, 0) / overall.overall_actual_ctr)::numeric(12, 6)
    end as top_decile_lift_vs_batch_ctr,
    (coalesce(top_decile.top_decile_avg_predicted_ctr, 0) - overall.overall_avg_predicted_ctr)::numeric(12, 6)
        as top_decile_predicted_ctr_gap,
    (coalesce(top_decile.top_decile_actual_ctr, 0) - overall.overall_actual_ctr)::numeric(12, 6)
        as top_decile_actual_ctr_gap,
    overall.first_scored_at,
    overall.last_scored_at
from overall_batch_performance overall
left join top_decile_batch_performance top_decile
  on top_decile.batch_id = overall.batch_id
 and top_decile.model_id = overall.model_id
 and top_decile.model_name = overall.model_name
 and top_decile.model_version = overall.model_version;


create or replace view ml.score_drift_summary as
with ranked_batches as (
    select
        performance.*,
        lag(performance.batch_id) over (
            partition by performance.model_name, performance.model_version
            order by performance.batch_id
        ) as previous_batch_id,
        lag(performance.batch_name) over (
            partition by performance.model_name, performance.model_version
            order by performance.batch_id
        ) as previous_batch_name,
        lag(performance.overall_avg_predicted_ctr) over (
            partition by performance.model_name, performance.model_version
            order by performance.batch_id
        ) as previous_avg_predicted_ctr,
        lag(performance.overall_actual_ctr) over (
            partition by performance.model_name, performance.model_version
            order by performance.batch_id
        ) as previous_actual_ctr,
        lag(performance.top_decile_actual_ctr) over (
            partition by performance.model_name, performance.model_version
            order by performance.batch_id
        ) as previous_top_decile_actual_ctr,
        lag(performance.top_decile_lift_vs_batch_ctr) over (
            partition by performance.model_name, performance.model_version
            order by performance.batch_id
        ) as previous_top_decile_lift
    from ml.top_decile_performance performance
)
select
    batch_id,
    batch_name,
    model_id,
    training_run_id,
    model_name,
    model_version,
    rows_scored,
    overall_avg_predicted_ctr,
    overall_actual_ctr,
    top_decile_rows,
    top_decile_avg_predicted_ctr,
    top_decile_actual_ctr,
    top_decile_lift_vs_batch_ctr,
    previous_batch_id,
    previous_batch_name,
    previous_avg_predicted_ctr,
    previous_actual_ctr,
    previous_top_decile_actual_ctr,
    previous_top_decile_lift,
    case
        when previous_avg_predicted_ctr is null then null
        else (overall_avg_predicted_ctr - previous_avg_predicted_ctr)::numeric(12, 6)
    end as avg_predicted_ctr_delta,
    case
        when previous_actual_ctr is null then null
        else (overall_actual_ctr - previous_actual_ctr)::numeric(12, 6)
    end as actual_ctr_delta,
    case
        when previous_top_decile_actual_ctr is null then null
        else (top_decile_actual_ctr - previous_top_decile_actual_ctr)::numeric(12, 6)
    end as top_decile_actual_ctr_delta,
    case
        when previous_top_decile_lift is null then null
        else (top_decile_lift_vs_batch_ctr - previous_top_decile_lift)::numeric(12, 6)
    end as top_decile_lift_delta,
    first_scored_at,
    last_scored_at
from ranked_batches;
