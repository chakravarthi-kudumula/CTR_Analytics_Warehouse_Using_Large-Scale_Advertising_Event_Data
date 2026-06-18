select count(*) as overall_ctr_summary_rows from marts.overall_ctr_summary;
select count(*) as event_day_ctr_trend_rows from marts.event_day_ctr_trend;
select count(*) as numeric_bucket_ctr_rows from marts.numeric_bucket_ctr;
select count(*) as feature_ctr_summary_rows from marts.feature_ctr_summary;
select count(*) as missing_value_impact_rows from marts.missing_value_impact;
select count(*) as feature_interaction_ctr_rows from marts.feature_interaction_ctr;
select count(*) as top_numeric_bucket_ctr_rows from marts.top_numeric_bucket_ctr;
select count(*) as top_feature_ctr_summary_rows from marts.top_feature_ctr_summary;
select count(*) as top_feature_interaction_ctr_rows from marts.top_feature_interaction_ctr;

select * from marts.overall_ctr_summary;

select
    event_batch,
    event_day_number,
    impressions,
    clicks,
    ctr
from marts.event_day_ctr_trend
order by event_day_number;

select
    feature_name,
    bucket_label,
    impressions,
    clicks,
    ctr,
    ctr_lift_vs_overall
from marts.numeric_bucket_ctr
where ctr_rank_in_feature <= 3
order by feature_name, ctr_rank_in_feature, impressions desc
limit 20;

select
    feature_name,
    bucket_label,
    impressions,
    clicks,
    ctr,
    ctr_lift_vs_overall,
    volume_band
from marts.top_numeric_bucket_ctr
order by feature_name, ctr_rank_in_feature, impressions desc
limit 20;

select
    feature_name,
    feature_value,
    impressions,
    clicks,
    ctr,
    ctr_lift_vs_overall
from marts.feature_ctr_summary
where ctr_rank_in_feature <= 3
order by feature_name, ctr_rank_in_feature, impressions desc
limit 20;

select
    feature_name,
    feature_value,
    impressions,
    clicks,
    ctr,
    ctr_lift_vs_overall,
    volume_band
from marts.top_feature_ctr_summary
order by feature_name, ctr_rank_in_feature, impressions desc
limit 20;

select
    missing_type,
    missing_count,
    impressions,
    clicks,
    ctr,
    ctr_lift_vs_overall
from marts.missing_value_impact
order by missing_type, missing_count;

select
    interaction_name,
    left_feature_name,
    left_feature_value,
    right_feature_name,
    right_feature_value,
    impressions,
    clicks,
    ctr,
    ctr_lift_vs_overall
from marts.feature_interaction_ctr
where ctr_rank_in_interaction <= 10
order by interaction_name, ctr_rank_in_interaction, impressions desc
limit 30;

select
    interaction_name,
    left_feature_value,
    right_feature_value,
    impressions,
    clicks,
    ctr,
    ctr_lift_vs_overall,
    volume_band
from marts.top_feature_interaction_ctr
order by interaction_name, ctr_rank_in_interaction, impressions desc
limit 20;

select
    audit_id,
    source_file,
    row_count,
    check_status,
    checked_at
from quality.load_audit
where layer_name = 'marts'
  and table_name = 'marts layer'
order by audit_id desc
limit 10;
