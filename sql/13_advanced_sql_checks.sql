select count(*) as feature_ctr_ranked_rows from marts.feature_ctr_ranked;
select count(*) as event_day_ctr_rolling_rows from marts.event_day_ctr_rolling;
select count(*) as high_value_segments_rows from marts.high_value_segments;
select count(*) as low_performing_segments_rows from marts.low_performing_segments;
select count(*) as feature_ctr_lift_ranked_rows from marts.feature_ctr_lift_ranked;
select count(*) as feature_interaction_ranked_rows from marts.feature_interaction_ranked;

select
    event_batch,
    event_day_number,
    ctr,
    previous_day_ctr,
    ctr_change_from_previous_day,
    cumulative_ctr,
    rolling_3_day_avg_ctr,
    rolling_3_day_weighted_ctr
from marts.event_day_ctr_rolling
order by event_day_number;

select
    feature_name,
    feature_value,
    impressions,
    clicks,
    ctr,
    ctr_lift_vs_overall,
    volume_band,
    dense_rank_in_feature,
    percent_rank_in_feature,
    ctr_quartile_in_feature
from marts.feature_ctr_ranked
where dense_rank_in_feature <= 3
order by feature_name, dense_rank_in_feature, impressions desc
limit 25;

select
    segment_type,
    segment_name,
    segment_value,
    impressions,
    clicks,
    ctr,
    ctr_lift_vs_overall,
    volume_band,
    segment_rank_by_lift
from marts.high_value_segments
where segment_rank_by_lift <= 10
order by segment_type, segment_rank_by_lift, impressions desc
limit 30;

select
    segment_type,
    segment_name,
    segment_value,
    impressions,
    clicks,
    ctr,
    ctr_lift_vs_overall,
    volume_band,
    segment_rank_by_underperformance
from marts.low_performing_segments
where segment_rank_by_underperformance <= 10
order by segment_type, segment_rank_by_underperformance, impressions desc
limit 30;

select
    feature_family,
    feature_name,
    segment_value,
    impressions,
    clicks,
    ctr,
    ctr_lift_vs_overall,
    volume_band,
    dense_rank_by_lift,
    lift_quintile
from marts.feature_ctr_lift_ranked
where dense_rank_by_lift <= 5
order by feature_family, feature_name, dense_rank_by_lift, impressions desc
limit 30;

select
    interaction_name,
    left_feature_value,
    right_feature_value,
    impressions,
    clicks,
    ctr,
    ctr_lift_vs_overall,
    volume_band,
    dense_rank_in_interaction,
    percent_rank_in_interaction,
    cumulative_clicks_by_interaction
from marts.feature_interaction_ranked
where dense_rank_in_interaction <= 10
order by interaction_name, dense_rank_in_interaction, impressions desc
limit 30;

select
    audit_id,
    source_file,
    row_count,
    check_status,
    checked_at
from quality.load_audit
where layer_name = 'advanced_sql'
  and table_name = 'advanced sql layer'
order by audit_id desc
limit 10;

