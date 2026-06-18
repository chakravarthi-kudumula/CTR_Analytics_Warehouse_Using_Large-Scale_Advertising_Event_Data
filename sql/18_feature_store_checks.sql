select count(*) as ctr_training_features_rows
from feature_store.ctr_training_features;

select
    count(*) filter (where label not in (0, 1)) as invalid_label_rows,
    count(*) filter (where raw_event_id is null) as null_raw_event_id_rows,
    count(*) filter (where batch_id is null) as null_batch_id_rows,
    count(*) filter (where overall_ctr < 0 or overall_ctr > 1) as invalid_overall_ctr_rows,
    count(*) filter (where event_day_number not between 1 and 7) as invalid_event_day_rows
from feature_store.ctr_training_features;

select count(*) as duplicate_raw_event_id_groups
from (
    select raw_event_id
    from feature_store.ctr_training_features
    group by raw_event_id
    having count(*) > 1
) duplicates;

select
    batch_id,
    label,
    count(*) as rows,
    round(avg(c22_ctr_lift)::numeric, 6) as avg_c22_ctr_lift,
    round(avg(i1_bucket_ctr_lift)::numeric, 6) as avg_i1_bucket_ctr_lift
from feature_store.ctr_training_features
group by batch_id, label
order by batch_id, label;
