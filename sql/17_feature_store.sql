create table if not exists feature_store.ctr_training_features as
with base as (
    select
        s.raw_event_id,
        s.batch_id,
        s.label,
        s.event_day_number,
        s.event_batch,
        s.click_flag,
        s.impression_count,
        s.click_count,
        s.missing_numeric_count,
        s.missing_categorical_count,
        s.i1, s.i2, s.i3, s.i4, s.i5, s.i6, s.i7, s.i8, s.i9, s.i10, s.i11, s.i12, s.i13,
        s.c1, s.c6, s.c19, s.c20, s.c22, s.c25, s.c26,
        case when s.i1 < 0 then 'negative' when s.i1 = 0 then 'zero' when s.i1 between 1 and 10 then '1_10' when s.i1 between 11 and 100 then '11_100' when s.i1 between 101 and 1000 then '101_1000' when s.i1 between 1001 and 10000 then '1001_10000' else '10001_plus' end as i1_bucket_code,
        case when s.i2 < 0 then 'negative' when s.i2 = 0 then 'zero' when s.i2 between 1 and 10 then '1_10' when s.i2 between 11 and 100 then '11_100' when s.i2 between 101 and 1000 then '101_1000' when s.i2 between 1001 and 10000 then '1001_10000' else '10001_plus' end as i2_bucket_code,
        case when s.i3 < 0 then 'negative' when s.i3 = 0 then 'zero' when s.i3 between 1 and 10 then '1_10' when s.i3 between 11 and 100 then '11_100' when s.i3 between 101 and 1000 then '101_1000' when s.i3 between 1001 and 10000 then '1001_10000' else '10001_plus' end as i3_bucket_code,
        case when s.i4 < 0 then 'negative' when s.i4 = 0 then 'zero' when s.i4 between 1 and 10 then '1_10' when s.i4 between 11 and 100 then '11_100' when s.i4 between 101 and 1000 then '101_1000' when s.i4 between 1001 and 10000 then '1001_10000' else '10001_plus' end as i4_bucket_code,
        case when s.i5 < 0 then 'negative' when s.i5 = 0 then 'zero' when s.i5 between 1 and 10 then '1_10' when s.i5 between 11 and 100 then '11_100' when s.i5 between 101 and 1000 then '101_1000' when s.i5 between 1001 and 10000 then '1001_10000' else '10001_plus' end as i5_bucket_code,
        case when s.i6 < 0 then 'negative' when s.i6 = 0 then 'zero' when s.i6 between 1 and 10 then '1_10' when s.i6 between 11 and 100 then '11_100' when s.i6 between 101 and 1000 then '101_1000' when s.i6 between 1001 and 10000 then '1001_10000' else '10001_plus' end as i6_bucket_code,
        case when s.i7 < 0 then 'negative' when s.i7 = 0 then 'zero' when s.i7 between 1 and 10 then '1_10' when s.i7 between 11 and 100 then '11_100' when s.i7 between 101 and 1000 then '101_1000' when s.i7 between 1001 and 10000 then '1001_10000' else '10001_plus' end as i7_bucket_code,
        case when s.i8 < 0 then 'negative' when s.i8 = 0 then 'zero' when s.i8 between 1 and 10 then '1_10' when s.i8 between 11 and 100 then '11_100' when s.i8 between 101 and 1000 then '101_1000' when s.i8 between 1001 and 10000 then '1001_10000' else '10001_plus' end as i8_bucket_code,
        case when s.i9 < 0 then 'negative' when s.i9 = 0 then 'zero' when s.i9 between 1 and 10 then '1_10' when s.i9 between 11 and 100 then '11_100' when s.i9 between 101 and 1000 then '101_1000' when s.i9 between 1001 and 10000 then '1001_10000' else '10001_plus' end as i9_bucket_code,
        case when s.i10 < 0 then 'negative' when s.i10 = 0 then 'zero' when s.i10 between 1 and 10 then '1_10' when s.i10 between 11 and 100 then '11_100' when s.i10 between 101 and 1000 then '101_1000' when s.i10 between 1001 and 10000 then '1001_10000' else '10001_plus' end as i10_bucket_code,
        case when s.i11 < 0 then 'negative' when s.i11 = 0 then 'zero' when s.i11 between 1 and 10 then '1_10' when s.i11 between 11 and 100 then '11_100' when s.i11 between 101 and 1000 then '101_1000' when s.i11 between 1001 and 10000 then '1001_10000' else '10001_plus' end as i11_bucket_code,
        case when s.i12 < 0 then 'negative' when s.i12 = 0 then 'zero' when s.i12 between 1 and 10 then '1_10' when s.i12 between 11 and 100 then '11_100' when s.i12 between 101 and 1000 then '101_1000' when s.i12 between 1001 and 10000 then '1001_10000' else '10001_plus' end as i12_bucket_code,
        case when s.i13 < 0 then 'negative' when s.i13 = 0 then 'zero' when s.i13 between 1 and 10 then '1_10' when s.i13 between 11 and 100 then '11_100' when s.i13 between 101 and 1000 then '101_1000' when s.i13 between 1001 and 10000 then '1001_10000' else '10001_plus' end as i13_bucket_code
    from staging.stg_criteo_events s
),
overall as (
    select ctr from marts.overall_ctr_summary
)
select
    b.raw_event_id,
    b.batch_id,
    b.label,
    b.event_day_number,
    b.event_batch,
    b.click_flag,
    b.impression_count,
    b.click_count,
    b.missing_numeric_count,
    b.missing_categorical_count,
    (b.missing_numeric_count > 0)::int as has_missing_numeric,
    (b.missing_categorical_count > 0)::int as has_missing_categorical,
    (b.missing_numeric_count + b.missing_categorical_count >= 5)::int as high_missingness_flag,
    b.i1, b.i2, b.i3, b.i4, b.i5, b.i6, b.i7, b.i8, b.i9, b.i10, b.i11, b.i12, b.i13,
    ln(1 + greatest(b.i1, 0)) as i1_log_scale,
    ln(1 + greatest(b.i2, 0)) as i2_log_scale,
    ln(1 + greatest(b.i3, 0)) as i3_log_scale,
    ln(1 + greatest(b.i4, 0)) as i4_log_scale,
    ln(1 + greatest(b.i5, 0)) as i5_log_scale,
    ln(1 + greatest(b.i6, 0)) as i6_log_scale,
    ln(1 + greatest(b.i7, 0)) as i7_log_scale,
    ln(1 + greatest(b.i8, 0)) as i8_log_scale,
    ln(1 + greatest(b.i9, 0)) as i9_log_scale,
    ln(1 + greatest(b.i10, 0)) as i10_log_scale,
    ln(1 + greatest(b.i11, 0)) as i11_log_scale,
    ln(1 + greatest(b.i12, 0)) as i12_log_scale,
    ln(1 + greatest(b.i13, 0)) as i13_log_scale,
    b.c1,
    b.c6,
    b.c19,
    b.c20,
    b.c22,
    b.c25,
    b.c26,
    b.i1_bucket_code,
    b.i2_bucket_code,
    b.i3_bucket_code,
    b.i4_bucket_code,
    b.i5_bucket_code,
    b.i6_bucket_code,
    b.i7_bucket_code,
    b.i8_bucket_code,
    b.i9_bucket_code,
    b.i10_bucket_code,
    b.i11_bucket_code,
    b.i12_bucket_code,
    b.i13_bucket_code,
    coalesce(nb1.ctr_lift_vs_overall, 0) as i1_bucket_ctr_lift,
    coalesce(nb2.ctr_lift_vs_overall, 0) as i2_bucket_ctr_lift,
    coalesce(nb3.ctr_lift_vs_overall, 0) as i3_bucket_ctr_lift,
    coalesce(nb4.ctr_lift_vs_overall, 0) as i4_bucket_ctr_lift,
    coalesce(nb5.ctr_lift_vs_overall, 0) as i5_bucket_ctr_lift,
    coalesce(nb6.ctr_lift_vs_overall, 0) as i6_bucket_ctr_lift,
    coalesce(nb7.ctr_lift_vs_overall, 0) as i7_bucket_ctr_lift,
    coalesce(nb8.ctr_lift_vs_overall, 0) as i8_bucket_ctr_lift,
    coalesce(nb9.ctr_lift_vs_overall, 0) as i9_bucket_ctr_lift,
    coalesce(nb10.ctr_lift_vs_overall, 0) as i10_bucket_ctr_lift,
    coalesce(nb11.ctr_lift_vs_overall, 0) as i11_bucket_ctr_lift,
    coalesce(nb12.ctr_lift_vs_overall, 0) as i12_bucket_ctr_lift,
    coalesce(nb13.ctr_lift_vs_overall, 0) as i13_bucket_ctr_lift,
    coalesce(fc1.ctr_lift_vs_overall, 0) as c1_ctr_lift,
    coalesce(fc6.ctr_lift_vs_overall, 0) as c6_ctr_lift,
    coalesce(fc19.ctr_lift_vs_overall, 0) as c19_ctr_lift,
    coalesce(fc20.ctr_lift_vs_overall, 0) as c20_ctr_lift,
    coalesce(fc22.ctr_lift_vs_overall, 0) as c22_ctr_lift,
    coalesce(fc25.ctr_lift_vs_overall, 0) as c25_ctr_lift,
    coalesce(fc26.ctr_lift_vs_overall, 0) as c26_ctr_lift,
    coalesce(fc1.impressions, 0) as c1_feature_impressions,
    coalesce(fc6.impressions, 0) as c6_feature_impressions,
    coalesce(fc19.impressions, 0) as c19_feature_impressions,
    coalesce(fc20.impressions, 0) as c20_feature_impressions,
    coalesce(fc22.impressions, 0) as c22_feature_impressions,
    coalesce(fc25.impressions, 0) as c25_feature_impressions,
    coalesce(fc26.impressions, 0) as c26_feature_impressions,
    o.ctr as overall_ctr,
    now() as feature_recorded_at
from base b
cross join overall o
left join marts.numeric_bucket_ctr nb1 on nb1.feature_name = 'i1' and nb1.bucket_code = b.i1_bucket_code
left join marts.numeric_bucket_ctr nb2 on nb2.feature_name = 'i2' and nb2.bucket_code = b.i2_bucket_code
left join marts.numeric_bucket_ctr nb3 on nb3.feature_name = 'i3' and nb3.bucket_code = b.i3_bucket_code
left join marts.numeric_bucket_ctr nb4 on nb4.feature_name = 'i4' and nb4.bucket_code = b.i4_bucket_code
left join marts.numeric_bucket_ctr nb5 on nb5.feature_name = 'i5' and nb5.bucket_code = b.i5_bucket_code
left join marts.numeric_bucket_ctr nb6 on nb6.feature_name = 'i6' and nb6.bucket_code = b.i6_bucket_code
left join marts.numeric_bucket_ctr nb7 on nb7.feature_name = 'i7' and nb7.bucket_code = b.i7_bucket_code
left join marts.numeric_bucket_ctr nb8 on nb8.feature_name = 'i8' and nb8.bucket_code = b.i8_bucket_code
left join marts.numeric_bucket_ctr nb9 on nb9.feature_name = 'i9' and nb9.bucket_code = b.i9_bucket_code
left join marts.numeric_bucket_ctr nb10 on nb10.feature_name = 'i10' and nb10.bucket_code = b.i10_bucket_code
left join marts.numeric_bucket_ctr nb11 on nb11.feature_name = 'i11' and nb11.bucket_code = b.i11_bucket_code
left join marts.numeric_bucket_ctr nb12 on nb12.feature_name = 'i12' and nb12.bucket_code = b.i12_bucket_code
left join marts.numeric_bucket_ctr nb13 on nb13.feature_name = 'i13' and nb13.bucket_code = b.i13_bucket_code
left join marts.feature_ctr_summary fc1 on fc1.feature_name = 'c1' and fc1.feature_value = b.c1
left join marts.feature_ctr_summary fc6 on fc6.feature_name = 'c6' and fc6.feature_value = b.c6
left join marts.feature_ctr_summary fc19 on fc19.feature_name = 'c19' and fc19.feature_value = b.c19
left join marts.feature_ctr_summary fc20 on fc20.feature_name = 'c20' and fc20.feature_value = b.c20
left join marts.feature_ctr_summary fc22 on fc22.feature_name = 'c22' and fc22.feature_value = b.c22
left join marts.feature_ctr_summary fc25 on fc25.feature_name = 'c25' and fc25.feature_value = b.c25
left join marts.feature_ctr_summary fc26 on fc26.feature_name = 'c26' and fc26.feature_value = b.c26
where false;

create unique index if not exists idx_ctr_training_features_raw_event_id
    on feature_store.ctr_training_features (raw_event_id);

create index if not exists idx_ctr_training_features_batch
    on feature_store.ctr_training_features (batch_id);

create index if not exists idx_ctr_training_features_label
    on feature_store.ctr_training_features (label);

create index if not exists idx_ctr_training_features_event_day
    on feature_store.ctr_training_features (event_day_number);
