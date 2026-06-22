-- ML foundation validation checks

select 'model_registry_exists' as check_name, count(*) as row_count
from information_schema.tables
where table_schema = 'ml' and table_name = 'model_registry';

select 'training_runs_exists' as check_name, count(*) as row_count
from information_schema.tables
where table_schema = 'ml' and table_name = 'training_runs';

select 'model_metrics_exists' as check_name, count(*) as row_count
from information_schema.tables
where table_schema = 'ml' and table_name = 'model_metrics';

select 'prediction_scores_exists' as check_name, count(*) as row_count
from information_schema.tables
where table_schema = 'ml' and table_name = 'prediction_scores';

select 'latest_training_metrics_view_exists' as check_name, count(*) as row_count
from information_schema.views
where table_schema = 'ml' and table_name = 'latest_training_metrics';

select
    tablename as table_name,
    count(*) as indexed_columns
from pg_indexes
where schemaname = 'ml'
group by tablename
order by tablename;

select
    column_name,
    data_type,
    is_nullable
from information_schema.columns
where table_schema = 'ml'
  and table_name = 'prediction_scores'
order by ordinal_position;
