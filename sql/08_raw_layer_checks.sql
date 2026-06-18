select
    count(*) as raw_row_count
from raw.criteo_events;

select
    source_file,
    count(*) as row_count,
    min(ingested_at) as first_ingested_at,
    max(ingested_at) as last_ingested_at
from raw.criteo_events
group by source_file
order by source_file;

select
    count(*) as audit_rows
from quality.load_audit
where layer_name = 'raw'
  and table_name = 'raw.criteo_events';

select
    audit_id,
    source_file,
    row_count,
    check_status,
    checked_at
from quality.load_audit
where layer_name = 'raw'
  and table_name = 'raw.criteo_events'
order by audit_id desc
limit 10;

