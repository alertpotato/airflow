select
    l.column_name as dds_column_name,
    r.column_name as ods_column_name
from
    information_schema.columns as l
    inner join information_schema.columns as r
        on l.table_name = r.table_name
            and r.column_name = l.column_name || '_ods'
where
    l.table_name = '{{ params.table_name}}_{{ params.temp_suffix }}'
    and l.table_schema = '{{ params.stage_schema }}'