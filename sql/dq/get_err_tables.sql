select distinct
     table_id
    ,t.name as table_name
from
    dq.log_dq_err as err
    left join meta.mt_table as t
        on err.table_id = t.id
where
    etl_proc_id={{ params.etl_proc_id }}