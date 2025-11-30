select
    id as id
from
    meta.log_etl_proc
where
    etl_proc_id=37
    and status_cd = 'C'
    and begin_dttm >= '{{ params.date_from }}'
    and finish_dttm < '{{ params.date_to }}'