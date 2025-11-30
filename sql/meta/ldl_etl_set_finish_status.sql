update {{ params.meta_schema }}.log_document_load as ldl
set etl_status_cd = 'C',
    etl_finish_dttm = CURRENT_TIMESTAMP,
    update_dttm = CURRENT_TIMESTAMP
from
    meta.log_etl_proc as proc
where
    ldl.region_cd = {{ params.region_cd }}
    and ldl.etl_status_cd = 'R'
    and proc.etl_run_id = {{ params.etl_run_id }}
    and proc.status_cd = 'C'
    and ldl.etl_proc_id = proc.id