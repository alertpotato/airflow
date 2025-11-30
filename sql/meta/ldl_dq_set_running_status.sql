update meta.log_document_load
set etl_dq_status_cd = 'R',
    etl_dq_begin_dttm = CURRENT_TIMESTAMP,
    etl_dq_finish_dttm = null
where
    doc_type_cd = 'S_OMS'
    and etl_proc_id = {{ params.etl_proc_id }}