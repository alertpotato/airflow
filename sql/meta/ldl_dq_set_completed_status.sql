with not_errors as (
    select distinct
         l.id
    from
        meta.log_document_load as l
        left join dq.log_dq_err as e
            on l.id = e.document_load_id
    where
        l.etl_proc_id={{ params.etl_proc_id }}
        and e.document_load_id is null
)

update meta.log_document_load as l set
     update_dttm=CURRENT_TIMESTAMP
    ,etl_dq_finish_dttm=CURRENT_TIMESTAMP
    ,etl_dq_status_cd='C'
    ,err_descr=null
where
    l.etl_proc_id={{ params.etl_proc_id }}
    and l.id in (select id from not_errors)