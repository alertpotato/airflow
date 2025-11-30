delete from dq.log_dq_err
where document_load_id in (
    select id
    from meta.log_document_load
    where file_name in (
        select file_name
        from meta.log_document_load
        where etl_proc_id = {{ params.etl_proc_id }}
        )
)