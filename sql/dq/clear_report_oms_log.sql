delete from dq.report_oms_log
where zl_list_id in (
    select distinct
        doc_ods_id
    from
        dq.log_dq_err
    where
        etl_proc_id={{ params.etl_proc_id }}
)