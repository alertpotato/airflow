with errors as (
    select
         e.document_load_id
         --временный фикс. если есть и error и warning записываем последним error
        ,min(s.err_status_cd) as err_status_cd
    from
        meta.log_document_load as l
        inner join dq.log_dq_err as e
            on l.id = e.document_load_id
        left join meta.mt_dq_rule_doc_scope as s
            on e.rule_id = s.id
    where
        l.etl_proc_id={{ params.etl_proc_id }}
    group by 1

)

update meta.log_document_load as l set
     update_dttm=CURRENT_TIMESTAMP
    ,etl_dq_finish_dttm=CURRENT_TIMESTAMP
    ,etl_dq_status_cd=e.err_status_cd
from
    errors as e
where
    l.etl_dq_status_cd in ('R', 'W', 'E')
    and l.id = e.document_load_id;