with errors as (
    {{ params.query_check }}
)

insert into dq.log_dq_err (document_load_id, etl_proc_id, doc_ods_id, src_ods_id,table_id, rule_id, column_name, column_value, insert_dttm)
select
     l.id as document_load_id
    ,{{ params.etl_proc_id }} as etl_proc_id
    ,err.doc_ods_id as doc_ods_id
    ,err.ods_id as ods_id
    ,{{ params.table_id }} as table_id
    ,{{ params.rule_id }} as rule_id
    ,err.column_name
    ,err.column_value
    ,CURRENT_TIMESTAMP as insert_dttm
from
    errors as err
    left join meta.log_document_load as l
        on err.doc_ods_id = l.doc_ods_id;