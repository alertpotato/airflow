update {{ params.meta_schema }}.log_document_load
set etl_status_cd = 'R',
    etl_proc_id = {{ params.etl_proc_id }},
    etl_begin_dttm = CURRENT_TIMESTAMP,
    etl_finish_dttm = null,
    update_dttm = CURRENT_TIMESTAMP
where
    doc_type_cd = 'S_OMS'
    and stream_status_cd = 'C'
    and file_type_cd = 'E'
    and region_cd = {{ params.region_cd }}
    and etl_status_cd is null;

update {{ params.meta_schema }}.log_document_load as ldl
set doc_cd = z.schet_code,
    doc_num = z.schet_nschet,
    doc_dt = z.schet_dschet::date
from {{ params.ods_schema }}.zl_list_schet as z
where z.ods_id = ldl.doc_ods_id
and ldl.etl_proc_id = {{ params.etl_proc_id }};