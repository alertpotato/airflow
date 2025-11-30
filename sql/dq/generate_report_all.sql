with stg as (
    select
         b.doc_ods_id as zl_list_id
        ,b.doc_cd as schet_code
        ,s.cd as oshib
        ,r.group_cd as nsi_check
        ,err.column_name as im_pol
        ,err.column_value as zn_pol
        ,b.doc_num as nschet
        ,s.file_element as bas_el
        ,r.descr as comment
        ,2 as phase_k
        ,1 as chk_step
        ,err.src_ods_id as src_ods_id
        ,b.file_type_cd as type_d
        ,b.region_cd as tf
        ,b.doc_dt as period
        ,b.etl_dq_finish_dttm as date_chk
        ,'{{ params.parent_el }}' as parent_el
        ,stg.{{ params.parent_el_id }} as parent_el_id
        ,s.err_status_cd as err_status_cd
    from
        dq.log_dq_err as err
        left join meta.log_document_load as b
            on b.id = err.document_load_id
                and b.etl_proc_id = err.etl_proc_id
        left join meta.mt_dq_rule as r
            on err.rule_id = r.id
        left join meta.mt_dq_rule_doc_scope as s
            on r.cd = s.cd
        {% if params.table_name in ['b_diag', 'b_prot'] %}
        left join {{ params.stage_schema }}.{{ params.table_name }}_{{ params.temp_suffix }} as b_diag
            on err.doc_ods_id = b_diag.doc_ods_id
                and err.src_ods_id = b_diag.ods_id
        left join {{ params.stage_schema }}.onk_sl_{{ params.temp_suffix }} as stg
            on b_diag.onk_sl_id = stg.id
                and b_diag.doc_ods_id = stg.doc_ods_id
        {% elif params.table_name == 'med_dev' %}
         left join {{ params.stage_schema }}.{{ params.table_name }}_{{ params.temp_suffix }} as med_dev
            on err.doc_ods_id = med_dev.doc_ods_id
                and err.src_ods_id = med_dev.ods_id
        left join {{ params.stage_schema }}.usl_{{ params.temp_suffix }} as stg
            on med_dev.usl_id = stg.id
                and med_dev.doc_ods_id = stg.doc_ods_id
        {% elif params.table_name == 'onk_usl' %}
        left join {{ params.stage_schema }}.{{ params.table_name }}_{{ params.temp_suffix }} as onk_usl
            on err.doc_ods_id = onk_usl.doc_ods_id
                and err.src_ods_id = onk_usl.ods_id
        left join {{ params.stage_schema }}.onk_sl_{{ params.temp_suffix }} as stg
            on onk_usl.onk_sl_id = stg.id
                and onk_usl.doc_ods_id = stg.doc_ods_id
        {% elif params.table_name == 'ksg_kpg_crit' %}
        left join {{ params.stage_schema }}.{{ params.table_name }}_{{ params.temp_suffix }} as ksg_kpg_crit
            on err.doc_ods_id = ksg_kpg_crit.doc_ods_id
                and err.src_ods_id = ksg_kpg_crit.ods_id
        left join {{ params.stage_schema }}.ksg_kpg_{{ params.temp_suffix }} as stg
            on ksg_kpg_crit.ksg_kpg_id = stg.id
        {% elif params.table_name == 'sl_koef' %}
        left join {{ params.stage_schema }}.{{ params.table_name }}_{{ params.temp_suffix }} as sl_koef
             on err.doc_ods_id = sl_koef.doc_ods_id
                and err.src_ods_id = sl_koef.ods_id
        left join {{ params.stage_schema }}.ksg_kpg_{{ params.temp_suffix }} as stg
            on sl_koef.ksg_kpg_id = stg.id
        {% elif params.table_name == 'lek_dose' %}
        left join {{ params.stage_schema }}.{{ params.table_name }}_{{ params.temp_suffix }} as lek_dose
            on err.doc_ods_id = lek_dose.doc_ods_id
                and err.src_ods_id = lek_dose.ods_id
        left join {{ params.stage_schema }}.lek_pr_{{ params.temp_suffix }} as stg
            on lek_dose.lek_pr_id = stg.id
        {% else %}
        left join {{ params.stage_schema }}.{{ params.table_name }}_{{ params.temp_suffix }} as stg
            on err.doc_ods_id = stg.doc_ods_id
                and err.src_ods_id = stg.ods_id
        {% endif %}
    where
        err.table_id={{ params.table_id }}
        and err.etl_proc_id = {{ params.etl_proc_id }}
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
)

insert into dq.report_oms_log
select
     nextval('dq.seq_report_oms_log'::regclass) AS id
    ,zl_list_id
    ,schet_code
    ,oshib
    ,nsi_check
    ,im_pol
    ,zn_pol
    ,nschet
    ,bas_el
    ,comment
    ,phase_k
    ,chk_step
    ,src_ods_id
    ,type_d
    ,tf
    ,period
    ,date_chk
    ,parent_el
    ,parent_el_id
    ,null
    ,null
    ,null
    ,err_status_cd
from
    stg