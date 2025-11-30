drop table if exists log_etl_proc_table_tmp_{{ ti.job_id }};

create temp table log_etl_proc_table_tmp_{{ ti.job_id }} as
with changed_rows as (
    {% for table_name, qnt in params.log_params.items() -%}
        select
            '{{table_name}}' as table_name,
            {{ qnt.rec_ins_qnt }} as rec_ins_qnt,
            {{ qnt.rec_upd_qnt }} as rec_upd_qnt,
            {{ qnt.rec_del_qnt }} as rec_del_qnt
    {{ 'union' if not loop.last }}
    {% endfor %}
)

select
    {{ params.etl_proc_id }} as log_etl_proc_id,
    mt.id as table_id,
    'C' as status_cd,
    null as status_desc,
    rows.rec_ins_qnt,
    rows.rec_upd_qnt,
    rows.rec_del_qnt,
--     '{{ params.begin_dttm }}' as begin_dttm,
--     '{{ params.finish_dttm }}' as finish_dttm,
    {{ params.duration }} as duration
from
    meta.mt_table as mt
    inner join changed_rows as rows
        on mt.name = rows.table_name
where
    id = {{ params.table_id }};

update meta.log_etl_proc_table as l
set
    rec_ins_qnt = l.rec_ins_qnt+tmp.rec_ins_qnt,
    rec_upd_qnt = l.rec_upd_qnt+tmp.rec_upd_qnt,
    rec_del_qnt = l.rec_del_qnt+tmp.rec_del_qnt,
    duration = l.duration+tmp.duration
from
    log_etl_proc_table_tmp_{{ ti.job_id }} as tmp
where
    l.log_etl_proc_id = tmp.log_etl_proc_id
    and l.table_id = tmp.table_id;
