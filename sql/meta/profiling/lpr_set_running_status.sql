delete from {{ params.dq_schema }}.log_prof_rule where status_cd = 'R';

insert into {{ params.dq_schema }}.log_prof_rule (id, rule_id, status_cd, status_desc, log_info, begin_dttm, finish_dttm)
select
     nextval('{{ params.dq_schema }}.log_prof_rule_id_seq') as id
    ,id as rule_id
    ,'R' as status_cd
    ,'Выполняется' as status_desc
    ,null as log_info
    ,now() as begin_dttm
    ,null as finish_dttm
from
    {{ params.meta_schema }}.mt_prof_rule
where act_flg=1