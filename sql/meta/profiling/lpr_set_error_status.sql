update {{ params.dq_schema }}.log_prof_rule
set status_cd='E',
    status_desc='Ошибка'
where status_cd='R'