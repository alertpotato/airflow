update {{ params.dq_schema }}.log_prof_rule
set status_cd='C',
    status_desc='Завершен',
    finish_dttm=now()
where status_cd in ('R', 'E')