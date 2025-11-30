select
     r.id
    ,t.name as table_name
from
    {{ params.dq_schema }}.log_prof_rule as l
    left join {{ params.meta_schema }}.mt_prof_rule as r
        on l.rule_id = r.id
    left join {{ params.meta_schema }}.mt_table as t
        on r.table_id = t.id
where
    l.status_cd = 'R'
order by r.id;


select
     r.id
    ,t.name as table_name
    ,m.meas_cd
    ,m.sort_num
from
    {{ params.meta_schema }}.mt_prof_rule as r
    inner join {{ params.meta_schema }}.mt_prof_rule_meas as m
        on r.id = m.rule_id
    left join {{ params.meta_schema }}.mt_table as t
        on r.table_id = t.id
where
    r.act_flg=1
order by r.id, m.sort_num;





