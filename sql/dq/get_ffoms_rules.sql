select
     r.id as rule_id
    ,r.group_cd as group_cd
    ,t.id::int as table_id
    ,t.name as table_name
    ,s.file_type_cd as type_d
    ,s.file_ver as file_ver
from
    (select distinct to_date(period_cd::text, 'YYYYMM01') as period_dt, file_type_cd, file_ver
     from meta.log_document_load
     where etl_proc_id={{ params.etl_proc_id }}
    ) as b
    inner join meta.mt_dq_rule_doc_scope as s
        on b.period_dt between s.start_dt and s.end_dt
            and position(b.file_type_cd in s.file_type_cd)>0
                and s.file_ver = b.file_ver
                    and s.doc_type_cd = 'S_OMS'
    inner join meta.mt_dq_rule as r
        on s.cd = r.cd and r.act_flg = 1
            and b.period_dt between r.start_dt and r.end_dt
    left join meta.mt_table as t
        on r.table_id = t.id
group by 1,2,3,4,5,6
order by rule_id