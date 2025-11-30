insert into dq.log_dq_erzl
select
    now() as check_dtm,
    '{{ params.period }}' as period,
    null as region,
    {{ params.rule_id }} as rule_id,
    q.value
from
    ({{ params.inner_query }}) as q
