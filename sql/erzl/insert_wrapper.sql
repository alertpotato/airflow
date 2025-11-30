insert into dq.{{ params.log_table_name }}
select
    '{{ params.check_dtm }}' as check_dtm,
    '{{ params.period }}' as period,
    null as region,
    {{ params.rule_id }} as rule_id,
    q.value
from
    ({{ params.query_check }}) as q