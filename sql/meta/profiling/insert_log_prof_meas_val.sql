insert into {{ params.dq_schema }}.log_prof_meas_val (id, meas_cd, rule_id, meas_val, period_cd, region_cd, val_comment, calc_dttm)

{% for meas in params.meas_list -%}

select nextval('{{ params.dq_schema }}.log_prof_meas_val_id_seq') as id, '{{ meas[1] }}' as meas_cd, {{ meas[0] }} as rule_id, meas_{{ meas[2] }} as meas_val,
period_cd, region_cd, '' as val_comment, now() as calc_dttm
from {{ params.stage_schema }}.{{ params.table_name }}
{{ 'union' if not loop.last }}

{% endfor %}