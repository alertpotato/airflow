select 
    wf.wf_setting::json->>'{{ params.field_key }}'
from meta.rakurs_wf_settings wf
where wf.wf_key = '{{ params.wf_key }}';