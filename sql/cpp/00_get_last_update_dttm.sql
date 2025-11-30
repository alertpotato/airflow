select
    wf_key,
    coalesce((wf_setting::json->>'last_update_dttm')::timestamp, '1900-01-01'::timestamp) as last_update_dttm
from meta.rakurs_wf_settings
where wf_key = 'cpp_patient_src_ids'
union
select
    wf_key,
    coalesce((wf_setting::json->>'last_update_dttm')::timestamp, '1900-01-01'::timestamp) as last_update_dttm
from meta.rakurs_wf_settings
where wf_key = 'cpp_ldl'
union
select
    wf_key,
    coalesce((wf_setting::json->>'last_update_dttm')::timestamp, '1900-01-01'::timestamp)
from meta.rakurs_wf_settings
where wf_key = 'cpp_hist_mpi'
union
select
    wf_key,
    coalesce((wf_setting::json->>'last_update_dttm')::timestamp, '1900-01-01'::timestamp)
from meta.rakurs_wf_settings
where wf_key = 'cpp_load'
union
select
    wf_key,
    coalesce((wf_setting::json->>'last_update_dttm')::timestamp, '1900-01-01'::timestamp)
from meta.rakurs_wf_settings
where wf_key = 'cpp_hist_sch'
union
select
    wf_key,
    coalesce((wf_setting::json->>'last_update_dttm')::timestamp, '1900-01-01'::timestamp)
from meta.rakurs_wf_settings
where wf_key = 'cpp_mrf'
