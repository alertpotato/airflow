-- обновление update_dttm из master_person_disable_data
update meta.rakurs_wf_settings upd
set wf_setting = u_max.wf_setting,
    update_dttm = now()
from (
    select 
        ('{"last_update_dttm": "'||(max(u.update_dttm))::text|| '"}')::json as wf_setting
    from dma_calc.cpp_master_person_upd_{{params.temp_suffix_run_id}} u
) u_max
where upd.wf_key = 'cpp_load'
    and u_max.wf_setting is not null;

-- обновление update_dttm из ldl   
update meta.rakurs_wf_settings upd
set wf_setting = u_max.wf_setting,
    update_dttm = now()
from (
    select 
        ('{"last_update_dttm": "'||(max(u.update_dttm))::text|| '"}')::json as wf_setting
    from dma_calc.cpp_ldl_{{params.temp_suffix_run_id}} u
) u_max  
where upd.wf_key = 'cpp_ldl'
    and u_max.wf_setting is not null;

-- обновление update_dttm из patient_src_ids  
update meta.rakurs_wf_settings upd
set wf_setting = u_max.wf_setting,
    update_dttm = now()
from (
    select 
        ('{"last_update_dttm": "'||(max(u.update_dttm))::text|| '"}')::json as wf_setting
    from dma_calc.cpp_patient_src_ids_{{params.temp_suffix_run_id}} u
) u_max      
where upd.wf_key = 'cpp_patient_src_ids'
    and u_max.wf_setting is not null;


-- обновление update_dttm из hist_master_person_id_del   
update meta.rakurs_wf_settings upd
set wf_setting = u_max.wf_setting,
    update_dttm = now()
from (
    select 
        ('{"last_update_dttm": "'||(max(u.update_dttm))::text|| '"}')::json as wf_setting
    from dma_calc.cpp_hist_mpi_{{params.temp_suffix_run_id}} u
) u_max   
where upd.wf_key = 'cpp_hist_mpi'
    and u_max.wf_setting is not null;


-- обновление update_dttm из hist_schet_del  
update meta.rakurs_wf_settings upd
set wf_setting = u_max.wf_setting,
    update_dttm = now()
from (
    select 
        ('{"last_update_dttm": "'||(max(u.update_dttm))::text|| '"}')::json as wf_setting
    from dma_calc.cpp_hist_sch_{{params.temp_suffix_run_id}} u
) u_max  
where upd.wf_key = 'cpp_hist_sch'
    and u_max.wf_setting is not null;


-- обновление update_dttm из patient_src_ids  
update meta.rakurs_wf_settings upd
set wf_setting = u_max.wf_setting,
    update_dttm = now()
from (
    select 
        ('{"last_update_dttm": "'||(max(u.update_dttm))::text|| '"}')::json as wf_setting
    from dma_calc.cpp_patient_src_ids_{{params.temp_suffix_run_id}} u
) u_max      
where upd.wf_key = 'cpp_patient_src_ids'
    and u_max.wf_setting is not null;


-- обновление mrf_update_dttm из dma_pre.cpp_patient
update meta.rakurs_wf_settings upd
set wf_setting = u_max.wf_setting,
    update_dttm = now()
from (
    select 
        ('{"last_update_dttm": "'||(max(u.mrf_update_dttm))::text|| '"}')::json as wf_setting
    from dma_calc.rakurs_med_flg_inc_{{params.temp_suffix_run_id}} u
) u_max      
where upd.wf_key = 'cpp_mrf'
    and u_max.wf_setting is not null;
