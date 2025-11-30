

-- создание таблицы с необработанными записями по med_rakurs_flg из таблицы dma_pre.cpp_patient
drop table if exists {{ params.dma_calc_schema }}.rakurs_med_flg_inc_{{ params.temp_suffix_run_id }};
create table {{ params.dma_calc_schema }}.rakurs_med_flg_inc_{{ params.temp_suffix_run_id }} as
select
    cp.master_person_id
    ,cp.med_rakurs_flg
    ,cp.mrf_update_dttm
from dma_pre.cpp_patient cp
where cp.mrf_update_dttm > '{{ params.cpp_mrf }}'::timestamptz
distributed by (master_person_id);
