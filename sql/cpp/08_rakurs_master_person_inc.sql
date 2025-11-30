

begin;
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


-- создание таблицы с master_person_id для инкрементального обновления таблиц ракурсов 
drop table if exists {{ params.dma_calc_schema }}.rakurs_master_person_inc_{{ params.temp_suffix_run_id }};
create table {{ params.dma_calc_schema }}.rakurs_master_person_inc_{{ params.temp_suffix_run_id }} as
select
    cm.master_person_id
    ,cp.med_rakurs_flg
from {{ params.dma_calc_schema }}.cpp_master_person_inc_{{ params.temp_suffix_run_id }} cm
left join dma_pre.cpp_patient cp on cp.master_person_id = cm.master_person_id
union 
select
    rm.master_person_id
    ,rm.med_rakurs_flg
from {{ params.dma_calc_schema }}.rakurs_med_flg_inc_{{ params.temp_suffix_run_id }} rm
distributed by (master_person_id);
commit;