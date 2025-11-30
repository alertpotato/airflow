-- сбор таблицы со счетами для удаления
drop table if exists dma_calc.cpp_hist_sch_{{params.temp_suffix_run_id}};
create table dma_calc.cpp_hist_sch_{{params.temp_suffix_run_id}} as
select -- счета для удаления
    sch.schet_id
    ,{{params.form_schet_b_id}} as schet_b_id -- для удаления данных
    ,sch.update_dttm
from dds.hist_schet_del sch
where sch.update_dttm > '{{ params.cpp_hist_sch }}'::timestamptz;

analyze dma_calc.cpp_hist_sch_{{params.temp_suffix_run_id}};