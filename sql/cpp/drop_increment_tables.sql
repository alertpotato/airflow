/*
-- удаление таблицы с документами для обновления
drop table if exists dma_calc.cpp_ldl_{{params.temp_suffix_run_id}};
*/

drop table if exists dma_calc.cpp_doc_mpi_inc_{{params.temp_suffix_run_id}};
-- удаление таблицы с документами и ЗЛ для обновления
drop table if exists dma_calc.cpp_patient_src_ids_{{params.temp_suffix_run_id}};
-- удаление таблицы с обновлёнными пациентами
drop table if exists dma_calc.cpp_hist_mpi_{{params.temp_suffix_run_id}};
-- удаление таблицы с ЗЛ,у которых поменялся master_person_id
drop table if exists dma_calc.cpp_master_person_upd_{{params.temp_suffix_run_id}};
-- удаление таблциы с master_person_id
drop table if exists dma_calc.cpp_master_person_inc_{{params.temp_suffix_run_id}};
-- удаление таблицы со счетами для удаления
drop table if exists dma_calc.cpp_hist_sch_{{params.temp_suffix_run_id}};
-- удаление таблицы со счетами
drop table if exists dma_calc.cpp_schet_inc_{{params.temp_suffix_run_id}};
-- удаление таблицы с необработанными записями по med_rakurs_flg из таблицы dma_pre.cpp_patient
drop table if exists {{ params.dma_calc_schema }}.rakurs_med_flg_inc_{{ params.temp_suffix_run_id }};
-- удаление таблицы с master_person_id для инкрементального обновления таблиц ракурсов 
drop table if exists {{ params.dma_calc_schema }}.rakurs_master_person_inc_{{ params.temp_suffix_run_id }};