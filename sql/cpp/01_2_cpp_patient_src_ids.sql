-- сбор таблицы с документами и ЗЛ для обновления
drop table if exists dma_calc.cpp_patient_src_ids_{{params.temp_suffix_run_id}};
create table dma_calc.cpp_patient_src_ids_{{params.temp_suffix_run_id}} as
select
    psi.master_person_id,
    psi.src01_zl_list_id,
    psi.update_dttm
from dds.patient_src_ids psi 
where psi.update_dttm > '{{ params.cpp_patient_src_ids }}'::timestamptz
distributed randomly;

analyze dma_calc.cpp_patient_src_ids_{{params.temp_suffix_run_id}};

