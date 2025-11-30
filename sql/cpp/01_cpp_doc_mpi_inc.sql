-- сбор таблицы с документами и ЗЛ для обновления
drop table if exists dma_calc.cpp_doc_mpi_inc_{{params.temp_suffix_run_id}};
create table dma_calc.cpp_doc_mpi_inc_{{params.temp_suffix_run_id}} as
select
    psi.master_person_id
    ,psi.src01_zl_list_id
from dma_calc.cpp_patient_src_ids_{{params.temp_suffix_run_id}} psi
union
select
    psi.master_person_id
    ,ldl.src01_zl_list_id
from dma_calc.cpp_ldl_{{params.temp_suffix_run_id}} ldl
inner join dds.patient_src_ids psi on psi.src01_zl_list_id = ldl.src01_zl_list_id 
distributed randomly;
analyze dma_calc.cpp_doc_mpi_inc_{{params.temp_suffix_run_id}};

