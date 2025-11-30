-- сбор таблицы со счетами
drop table if exists dma_calc.cpp_schet_inc_{{params.temp_suffix_run_id}};
create table dma_calc.cpp_schet_inc_{{params.temp_suffix_run_id}} as
select 
    sch.id as schet_id
    ,{{params.form_schet_b_id}} as schet_b_id
from dds.schet sch
inner join dds.schet_src_ids si on sch.id = si.schet_id 
--inner join dma_calc.cpp_ldl_{{params.temp_suffix_run_id}} ldl on ldl.doc_ods_id = si.src01_zl_list_id 
--inner join dma_calc.cpp_patient_src_ids_{{params.temp_suffix_run_id}} psi on psi.src01_zl_list_id = si.src01_zl_list_id 
inner join dma_calc.cpp_doc_mpi_inc_{{params.temp_suffix_run_id}} psi on psi.src01_zl_list_id = si.src01_zl_list_id 
union
select -- счета для удаления
    sch.schet_id
    ,sch.schet_b_id
from dma_calc.cpp_hist_sch_{{params.temp_suffix_run_id}} sch
distributed randomly;

analyze dma_calc.cpp_schet_inc_{{params.temp_suffix_run_id}};