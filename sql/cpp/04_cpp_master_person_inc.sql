-- сбор таблциы с master_person_id
drop table if exists dma_calc.cpp_master_person_inc_{{params.temp_suffix_run_id}};
create table dma_calc.cpp_master_person_inc_{{params.temp_suffix_run_id}} as
select
    q.master_person_id
from ( -- отбор пациентов для перерасчёта метрик по всем периодам и регонам
    select -- новые пациенты
        ps.master_person_id
    --from dds.patient_src_ids ps
    --inner join dma_calc.cpp_ldl_{{params.temp_suffix_run_id}} ldl on ldl.doc_ods_id = ps.src01_zl_list_id 
    --from dma_calc.cpp_patient_src_ids_{{params.temp_suffix_run_id}} ps
    from dma_calc.cpp_doc_mpi_inc_{{params.temp_suffix_run_id}} ps
    where ps.master_person_id > 0
    union 
    select -- "обновлённые пациенты"
        mpi_d.master_person_id
    from dma_calc.cpp_hist_mpi_{{params.temp_suffix_run_id}} mpi_d
    union
    select 
        dd1.old_master_person_id    -- пациенты,данные по которым нужно удалить
        from dma_calc.cpp_master_person_upd_{{params.temp_suffix_run_id}} dd1
    union
    select 
        dd2.master_person_id -- пациенты,данные по которым нужно пересчитать      
    from dma_calc.cpp_master_person_upd_{{params.temp_suffix_run_id}} dd2    
) q 
distributed randomly;

analyze dma_calc.cpp_master_person_inc_{{params.temp_suffix_run_id}};