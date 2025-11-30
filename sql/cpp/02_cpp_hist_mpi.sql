-- cбор таблицы с обновлёнными пациентами
drop table if exists dma_calc.cpp_hist_mpi_{{params.temp_suffix_run_id}};
create table dma_calc.cpp_hist_mpi_{{params.temp_suffix_run_id}} as
select -- "обновлённые пациенты"
    mpi_d.master_person_id
    ,mpi_d.update_dttm
from dds.hist_master_person_id_del mpi_d
where mpi_d.update_dttm > '{{ params.cpp_hist_mpi }}'::timestamptz;