-- сбор таблицы с ЗЛ,у которых поменялся master_person_id
drop table if exists dma_calc.cpp_master_person_upd_{{params.temp_suffix_run_id}};
create table dma_calc.cpp_master_person_upd_{{params.temp_suffix_run_id}}
with(appendonly=true, orientation=column, compresstype=zstd) as
    select
        dd.old_master_person_id
        ,dd.master_person_id
        ,dd.update_dttm
    from dds.master_person_disable_data dd
    where dd.update_dttm > '{{ params.cpp_load }}'::timestamptz
distributed by (old_master_person_id);

analyze dma_calc.cpp_master_person_upd_{{params.temp_suffix_run_id}};