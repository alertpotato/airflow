-- сбор таблицы с документами для обновления
drop table if exists dma_calc.cpp_ldl_{{params.temp_suffix_run_id}};
create table dma_calc.cpp_ldl_{{params.temp_suffix_run_id}} as
select
    ldl.doc_ods_id as src01_zl_list_id,
    ldl.update_dttm
from meta.log_document_load ldl
where ldl.update_dttm > '{{ params.cpp_ldl }}'::timestamptz
    and ldl.etl_status_cd = 'C' -- только те документы,которые успешно дошли до dds
distributed randomly;

analyze dma_calc.cpp_ldl_{{params.temp_suffix_run_id}};