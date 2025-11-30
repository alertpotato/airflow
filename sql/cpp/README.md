
## Описание скриптов


- **cpp_ldl.sql**
Сборка таблицы с необработанными документами из meta.log_document_load

- **cpp_hist_mpi.sql**
Сборка таблицы c необработанными обновлёнными и удалёнными пациентами из таблицы dds.hist_master_person_id_del

- **cpp_master_person_upd.sql**
Cборка таблицы с необработанными ЗЛ из dds.master_person_disable_data, у которых поменялся mpi.

- **cpp_master_person_inc.sql**
Сборка таблицы с инкрементом по ЗЛ из таблиц:
    * dma_calc.cpp_ldl \ 
    * dma_calc.cpp_hist_mpi \
    * dma_calc.cpp_master_person_upd 

- **cpp_hist_sch.sql**    
Сборка таблицы с необработанными обновлёнными и удалёнными счетами из таблицы dds.hist_schet_del.

- **cpp_schet_inc.sql**
Сборка таблицы с инкрементом по счёту из таблиц:
    * cpp_ldl \
    * cpp_hist_sch 

- **drop_increment_tables.sql** 
Удаление таблиц с инкрементом

- **cpp_table_mpi_upd.sql** 
Обновление таблиц cpp_, которые обновляются по счёту

- **get_last_update_dttm.sql** 
Скрипт получения последних обработанных update_dttm

- **last_update_dttm_upd.sql** 
Обновление последнего обработанного update_dttm в таблице meta.rakurs_wf_setting
