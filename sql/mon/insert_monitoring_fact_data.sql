delete from dma_calc.monitoring_fact_data
where
    log_etl_proc_id in ({{params.log_etl_proc_id}})
    and fact_table_name = '{{ params.fact_table_name }}';

insert into dma_calc.monitoring_fact_data(log_etl_proc_id,
                                     etl_id,
                                     begin_dttm,
                                     finish_dttm,
                                     etl_duration,
                                     fact_table_name,
                                     error_table_name,
                                     fact_count_rec,
                                     error_count_rec,
                                     fact_count_insert,
                                     fact_count_update,
                                     fact_count_delete)

select p.id                                                 as log_etl_proc_id,
       p.etl_proc_id                                        as etl_id,
       p.begin_dttm                                         as begin_dttm,
       p.finish_dttm                                        as finish_dttm,
       extract(epoch from (p.finish_dttm - p.begin_dttm))   as etl_duration,
       '{{ params.fact_table_name }}'                                   as fact_table_name,
       '{{ params.error_table_name }}'                                  as error_table_name,
       (select count(*) from dma_pre_oms.{{ params.fact_table_name }})  as fact_count_rec,
       (select count(*) from dma_calc.{{ params.error_table_name }})    as error_count_rec,
       lept.rec_ins_qnt                                                 as fact_count_insert,
       lept.rec_upd_qnt                                                 as fact_count_update,
       lept.rec_del_qnt                                                 as fact_count_delete
from meta.log_etl_proc as p
         inner join meta.log_etl_proc_table as lept
                    on p.id = lept.log_etl_proc_id
where
    p.id in ({{params.log_etl_proc_id}})
    and lept.table_id = {{ params.table_id }}