delete
from dma_calc.monitoring_data
where log_etl_proc_id in ({{params.log_etl_proc_id}});

insert into dma_calc.monitoring_data(log_etl_proc_id,
                                     etl_proc_id,
                                     begin_dttm,
                                     finish_dttm,
                                     etl_proc_duration,
                                     duration_w_inv_mcare_f,
                                     insert_w_inv_mcare_f,
                                     insert_err_inv_mcare_fs,
                                     count_err_inv_mcare_fs)
select p.id                                               as log_etl_proc_id,
       p.etl_proc_id                                      as etl_proc_id,
       p.begin_dttm                                       as begin_dttm,
       p.finish_dttm                                      as finish_dttm,
       extract(epoch from (p.finish_dttm - p.begin_dttm)) as etl_proc_duration,
       sum(case
               when tbl.name = 'w_inv_mcare_f' then
                   lept.duration
               else
                   0
           end)                                           as duration_w_inv_mcare_f,
       sum(case
               when tbl.name = 'w_inv_mcare_f' then
                   lept.rec_ins_qnt
               else
                   0
           end)                                           as insert_w_inv_mcare_f,
       sum(case
               when tbl.name = 'err_inv_mcare_fs' then
                   lept.rec_ins_qnt
               else
                   0
           end)                                           as insert_err_inv_mcare_fs,
       (select count(*) from dma_calc.err_inv_mcare_fs)   as count_err_inv_mcare_fs
from meta.log_etl_proc as p
         inner join meta.log_etl_proc_table as lept
                    on p.id = lept.log_etl_proc_id
         left join meta.mt_table as tbl
                   on lept.table_id = tbl.id
where p.id in ({{params.log_etl_proc_id}})
  and lept.table_id in (828, 990)
group by 1, 2, 3, 4, 5, 9;