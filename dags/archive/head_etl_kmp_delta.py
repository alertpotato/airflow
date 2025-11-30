import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils import common
from utils.logs_meta.log_tables import TableAction as act, log_etl_run
from utils.logs_meta import shared
import logging
import time
from utils.sql_operator import CustomSQLOperator

DB_CONN_ID = 'nds_kmp'
SQL_FILES_DIR = common.get_sql_home_path() + '/dataflows/kmp'

DAG_DESCRIPTION = f"""
# Обновление дельты данных витрины KMP

### Назначение
Даг предназначен выявления записей о МП, которые по результатам выполнения методов API или через создание нового задания в Приложении КМП были добавлены в таблицу kmp.kmp_resullt_cases_dds в качестве ориентиров. Данные случаи МП также необходимо учесть в расчетах при наполнении витрин kmp* в схеме dma.

### Таблицы в БД, от которых зависит поток данных
- kmp.kmp_jobs
- kmp.kmp_job_rules
- kmp.kmp_job_periods
- kmp.kmp_job_regions
- dma.kmp_result_job

### Таблицы в БД, на которые влияет поток данных:
- dma.kmp_result_job
- dma.kmp_dashbord_gl_period
- dma.kmp_dashbord_gl_founder
- dma.kmp_dashbord_st_rule
- dma.kmp_indicator_mo_profil

### Логика

- Скрипт возвращает множество наборов параметров region_cd,period_cd и rules - [/dataflows/kmp/head_etl_kmp_delta_logic.sql](https://git.element-lab.ru/oms-management-services/etl/sql/-/tree/master/dataflows/kmp/head_etl_kmp_delta_logic.sql)
- По этим наборам запускается etl_fd_kmp

Ссылка на описание процесса/целевой таблицы в вики:
- [раздел 7](https://wiki.element-lab.ru/pages/viewpage.action?pageId=55773915)

Задача: 
- [KMP-2335](https://jira.element-lab.ru/browse/KMP-2335)
"""

INIT_TASK_ID = 'init_run_id'


@task
def get_delta_rules():
    context = get_current_context()
    run_id = common.get_params_from_xcom(
        context, INIT_TASK_ID, ('etl_run_id',))['etl_run_id']

    query_op = CustomSQLOperator(
        task_id='rules_query',
        conn_id=DB_CONN_ID,
        sql="head_etl_kmp_delta_logic.sql",
    )
    result = query_op.execute(context)
    logging.info(f'Результат: {result}')
    rules_list = [
        {'etl_run_id': run_id,'region_cd':  str(p[0]), 'period_cd': str(p[1]), 'rules': str(p[2])}
        for p in result
    ]
    return rules_list


@task(task_id=INIT_TASK_ID)
def init_run_id():
    context = get_current_context()
    dag_run = context.get('dag_run')
    if dag_run and dag_run.external_trigger:
        print("Это ручной запуск, пропускаем инициализацию, `etl_run_id`: 0")
        init_vars = {'etl_run_id': 0}
    else:
        run_id = log_etl_run(act.get_new_id, context=context)
        init_vars = {'etl_run_id': run_id}

    common.xcom_save(get_current_context(), init_vars, with_log=True)


@task
def finish_etl_run():
    context = get_current_context()
    dag_run = context.get('dag_run')
    if dag_run and dag_run.external_trigger:
        print("Это ручной запуск, пропускаем финализацию, `etl_run_id`: 0")
    else:
        p = common.get_params_from_xcom(context, INIT_TASK_ID, ('etl_run_id',))
        log_etl_run(act.finish, p)


@dag(
    start_date=pendulum.local(2023, 12, 26),
    schedule = '0 23 * * *',
    catchup=False,
    tags=['КМП', 'УС'],
    description=DAG_DESCRIPTION.splitlines()[1],
    #params={'clean_temp_tables': False, 'pairs': [
    #    {'period_cd': '200001', 'region_cd': '999'}, {'period_cd': '200002', 'region_cd': '999'}]},
    doc_md=DAG_DESCRIPTION,
    template_searchpath=SQL_FILES_DIR,
    default_args={
        'on_failure_callback': shared.get_on_failure_function(
            tables_to_finish=('log_etl_run', 'log_etl_proc',
                              'log_etl_proc_table'),
            xcom_keys=('etl_run_id', 'etl_proc_id', 'all_tbl_ids'),
        )
    }
)
def head_etl_kmp_delta():

    rules_params = get_delta_rules.override(
        task_id='Дельта_правил')()

    trigger_etl_fd_kmp = TriggerDagRunOperator.partial(
        task_id='Запуск_etl_fd_kmp',
        trigger_dag_id='etl_fd_kmp',
        wait_for_completion=True,
        poke_interval=5,
        max_active_tis_per_dag=5,
    ).expand(conf=rules_params)

    init_run_id() >> rules_params >> trigger_etl_fd_kmp >> finish_etl_run()


head_etl_kmp_delta()
