import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from utils import common
from utils.logs_meta.log_tables import TableAction as act, log_etl_run
from utils.logs_meta import shared
import logging
import time
from utils.sql_operator import CustomSQLOperator

REQUEST_ARGS = {'timeout': (10, 60)}  # подключение, ответ
KMP_CONN_ID = 'nds_kmp'
SQL_FILES_DIR = common.get_sql_home_path() + '/dataflows/kmp'

DAG_DESCRIPTION = f'''
Событие "закрытие периода"

[Описание логики](https://wiki.element-lab.ru/pages/viewpage.action?pageId=34283129)

Задача: [KMP-2149](https://jira.element-lab.ru/browse/KMP-2149)
'''

INIT_TASK_ID = 'init_run_id'


@task
def check_period_region():
    context = get_current_context()
    run_id = common.get_params_from_xcom_any_task(
        get_current_context(), ('etl_run_id',)
    )['etl_run_id']
    logging.info('Создание таблицы различий period_region_diff_tmp')
    diff_query = '''
    truncate kmp.period_region_diff_tmp
    ;
    insert into kmp.period_region_diff_tmp
    select ps.id, ps.region_cd, ps.period_cd, ps.status_cd status_cd_new, rps.status_cd status_cd_old, now() status_ch_dttm_new, rps.status_ch_dttm status_ch_dttm_old   from
    (select id, region_cd, period_cd, case when status_id=1 then 'O' when status_id=2 then 'OL' when status_id=3 then 'C' else null end as status_cd  from ods_portal_r.period_status) ps
    left join kmp.region_period_status rps
    on ps.period_cd=rps.period_cd and ps.region_cd =rps.region_cd
    ;
    '''
    query_op = CustomSQLOperator(
        task_id='diff_query',
        conn_id=KMP_CONN_ID,
        sql=diff_query,
    ).execute(context)
    hist_query = f'''
    UPDATE kmp.region_period_status
    SET
        status_cd = diff.status_cd
        ,status_ch_dttm = diff.status_ch_dttm_new
    from (select region_cd,period_cd,status_cd_new status_cd,status_ch_dttm_new from kmp.period_region_diff_tmp where status_cd_new!=status_cd_old and status_cd_new!='C') diff
    where region_period_status.region_cd = diff.region_cd and region_period_status.period_cd = diff.period_cd
    ;
    INSERT INTO kmp.region_period_status_hist
    (id, region_cd, period_cd, from_status_cd, to_status_cd, from_status_ch_dttm, to_status_ch_dttm, kmp_etl_run_id, insert_dttm)
    select nextval('kmp.region_period_seq_tmp'::regclass), diff.region_cd, diff.period_cd, diff.status_cd_old, diff.status_cd_new, diff.status_ch_dttm_new, diff.status_ch_dttm_old, {run_id}, now()
    from (select region_cd,period_cd,status_cd_old,status_cd_new,status_ch_dttm_new,status_ch_dttm_old from kmp.period_region_diff_tmp where status_cd_new!=status_cd_old and status_cd_new!='C') diff
    ;
    '''
    query_op = CustomSQLOperator(
        task_id='hist_query',
        conn_id=KMP_CONN_ID,
        sql=hist_query,
    ).execute(context)
    logging.info('Получение пар закрытых периодов')
    pairs_query = '''
    select dd.region_cd,dd.period_cd from kmp.period_region_diff_tmp dd
    inner join (
    select region_cd,period_cd from 
        (select region_cd,period_cd,status_cd,begin_dttm,RANK () OVER (PARTITION by region_cd,period_cd ORDER BY begin_dttm DESC) rr
    from meta.log_etl_proc) meta where meta.rr=1 and meta.status_cd='C'
    ) meta
    on dd.region_cd = meta.region_cd and dd.period_cd = meta.period_cd 
    where dd.status_cd_new!=dd.status_cd_old and dd.status_cd_new='C';
    '''
    query_op = CustomSQLOperator(
        task_id='pairs_query',
        conn_id=KMP_CONN_ID,
        sql=pairs_query,
    )
    result = query_op.execute(context)
    logging.info(f'Результат: {result}')
    pairs_list = {"pairs": [
        {'etl_run_id': run_id, 'region_cd': str(p[0]), 'period_cd': str(p[1])}
        for p in result
    ]}

    return pairs_list


@task
def close_period_region(pairs_list):
    context = get_current_context()
    for pair in pairs_list["pairs"]:
        region_cd = pair["region_cd"]
        period_cd = pair["period_cd"]
        run_id = pair["etl_run_id"]
        logging.info('Закрытие периодов period_region_diff_tmp')
        close_query = f'''
        UPDATE kmp.region_period_status
        SET
            status_cd = diff.status_cd
            ,status_ch_dttm = diff.status_ch_dttm_new

        from (select region_cd,period_cd,status_cd_new status_cd,status_ch_dttm_new from kmp.period_region_diff_tmp where status_cd_new!=status_cd_old and status_cd_new='C') diff
        where region_period_status.region_cd = diff.region_cd and region_period_status.period_cd = diff.period_cd and region_period_status.region_cd = {region_cd} and region_period_status.period_cd = {period_cd}
        ;
        INSERT INTO kmp.region_period_status_hist
        (id, region_cd, period_cd, from_status_cd, to_status_cd, from_status_ch_dttm, to_status_ch_dttm, kmp_etl_run_id, insert_dttm)
        select nextval('kmp.region_period_seq_tmp'::regclass), diff.region_cd, diff.period_cd, diff.status_cd_old, diff.status_cd_new, diff.status_ch_dttm_new, diff.status_ch_dttm_old, {run_id}, now()
        from (select region_cd,period_cd,status_cd_old,status_cd_new,status_ch_dttm_new,status_ch_dttm_old from kmp.period_region_diff_tmp where status_cd_new!=status_cd_old and status_cd_new='C') diff
        where diff.region_cd = {region_cd} and diff.period_cd = {period_cd}
        ;
        '''
        query_op = CustomSQLOperator(
            task_id='close_query',
            conn_id=KMP_CONN_ID,
            sql=close_query,
        ).execute(context)


@task(task_id=INIT_TASK_ID)
def init_run_id():
    context = get_current_context()
    dag_run = context.get('dag_run')
    run_id = log_etl_run(act.get_new_id, context=context)
    init_vars = {'etl_run_id': run_id}

    common.xcom_save(get_current_context(), init_vars, with_log=True)


@task
def finish_etl_run():
    context = get_current_context()
    dag_run = context.get('dag_run')
    p = common.get_params_from_xcom(context, INIT_TASK_ID, ('etl_run_id',))
    log_etl_run(act.finish, p)


@task.branch(task_id="Проверка_наличия_закрытий")
def branch_func(pairs_list):
    """
    Проверяет, что есть пары период+регион для закрытия
    """
    pairs = pairs_list["pairs"]
    logging.info(f"Получили список: {pairs}")
    if len(pairs) > 0:
        return "Запуск_risk_vzaim_lek"
    else:
        logging.info(f"Список пуст, пропускаем последующие задачи")
        return "finish_etl_run"


@task
def get_params_risk_vzaim_lek(pairs_params):
    kwargs = []

    for pair in pairs_params["pairs"]:
        conf = {
            "clean_temp_tables": True,
            "period_cd": pair["period_cd"],
            "period_dt": pendulum.from_format(pair["period_cd"], "YYYYMM").strftime("%Y-%m-%d"),
        }

        kwargs.append(conf)

    return kwargs


@dag(
    schedule="0/15 * * * *",
    start_date=pendulum.local(2023, 12, 1),
    catchup=False,
    tags=['КМП', 'УС', 'СЛН'],
    description=DAG_DESCRIPTION.splitlines()[1],
    params={'pairs': [
        {'period_cd': '200001', 'region_cd': '999'}, {'period_cd': '200002', 'region_cd': '999'}]},
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
def head_period_region_close():
    pairs_params = check_period_region.override(
        task_id='Получение_закрытых_периодов')()

    risk_vzaim_lek_params = get_params_risk_vzaim_lek(pairs_params)

    check_if_close_task = branch_func.override(task_id='Проверка_наличия_закрытий')(pairs_params)

    trigger_sln = TriggerDagRunOperator.partial(
        task_id='Запуск_risk_vzaim_lek',
        trigger_dag_id='risk_vzaim_lek',
        wait_for_completion=True,
        poke_interval=5,
        max_active_tis_per_dag=1
    ).expand(conf=risk_vzaim_lek_params)

    trigger_kmp = TriggerDagRunOperator(
        task_id='Запуск_head_etl_kmp',
        trigger_dag_id='head_etl_kmp',
        wait_for_completion=True,
        poke_interval=5,
        max_active_tis_per_dag=1,
        conf=pairs_params,
    )
    close_period_task = close_period_region.override(task_id='Закрытие_периодов')(pairs_params)
    finish = finish_etl_run.override(trigger_rule=TriggerRule.NONE_FAILED)()
    init = init_run_id()

    trigger_sln.set_downstream(trigger_kmp)
    trigger_kmp.set_downstream(close_period_task)

    init >> pairs_params >> risk_vzaim_lek_params >> check_if_close_task >> trigger_sln >> trigger_kmp >> close_period_task >> finish


head_period_region_close()
