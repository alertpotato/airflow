import pendulum
from airflow.models import Variable
from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils import common, common_dag_params
from utils.logs_meta.log_tables import TableAction as act, log_etl_run


DB_CONN_ID = 'nds_dds'

DAG_DESCRIPTION = '''
Головной даг:
критерий начала загрузки > получение run_id > запуск других дагов с параметрами

Jira: https://jira.element-lab.ru/browse/KMP-501
'''


@task.short_circuit
def check_ods():
    """
    Проверяет - загружены ли данные в ODS и
    прерывает выполнение, если нет.
    """

    return True


@task
def init_run_id():
    p = {'etl_run_id': log_etl_run(act.get_new_id, conn_id=DB_CONN_ID)}
    # p = {'etl_run_id': -1}  # NOTE: на время тестов
    common.xcom_save(get_current_context(), p, with_log=True)


@task
def finish_etl_run():
    run_id = get_current_context()['ti'].xcom_pull(key='etl_run_id')
    log_etl_run(act.finish, {'etl_run_id': run_id}, conn_id=DB_CONN_ID)


@task
def get_period_region_pairs():
    ''' Возвращает список параметров для дага счетов, пример:
    ```json
    [{"etl_run_id": 1, "period": "XXX", "region": "XXX"}]
    ```
    '''

    run_id = common.get_params_from_xcom_any_task(
        get_current_context(), ('etl_run_id',)
    )
    # pairs_query = '''
    # SELECT
    #     to_char(min("period"), 'YYYYMM') AS "period",
    #     tf AS "region"
    # FROM dq.log_list_oms
    # WHERE chk_step=10
    #     AND chk_rslt=1
    #     AND state=4
    #     AND tf <> '0'
    # GROUP BY tf
    # '''
    pairs_query = '''
    SELECT 202201 as "period", 12 as "region"
    '''
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(pairs_query)
            pairs_list = [
                {
                    **get_current_context()['params'],
                    **run_id, 'period': str(p), 'region': str(r)}
                for p, r in cur.fetchall()]

    return pairs_list


@task
def get_filtered_nsis():
    ''' Возвращет отфильтрованный список справочников '''

    from utils.dds_variables import nsi_dicts_all
    # решили пока возвщрать все
    return nsi_dicts_all

    # критерии выборки справочников
    # test_filter = ['nsi_n016', 'nsi_n017', 'nsi_n018', 'nsi_n019', 'nsi_n020']

    # return {
    #    group_name: {
    #        nsi_name: nsi_params
    #        for nsi_name, nsi_params in group_value.items()
    #        if nsi_name in test_filter
    #    }
    #    for group_name, group_value in nsi_dicts_all.items()
    # }


@task
def set_etl_dict_keys(filtered_nsis):
    Variable.set("dds_etl_dictionary_keys", list(filtered_nsis.keys()),
                 serialize_json=True)


@task
def add_dag_params(input_dict: dict):
    return {**get_current_context()['params'], **input_dict}

@task
def kmp_rules():
    # Временная заглушка
    print(1)

@task
def update_dictionaries():
    # Временная заглушка
    print(2)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2022, 1, 1),
    catchup=False,
    # tags=['dev'],
    params={
        **common_dag_params.PARAMS_DICT,
        # "dq_schema": "test_dq",
    },
    description=DAG_DESCRIPTION.split('\n\n')[0],
    doc_md=DAG_DESCRIPTION
)
def head_ods_etl():
    with TaskGroup("Инициализация_потока", tooltip='Инициализация потока') as init_group:
        etl_run_id = init_run_id()
        check_ods() >> etl_run_id

    @task_group(tooltip=('Запуск дага обновления справочников'
                         ' (dds_etl_dictionary)'))
    def dictionary_dag_tg():
        jinja_run_id = (
            "{{ ti.xcom_pull("
            "task_ids='init_run_id', key='etl_run_id'"
            ") }}")
        filtered_nsis = get_filtered_nsis()
        params_for_dict = add_dag_params({
            "etl_run_id": jinja_run_id,
            "nsis": filtered_nsis
        })
    with TaskGroup("Загрузка_счетов", tooltip='Запуск дага загрузки счетов (dds_etl_scheta)') as sheta_group:
        @task
        def scheta_dag_tg():
            params_for_scheta = get_period_region_pairs()
            TriggerDagRunOperator.partial(
                task_id='cheta_batch',
                # trigger_dag_id='test_params_dag',
                trigger_dag_id='dds_etl_scheta_dq_test',
                wait_for_completion=True,
                poke_interval=5,
                max_active_tis_per_dag=1,
            ).expand(conf=params_for_scheta)
        scheta_dag_tg_task = scheta_dag_tg.override(task_id="Загрузка_счетов")()
        scheta_dag_tg_task

    @task.short_circuit(trigger_rule='none_failed')
    def has_init():
        """ Проверяем - завершился ли успехом шаг инициализации """
        context = get_current_context()
        return common.is_this_task_has_state(
            context,
            'init_run_id',
            'success'
        )
    with TaskGroup("Завершение_потока", tooltip='финализация логов') as fin_group:
        has_init() >> finish_etl_run()

    kmp_rules_task=kmp_rules.override(task_id="Выполнение_правил_КМП")()
    update_dictionaries_task=update_dictionaries.override(task_id="Обновление_справочников")()

    init_group >> update_dictionaries_task >> sheta_group >> kmp_rules_task >> fin_group


head_ods_etl()
