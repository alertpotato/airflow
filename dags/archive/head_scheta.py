import pendulum
from os import path
from airflow.models import Variable
from airflow.datasets import Dataset
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils import common, common_dag_params
from utils.logs_meta.log_tables import TableAction as act, log_etl_run
from utils.sql_operator import CustomSQLOperator

AIRFLOW_HOME = common.get_airflow_home_path()

META_SCRIPTS_SQL_PATH = f'{AIRFLOW_HOME}/sql/meta'
CPP_SCRIPTS_SQL_PATH = f'{AIRFLOW_HOME}/sql/cpp'
DMA_SCRIPTS_SQL_PATH = f'{common.get_sql_home_path()}/dataflows/dma'

TMPL_SEARCH_PATH = []
TMPL_SEARCH_PATH.append(path.join(META_SCRIPTS_SQL_PATH))
TMPL_SEARCH_PATH.append(path.join(CPP_SCRIPTS_SQL_PATH))
TMPL_SEARCH_PATH.append(path.join(DMA_SCRIPTS_SQL_PATH))

DB_CONN_ID = 'nds_dds'
DMA_CONN_ID = 'nds_dma'

DAG_DESCRIPTION = '''
Процесс нормализации счетов ОМС
Головной даг:
критерий начала загрузки > получение run_id > запуск других дагов с параметрами

Jira: https://jira.element-lab.ru/browse/KMP-501
'''

dataset_batch_head_scheta_success = Dataset("batch_head_scheta_success")


@task(outlets=[dataset_batch_head_scheta_success])
def update_datasets(ti=None, **context):
    dataset_batch_head_scheta_success.extra = {
        "etl_run_id": ti.xcom_pull(key="etl_run_id"),
        "period_cd": context["params"]["period_cd"],
        "region_cd": context["params"]["region_cd"],
    }

    common.log_it({
        "for dataset": dataset_batch_head_scheta_success.uri,
        "set params": dataset_batch_head_scheta_success.extra
    }, ti.log.info)


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
def get_params_for_scheta(**context):
    ''' Возвращает список параметров для дага счетов, пример:
    ```json
    [{"etl_run_id": 1, "period_cd": "XXX", "region_cd": "XXX"}]
    ```
    '''

    run_id = common.get_params_from_xcom_any_task(
        get_current_context(), ('etl_run_id',)
    )

    ods_schema = context['params']['ods_schema']
    region = context['params']['region_cd']
    pairs_query = '''
    select distinct period_cd::varchar from meta.log_document_load
    inner join {{params.ods_schema}}.zl_list_zglv as z
    on doc_ods_id = z.ods_zl_list_id   
    where region_cd = {{params.region_cd}} and period_cd is not null and stream_status_cd ='C' and etl_status_cd is null
    order by period_cd;
    '''

    query = CustomSQLOperator(
        task_id='pairs_query',
        conn_id=DB_CONN_ID,
        sql=pairs_query,
    )
    query_result = query.execute(context)

    result = [
        {
            "conf": {
                'etl_run_id': run_id['etl_run_id'],
                'ods_schema': ods_schema,
                'dds_schema': context['params']['dds_schema'],
                'region_cd': region,
                'period_cd': str(p[0])
            },
            "trigger_run_id": f"{context['run_id']}_{run_id['etl_run_id']}_{region}_{str(p[0])}"
        }

        for p in query_result
    ]

    return result


@task(trigger_rule="none_failed")
def get_params_for_exp(**context):
    ''' Возвращает параметры для дага экспертиз, пример:
    ```json
    [{"etl_run_id": XXX, "ods_schema": "XXX", "region_cd": "XXX", "temp_suffix": "XXX"}]
    ```
    '''

    run_id = common.get_params_from_xcom_any_task(
        get_current_context(), ('etl_run_id',)
    )

    region = context['params']['region_cd']

    return {
        "etl_run_id": run_id["etl_run_id"],
        "ods_schema": context['params']['ods_schema'],
        'dds_schema': context['params']['dds_schema'],
        "region_cd": region,
        "temp_suffix": f'{region}_tmp',
    }


@task
def set_etl_finish_status():
    context = get_current_context()
    run_id = common.get_params_from_xcom_any_task(
        context, ('etl_run_id',)
    )
    CustomSQLOperator(
        task_id='ldl_etl_set_finish_status',
        conn_id=DB_CONN_ID,
        sql=f"ldl_etl_set_finish_status.sql",
        params={"etl_run_id": run_id["etl_run_id"]}
    ).execute(context)


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


@dag(
    schedule=None,
    start_date=pendulum.datetime(2022, 1, 1),
    template_searchpath=TMPL_SEARCH_PATH,
    catchup=False,
    max_active_runs=1,
    params={
        **common_dag_params.PARAMS_DICT,
    },
    description=DAG_DESCRIPTION.splitlines()[1],
    doc_md=DAG_DESCRIPTION,
    tags=["head"]
)
def head_scheta():
    etl_run_id = init_run_id()
    check_ods() >> etl_run_id

    # @task_group(tooltip=('Запуск дага обновления справочников'
    #                      ' (dds_etl_dictionary)'))
    # def dictionary_dag_tg():
    #     jinja_run_id = (
    #         "{{ ti.xcom_pull("
    #         "task_ids='init_run_id', key='etl_run_id'"
    #         ") }}")
    #     filtered_nsis = get_filtered_nsis()
    #     params_for_dict = add_dag_params({
    #         "etl_run_id": jinja_run_id,
    #         "nsis": filtered_nsis
    #     })

    @task_group(tooltip='Запуск дага загрузки счетов (dds_etl_scheta)', group_id='Загрузка_счетов_ОМС')
    def scheta_dag_tg():
        params_for_scheta = get_params_for_scheta.override(
            task_id='получение_пар_регион_период')()

        TriggerDagRunOperator.partial(
            task_id='Запуск_экземпляров_дага_счетов',
            trigger_dag_id='dds_etl_scheta',
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=5,
            max_active_tis_per_dag=5,
        ).expand_kwargs(params_for_scheta)

    @task_group(tooltip='Запуск дага загрузки экспертиз (dma_exp_sank)', group_id='Загрузка_экспертиз_ОМС')
    def exp_dag_tg():
        params_for_exp = get_params_for_exp.override(
            task_id='получение_регионов')()
        TriggerDagRunOperator(
            task_id='Запуск_экземпляров_дага_экспертиз',
            trigger_dag_id='dma_exp_sank',
            trigger_run_id=f"{{{{ run_id }}}}_{{{{ ti.xcom_pull(task_ids='init_run_id', key='etl_run_id') }}}}_{{{{ params['region_cd'] }}}}",
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=5,
            max_active_tis_per_dag=5,
            conf=params_for_exp
        )

    set_etl_finish_status_task = set_etl_finish_status.override(
        task_id='Финализация_статусов_LDL')()

    @task.short_circuit(trigger_rule='none_failed')
    def has_init():
        """ Проверяем - завершился ли успехом шаг инициализации """
        context = get_current_context()
        return common.is_this_task_has_state(
            context,
            'init_run_id',
            'success'
        )

    etl_run_id >> scheta_dag_tg() >> exp_dag_tg() >> set_etl_finish_status_task >> has_init() >> finish_etl_run() >> update_datasets()


head_scheta()
