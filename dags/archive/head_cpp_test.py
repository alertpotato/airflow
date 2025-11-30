import pendulum
from airflow.models import Variable
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils import common, common_dag_params
from utils.logs_meta.log_tables import TableAction as act, log_etl_run
from utils.sql_operator import CustomSQLOperator

DB_CONN_ID = 'nds_dma'
DQ_SCRIPTS_SQL_PATH = f'{common.get_airflow_home_path()}/sql/cpp'
DAG_DESCRIPTION = '''
ТЕСТ ЦПП
'''


@task
def get_params():
    import logging
    context = get_current_context()
    ids = context['params']['cpp_test_params']
    logging.info(f'ids : {ids}')
    etl_proc_ids = ''
    for id in ids:
        logging.info(id["scheta_proc_id"])
        if etl_proc_ids == '':
            etl_proc_ids = f'{id["scheta_proc_id"]}'
        else:
            etl_proc_ids = f'{etl_proc_ids},{id["scheta_proc_id"]}'
    logging.info(f'scheta_proc_ids : {etl_proc_ids}')
    cpp_params = {"cnt_scheta_proc_ids": len(
        ids), "scheta_proc_ids": etl_proc_ids}
    logging.info(f'Параметры ЦПП логики: {cpp_params}')

    CustomSQLOperator(
        task_id="cpp_logic",
        conn_id=DB_CONN_ID,
        sql="cpp_logic.sql",
        log_sql=True,
        params=cpp_params,
    ).execute(context)

    return ids


@dag(
    schedule=None,
    start_date=pendulum.datetime(2022, 1, 1),
    catchup=False,
    tags=['dev', 'cpp'],
    params={
        **common_dag_params.PARAMS_DICT,
        "cpp_test_params": [
            {"period_cd": "200001", "region_cd": "00", "scheta_proc_id": 1},
            {"period_cd": "200001", "region_cd": "00", "scheta_proc_id": 2}
        ],
        "dry_run": True,
    },
    description=DAG_DESCRIPTION.split('\n\n')[0],
    doc_md=DAG_DESCRIPTION,
    template_searchpath=DQ_SCRIPTS_SQL_PATH,
)
def head_cpp_test():

    @task_group(tooltip='Запуск дага ЦПП')
    def cpp_dag_trigger():
        params_for_cpp = get_params()
        TriggerDagRunOperator.partial(
            task_id='cpp_batch',
            trigger_dag_id='dma_etl_cpp_test_inc',
            wait_for_completion=True,
            poke_interval=5,
            max_active_tis_per_dag=5,
        ).expand(conf=params_for_cpp)

    cpp_dag_trigger()


head_cpp_test()
