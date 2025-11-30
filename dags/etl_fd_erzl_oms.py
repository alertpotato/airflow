import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from utils import common
from utils.yaml_dag import yaml_reader
from utils.sql_operator import CustomSQLOperator
from airflow.datasets import Dataset
from airflow.datasets.metadata import Metadata

builder = yaml_reader.YamlDagBuilder('dataflows/FD/fd_erzl_oms/fd_erzl_oms.yaml')
dataset_etl_fd_erzl_oms_success = Dataset("etl_fd_erzl_oms_success")

@task
def init_sql_variables():
    context = get_current_context()
    period_sql = '''
        select param_value
        from dma_pre_oms.w_etl_param etl
        where 1=1
        and fd_name = 'fd_erzl'
        and param_name = 'period'
    '''
    vars_op = CustomSQLOperator(
        task_id='vars_op',
        conn_id=context['params']['DB_CONN_ID'],
        sql=period_sql,
        dry_run=False,
        return_last=False
    )
    result = vars_op.execute(context)
    if not result:
        from airflow.exceptions import AirflowFailException
        raise AirflowFailException("Не удалось получить переменные!")

    period = str(result[0][0])
    sql_vars = {
        'period': period
    }
    common.xcom_save(context, {'sql_vars': sql_vars}, True)

@task(outlets=[dataset_etl_fd_erzl_oms_success])
def update_datasets(ti=None):
    log_etl_proc_id = ti.xcom_pull(task_ids="initialize_all", key="etl_proc_id")

    extra = {
        "log_etl_proc_id": log_etl_proc_id
    }

    common.log_it({
        "for dataset": dataset_etl_fd_erzl_oms_success.uri,
        "set params": extra
    }, ti.log.info)

    yield Metadata(dataset_etl_fd_erzl_oms_success, extra)

@dag(
    start_date=pendulum.datetime(2023, 1, 1),
    tags=['erzl', 'dma', 'УС'],
    **builder.get_dag_args(),
)
def etl_fd_erzl_oms():
    builder.create_tasks(
        pre_processing_task=init_sql_variables(),
        after_all_task=update_datasets()
    )


etl_fd_erzl_oms()
