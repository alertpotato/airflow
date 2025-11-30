import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from utils import common
from utils.yaml_dag import yaml_reader
from utils.sql_operator import CustomSQLOperator
from airflow.datasets import Dataset
from airflow.datasets.metadata import Metadata

builder = yaml_reader.YamlDagBuilder('dataflows/FD/fd_ms_oms/fd_ms_oms.yaml')
dataset_etl_fd_ms_oms_success = Dataset("etl_fd_ms_oms_success")


@task
def init_sql_variables():
    cur_date_dds = '''
    select param_value
    from dma_pre_oms.w_etl_param
    where param_name = 'cur_date_dds'
    and fd_name = 'fd_ms_oms'
    '''

    cur_date_dma = '''
    select param_value
    from dma_pre_oms.w_etl_param
    where param_name = 'cur_date_dma'
    and fd_name = 'fd_ms_oms';
    '''

    context = get_current_context()
    vars_op = CustomSQLOperator(
        task_id='vars_op',
        conn_id=context['params']['DB_CONN_ID'],
        sql=[cur_date_dds, cur_date_dma],
        dry_run=False,
        return_last=False
    )
    results = vars_op.execute(context)
    if not results:
        from airflow.exceptions import AirflowFailException
        raise AirflowFailException("Не удалось получить переменные!")

    cur_date_dds, cur_date_dma = (x[0][0] for x in results)
    sql_vars = {
        'cur_date_dds': cur_date_dds,
        'cur_date_dma': cur_date_dma
    }
    common.xcom_save(context, {'sql_vars': sql_vars}, True)


@task(outlets=[dataset_etl_fd_ms_oms_success])
def update_datasets(ti=None):
    log_etl_proc_id = ti.xcom_pull(task_ids="initialize_all", key="etl_proc_id")

    extra = {
        "log_etl_proc_id": log_etl_proc_id
    }

    common.log_it({
        "for dataset": dataset_etl_fd_ms_oms_success.uri,
        "set params": extra
    }, ti.log.info)

    yield Metadata(dataset_etl_fd_ms_oms_success, extra)


@dag(
    start_date=pendulum.datetime(2023, 1, 1),
    tags=['УС', 'dma', 'dma_calc', 'dma_pre'],
    **builder.get_dag_args(),
)
def etl_fd_ms_oms():
    builder.create_tasks(
        pre_processing_task=init_sql_variables(),
        after_all_task=update_datasets()
    )


etl_fd_ms_oms()
