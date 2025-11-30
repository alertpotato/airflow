import pendulum
from os import path
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from utils.yaml_dag import yaml_reader
from utils import common
from utils.sql_operator import CustomSQLOperator

DMA_CONN_ID = 'nds_dma'
CPP_SCRIPT_PATH = common.get_airflow_home_path() + '/sql/cpp'

builder = yaml_reader.YamlDagBuilder(
    'dataflows/dma/rakurs_surg/dma_etl_rakurs_surg.yaml')

@task
def get_increment_params():
    context = get_current_context()
    sql_op = CustomSQLOperator(
        task_id='get_surg_risk_params',
        conn_id=DMA_CONN_ID,
        sql="get_wf_setting.sql",
        dry_run=False,
        x_params={
            "field_key": "last_update_dttm",
            "wf_key": "surg_risk"
        } 

    )
    result = sql_op.execute(context)
    last_update_dttm = str(result[0][0])
    sql_vars = {
        "surg_risk": last_update_dttm
    }

    common.xcom_save(context, {"sql_vars": sql_vars} ,True)

@task.short_circuit()
def check_last_dagrun_completely_successful(dag_id: str):
    from utils.common import last_dagrun_completely_successful

    return last_dagrun_completely_successful(dag_id, True)


@dag(
    start_date=pendulum.datetime(2023, 9, 5, 4, tz='Europe/Moscow'),
    tags=['ракурс', 'cpp', 'dma_calc', 'dma', 'ЦМП'],
    **builder.get_dag_args(),
)
def dma_rakurs_surg_inc():

    check = check_last_dagrun_completely_successful('dma_etl_cpp_inc')
    builder.create_tasks(
        pre_init_task=check,
        pre_processing_task=get_increment_params()
    )


load_dma_rakurs_surg_inc = dma_rakurs_surg_inc()
load_dma_rakurs_surg_inc.template_searchpath.append(path.join(CPP_SCRIPT_PATH))