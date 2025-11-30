import pendulum
from os import path
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from utils import common
from utils.yaml_dag import yaml_reader
from utils.sql_operator import CustomSQLOperator

DMA_CONN_ID = 'nds_dma'

CPP_SCRIPT_PATH = common.get_airflow_home_path() + '/sql/cpp'

builder = yaml_reader.YamlDagBuilder(
    'dataflows/dma/rakurs_card/dma_etl_rakurs_card.yaml')

@task
def get_card_incr_params():
    context = get_current_context()  

    sql = """
        select 
            wf.wf_setting::json->>'last_update_dttm'
        from meta.rakurs_wf_settings wf
        where wf.wf_key = 'card_risk';
    """
    sql_op = CustomSQLOperator(
        task_id='get_card_risk_params',
        conn_id=DMA_CONN_ID,
        sql="get_wf_setting.sql",
        #sql=sql,
        dry_run=False,
        x_params={
            "field_key": "last_update_dttm",
            "wf_key": "card_risk" 
        }

    )
    result = sql_op.execute(context)
    last_update_dttm = str(result[0][0])
    sql_vars = {
        'card_risk': last_update_dttm
    }

    common.xcom_save(context, {"sql_vars": sql_vars}, True)

@task.short_circuit()
def check_last_dagrun_completely_successful(dag_id: str):
    from utils.common import last_dagrun_completely_successful

    return last_dagrun_completely_successful(dag_id, True)


@dag(
    start_date=pendulum.datetime(2023, 9, 5, 4, tz='Europe/Moscow'),
    tags=['ракурс', 'cpp', 'dma_calc', 'dma', 'ЦМП'],
    **builder.get_dag_args(),
)
def dma_rakurs_card_inc():

    check = check_last_dagrun_completely_successful('dma_etl_cpp_inc')
    builder.create_tasks(
        pre_init_task=check,
        pre_processing_task=get_card_incr_params()
    )


dag_rakurs_card = dma_rakurs_card_inc()
dag_rakurs_card.template_searchpath.append(path.join(CPP_SCRIPT_PATH))