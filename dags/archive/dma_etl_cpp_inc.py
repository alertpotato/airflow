import pendulum
from os import path
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from utils.yaml_dag import yaml_reader
from utils import common
from utils.sql_operator import CustomSQLOperator

DMA_CONN_ID = 'nds_dma'

builder = yaml_reader.YamlDagBuilder('dataflows/dma/cpp2/dma_etl_cpp.yaml')

@task
def get_increment_params():
    context = get_current_context()
    sql_script = """
        select 
            wf.wf_setting
        from meta.rakurs_wf_settings wf
        where wf.wf_key = 'cpp_risk';
    """
    sql_op = CustomSQLOperator(
        task_id='get_cpp_risk_params',
        conn_id=DMA_CONN_ID,
        sql=sql_script,
        )
    result = sql_op.execute(context)
    
    result_json = result[0][0]
    
    common.xcom_save(context, {"sql_vars": result_json} , True)



@dag(
    start_date=pendulum.datetime(2023, 7, 11, 16, 0, 0, tz='Europe/Moscow'),
    tags=['cpp', 'dma', 'ЦМП'],
    **builder.get_dag_args(),
)
def dma_etl_cpp_inc():

    builder.create_tasks(
        pre_processing_task=get_increment_params()
    )


run_dma_etl_cpp_inc =  dma_etl_cpp_inc()