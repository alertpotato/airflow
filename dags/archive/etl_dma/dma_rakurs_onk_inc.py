import pendulum
from airflow.decorators import dag, task
from utils.yaml_dag import yaml_reader


builder = yaml_reader.YamlDagBuilder(
    'dataflows/dma/rakurs_onk_incr/dma_etl_onkolog_incr.yaml')


@task.short_circuit()
def check_last_dagrun_completely_successful(dag_id: str):
    import logging
    from utils.common import last_dagrun_completely_successful
    logger = logging.getLogger('airflow.task')
    return last_dagrun_completely_successful(dag_id, logger)


@dag(
    start_date=pendulum.datetime(2023, 9, 5, 4, tz='Europe/Moscow'),
    tags=['ракурс', 'cpp', 'dma_calc', 'dma', 'ЦМП'],
    **builder.get_dag_args(),
)
def dma_rakurs_onk_inc():

    check = check_last_dagrun_completely_successful('dma_etl_cpp_inc')
    builder.create_tasks(pre_init_task=check)


dma_rakurs_onk_inc()
