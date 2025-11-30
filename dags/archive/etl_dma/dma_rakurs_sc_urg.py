import pendulum
from airflow.decorators import dag, task
from utils.yaml_dag import yaml_reader


builder = yaml_reader.YamlDagBuilder(
    'dataflows/dma/rakurs_archive/rakurs_sc_urg_arch/dma_etl_rakurs_sc_urg.yaml')


@task.short_circuit()
def check_last_dagrun_completely_successful(dag_id: str):
    from utils.common import last_dagrun_completely_successful

    return last_dagrun_completely_successful(dag_id, True)


@dag(
    start_date=pendulum.datetime(2023, 9, 5, 4, tz='Europe/Moscow'),
    tags=['ракурс', 'cpp', 'dma_calc', 'dma', 'ЦМП'],
    **builder.get_dag_args(),
)
def dma_rakurs_sc_urg():

    check = check_last_dagrun_completely_successful('dma_etl_cpp')
    builder.create_tasks(pre_init_task=check)


dma_rakurs_sc_urg()
