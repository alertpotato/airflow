import pendulum
from airflow.decorators import dag, task
from utils.yaml_dag import yaml_reader

builder = yaml_reader.YamlDagBuilder(
    'dataflows/dma/plan_dispans/dma_etl_plan_dispans.yaml')


@task.short_circuit()
def check_last_dagrun_completely_successful(dag_id: str):
    from utils.common import last_dagrun_completely_successful

    return last_dagrun_completely_successful(dag_id, True)


@dag(
    start_date=pendulum.local(2022, 1, 1),
    tags=['ракурс', 'cpp', 'dma_calc', 'dma', 'ЦМП'],
    **builder.get_dag_args(),
)
def dma_rakurs_plan_disp():
    check = check_last_dagrun_completely_successful('dma_etl_cpp')
    builder.create_tasks(
        pre_init_task=check,
    )


dma_rakurs_plan_disp()
