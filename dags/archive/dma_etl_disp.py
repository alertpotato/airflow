import pendulum
from airflow.decorators import dag
from utils.yaml_dag import yaml_reader

builder = yaml_reader.YamlDagBuilder('dataflows/dma/disp/dma_etl_disp.yaml')


@dag(
    start_date=pendulum.datetime(2023, 7, 9),
    tags=['dma_calc', 'dma', 'ЦМП'],
    **builder.get_dag_args(),
)
def dma_etl_disp():
    builder.create_tasks()


dma_etl_disp()
