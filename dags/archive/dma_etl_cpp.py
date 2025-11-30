import pendulum
from airflow.decorators import dag
from utils.yaml_dag import yaml_reader

builder = yaml_reader.YamlDagBuilder('dataflows/dma/cpp/dma_etl_cpp.yaml')


@dag(
    start_date=pendulum.datetime(2023, 7, 11, 16, 0, 0, tz='Europe/Moscow'),
    tags=['cpp', 'dma', 'ЦМП'],
    **builder.get_dag_args(),
)
def dma_etl_cpp():

    builder.create_tasks()


dma_etl_cpp()
