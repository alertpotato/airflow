import pendulum
from airflow.decorators import dag
from utils.yaml_dag import yaml_reader

builder = yaml_reader.YamlDagBuilder('dataflows/dma/dma_dds_comparing/dma_dds_comparing.yml')


@dag(
    start_date=pendulum.datetime(2025, 2, 10, 16, 0, 0, tz='Europe/Moscow'),
    tags=['dma', 'УС'],
    **builder.get_dag_args(),
)
def dma_dds_comparing():

    builder.create_tasks()


dma_dds_comparing()
