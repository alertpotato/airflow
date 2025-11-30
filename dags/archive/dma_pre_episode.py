import pendulum
from airflow.decorators import dag
from utils.yaml_dag import yaml_reader

builder = yaml_reader.YamlDagBuilder(
    'dataflows/dma/dma_pre_episode/dma_etl_episodes.yaml')


@dag(
    start_date=pendulum.datetime(2023, 9, 5, 0, 0, 0, tz='Europe/Moscow'),
    tags=['dma_pre', 'dma_calc', 'dma', 'episode', 'ЦМП'],
    **builder.get_dag_args(),
)
def dma_pre_episode():
    builder.create_tasks()


dma_pre_episode()
