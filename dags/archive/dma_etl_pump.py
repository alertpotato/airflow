import pendulum
from airflow.decorators import dag
from utils.yaml_dag import yaml_reader

builder = yaml_reader.YamlDagBuilder('dataflows/dma/pump/dma_etl_pump.yaml')

@dag(
    start_date=pendulum.datetime(2024, 4, 9, 6, 0, 0, tz="Europe/Moscow"),
    tags=['pump', 'dma', 'ЦМП'],
    **builder.get_dag_args(),
)
def dma_etl_pump():
    
    builder.create_tasks()

dma_etl_pump()