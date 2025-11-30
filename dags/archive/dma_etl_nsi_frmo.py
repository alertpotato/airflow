import pendulum
from airflow.decorators import dag
from utils.yaml_dag import yaml_reader

builder = yaml_reader.YamlDagBuilder('dataflows/nsi_dds/nsi_frmo/nsi_frmo.yaml')

@dag(
    start_date=pendulum.datetime(2023, 9, 1),
    tags=['frmo', 'nsi','ЦМП'],
    **builder.get_dag_args(),
)
def dma_etl_nsi_frmo():
    builder.create_tasks()

dma_etl_nsi_frmo()