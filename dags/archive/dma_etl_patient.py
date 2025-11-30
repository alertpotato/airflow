import pendulum
from airflow.decorators import dag
from utils.yaml_dag import yaml_reader

builder = yaml_reader.YamlDagBuilder('dataflows/dma/patient/patient.yaml')


@dag(
    start_date=pendulum.datetime(2023, 8, 1, 0, 0, 0, tz='Europe/Moscow'),
    tags=['patient', 'dma', 'ЦМП'],
    **builder.get_dag_args(),
)
def dma_etl_patient():
    builder.create_tasks()


dma_etl_patient()
