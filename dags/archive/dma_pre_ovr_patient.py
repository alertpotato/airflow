import pendulum
from airflow.decorators import dag
from utils.yaml_dag import yaml_reader

builder = yaml_reader.YamlDagBuilder(
    'dataflows/dma/dma_pre_ovr_patient/dma_pre_ovr_patient.yaml')


@dag(
    start_date=pendulum.datetime(2023, 8, 16, 0, 0, 0, tz='Europe/Moscow'),
    tags=['patient', 'dma_pre', 'dma_calc', 'dma', 'ЦМП'],
    **builder.get_dag_args(),
)
def dma_pre_ovr_patient():
    builder.create_tasks()


dma_pre_ovr_patient()
