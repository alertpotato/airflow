import pendulum
from airflow.decorators import dag
from utils.yaml_dag import yaml_reader

builder = yaml_reader.YamlDagBuilder('dataflows/dma/dn_mp/dn_patient.yaml')


@dag(
    start_date=pendulum.datetime(2023, 7, 11, 16, 0, 0, tz='Europe/Moscow'),
    tags=['patient', 'dma', 'ЦМП'],
    **builder.get_dag_args(),
)
def dma_dn_patient():

    builder.create_tasks()


dma_dn_patient()
