import pendulum
from airflow.decorators import dag
from utils.yaml_dag import yaml_reader

builder = yaml_reader.YamlDagBuilder('dataflows/dma/dn_exp/dn_patient_export.yaml')


@dag(
    start_date=pendulum.datetime(2023, 7, 11, 16, 0, 0, tz='Europe/Moscow'),
    tags=['dma_calc', 'patient', 'ЦМП'],
    **builder.get_dag_args(),
)
def dma_calc_dn_patient_export():

    builder.create_tasks()


dma_calc_dn_patient_export()
