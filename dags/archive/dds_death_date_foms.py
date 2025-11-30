import pendulum
from airflow.decorators import dag
from utils.yaml_dag import yaml_reader

builder = yaml_reader.YamlDagBuilder(
    'dataflows/dds_death_date_foms/death_date_foms.yaml')

@dag(
    start_date=pendulum.datetime(2023, 9, 5, 0, 0, 0, tz='Europe/Moscow'),
    tags=['dds', 'death_info', 'ЦМП'],
    **builder.get_dag_args(),
)

def dds_death_date_foms():
    builder.create_tasks()

dds_death_date_foms()
