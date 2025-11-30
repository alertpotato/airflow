import pendulum
from airflow.decorators import dag
from utils.yaml_dag import yaml_reader

builder = yaml_reader.YamlDagBuilder(
    'dataflows/dma/dma_pre_death_info_sl_post_mortal/death_info_sl_post_mortal.yaml')

@dag(
    start_date=pendulum.datetime(2023, 9, 5, 0, 0, 0, tz='Europe/Moscow'),
    tags=['dma_pre', 'death_info', 'ЦМП'],
    **builder.get_dag_args(),
)

def death_info_sl_post_mortal():
    builder.create_tasks()

death_info_sl_post_mortal()
