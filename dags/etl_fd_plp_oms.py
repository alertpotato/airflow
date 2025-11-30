import pendulum
from airflow.decorators import dag
from utils.yaml_dag import yaml_reader

builder = yaml_reader.YamlDagBuilder('dataflows/FD/fd_plp_oms/fd_plp_oms.yaml')


@dag(
    start_date=pendulum.datetime(2023, 7, 11, 16, 0, 0, tz='Europe/Moscow'),
    tags=['patient', 'dma', 'УС'],
    **builder.get_dag_args(),
)
def etl_fd_plp_oms():

    builder.create_tasks()


etl_fd_plp_oms()
