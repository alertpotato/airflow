import pendulum
from airflow.decorators import dag
from utils.yaml_dag import yaml_reader

builder = yaml_reader.YamlDagBuilder("dataflows/FD/fd_kmp/fd_kmp.yaml")


@dag(
    start_date=pendulum.datetime(2023, 9, 5, 0, 0, 0, tz='Europe/Moscow'),
    tags=['КМП', 'УС'],
    **builder.get_dag_args(),
)
def etl_fd_kmp():

    builder.create_tasks()


etl_fd_kmp()
