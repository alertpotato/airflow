import pendulum
from airflow.decorators import dag
from utils.yaml_dag import yaml_reader

builder = yaml_reader.YamlDagBuilder(
    'dataflows/dev/monitoring/etl_monitoring.yaml')


@dag(
    start_date=pendulum.datetime(2023, 7, 11, 16, 0, 0, tz='Europe/Moscow'),
    tags=['dev', 'sys', 'ЦМП'],
    **builder.get_dag_args(),
)
def dev_monitoring():

    builder.create_tasks()


dev_monitoring()
