import pendulum
from airflow.decorators import dag
from utils.yaml_dag import yaml_reader

builder = yaml_reader.YamlDagBuilder('dataflows/meta/cmp_change_owner/dwh_change_owner.yaml')


@dag(
    start_date=pendulum.datetime(2023, 7, 11, 16, 0, 0, tz='Europe/Moscow'),
    tags=['meta', 'ЦМП'],
    **builder.get_dag_args(),
)
def meta_table_cmp_owner_changer():

    builder.create_tasks()


meta_table_cmp_owner_changer()
