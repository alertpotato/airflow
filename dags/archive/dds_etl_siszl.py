import pendulum
from airflow.decorators import dag
from utils.yaml_dag import yaml_reader

builder = yaml_reader.YamlDagBuilder('dataflows/dds_etl_siszl/dds_etl_siszl.yaml')

@dag(
    start_date=pendulum.datetime(2023, 9, 5, 0, 0, 0, tz='Europe/Moscow'),
    tags=['dma', 'siszl', 'СИСЗЛ', 'ЦМП'],
    **builder.get_dag_args()
)

def dds_etl_siszl():
    builder.create_tasks()

dds_etl_siszl()
