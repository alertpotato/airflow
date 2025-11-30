import pendulum

from airflow.decorators import dag, task
from datetime import datetime
import logging


t_logger = logging.getLogger('airflow.task')


@task()
def print_path():
    import sys
    import os

    t_logger.info("current file path: %s", os.path.dirname(__file__))
    t_logger.info("cwd(): %s", os.getcwd())
    t_logger.info("PATH: \n%s", '\n\t'.join(sys.path))
    path_to_file = "dags/dds/dictionary/pg_params.sql"
    with open(path_to_file, 'r', encoding='utf-8') as file:
        cnt = len(file.readlines())
    t_logger.info("relative test to pg_params.sql: %s lines", cnt)

    return '>>> PATH:\n\t' + '\n\t'.join(sys.path)


@task()
def test_import():
    from utils.dds_variables import scheta_tables
    return ('>>> import successful !\t'
            f'scheta_tables cnt: {len(scheta_tables.keys())}')


@dag(
    "test_paths_dag",
    # default_args=default_args,
    start_date=pendulum.datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
)
def test_paths_dag():

    print_path() >> test_import()


test_paths = test_paths_dag()
