import pendulum
from os import path
from airflow.decorators import dag
from utils import common
from utils.yaml_dag import yaml_reader
from utils.sql_operator import CustomSQLOperator

builder = yaml_reader.YamlDagBuilder(
    'dataflows/dds_etl_expertise/exp_sank.yaml')

DB_CONN_ID = 'nds_dds'

AIRFLOW_HOME = common.get_airflow_home_path()

META_SCRIPTS_SQL_PATH = f'{AIRFLOW_HOME}/sql/meta'

DQ_FFOMS_SQL_PATH = f'{AIRFLOW_HOME}/sql/dq/ffoms'
DQ_RULES_SQL_PATH = common.get_dq_home_path() + '/sql'


@dag(
    start_date=pendulum.datetime(2023, 7, 11, 16, 0, 0, tz='Europe/Moscow'),
    tags=['exp', 'dds', 'dma', 'ЦМП'],
    # template_searchpath=TMPL_SEARCH_PATH,
    **builder.get_dag_args(),
)
def dma_exp_sank():
    set_etl_running_status_task = CustomSQLOperator(
        task_id="set_etl_running_status",
        conn_id=DB_CONN_ID,
        sql=f"ldl_etl_set_running_status_exp.sql",
        split_statements=True
    )

    builder.create_tasks(pre_processing_task=set_etl_running_status_task)


dag = dma_exp_sank()
dag.template_searchpath.append(path.join(DQ_RULES_SQL_PATH))
dag.template_searchpath.append(path.join(META_SCRIPTS_SQL_PATH))
dag.template_searchpath.append(path.join(DQ_FFOMS_SQL_PATH))


