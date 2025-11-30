import pendulum
from airflow.models import DAG, Variable
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from utils.dds_variables import ods_tables
from utils import common

ODS_CONN_ID = "nds_dq"
GE_ROOT_DIR = "/home/airflow/ge/great_expectations"
SCHEMA = "ods"

ODS_TABLES = Variable.get("ods_tables", {}, deserialize_json=True)
SQL_SCRIPT_PATH = common.get_airflow_home_path() + "/sql/ods_dq"

START_DATE = pendulum.now().add(days=-2)
SCHEDULE = None

default_args = {
    "owner": "airflow",
    "pool": "dq_ods",
    # "on_failure_callback": etl_proc_failure_function
}


@task
def get_report_params(dag_run=None):
    return [{"schema_name": f"ods_{x}", "batch_table_name": dag_run.conf.get('batch_table_name'), "tf": int(x)} for x in
            dag_run.conf.get("queries", {}).keys()]


@task
def get_rule_sql(tfoms, table_name, dag_run=None):
    return dag_run.conf.get("queries", {}).get(tfoms, {}).get(table_name, [])


with DAG(
        dag_id="ods_dq_run_batch",
        start_date=START_DATE,
        catchup=False,
        schedule=SCHEDULE,
        description='',
        template_searchpath=SQL_SCRIPT_PATH,
        params={},
        tags=['dq', 'ods']

) as dag:
    clear_log_task = SQLExecuteQueryOperator(
        task_id="clear_log",
        conn_id=ODS_CONN_ID,
        database="nds",
        sql="clear_log.sql",
        parameters={
            "batch_table_name": "{{ dag_run.conf.get('batch_table_name') }}"
        }
    )

    check_dq_group = TaskGroup("check_dq")

    for tfoms in range(1, 87):
        tfoms = f"{tfoms:02d}"

        with TaskGroup(tfoms, parent_group=check_dq_group) as tfoms_group:
            for table_name, _ in ODS_TABLES.items():
                check_rules_task = SQLExecuteQueryOperator.partial(
                    task_id=table_name,
                    conn_id=ODS_CONN_ID,
                    database="nds",
                ).expand(sql=get_rule_sql.override(task_id=f"get_sql_{tfoms}_{table_name}")(tfoms, table_name))

    set_error_status_task = PostgresOperator(
        task_id="set_error_status",
        postgres_conn_id=ODS_CONN_ID,
        trigger_rule="none_failed",
        sql="set_error_status.sql",
        parameters={
            "batch_table_name": "{{ dag_run.conf.get('batch_table_name') }}"
        }

    )

    set_successful_status_task = PostgresOperator(
        task_id="set_successful_status",
        postgres_conn_id=ODS_CONN_ID,
        trigger_rule="none_failed",
        sql="set_successful_status.sql",
        parameters={
            "batch_table_name": "{{ dag_run.conf.get('batch_table_name') }}"
        }

    )

    with TaskGroup("generate_report") as generate_report_group:

        generate_reports_task = SQLExecuteQueryOperator.partial(
            task_id=f"generate_reports",
            conn_id=ODS_CONN_ID,
            database="nds",
            do_xcom_push=False,
            sql=f"generate_report.sql"
        ).expand(parameters=get_report_params())

    cleaning_task = PostgresOperator(
        task_id="cleaning",
        postgres_conn_id=ODS_CONN_ID,
        trigger_rule="none_failed",
        sql="cleaning.sql",
        parameters={
            "batch_table_name": "{{ dag_run.conf.get('batch_table_name') }}"
        }

    )

    clear_log_task >> check_dq_group >> set_error_status_task >> set_successful_status_task >> generate_report_group >> cleaning_task
