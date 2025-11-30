import pendulum
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils import common

ODS_CONN_ID = "nds_dq"
GE_ROOT_DIR = "/home/airflow/ge/great_expectations"
SCHEMA = "ods"

SQL_SCRIPT_PATH = common.get_airflow_home_path() + "/sql/ods_dq"

START_DATE = pendulum.now().add(days=-2)
SCHEDULE = None

default_args = {
    "owner": "airflow",
    "pool": "dq_ods",
    # on_failure_callback=etl_proc_failure_function
}


@task(task_id="prepare_params", multiple_outputs=False)
def prepare_params(dq_rules, dag_run_id, **context):
    import json

    result = {}

    for rule in dq_rules:
        tfoms = int(rule[0])
        period = rule[1]
        rule_id = rule[2]
        rule_cd = rule[3].replace("/", "_").replace(".", "_")
        table_id = rule[4]
        table_name = rule[5]

        check_params = {
            "schema": f"ods_{tfoms}",
            # только для таблицы zl_list колонка с zl_list_id называется id
            "where_condition": f"t.period = '{period}' "
                               f"and t.{'id' if table_id==213 else 'zl_list_id'} in (select distinct zl_list_id from ods_dq_test.ods_dq_batch_{dag_run_id}_tmp)"
        }

        query_check = common.get_rendered_file_content(
            context,
            f"{SQL_SCRIPT_PATH}/{rule_cd}.sql",
            {"params": check_params})

        query_params = {
            "tfoms": tfoms,
            "table_id": table_id,
            "rule_id": rule_id,
            "query_check": query_check,
            "dag_run_id": dag_run_id
        }

        final_sql = common.get_rendered_file_content(
            context,
            f"{SQL_SCRIPT_PATH}/insert_log.sql",
            {"params": query_params})

        # ge_params = {
        #     "run_name": f"{tfoms}_{period}_{table_name}_{rule_cd}_%Y-%m-%dT%H:%M:%S",
        #     "data_asset_name": rule_cd,
        #     "query_to_validate": common_functions.render_file(f"{SQL_SCRIPT_PATH}/{rule_cd}.sql",
        #                                                       {"params": query_params}),
        # }

        if tfoms not in result.keys():
            result[tfoms] = {table_name: []}

        if table_name not in result[tfoms].keys():
            result[tfoms][table_name] = []

        result[tfoms][table_name].append(final_sql)

    return json.dumps({
        "batch_table_name": f"ods_dq_batch_{dag_run_id}_tmp",
        "queries": result
    })


@dag(
    start_date=START_DATE,
    catchup=False,
    schedule=SCHEDULE,
    description='',
    template_searchpath=SQL_SCRIPT_PATH,
    tags=['dq', 'ods'],

)
def ods_dq_prepare_batch():
    create_batch_task = SQLExecuteQueryOperator(
        task_id="create_batch",
        conn_id=ODS_CONN_ID,
        database="nds",
        sql="create_batch.sql"
    )

    get_rules_task = SQLExecuteQueryOperator(
        task_id="get_rules",
        conn_id=ODS_CONN_ID,
        database="nds",
        sql="get_rules.sql"
    )

    # парсим sql, готовим список регионов, список таблиц для проверки
    prepare_params_task = prepare_params(
        get_rules_task.output, "{{ dag_run.id }}")

    trigger_dag_run_batch_task = TriggerDagRunOperator(
        task_id="trigger_dag_run_batch",
        trigger_dag_id="ods_dq_run_batch",
        conf="{{ ti.xcom_pull(task_ids='prepare_params', key='return_value') }}",
    )

    create_batch_task >> get_rules_task >> prepare_params_task >> trigger_dag_run_batch_task


ods_dq_prepare_batch()
