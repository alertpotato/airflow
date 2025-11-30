import pendulum
import logging
from os import path
from airflow.decorators import dag, task
from plugins.operators.tmp_table_operator import CreateTmpTable, DropTmpTable
from utils.sql_operator import CustomSQLOperator

from utils import common

DDS_CONN_ID = 'nds_dds'
DQ_CONN_ID = 'nds_dq'

META_SCHEMA = "meta"
STAGE_SCHEMA = "dq_calc"
DQ_SCHEMA = "dq"
DDS_SCHEMA = "dds"

AIRFLOW_HOME = common.get_airflow_home_path()

META_SCRIPTS_SQL_PATH = f"{AIRFLOW_HOME}/sql/meta/profiling"
DQ_RULES_SQL_PATH = common.get_dq_home_path() + '/sql/PROFILING'

TMPL_SEARCH_PATH = []
TMPL_SEARCH_PATH.append(path.join(META_SCRIPTS_SQL_PATH))
TMPL_SEARCH_PATH.append(path.join(DQ_RULES_SQL_PATH))


def log_error(context):
    log_error_task = CustomSQLOperator(
        task_id="log_error",
        conn_id=DQ_CONN_ID,
        sql="lpr_set_error_status.sql",
        params={"dq_schema": DQ_SCHEMA}
    )

    log_error_task.execute(context)


default_args = {
    "owner": "airflow",
    "on_failure_callback": log_error
}


def find_dq_sql_file(find_path, rule_id):
    import glob
    import os

    logging.info(f"Ищем файл правил: {find_path}/{rule_id}_*.sql")

    os.chdir(f"{find_path}/")
    for file in glob.glob(f"{rule_id}_*.sql"):
        logging.info(f"Найден файл правил: {find_path}/{file}")
        return file


@task
def get_kwargs_for_insert_result(**context):
    measurements = context["ti"].xcom_pull("get_prof_rules")[1]

    rule_params = {}
    for meas in measurements:
        rule_id = meas[0]
        table_name = meas[1]
        meas_cd = meas[2]
        sort_num = meas[3]

        if not rule_params.get(table_name):
            rule_params[table_name] = {
                "stage_schema": STAGE_SCHEMA,
                "dq_schema": DQ_SCHEMA,
                "table_name": f"prof_meas_val_{rule_id}_{table_name}_tmp",
                "meas_list": []
            }

        rule_params[table_name]["meas_list"].append([rule_id, meas_cd, sort_num])

    # kwargs_for_insert_log = [{"params": v} for v in rule_params.values()]
    kwargs_for_insert_log = [v for v in rule_params.values()]

    return kwargs_for_insert_log


@task
def get_kwargs_for_create_tmp(**context):
    rules = context["ti"].xcom_pull("get_prof_rules")[0]

    kwargs_for_collect = []
    for rule in rules:
        rule_id = rule[0]
        table_name = rule[1]

        file_name = find_dq_sql_file(DQ_RULES_SQL_PATH, rule_id)

        d = {
            "sql": file_name,
            "table_name": table_name,
            "temp_prefix": f"prof_meas_val_{rule_id}",
            "params": {
                "dds_schema": DDS_SCHEMA
            }
        }

        kwargs_for_collect.append(d)

    return kwargs_for_collect


@task
def get_kwargs_for_drop_tmp(**context):
    rules = context["ti"].xcom_pull("get_prof_rules")[0]

    kwargs_for_drop = []
    for rule in rules:
        rule_id = rule[0]
        table_name = rule[1]

        d = {
            "table_name": table_name,
            "temp_prefix": f"prof_meas_val_{rule_id}"
        }

        kwargs_for_drop.append(d)

    return kwargs_for_drop


@dag(
    start_date=pendulum.datetime(2023, 7, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params=None,
    template_searchpath=TMPL_SEARCH_PATH,
    description=None,
    doc_md=None,
    tags=["dds", "dq"]
)
def dds_dq_profiling():
    set_log_running_task = CustomSQLOperator(
        task_id="set_log_running",
        conn_id=DQ_CONN_ID,
        sql="lpr_set_running_status.sql",
        params=
        {
            "dq_schema": DQ_SCHEMA,
            "meta_schema": META_SCHEMA
        }
    )

    get_prof_rules_task = CustomSQLOperator(
        task_id="get_prof_rules",
        conn_id=DDS_CONN_ID,
        sql="get_prof_rules.sql",
        params={
            "dq_schema": DQ_SCHEMA,
            "meta_schema": META_SCHEMA,
        },
        do_xcom_push=True,
        split_statements=True,
        return_last=False
    )

    kwargs_for_collect = get_kwargs_for_create_tmp()

    collect_meas_tmp_task = CreateTmpTable.partial(
        task_id="collect_meas_tmp",
        conn_id=DQ_CONN_ID,
        drop_if_exists=True,
        log_sql=True,
        schema=STAGE_SCHEMA,
        temp_suffix="tmp"
    ).expand_kwargs(kwargs_for_collect)

    kwargs_for_insert = get_kwargs_for_insert_result()

    insert_meas_log_prof_task = CustomSQLOperator.partial(
        task_id="insert_meas_log_prof",
        conn_id=DQ_CONN_ID,
        sql="insert_log_prof_meas_val.sql",
    ).expand(params=kwargs_for_insert)

    kwargs_for_drop_tmp = get_kwargs_for_drop_tmp()

    drop_tmp_table_task = DropTmpTable.partial(
        task_id="drop_tmp_table",
        conn_id=DQ_CONN_ID,
        schema=STAGE_SCHEMA,
        temp_suffix="tmp"
    ).expand_kwargs(kwargs_for_drop_tmp)

    set_log_finish_task = CustomSQLOperator(
        task_id="set_log_finish",
        conn_id=DQ_CONN_ID,
        sql="lpr_set_finish_status.sql",
        params=
        {
            "dq_schema": DQ_SCHEMA
        }
    )

    set_log_running_task >> get_prof_rules_task >> kwargs_for_collect >> collect_meas_tmp_task \
    >> kwargs_for_insert >> insert_meas_log_prof_task >> kwargs_for_drop_tmp >> drop_tmp_table_task \
    >> set_log_finish_task


dds_dq_profiling()
