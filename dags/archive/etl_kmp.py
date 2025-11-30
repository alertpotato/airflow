import time
import json
import pendulum
import requests
import logging
from airflow.operators.python import get_current_context
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from utils.logs_meta import shared
from utils import common
from utils.sql_operator import CustomSQLOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

DESCRIPTION = f"""
Процесс последовательного выполнения активных правил КМП
"""

TASK_ID_FOR_PARAMS = 'initialize_all'

DB_CONN_ID = 'nds_kmp'
API_CONN_ID = 'kmp_api'

MT_ETL_PROC_ID = 25

SQL_FILES_DIR = common.get_sql_home_path() + '/dataflows/kmp'

REQUEST_ARGS = {
    # подключение, ответ
    'timeout': (10, 60),
    'verify': False
}


def response_check(response):
    from airflow.exceptions import AirflowFailException

    response = response.json()

    logging.info(
        f"Текущий статус: {response['status']} rules: {response['rules']} regions: {response['regions']} periods: {response['periods']}")

    for rule_version in response["rule_versions"]:
        if rule_version["status"] == "ERROR":
            raise AirflowFailException(f"Ошибка API KMP: {rule_version['status_desc']}")

    match response["status"]:
        case "ERROR":
            raise AirflowFailException(f"Ошибка API KMP: {response['status_desc']}")
        case "OK":
            return True
        case _:
            return False


def get_response_api_params(job_id):
    return f"/api/v1/api/jobs/{job_id}"


@task
# получаем все правила кроме 25,26
def get_rules_without_25_26():
    context = get_current_context()

    period = context["params"]["period_cd"]
    region = context["params"]["region_cd"]

    rules = set(context["params"]["kmp_rules"])
    rules_out = {25, 26}

    rules.difference_update(rules_out)

    result = []

    for rule in rules:
        job = {
            "periods": [period],
            "regions": [region],
            "rules": [rule],
            "is_use_cache": 0
        }

        result.append(json.dumps(job))

    return result


@task
def get_rules_25_26():
    context = get_current_context()

    period = context["params"]["period_cd"]
    region = context["params"]["region_cd"]

    rules = set(context["params"]["kmp_rules"])
    rules_25_26 = {25, 26}

    rules.intersection_update(rules_25_26)

    result = []

    for rule in rules:
        job = {
            "periods": [period],
            "regions": [region],
            "rules": [rule],
            "is_use_cache": 0
        }

        result.append(json.dumps(job))

    return result


@task(task_id=TASK_ID_FOR_PARAMS)
def init_all():
    context = get_current_context()
    dag_params = context['params']
    log_ids = {
        'etl_run_id': dag_params.get('etl_run_id'),
        'etl_proc_id': None,
        'all_tbl_ids': None
    }
    if dag_params.get('dry_run', False):
        print('dry_run, сохраняем только переданные id, без инициализации')
        common.xcom_save(context, log_ids, with_log=True)
        return  # NOTE: dry_run

    log_ids = shared.init_all(log_ids, context, MT_ETL_PROC_ID)
    common.xcom_save(context, log_ids, with_log=True)


@task(trigger_rule="none_failed")
def update_period_region():
    context = get_current_context()

    dag_params = context['params']

    region_cd = dag_params.get('region_cd')
    period_cd = dag_params.get('period_cd')

    etl_run_id = common.get_params_from_xcom_any_task(
        context, ('etl_run_id',)
    )['etl_run_id']

    logging.info('Обновление статуса period_region_status')

    update_query = f'''
    UPDATE kmp.region_period_status
    SET
        kmp_calc_status_cd = 'C',
        kmp_calc_status_ch_dttm = now(),
        last_kmp_etl_run_id = {etl_run_id}
    WHERE region_period_status.region_cd = {region_cd} 
    AND region_period_status.period_cd = {period_cd};
    '''

    query_op = CustomSQLOperator(
        task_id='update_query',
        conn_id=DB_CONN_ID,
        sql=update_query,
    ).execute(context)


@task(task_id='finish_all_log_tables')
def finish_all():
    context = get_current_context()
    if context['params'].get('dry_run', False):
        print('dry_run, не запускаем финализацию')
        return  # NOTE: dry_run

    log_etl_params = common.get_params_from_xcom(
        context,
        TASK_ID_FOR_PARAMS,
        ('etl_run_id', 'etl_proc_id', 'all_tbl_ids')
    )
    shared.finish_all(
        log_etl_params=log_etl_params,
    )


@dag(
    start_date=pendulum.local(2023, 12, 1),
    catchup=False,
    schedule=None,
    description=DESCRIPTION,
    tags=['КМП', 'УС'],
    params={'kmp_rules': [{"rule": 0}, {"rule": 0}]},
    template_searchpath=SQL_FILES_DIR,
    default_args={
        'on_failure_callback': shared.get_on_failure_function(
            tables_to_finish=('log_etl_run', 'log_etl_proc',
                              'log_etl_proc_table'),
            xcom_keys=('etl_run_id', 'etl_proc_id', 'all_tbl_ids'),
        )
    }
)
def etl_kmp():
    init_task = init_all()

    get_rules_without_25_26_task = get_rules_without_25_26()

    run_api_any_rules_task = SimpleHttpOperator.partial(
        task_id="run_api_any_rules",
        http_conn_id=API_CONN_ID,
        method="POST",
        extra_options={**REQUEST_ARGS},
        endpoint="/api/v1/api/jobs",
        response_check=lambda response: response.status_code == 200,
        response_filter=lambda response: response.json()["job_id"],
        log_response=True,
        max_active_tis_per_dagrun=1
    ).expand(data=get_rules_without_25_26_task)

    done_api_any_rules_task = HttpSensor.partial(
        task_id="done_api_any_rules",
        http_conn_id=API_CONN_ID,
        method="GET",
        poke_interval=120,
        extra_options={**REQUEST_ARGS},
        response_check=response_check,
    ).expand(endpoint=run_api_any_rules_task.output.map(get_response_api_params))

    get_rules_25_26_task = get_rules_25_26()

    run_api_25_26_rules_task = SimpleHttpOperator.partial(
        task_id="run_api_25_26_rules",
        http_conn_id=API_CONN_ID,
        method="POST",
        extra_options={**REQUEST_ARGS},
        endpoint="/api/v1/api/jobs",
        response_check=lambda response: response.status_code == 200,
        response_filter=lambda response: response.json()["job_id"],
        log_response=True
    ).expand(data=get_rules_25_26_task)

    done_api_25_26_rules_task = HttpSensor.partial(
        task_id="done_api_25_26_rules",
        http_conn_id=API_CONN_ID,
        method="GET",
        poke_interval=120,
        extra_options={**REQUEST_ARGS},
        response_check=response_check,
    ).expand(endpoint=run_api_25_26_rules_task.output.map(get_response_api_params))

    update_period_region_task = update_period_region()

    finish_task = finish_all()

    init_task >> get_rules_without_25_26_task >> run_api_any_rules_task >> done_api_any_rules_task >> get_rules_25_26_task >> \
    run_api_25_26_rules_task >> done_api_25_26_rules_task >> update_period_region_task >> finish_task


etl_kmp()
