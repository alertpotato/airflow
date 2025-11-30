import pendulum
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context
from utils.sql_operator import CustomSQLOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from utils import common
from utils.logs_meta import shared
import logging

REQUEST_ARGS = {
    # подключение, ответ
    'timeout': (10, 60),
    'verify': False
}

DB_CONN_ID = 'nds_kmp'
API_CONN_ID = 'kmp_api'

SQL_FILES_DIR = common.get_sql_home_path() + '/dataflows/kmp'

MT_ETL_PROC_ID = 38
TASK_ID_FOR_PARAMS = 'init_all'

DAG_DESCRIPTION = f"""
    Даг предназначен для обновления оптимизационных витрин КМП в схеме kmp. 
    После того как завершается загрузка счетов (даг dds_etl_scheta) по одному значению регион+период 
    стартует данный даг обновления витрины которому передаются данные параметры: region_cd, period_cd.
"""

dataset_scheta_region_period_success = Dataset("scheta_region_period_success")


def response_check(response):
    response = response.json()

    logging.info(f"Текущий статус: {response['status']} {response['status_desc']} {response['period_cd']}")

    match response["status"]:
        case "ERROR":
            raise AirflowFailException(f"Ошибка API KMP: {response['status_desc']}")
        case "OK":
            return True
        case _:
            return False


def get_response_api_params(job_id):
    return f"/api/v1/admin/refresh/{job_id}"


def get_request_api_params(period):
    return {
        "endpoint": f"/api/v1/admin/refresh?period_cd={period['period_cd']}",
        "data": {
            **period,
            "status": "NEW",
            "status_desc": "Новый запрос"
        }
    }


@task
def update_run_config(triggering_dataset_events=None, **contex):
    dag_run = contex["dag_run"]

    if not dag_run.conf and dag_run.run_type == 'dataset_triggered':
        conf = {
            "clean_temp_tables": False,
            "etl_run_id": 0,
            "pairs": []
        }

        for dataset, events_list in triggering_dataset_events.items():
            logging.info(f"dataset_list: {events_list}")

            for event in events_list:
                item = {
                    "region_cd": event.extra["region_cd"],
                    "period_cd": event.extra["period_cd"]
                }
                conf["etl_run_id"] = event.extra["etl_run_id"]
                conf["pairs"].append(item)

        dag_run.conf = conf


@task()
def get_unique_periods(**context):
    # TODO Wait fo bug fix
    # uniq_period = {v['period_cd'] for v in context['params']['pairs']}
    uniq_period = {
        eval(v)['period_cd']
        if isinstance(v, str) else v['period_cd']
        for v in context['params']['pairs']
    }
    period_dict = [{'period_cd': v} for v in uniq_period]
    logging.info('Уникальный набор периодов:')

    return period_dict


@task(task_id=TASK_ID_FOR_PARAMS)
def init_all():
    """
    Получает из базы новые id сохраняет в XCOM.
    Структуру см. в описании `shared.init_all(...)`
    """

    context = get_current_context()
    dag_params = context['params']

    log_ids = {
        **common.get_sql_params(context),
        'etl_run_id': dag_params.get('etl_run_id', None),
        'etl_proc_id': None,
    }
    log_ids = shared.init_all(log_ids, context, MT_ETL_PROC_ID)
    common.xcom_save(context, log_ids, with_log=True)


@task(trigger_rule="none_failed")
def finish_all():
    log_etl_params = common.get_params_from_xcom(
        get_current_context(),
        TASK_ID_FOR_PARAMS,
        ('etl_run_id', 'etl_proc_id')
    )

    shared.finish_partial(
        tables_to_finish=('log_etl_proc',),
        log_etl_params=log_etl_params,
    )


@dag(
    schedule=[dataset_scheta_region_period_success],
    start_date=pendulum.datetime(2023, 9, 19, tz='Europe/Moscow'),
    catchup=False,
    max_active_runs=3,
    tags=['КМП', 'УС'],
    template_searchpath=SQL_FILES_DIR,
    description=DAG_DESCRIPTION.splitlines()[1],
    params={'clean_temp_tables': False, 'pairs': [
        {'period_cd': '200001', 'region_cd': '999'}, {'period_cd': '200002', 'region_cd': '999'}]},
    doc_md=DAG_DESCRIPTION,
    default_args={}
)
def etl_upd_kmp_datamarts():
    update_run_config_task = update_run_config()

    get_unique_periods_task = get_unique_periods.override(
        task_id='get_unique_periods')()

    run_api_job_task = SimpleHttpOperator.partial(
        task_id="run_api_job",
        http_conn_id=API_CONN_ID,
        method="POST",
        extra_options={**REQUEST_ARGS},
        response_check=lambda response: response.status_code == 200,
        response_filter=lambda response: response.json()["refresh_id"],
        log_response=True
    ).expand_kwargs(get_unique_periods_task.map(get_request_api_params))

    done_api_job_task = HttpSensor.partial(
        task_id="done_api_job",
        http_conn_id=API_CONN_ID,
        method="GET",
        poke_interval=90,
        extra_options={**REQUEST_ARGS},
        response_check=response_check,
    ).expand(endpoint=run_api_job_task.output.map(get_response_api_params))

    dn_placement_info_task = CustomSQLOperator.partial(
        task_id="dn_placement_info",
        conn_id=DB_CONN_ID,
        sql="dn_placement_info.sql",
        clean_file_check_path="dn_placement_info.sql",
        log_sql=True,
    ).expand(params=get_unique_periods_task)

    dn_check_incl_task = CustomSQLOperator.partial(
        task_id="dn_check_incl",
        conn_id=DB_CONN_ID,
        sql="dn_check_incl.sql",
        clean_file_check_path="dn_check_incl.sql",
        log_sql=True,
    ).expand(params=get_unique_periods_task)

    update_run_config_task >> init_all() >> get_unique_periods_task >> run_api_job_task >> done_api_job_task >> dn_placement_info_task >> dn_check_incl_task >> finish_all()


dag = etl_upd_kmp_datamarts()
