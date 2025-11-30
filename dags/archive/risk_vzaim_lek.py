import json
import pendulum
from datetime import datetime
from time import perf_counter
import requests
import logging
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import get_current_context
from airflow.decorators import dag, task, task_group
from utils import clean_files
from utils.logs_meta.log_tables import (
    TableAction as act,
    log_etl_proc_table
)
from utils import common
from utils.logs_meta import shared
from utils.logs_meta.extra import get_table_meta_id
from utils.sql_operator import CustomSQLOperator

from airflow.operators.python import ShortCircuitOperator

DESCRIPTION = '''
Формирование витрины рисков взаимодействия лекарственных средств.
Даг формирует в базе возможные комбинации лекарств,
использованных одновременно и запрашивает сервис
на наличие риска между этими лекарствами.
'''

SQL_FILES_DIR = []
SQL_FILES_DIR.append(common.get_sql_home_path() + '/dataflows/risk_vzaim_lek/drug_drug_interactions')
SQL_FILES_DIR.append(common.get_sql_home_path() + '/dataflows/risk_vzaim_lek/disease_contraindications')

TASK_ID_FOR_PARAMS = 'initialize_all'

DB_CONN_ID = 'nds_dds'

API_CONN_ID = 'sln_api'
AIRFLOW_API = 'airflow_api'

LOG_TABLE_NAME = 'risk_vzaim_lek'
MT_ETL_PROC_ID = 6

HEADERS = {"Content-Type": "application/json"}
API_CREDS = json.dumps(
    {
        "ClientId": Connection.get_connection_from_secrets(API_CONN_ID).login,
        "ClientSecret": Connection.get_connection_from_secrets(API_CONN_ID).password
    }
)

START_DATE = pendulum.local(2019, 1, 1)


@task
def get_period_cd():
    context = get_current_context()
    dag_run = context["dag_run"]

    if dag_run.run_type == "scheduled":
        period_cd = dag_run.data_interval_end.strftime("%Y%m")
        period_dt = dag_run.data_interval_end.strftime("%Y-%m-01")
    else:
        if dag_run.conf:
            period_cd = dag_run.conf["period_cd"]
            period_dt = pendulum.from_format(period_cd, "YYYYMM").strftime("%Y-%m-%d")
        else:
            period_cd = dag_run.data_interval_end.strftime("%Y%m")
            period_dt = dag_run.data_interval_end.strftime("%Y-%m-01")

    logging.info(f"Период для обработки: {period_cd}")
    logging.info(f"Период в формате даты: {period_dt}")

    context["ti"].xcom_push(key="period_cd", value=period_cd)
    context["ti"].xcom_push(key="period_dt", value=period_dt)


@task.short_circuit(ignore_downstream_trigger_rules=False)
def compare_api_build_date():
    context = get_current_context()

    ti = context["ti"]
    previous_dagrun = ti.get_previous_dagrun()

    if previous_dagrun:
        previos_ti = previous_dagrun.get_task_instance("get_api_db_build_date")

        previos_api_build_date = previos_ti.xcom_pull(task_ids="get_api_db_build_date")
        current_api_build_date = ti.xcom_pull(task_ids="get_api_db_build_date")

        res = previos_api_build_date != current_api_build_date
        if res:
            logging.info(f"Дата базы данных API изменилась. Будет выполнена перезагрузка всех прошлых периодов.")

    else:
        return False


@task
def clean_old_dag_runs():
    import pandas as pd

    context = get_current_context()
    ti = context["ti"]

    start_date = START_DATE.add(months=-1).strftime("%Y-%m-%d")
    end_date = ti.execution_date.strftime("%Y-%m-%d")

    logging.info(f"Будет выполнен перезапуск периодов для загрузки. С {start_date} по {end_date}")

    daterange = pd.date_range(start_date, end_date, freq="M")
    for single_date in daterange:
        run_id = f"scheduled__{single_date.strftime('%Y-%m-%dT21:00:00+00:00')}"

        clear_dag_run = SimpleHttpOperator(
            task_id="clear_dag_run",
            http_conn_id=AIRFLOW_API,
            method="POST",
            endpoint=f"/api/v1/dags/risk_vzaim_lek/dagRuns/{run_id}/clear",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json"
            },
            data=json.dumps({
                "dry_run": False
            }),
            response_check=lambda response: response.status_code == 200,
            log_response=True,
            extra_options={"verify":False}
        ).execute(context)


@task
def process_table(action: str):
    """ Обработка таблицы """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    context = get_current_context()
    etl_proc_id, all_tbl_ids, db_date = common.get_params_from_xcom_any_task(
        context=context,
        keys=('etl_proc_id', 'all_tbl_ids', 'db_date_ansi')).values()

    tbl_ids = all_tbl_ids[LOG_TABLE_NAME]  # в угоду унификации
    sql_main_query_file_path = f"{SQL_FILES_DIR}/{action}.sql"
    sql_params_file_path = f"{SQL_FILES_DIR}/pg_params.sql"

    sql_params = common.get_file_content(sql_params_file_path)

    jinja_args = {'db_date': db_date}
    main_query = common.get_rendered_file_content(
        context, sql_main_query_file_path, jinja_args)
    result_query = '\n'.join([sql_params, main_query])

    log_params = {
        'dag_action': 'insert',
        'table_name': LOG_TABLE_NAME,
        'new_log_tbl_id': tbl_ids['new_log_tbl_id'],
        'etl_proc_id': etl_proc_id,
    }

    pg_hook = PostgresHook(DB_CONN_ID)
    common.log_it({
        "Current params": "",
        **log_params,
        'sql_main_query_file_path': sql_main_query_file_path,
        "SQL query to execute": '\n' + result_query
    }, pg_hook.log.info)

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            clean_files.run_if_exist(
                file_path=sql_main_query_file_path,
                context=context,
                cur=cur,
                log_method=pg_hook.log.info)

            cur.execute(result_query)
            pg_hook.log.info(
                "Main query completed successfully, affected rows: %s",
                cur.rowcount)
            log_params['affected_rows'] = cur.rowcount
            log_etl_proc_table(act.progress, log_params, cur=cur)


@task
def get_drug_drug_interactions_from_api():
    """ Загрузка данных из API """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    context = get_current_context()
    access_token = context["ti"].xcom_pull(task_ids='get_access_token')
    period_cd = context["ti"].xcom_pull(key='period_cd')

    conn = BaseHook.get_connection(API_CONN_ID)

    screening_api_url = (
        f"http://{conn.host}:{conn.port}"
        f"/v1/screening?access_token={access_token}"
    )

    logging.info(
        f"Начало основного цикла перебора рисков для пар лекарств. Адрес запроса API: {screening_api_url}. Заполняем таблицу stage.drug_drug_interactions_stage.")
    # Основной цикл перебора рисков для пар лекарств
    # sql_data = common.get_file_content(
    #     f"{common.get_sql_home_path() + '/dataflows/risk_vzaim_lek/drug_drug_interactions'}/select_pairs.sql")

    file_sql = f"{common.get_sql_home_path() + '/dataflows/risk_vzaim_lek/drug_drug_interactions'}/select_pairs.sql"

    sql_query = common.get_rendered_file_content(context, file_sql, {"params": {"period_cd": period_cd}})

    pg_hook = PostgresHook(DB_CONN_ID)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:

            # Забираем имеющиеся пары лекаств в мед случаях
            logging.info("Забираем имеющиеся пары лекаств в мед случаях по запросу:")
            logging.info(sql_query)
            cur.execute(sql_query)
            sql_data = cur.fetchall()
            chunk_size = 100
            progress = 0
            total_rows = len(sql_data)
            start_perf = perf_counter()

            for cd1, name1, cd2, name2 in sql_data:

                if progress % chunk_size == 0 or progress == total_rows:
                    next_c = progress + chunk_size
                    mes_cnt = next_c if next_c < total_rows else total_rows
                    pg_hook.log.info(
                        "Start processing rows %s-%s of total %s:",
                        progress, mes_cnt, total_rows
                    )
                    start_perf = perf_counter()
                progress += 1

                risk = None

                # Для каждой пары лекарств перебираем разницу в
                # днях между применениями пока не получим пустой ответ
                for x in range(31):

                    # День в формате DD
                    xx = str(x + 1).zfill(2)

                    # Тело запроса для сервиса
                    drugs = {
                        "ScreeningTypes": "DrugDrugInteractions",
                        "Drugs": [
                            {
                                "Screen": True,
                                "Type": "urn:ffoms:n020",
                                "Code": f"{cd1}",
                                "Name": f"{name1}",
                                "customCode": "dr_0",
                                "customName": "hash",
                                "Schedule": {
                                    "FirstAdministration": "2023-01-01",
                                    "LastAdministration": "2023-01-01"
                                }
                            },
                            {
                                "Screen": True,
                                "Type": "urn:ffoms:n020",
                                "Code": f"{cd2}",
                                "Name": f"{name2}",
                                "customCode": "dr_0",
                                "customName": "hash",
                                "Schedule": {
                                    "FirstAdministration": f"2023-01-{xx}",
                                    "LastAdministration": f"2023-01-{xx}"
                                }
                            }
                        ],
                        "Options": {
                            "IncludeMonographs": False
                        }
                    }

                    resp = requests.post(screening_api_url, json=drugs)
                    try:
                        response = resp.json()
                    except:
                        logging.info(
                            f"Полученный ответ api некорректен. Ошибочный ответ: {resp.json()}")
                        raise
                    # Если нет кода риска выходим из цикла
                    if response['DrugDrugInteractions']['Items'] == []:
                        if risk is None:
                            risk = 0
                        break
                    # Если вернулся код риска - записываем этот риск
                    for item in response['DrugDrugInteractions']['Items']:
                        risk = item['Severity']['Code']

                # Запись в risk_vzaim_lek_stage
                # таблица должна быть пересоздана из risk_vzaim_lek_stage.sql

                sql_script = f"""
                insert into stage.drug_drug_interactions_stage_{period_cd}
                        (  cd1,     name1,     cd2,     name2,    risk,   days)
                values ('{cd1}', '{name1}', '{cd2}', '{name2}', {risk}, {x - 1})
                """
                cur.execute(sql_script)
                if (progress - chunk_size) % chunk_size == 0:
                    pg_hook.log.info(
                        '\t^ Chunk finished in in %.2f seconds.',
                        perf_counter() - start_perf)


@task
def get_disease_contraindications_from_api():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    context = get_current_context()
    access_token = context["ti"].xcom_pull(task_ids='get_access_token')
    period_cd = context["ti"].xcom_pull(key='period_cd')

    truncate_stage_table = SQLExecuteQueryOperator(
        task_id="truncate_stage_table",
        conn_id=DB_CONN_ID,
        sql=f"truncate table stage.disease_contraindications_stage_{period_cd}"
    ).execute(context)

    sql_query = f'''
    select
        mkb10_cd,
        mkb10_name,
        lek_cd,
        lek_name
    from
        stage.risk_vzaim_lek_drug_by_diagnosis_{period_cd}
    '''

    pg_hook = PostgresHook(DB_CONN_ID)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_query)
            sql_data = cur.fetchall()

            for mkb10_cd, mkb10_name, lek_cd, lek_name in sql_data:
                data = {
                    "ScreeningTypes": "DiseaseContraindications",
                    "drugs": [
                        {
                            "Type": "urn:ffoms:n020",
                            "Code": lek_cd,
                            "Name": lek_name
                        }
                    ],
                    "allergies": [],
                    "diseases": [
                        {
                            "type": "ICD10CM",
                            "code": mkb10_cd,
                            "name": mkb10_name
                        }
                    ],
                    "options": {
                        "includeMonographs": False
                    }
                }

                result = SimpleHttpOperator(
                    task_id="api_request",
                    http_conn_id=API_CONN_ID,
                    method="POST",
                    endpoint=f"/v1/screening?access_token={access_token}",
                    data=json.dumps(data),
                    headers=HEADERS,
                    response_check=lambda response: response.status_code == 200,
                    response_filter=lambda response: response.json()["DiseaseContraindications"]['Items'],
                    extra_options={"verify":False}
                ).execute(context)

                if result:
                    severity = result[0]['Severity']

                    SQLExecuteQueryOperator(
                        task_id="insert_api_result",
                        conn_id=DB_CONN_ID,
                        sql=f"""
                            insert into stage.disease_contraindications_stage_{period_cd} (mkb10_cd, mkb10_name, lek_cd, lek_name, severity_level, severity_name)
                            values ('{mkb10_cd}', '{mkb10_name}', '{lek_cd}', '{lek_name}', {severity['Level']}, '{severity['Name']}')
                            """
                    ).execute(context)


@task(task_id=TASK_ID_FOR_PARAMS)
def init_all():
    """
    Получает из базы новые id сохраняет в XCOM.
    Структуру см. в описании `shared.init_all(...)`
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id=DB_CONN_ID)
    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            mt_tbl_id = get_table_meta_id(
                schema_name='DDS',
                table_name=LOG_TABLE_NAME,
                cur=cur)

    log_ids = {
        'etl_run_id': None,
        'etl_proc_id': None,
        'all_tbl_ids': {
            LOG_TABLE_NAME: {
                'mt_tbl_id': mt_tbl_id
            }
        }
    }
    context = get_current_context()
    log_ids = shared.init_all(log_ids, context, MT_ETL_PROC_ID)
    common.xcom_save(context, log_ids, with_log=True)


@task(task_id='finish_all_log_tables')
def finish_all():
    log_etl_params = common.get_params_from_xcom(
        get_current_context(),
        TASK_ID_FOR_PARAMS,
        ('etl_run_id', 'etl_proc_id', 'all_tbl_ids')
    )
    shared.finish_all(
        log_etl_params=log_etl_params,
    )


@dag(
    start_date=START_DATE,
    catchup=True,
    schedule='@monthly',
    description=DESCRIPTION.splitlines()[1],
    tags=['dds', 'СЛН', 'УС'],
    template_searchpath=SQL_FILES_DIR,
    max_active_runs=1,
    params={
        "clean_temp_tables": True,
        "period_cd": None,
        "period_dt": None
    },
    default_args={
        'on_failure_callback': shared.get_on_failure_function(
            tables_to_finish=('log_etl_run', 'log_etl_proc',
                              'log_etl_proc_table'),
            xcom_keys=('etl_run_id', 'etl_proc_id', 'all_tbl_ids'),
        )
    }
)
def risk_vzaim_lek():
    get_period_cd_task = get_period_cd()

    get_access_token_task = SimpleHttpOperator(
        task_id="get_access_token",
        http_conn_id=API_CONN_ID,
        method="POST",
        endpoint="/v1/account/token",
        data=API_CREDS,
        headers=HEADERS,
        response_check=lambda response: response.status_code == 200,
        response_filter=lambda response: response.json()["Token"],
        extra_options={"verify":False}
    )

    get_api_db_build_date_task = SimpleHttpOperator(
        task_id="get_api_db_build_date",
        http_conn_id=API_CONN_ID,
        method="GET",
        endpoint="/v1/system/info",
        headers=HEADERS,
        response_check=lambda response: response.status_code == 200,
        response_filter=lambda response: response.json()["DbBuildDate"],
        extra_options={"verify":False},
        log_response=True
    )

    compare_api_build_date_task = compare_api_build_date()

    clean_old_dag_runs_task = clean_old_dag_runs()

    @task_group(group_id="DrugDrugInteractions")
    def drug_drug_interactions_tg():
        drug_by_period_cd_task = CustomSQLOperator(
            task_id="drug_by_period_cd",
            conn_id=DB_CONN_ID,
            sql="drug_by_period_cd.sql",
            clean_file_check_path="drug_by_period_cd.sql",
            trigger_rule="none_failed",
            x_params={
                "period_cd": "{{ ti.xcom_pull(key='period_cd') }}"
            }
        )

        drug_drug_crossover_task = CustomSQLOperator(
            task_id="drug_drug_crossover",
            conn_id=DB_CONN_ID,
            sql="drug_drug_crossover.sql",
            clean_file_check_path="drug_drug_crossover.sql",
            x_params={
                "period_cd": "{{ ti.xcom_pull(key='period_cd') }}"
            }
        )

        create_stage_table_task = CustomSQLOperator(
            task_id="create_stage_table",
            conn_id=DB_CONN_ID,
            sql="create_stage_drug_drug_interactions.sql",
            clean_file_check_path="create_stage_drug_drug_interactions.sql",
            x_params={
                "period_cd": "{{ ti.xcom_pull(key='period_cd') }}"
            }
        )

        get_drug_drug_interactions_from_api_task = get_drug_drug_interactions_from_api()

        update_risk_vzaim_lek_task = CustomSQLOperator(
            task_id="update_risk_vzaim_lek",
            conn_id=DB_CONN_ID,
            sql="update_risk_vzaim_lek.sql",
            x_params={
                "api_db_build_date": "{{ ti.xcom_pull(task_ids='get_api_db_build_date') }}",
                "period_cd": "{{ ti.xcom_pull(key='period_cd') }}"
            }
        )

        insert_risk_vzaim_lek_task = CustomSQLOperator(
            task_id="insert_risk_vzaim_lek",
            conn_id=DB_CONN_ID,
            sql="insert_risk_vzaim_lek.sql",
            x_params={
                "api_db_build_date": "{{ ti.xcom_pull(task_ids='get_api_db_build_date') }}",
                "period_cd": "{{ ti.xcom_pull(key='period_cd') }}"
            }
        )

        drug_by_period_cd_task >> drug_drug_crossover_task >> create_stage_table_task >> get_drug_drug_interactions_from_api_task \
        >> update_risk_vzaim_lek_task >> insert_risk_vzaim_lek_task

    @task_group(group_id="DiseaseContraindications")
    def disease_contraindications_tg():
        drug_by_diagnosis_task = CustomSQLOperator(
            task_id="drug_by_diagnosis",
            conn_id=DB_CONN_ID,
            sql="drug_by_diagnosis.sql",
            clean_file_check_path="drug_by_diagnosis.sql",
            trigger_rule="none_failed",
            x_params={
                "period_cd": "{{ ti.xcom_pull(key='period_cd') }}"
            }
        )

        create_stage_table_task = CustomSQLOperator(
            task_id="create_stage_table",
            conn_id=DB_CONN_ID,
            sql="create_stage_disease_contraindications.sql",
            clean_file_check_path="create_stage_disease_contraindications.sql",
            x_params={
                "period_cd": "{{ ti.xcom_pull(key='period_cd') }}"
            }
        )

        get_disease_contraindications_from_api_task = get_disease_contraindications_from_api()

        update_risk_vzaim_lek_ds_task = CustomSQLOperator(
            task_id="update_risk_vzaim_lek_ds",
            conn_id=DB_CONN_ID,
            sql="update_risk_vzaim_lek_ds.sql",
            x_params={
                "api_db_build_date": "{{ ti.xcom_pull(task_ids='get_api_db_build_date') }}",
                "period_cd": "{{ ti.xcom_pull(key='period_cd') }}"
            }
        )

        insert_risk_vzaim_lek_ds_task = CustomSQLOperator(
            task_id="insert_risk_vzaim_lek_ds",
            conn_id=DB_CONN_ID,
            sql="insert_risk_vzaim_lek_ds.sql",
            x_params={
                "api_db_build_date": "{{ ti.xcom_pull(task_ids='get_api_db_build_date') }}",
                "period_cd": "{{ ti.xcom_pull(key='period_cd') }}"
            }
        )

        drug_by_diagnosis_task >> create_stage_table_task >> get_disease_contraindications_from_api_task \
        >> update_risk_vzaim_lek_ds_task >> insert_risk_vzaim_lek_ds_task

    init_all() >> get_period_cd_task >> get_access_token_task >> get_api_db_build_date_task >> compare_api_build_date_task >> \
    clean_old_dag_runs_task >> [drug_drug_interactions_tg(), disease_contraindications_tg()] \
    >> finish_all() >> clean_files.cleanup_temp_tables_task()


risk_vzaim_lek()
