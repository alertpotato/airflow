import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import chain
from utils import common
from utils.logs_meta.log_tables import TableAction as act, log_etl_run
from utils.logs_meta import shared
import requests
import logging
import time
from utils.sql_operator import CustomSQLOperator

REQUEST_ARGS = {
    # подключение, ответ
    'timeout': (10, 60),
    'verify': False
}

DB_CONN_ID = 'nds_kmp'
API_CONN_ID = 'kmp_api'

SQL_FILES_DIR = common.get_sql_home_path() + '/dataflows/kmp'

DAG_DESCRIPTION = f"""
Головной даг КМП

Логика:
1)Получает список пар регион+период
2)Запускает DAG [ [etl_kmp] ](http://172.28.34.34/airflow/dags/etl_kmp/grid)
для каждой комбинации периода и региона.
3)После успешного завершения обработки запускает DAG также для каждой пары
[ [etl_fd_kmp] ](http://172.28.34.34/airflow/dags/etl_fd_kmp/grid)

Задача: [KMP-1687](https://jira.element-lab.ru/browse/KMP-1687)
"""

INIT_TASK_ID = 'init_run_id'


@task
def get_active_rules():
    context = get_current_context()
    run_id = common.get_params_from_xcom(
        context, INIT_TASK_ID, ('etl_run_id',))['etl_run_id']
    KMPCONF = []
    # Идем по всем парам период+регион чтобы сформировать для каждой список активных проверок
    for pair in context['params']['pairs']:
        # TODO Wait fo bug fix
        if isinstance(pair, str):
            pair = eval(pair)
        print(f"test {type(pair)} {pair}")
        period = int(pair['period_cd'])
        logging.info('PERIOD: ' + str(period))
        api_conn = BaseHook.get_connection(API_CONN_ID)
        # creds = {"ClientId": conn.login, "ClientSecret": conn.password}
        rules_api_url = f"{api_conn.host}/api/v1/api/rules"
        response_rules = requests.get(rules_api_url, **REQUEST_ARGS)
        try:
            response_job_json = response_rules.json()
            assert isinstance(response_job_json, list)
        except Exception as ex:
            from airflow.exceptions import AirflowFailException
            msgs = [
                f"API {rules_api_url}",
                "вернуло неожиданный ответ:",
                response_rules.text
            ]
            raise AirflowFailException('\n'.join(msgs)) from ex

        rules_to_run = []

        for rule in response_job_json:
            rule_code = rule["rule_code"]
            is_in_period = False
            consumer = [x for x in rule['consumers']
                        if x['consumer'] == 'УС КМП']
            version = [x for x in rule['versions']
                       if x['version_code'] == '001']
            if not version:
                logging.warning("Правило № %s не имеет версий", rule_code)
                continue
            for ver in rule['versions']:
                period_begin = ver["period_begin"]
                period_end = ver["period_end"]
                is_enabled = ver["is_enabled"]
                status = ver["status"]
                version_code = ver["version_code"]
                if (is_enabled == 1 and period >= period_begin and period <= period_end and status == 'OK'):
                    logging.info(
                        f"Найдена активная версия {version_code} правила {rule_code}")
                    is_in_period = True

            logging.debug("№ %s: %s, %s, %s, %s",
                          rule_code, consumer, is_enabled, period_begin, period_end)

            if (  # фильтр правил
                    consumer and
                    is_in_period
            ):
                rules_to_run.append(int(rule_code))
            else:
                logging.warning("Правило № %s не прошло фильтр", rule_code)
        logging.info("Отобраны правила: %s", rules_to_run)

        def sortByID(e):
            return int(e)

        rules_to_run.sort(key=sortByID)
        # Собираем параметр с набором правил для скрипта sql в виде ('N','N2',...)
        sql_rules = "(" + ",".join(map("'{0}'".format, rules_to_run)) + ")"

        CONFS = {"conf": {'period_cd': pair['period_cd'],
                          'region_cd': pair['region_cd'],
                          'rules': sql_rules,
                          'kmp_rules': rules_to_run,
                          'etl_run_id': run_id},
                 "trigger_run_id": f"{context['run_id']}_{run_id}_{pair['region_cd']}_{pair['period_cd']}"
                 }

        KMPCONF.append(CONFS)
        logging.info(CONFS)

    logging.info('Новый набор параметров KMP:')
    logging.info(KMPCONF)

    return KMPCONF


@task
def get_unique_periods():
    context = get_current_context()
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


@task(max_active_tis_per_dag=1)
def create_job_api(period_dict):
    """ Обновление подготовительных таблиц """
    context = get_current_context()
    if context['params'].get('dry_run'):
        logging.info("dry_run, задача не выполняется")
        return

    for period in period_dict:
        refresh_id = 0
        period_cd = period['period_cd']
        logging.info("Начало формирования задания. "
                     "Переданный период: %s",
                     period,
                     )

        api_conn = BaseHook.get_connection(API_CONN_ID)
        job_api_url = f"{api_conn.host}/api/v1/admin/refresh?period_cd={period_cd}"
        logging.info("Адрес задания: %s", job_api_url)
        # Тело запроса для сервиса
        job = {
            "period_cd": period_cd,
            "status": "NEW",
            "status_desc": "Новый запрос"
        }
        logging.info("Тело запроса: %s", job)
        # Создаем задание
        create_job = requests.post(job_api_url, json=job, **REQUEST_ARGS)
        try:
            response_job = create_job.json()
            refresh_id = response_job['refresh_id']
        except requests.exceptions.JSONDecodeError:
            logging.info(f"Ошибочный ответ: {response_job}")
            raise
        logging.info(
            f"Инициализированно задание {refresh_id}. Ответ сервиса: {response_job}")
        job_status_url = f"{api_conn.host}/api/v1/admin/refresh/{refresh_id}"

        try_count = 0
        max_trys = 5
        while response_job_status := requests.get(job_status_url, **REQUEST_ARGS):
            try:
                response = response_job_status.json()
                try_count = 0
            except requests.exceptions.JSONDecodeError:
                logging.info(f"Ошибочный ответ: {response}")
                if try_count >= max_trys:
                    raise
                else:
                    try_count += 1
                    logging.info(
                        f"Пробуем ещё раз, текущая попытка: {try_count} из {max_trys}")
                    time.sleep(10)
                    continue

            try:
                status = response['status']
                status_desc = response['status_desc']
            except:
                logging.info(
                    f"status или status_desc отсутствует в json: {response}")
                raise

            if status == 'OK':
                logging.info(
                    "Завершена работа над обновлением периода %s, задание %s", period_cd, refresh_id)
                break  # Если задание завершено выходим из цикла
            elif status == 'ERROR':
                from airflow.exceptions import AirflowFailException
                raise AirflowFailException('Ошибка API KMP: ' + status_desc)
            else:
                logging.info(f"Текущий статус: {status} | {status_desc}")
                time.sleep(10)


@task(task_id=INIT_TASK_ID)
def init_run_id():
    context = get_current_context()
    dag_run = context.get('dag_run')
    if dag_run and dag_run.external_trigger:
        print("Это ручной запуск, пропускаем инициализацию, `etl_run_id`: 0")
        init_vars = {'etl_run_id': 0}
    else:
        run_id = log_etl_run(act.get_new_id, context=context)
        init_vars = {'etl_run_id': run_id}

    common.xcom_save(get_current_context(), init_vars, with_log=True)


@task
def finish_etl_run():
    context = get_current_context()
    dag_run = context.get('dag_run')
    if dag_run and dag_run.external_trigger:
        print("Это ручной запуск, пропускаем финализацию, `etl_run_id`: 0")
    else:
        p = common.get_params_from_xcom(context, INIT_TASK_ID, ('etl_run_id',))
        log_etl_run(act.finish, p)


@dag(
    schedule='0 22 * * *',
    start_date=pendulum.local(2023, 9, 19),
    catchup=False,
    tags=['КМП', 'УС'],
    description=DAG_DESCRIPTION.splitlines()[1],
    params={'clean_temp_tables': False, 'pairs': [
        #{'period_cd': '200001', 'region_cd': '999'}, {'period_cd': '200002', 'region_cd': '999'}]},
        {'period_cd': '202010', 'region_cd': '67'}]},
    doc_md=DAG_DESCRIPTION,
    template_searchpath=SQL_FILES_DIR,
    default_args={
        'on_failure_callback': shared.get_on_failure_function(
            tables_to_finish=('log_etl_run', 'log_etl_proc',
                              'log_etl_proc_table'),
            xcom_keys=('etl_run_id', 'etl_proc_id', 'all_tbl_ids'),
        )
    }
)
def head_etl_kmp():
    rules_params = get_active_rules.override(
        task_id='Активные_правила')()
    uniq_regions = get_unique_periods.override(
        task_id='Уникальные_периоды')()

    trigger_etl_kmp = TriggerDagRunOperator.partial(
        task_id='Запуск_etl_kmp',
        trigger_dag_id='etl_kmp',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=5,
        max_active_tis_per_dag=5,
    ).expand_kwargs(rules_params)

    trigger_etl_fd_kmp = TriggerDagRunOperator.partial(
        task_id='Запуск_etl_fd_kmp',
        trigger_dag_id='etl_fd_kmp',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=5,
        max_active_tis_per_dag=5,
    ).expand_kwargs(rules_params)

    init_run_id() >> [
        rules_params, uniq_regions] >> trigger_etl_kmp >> trigger_etl_fd_kmp >> finish_etl_run()


head_etl_kmp()
