import pendulum
from airflow.decorators import (
    dag,
    task,
)
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils import common
from utils.logs_meta import log_tables as logger
from utils.logs_meta.log_tables import TableAction
from utils.logs_meta import shared
import os.path
from datetime import datetime
import logging

NSI_DICTIONARY = {}  # Словарь таблиц с описанием (получается из TABLES_LIST)
SQL_PATH = common.get_sql_home_path() + '/dataflows/nsi_dds'  # Путь к каталогу с SQL-скриптами
CONN_ID = 'nds_dds'  # Имя соединения в Airflow
TASK_ID_FOR_PARAMS = 'log_init'  # Название таски в которой хранятся XCOM-параметры
TABLES_LIST = {}  # Список таблиц для обработки (получается SQL-запросом)


def render_jinja(content: str, args: dict) -> str:
    """ Возвращает строку `content`, обработанную как шаблон Jinja """
    from airflow.utils.helpers import parse_template_string

    just_string, template = parse_template_string(content)
    if template:
        return template.render(args)
    return just_string


# Получить список таблиц в плоском формате
def get_flat_nsis(source: dict) -> dict:
    res = {}
    for _, sv in source.items():
        for nk, nv in sv.items():
            res[nk] = nv
    return res


# Выполнить SQL-запрос или запросы в БД (sql_statement - текст запроса/запросов)
def _execute_sql(sql_statement: str):
    pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_statement)
            if cur.description is None:
                return
            pg_hook.log.info(
                'Main query completed successfully, affected rows: %s',
                cur.rowcount,
            )
            if cur.description is None:
                return
            columns = cur.description
            result = cur.fetchall()

    return (
        {col.name: row[i] for i, col in enumerate(columns)}
        for row in result
    )


# Закончить логирование (финальные шаги)
def _finish_log(sql_params, action):
    pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            logger.log_etl_run(action, sql_params, cur=cur)
            logger.log_etl_proc(action, sql_params, cur=cur)


def _get_run_ids(context):
    return common.get_params_from_xcom(
        context,
        TASK_ID_FOR_PARAMS,
        ('etl_run_id', 'etl_proc_id'),
    ).values()


# Логирование выполнения операций со справочниками в собственную таблицу (на данный момент не используется)
def _log_execute(procname: str, **kwargs):
    from utils.logs_meta import functions

    pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            functions.change_table(
                table='meta.log_dwh',
                action='INSERT',
                cur=cur,
                sql_params={
                    'log_id': functions.get_nextval_for_sequence('meta.log_dwh_seq', cur),
                    'procname': procname,
                    'sysdate': 'NOW()',
                    'description': 'done, row count: ' + str(cur.rowcount),
                    **kwargs,
                }
            )


# Функция, которая выполняется в случае неудачного выполнения запроса. Логирует ошибку.
def etl_proc_failure_function(context):
    run_id, proc_id = _get_run_ids(context)
    dag_error = str(context.get('exception')).partition('\n')[0].replace("'", '')
    sql_params = {
        'log_info': dag_error,
        'etl_proc_id': proc_id,
        'etl_run_id': run_id,
    }
    _finish_log(sql_params, logger.TableAction.failure)


# Преобразовать TABLES_LIST в NSI_DICTIONARY
def create_nsi_rmz_dictionary():
    for nsi_rmz_tables in TABLES_LIST:
        if not ("nsi_stage_" + str(TABLES_LIST[nsi_rmz_tables]['sort_gr']) + "_dict" in NSI_DICTIONARY):
            NSI_DICTIONARY["nsi_stage_" + str(TABLES_LIST[nsi_rmz_tables]['sort_gr']) + "_dict"] = {}
        NSI_DICTIONARY["nsi_stage_" + str(TABLES_LIST[nsi_rmz_tables]['sort_gr']) + "_dict"][
            TABLES_LIST[nsi_rmz_tables]['table_name']] = {"dict_id": int(TABLES_LIST[nsi_rmz_tables]['dict_id']),
                                                          "dict_name": "..."}


# Вспомогательная функция, получаем имя подсказки для таски из двух строк
def get_group_tooltip(action: str, name: str) -> str:
    return f'Выполнение "{action}" для всех справочников [{name}]'


# Начальная таска: инициализация лога и получение начальной информации из БД
@task(task_id=TASK_ID_FOR_PARAMS)
def log_init():
    from utils.logs_meta.log_tables import (
        log_etl_run,
        log_etl_proc,
        log_etl_proc_table
    )

    pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            etl_run_id = logger.log_etl_run(logger.TableAction.get_new_id, cur=cur)
            etl_proc_id = logger.log_etl_proc(
                logger.TableAction.get_new_id,
                {
                    'etl_run_id': etl_run_id,
                    'dag_type': 'etl_nsi_rmz_logging',
                    'mt_etl_proc_id': 8,
                },
                cur=cur,
            )

            nsi_ids = {}
            flat_nsis = get_flat_nsis(NSI_DICTIONARY)
            for f_nsi_name, f_nsi_params in flat_nsis.items():
                sql_statement = common.get_file_content(
                    SQL_PATH + '/dynamic_sql/nsi_get_last_update_date.sql')
                rendered_sql_statements = render_jinja(sql_statement,
                                                       {'table_name_ods': TABLES_LIST[f_nsi_name]['tab_oid']})
                for dynamic_sql in _execute_sql(sql_statement=rendered_sql_statements):
                    last_update_date = dynamic_sql['last_update_date']

                nsi_ids[f_nsi_name] = {
                    'mt_tbl_id': f_nsi_params['dict_id'],
                    'new_log_tbl_id': log_etl_proc_table(
                        TableAction.get_new_id,
                        {
                            'etl_proc_id': etl_proc_id,
                            'mt_tbl_id': f_nsi_params['dict_id'],
                        },
                        cur=cur),
                    'table_name_ods': TABLES_LIST[f_nsi_name]['tab_oid'],
                    'last_update_date': last_update_date,
                }

            to_save = {
                'etl_run_id': etl_run_id,
                'etl_proc_id': etl_proc_id,
                'all_tbl_ids': nsi_ids,
            }

    common.xcom_save(get_current_context(), to_save, with_log=True)


# Таска: stage
@task
def stage(tables_meta):
    if tables_meta['full_sql_new_id'] is not None:
        if os.path.exists(
                SQL_PATH + '/' + tables_meta['table_name'] + '/' + tables_meta['table_name'] + '_stage_1.sql'):
            sql_statement = common.get_file_content(
                SQL_PATH + '/' + tables_meta['table_name'] + '/' + tables_meta['table_name'] + '_stage_1.sql')
            rendered_sql_statements = render_jinja(sql_statement, tables_meta)
            logging.info('*********************************** STAGE1 START ***********************************')
            logging.info(rendered_sql_statements)
            logging.info('*********************************** STAGE1 END   ***********************************')
            _execute_sql(sql_statement=rendered_sql_statements)
        if os.path.exists(
                SQL_PATH + '/' + tables_meta['table_name'] + '/' + tables_meta['table_name'] + '_stage_2.sql'):
            sql_statement = common.get_file_content(
                SQL_PATH + '/' + tables_meta['table_name'] + '/' + tables_meta['table_name'] + '_stage_2.sql')
            rendered_sql_statements = render_jinja(sql_statement, tables_meta)
            logging.info('*********************************** STAGE1 START ***********************************')
            logging.info(rendered_sql_statements)
            logging.info('*********************************** STAGE1 END   ***********************************')
            _execute_sql(sql_statement=rendered_sql_statements)
    else:
        for script_name in ['nsi_stage_create_1.sql', 'nsi_stage_drop_2.sql', 'nsi_stage_create_2.sql']:
            sql_statement = common.get_file_content(SQL_PATH + '/dynamic_sql/' + script_name)
            rendered_sql_statements = render_jinja(sql_statement, tables_meta)
            logging.info('*********************************** STAGE1 START ***********************************')
            logging.info(rendered_sql_statements)
            logging.info('*********************************** STAGE1 END   ***********************************')
            _execute_sql(sql_statement=rendered_sql_statements)


@task
def update(tables_meta):
    from airflow.exceptions import AirflowFailException
    from utils.logs_meta.log_tables import log_etl_proc_table

    context = get_current_context()
    run_id, proc_id = _get_run_ids(get_current_context())
    xparams = common.get_table_params_from_xcom(context=context, from_task_id=TASK_ID_FOR_PARAMS,
                                                tbl_name=tables_meta['table_name'])
    log_params = {
        'table_name': tables_meta['table_name'],
        'new_log_tbl_id': xparams['tbl_ids']['new_log_tbl_id'],
        'etl_proc_id': xparams['etl_proc_id'],
    }

    dynamic_sql_builder_statement = common.get_file_content(SQL_PATH + '/dynamic_sql/nsi_update.sql')
    rendered_sql_statement = render_jinja(dynamic_sql_builder_statement, tables_meta)
    dynamic_sql = next(_execute_sql(rendered_sql_statement))
    sql_statement = dynamic_sql['sql_txt']

    # Если есть статический update-файл, то он имеет приоритет
    if os.path.exists(SQL_PATH + '/' + tables_meta['table_name'] + '/' + tables_meta['table_name'] + '_update.sql'):
        sql_statement = render_jinja(common.get_file_content(
            SQL_PATH + '/' + tables_meta['table_name'] + '/' + tables_meta['table_name'] + '_update.sql'), tables_meta)

    logging.info('******************************** UPDATE SQL START ********************************')
    logging.info(sql_statement)
    logging.info('******************************** UPDATE SQL END   ********************************')

    try:
        postgres = PostgresHook(postgres_conn_id=CONN_ID)
        log_params['dag_action'] = 'update'
        with postgres.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_statement)
                postgres.log.info("Main query completed successfully, affected rows: %s", cur.rowcount)
                log_params['affected_rows'] = cur.rowcount
                log_etl_proc_table(TableAction.progress, log_params, cur=cur)
    except Exception as e:
        _log_execute(procname=tables_meta['table_name'], description=f'run_id={run_id}; proc_id={proc_id} - ошибка',
                     err=str(e), sql_txt=sql_statement, )
        raise AirflowFailException(
            'Ошибка при переносе справочника: таблица: ' + tables_meta['table_name'] + f' "{e}"', ) from e


@task
def insert(tables_meta):
    from airflow.exceptions import AirflowFailException
    from utils.logs_meta.log_tables import log_etl_proc_table

    context = get_current_context()
    run_id, proc_id = _get_run_ids(get_current_context())

    xparams = common.get_table_params_from_xcom(context=context, from_task_id=TASK_ID_FOR_PARAMS,
                                                tbl_name=tables_meta['table_name'])

    log_params = {
        'table_name': tables_meta['table_name'],
        'new_log_tbl_id': xparams['tbl_ids']['new_log_tbl_id'],
        'etl_proc_id': xparams['etl_proc_id'],
    }
    # nsi_insert.sql - файл, содержащий динамический код для формирования sql-выражения INSERT.
    dynamic_sql_builder_statement = common.get_file_content(SQL_PATH + '/dynamic_sql/nsi_insert.sql')
    # В качестве параметра передается имя таблиц в dds и ods соответствующие параметрам {{ table_name }} и {{ table_name_ods }}
    # Используется механизм распаковки словаря ** для добавления нового ключа 'table_name_ods' в существующий словарь
    # Используем функцию render_jinja для замены значений переменных
    rendered_sql_statement = render_jinja(dynamic_sql_builder_statement, {**tables_meta, 'table_name_ods':
        TABLES_LIST[tables_meta['table_name']]['tab_oid']})
    # Выполняем полученное SQL-выражение для динамической генерации текста INSERT
    # Ожидаем, что вернётся только одна строка с текстом INSERT-а, поэтому используем функцию next для получения первого значения генератора, который возращает функция _execute_sql
    dynamic_sql = next(_execute_sql(rendered_sql_statement))
    # Присваиваем текст выражения INSERT переменной для дальнейшего выполнения
    sql_statement = dynamic_sql['sql_txt']

    # Если есть статический insert-файл, то он имеет приоритет
    if os.path.exists(SQL_PATH + '/' + tables_meta['table_name'] + '/' + tables_meta['table_name'] + '_insert.sql'):
        sql_statement = render_jinja(common.get_file_content(
            SQL_PATH + '/' + tables_meta['table_name'] + '/' + tables_meta['table_name'] + '_insert.sql'), tables_meta)

    logging.info('******************************** INSERT SQL START ********************************')
    logging.info(sql_statement)
    logging.info('******************************** INSERT SQL END   ********************************')

    # Выполнение INSERT-выражения
    try:
        postgres = PostgresHook(postgres_conn_id=CONN_ID)
        log_params['dag_action'] = 'insert'
        with postgres.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_statement)
                postgres.log.info("Main query completed successfully, affected rows: %s", cur.rowcount)
                log_params['affected_rows'] = cur.rowcount
                log_etl_proc_table(TableAction.progress, log_params, cur=cur)
    except Exception as e:
        _log_execute(procname=tables_meta['table_name'], description=f'run_id={run_id}; proc_id={proc_id} - ошибка',
                     err=str(e), sql_txt=sql_statement, )
        raise AirflowFailException(
            'Ошибка при переносе справочника: таблица: ' + tables_meta['table_name'] + f' "{e}"', ) from e

    # Записываем дату последнего обновления для таблицы
    log_etl_params = common.get_params_from_xcom(
        get_current_context(),
        TASK_ID_FOR_PARAMS,
        ('etl_run_id', 'etl_proc_id', 'all_tbl_ids')
    )
    sql_statement = common.get_file_content(SQL_PATH + '/dynamic_sql/nsi_upd_last_update_date.sql')
    rendered_sql_statement = render_jinja(sql_statement, {'table_name_dds': tables_meta['table_name'],
                                                          'nsi_last_update_date':
                                                              log_etl_params['all_tbl_ids'][tables_meta['table_name']][
                                                                  'last_update_date']})
    _execute_sql(sql_statement=rendered_sql_statement)


# Таска: завершение
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


#    # Записываем дату последнего обновления для всех таблиц
#    for nsi in log_etl_params['all_tbl_ids']:
#        sql_statement = common.get_file_content(SQL_PATH + '/dynamic_sql/nsi_upd_last_update_date.sql')
#        rendered_sql_statements = render_jinja(sql_statement, {'table_name_dds': nsi, 'nsi_last_update_date' : log_etl_params['all_tbl_ids'][nsi]['last_update_date']})
#        _execute_sql(sql_statement=rendered_sql_statements)

# Описание DAG-а
@dag(
    start_date=pendulum.datetime(2023, 1, 29, tz='Europe/Moscow'),
    schedule=None,
    ### schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    default_args={'on_failure_callback': etl_proc_failure_function},
    description='Перенос справочников в DDS.',
    tags=['dds', 'nsi'],
)
# Главная функция DAG-а
def etl_nsi_dds():
    metadata_sql_statement = common.get_file_content(SQL_PATH + '/get_required_meta.sql')
    for tables_meta in _execute_sql(sql_statement=metadata_sql_statement):
        TABLES_LIST[tables_meta['table_name']] = tables_meta

    create_nsi_rmz_dictionary()

    with TaskGroup(f'all_tasks') as tg_action:
        prev_nsi_gr = None
        for group_name, nsi_object in NSI_DICTIONARY.items():
            # для каждой группы справочников. например:
            # group_name = "nsi_stage_0_dict"
            # nsi_object = {"XXX": {"dict_id": XXX, "dict_name": "XXX"},

            with TaskGroup(
                    group_id=f'tasks_{group_name}',
                    tooltip=get_group_tooltip('all_tasks', group_name)
            ) as tg:
                for nsi_name, _ in nsi_object.items():
                    tg_stage = stage.override(task_id=nsi_name)(TABLES_LIST[nsi_name])
                    tg_update = update.override(task_id=nsi_name)(TABLES_LIST[nsi_name])
                    tg_insert = insert.override(task_id=nsi_name)(TABLES_LIST[nsi_name])

                    tg_stage >> tg_update >> tg_insert

            if prev_nsi_gr:
                prev_nsi_gr >> tg
            prev_nsi_gr = tg

    log_init() >> tg_action >> finish_all()


etl_nsi_dds()
