import re
from pathlib import Path
import pendulum
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

# from utils.logs_meta.extra import get_table_meta_id
from utils import common
from utils.debug_cursor import DebugCursor
from utils.logs_meta import shared
from utils.logs_meta.log_tables import TableAction
from utils.clean_files import run_if_exist


DESCRIPTION = '''
Даг формирует поток данных ЕРЗЛ.

Описание процесса: [wiki]\
(https://wiki.element-lab.ru/pages/viewpage.action?pageId=34287282)
'''

PROCESS_GROUPS = ['SDE', 'SIL', 'PLP']

SQL_FILES_DIR = common.get_airflow_home_path() + '/sql/erzldma'
TASK_ID_FOR_PARAMS = 'initialize_all'
DB_CONN_ID = 'nds_dma'
MT_ETL_PROC_ID = 7

default_args = {
    'on_failure_callback': shared.get_on_failure_function(
        tables_to_finish=('log_etl_proc', 'log_etl_proc_table'),
        xcom_keys=('etl_run_id', 'etl_proc_id', 'all_tbl_ids'),
    )
}


@task()
def process_step(file_name: str):
    """ Обработка процесса """

    # from utils.logs_meta.log_tables import log_etl_proc_table

    context = get_current_context()
    # xparams = common.get_table_params_from_xcom(
    #     context=context,
    #     from_task_id=TASK_ID_FOR_PARAMS,
    #     tbl_name=file_name)
    proc_id = context['ti'].xcom_pull(
        task_ids=TASK_ID_FOR_PARAMS, key='etl_proc_id')

    # нет файла pg_params.sql
    sql_params_file_path = SQL_FILES_DIR + "/pg_params.sql"
    params_content = common.get_file_content(sql_params_file_path)

    dag_params = context['params']
    sql_main_query_file_path = f"{SQL_FILES_DIR}/{file_name}"

    rendered_sql = common.get_rendered_file_content(
        context,
        sql_main_query_file_path,
        {'params': dag_params}
    )

    sql_result_script = '\n'.join([
        params_content,
        rendered_sql,
    ])

    log_params = {
        # 'dag_action': action, # NOTE: пока неоткуда взять
        'table_name': file_name,
        # 'new_log_tbl_id': xparams['tbl_ids']['new_log_tbl_id'],
        'etl_proc_id': proc_id,
    }

    postgres = PostgresHook(postgres_conn_id=DB_CONN_ID)
    common.log_it({
        "Current params": "",
        **log_params,
        # 'sql_params_file_path': sql_params_file_path,
        'sql_main_query_file_path': sql_main_query_file_path,
        "SQL query to execute": '\n' + sql_result_script
    }, postgres.log.info)

    with postgres.get_conn() as conn:
        # DebugCursor - не запускает файлы, а выводит SQL в лог задачи
        # для выполнения - убрать `cursor_factory=DebugCursor`
        with conn.cursor(cursor_factory=DebugCursor) as cur:
            run_if_exist(
                file_path=sql_main_query_file_path,
                context=context,
                cur=cur,
                log_method=postgres.log.info)

            cur.execute(sql_result_script)
            postgres.log.info(
                "Main query completed successfully, affected rows: %s",
                cur.rowcount)

            log_params['affected_rows'] = cur.rowcount
            # NOTE: пока без логирования уровня таблиц
            # log_etl_proc_table(TableAction.progress, log_params, cur=cur)


@task(task_id='test_task')
def test_task(file_name: str):
    print(f"Тестовая задача, имя файла: [{file_name}]")


@task(task_id=TASK_ID_FOR_PARAMS)
def init_all():
    """
    Получает из базы новые id сохраняет в XCOM.
    Структуру см. в описании `shared.init_all(...)`
    """

    context = get_current_context()
    dag_params = context['params']

    if dag_params['etl_run_id'] == -1:
        return  # NOTE: на время отладки

    log_ids = {
        'etl_run_id': dag_params['etl_run_id'],
        'etl_proc_id': None,
        # 'all_tbl_ids': my_tables  - пока таблицы не инициализируем
    }
    log_ids = shared.init_all(log_ids, context, MT_ETL_PROC_ID)
    common.xcom_save(context, log_ids, with_log=True)


@task(task_id='finish_all_log_tables')
def finish_all():
    log_etl_params = common.get_params_from_xcom(
        get_current_context(),
        TASK_ID_FOR_PARAMS,
        ('etl_run_id', 'etl_proc_id', 'all_tbl_ids')
    )
    if log_etl_params['etl_run_id'] == -1:
        return  # NOTE: на время отладки

    shared.finish_partial(
        tables_to_finish=('log_etl_proc', 'log_etl_proc_table'),
        log_etl_params=log_etl_params,
    )


@dag(
    start_date=pendulum.datetime(2023, 1, 1),
    catchup=False,
    schedule=None,
    description=DESCRIPTION.splitlines()[0],
    doc_md=DESCRIPTION,
    tags=['dev', 'erzl', 'dma'],
    params={"clean_temp_tables": True, "etl_run_id": -1, "period_dt": None},
    default_args=default_args
    # так же в параметры прилетают etl_run_id и period_dt из головного
)
def dds_etl_erzl_dma():

    sql_entries_dir = Path(SQL_FILES_DIR).rglob('*.sql')  # только sql
    script_list = []
    order_pattern = re.compile(r'_(\d+)_')
    # файлы формата: {группа}_{порядок}_{имя_скрипта}.sql
    #   группа: значение из PROCESS_GROUPS
    #   порядок: целое число
    #   имя_скрипта: не обрабатывается

    for file in sql_entries_dir:

        # определяем группу файла
        match_group = [pg for pg in PROCESS_GROUPS if file.name.startswith(pg)]
        if not match_group:
            continue    # другие файлы пропускаем

        group_cd = match_group[0]
        order_search = order_pattern.search(file.name)
        # TODO: файл без цифр должен быть первым или последним?
        order_num = int(order_search.group(1)) if order_search else 0

        # создаем лист всех файлов в порядке номера шага с доп атрибутами
        script_list.append({
            'name': file.name,
            'group': group_cd,
            'order': order_num
        })

    # script_list.sort(key=lambda e: (e['group'], e['order']))

    prev_step = init_all()
    # Пробегаем по группа процессов
    for pg in PROCESS_GROUPS:
        with TaskGroup(group_id=pg) as tg_process:
            group_orders = set(x['order'] for x in script_list
                               if x['group'] == pg)

            prev_group = None
            for order in group_orders:
                with TaskGroup(group_id=f"{pg}_{order}") as order_tg:
                    scripts = [x for x in script_list
                               if x['group'] == pg and x['order'] == order]
                    for script in scripts:
                        process_step.override(
                            task_id=f"{script['name']}")(
                            file_name=script['name'])
                if prev_group:
                    prev_group >> order_tg
                prev_group = order_tg
        prev_step >> tg_process
        prev_step = tg_process

    prev_step >> finish_all()


dds_etl_erzl_dma()
