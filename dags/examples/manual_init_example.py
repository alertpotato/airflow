import pendulum

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils import common    # просто общие полезные функции
from utils.logs_meta.log_tables import (    # внутренее логирование
    TableAction as act,  # действия таблиц (для удобства и наглядности)
    # статус цельного процесса (в котором можно быть несколько дагов)
    log_etl_run,
    log_etl_proc,       # статус конкретного дага
    log_etl_proc_table  # статус для каждой таблицы
)
# Методы для работы с sql файлами для удаления временных таблиц
from utils import clean_files


MY_TABLES = {   # список таблиц для обработки и их параметры
    'ex_table1': {
        'mt_tbl_id': 123,   # id таблицы в схеме FIXME: это случайные цифры
        'my_param': 'param value'   # параметр для Jinja шаблона
    }
}
SQL_FILES_DIR = (       # директория, в которой находятся наши sql файлы
    common.get_sql_home_path() +
    '/dataflows/examples/manual_init_example'  # имя папки с sql файлами в идеале должно совпадать с dag_id
)
# task_id - ид инициализирующего шага, в котором будут сохранены ид процессов в мете
TASK_ID_FOR_PARAMS = 'initialize_all'
CONN_ID = 'nds_dds'     # используемый Airflow Connection Id в одном месте


@task()
def process_table(action: str, table_name: str, table_params: dict):
    """ Обработка таблицы """

    context = get_current_context()     # рекомендуемый метод получения контекста

    # параметры из xcom, сохранённые в задаче с task_id = TASK_ID_FOR_PARAMS для текущей таблицы:
    xparams = common.get_table_params_from_xcom(
        context=context,
        from_task_id=TASK_ID_FOR_PARAMS,
        tbl_name=table_name)

    file_key = 'ERROR__file_key__'
    if action == 'stage':
        file_key = 'stage'

    sql_main_query_file_path = '/'.join([
        SQL_FILES_DIR,
        table_name,
        f"{table_name}_{file_key}.sql"
    ])

    template_vars = {
        'table_name': table_name,
        **table_params
    }
    query = common.get_rendered_file_content(    # обработка Jinja шаблона
        context, sql_main_query_file_path, template_vars)

    log_params = {
        'dag_action': action,
        'table_name': table_name,
        'new_log_tbl_id': xparams['tbl_ids']['new_log_tbl_id'],
        'etl_proc_id': xparams['etl_proc_id'],
    }

    # Airflow Hook для Postgres
    pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
    common.log_it({
        "Current params": "",
        **log_params,
        'sql_main_query_file_path': sql_main_query_file_path,
        "SQL query to execute": '\n' + query
    }, pg_hook.log.info)    # удобно, что в хуке есть логгер

    with pg_hook.get_conn() as conn:    # подклчение к базе с контекст-менеджером
        with conn.cursor() as cur:  # курсор с контекст-менеджером

            # не нужно отдельно ссылаться на файл <name>_clean.sql, вызываем метод для основного файла:
            clean_files.run_if_exist(
                file_path=sql_main_query_file_path,
                context=context,
                cur=cur,    # в частности передаём открытый курсор
                jinja_args=template_vars,   # не забыть передать параметры шаблона, если они есть
                log_method=pg_hook.log.info)

            cur.execute(query)  # выполнение основного запроса
            pg_hook.log.info(   # выведем в лог количество обработанных строк
                "Main query completed successfully, affected rows: %s",
                cur.rowcount)

            # а так же оно нужно далее
            log_params['affected_rows'] = cur.rowcount

            # обновим статус прогресса для текущей таблицы
            log_etl_proc_table(act.progress, log_params, cur=cur)


def failure_function(context):
    """
    Если упали с ошибкой: запишем всё, что знаем
    """

    run_id, proc_id, all_tbl_ids = (
        common.get_params_from_xcom(
            context,
            TASK_ID_FOR_PARAMS,
            ('etl_run_id', 'etl_proc_id', 'all_tbl_ids')
        ).values()
    )
    dag_error = str(context.get('exception')
                    ).partition('\n')[0].replace("'", '')

    sql_params = {
        'log_info': dag_error,
        'etl_proc_id': proc_id,
        'etl_run_id': run_id,
    }

    postgres = PostgresHook(postgres_conn_id=CONN_ID)
    with postgres.get_conn() as conn:
        with conn.cursor() as cur:

            log_etl_run(act.failure, sql_params, cur=cur)
            log_etl_proc(act.failure, sql_params, cur=cur)

            for _, ids in all_tbl_ids.items():
                log_etl_proc_table(
                    act.failure,
                    {
                        **sql_params,
                        'new_log_tbl_id': ids['new_log_tbl_id'],
                        'mt_tbl_id': ids['mt_tbl_id'],
                    },
                    cur=cur,
                )


def prepare_task(action: str, table_name: str, table_params: dict):
    """ Подготовка задач

    этот метод используется для повышения читабельности дага
    """

    action_task_id = f'{table_name}__{action}'

    if action in ('stage', 'update', 'insert'):
        process_table.override(
            task_id=action_task_id)(action, table_name, table_params)


@task(task_id=TASK_ID_FOR_PARAMS)
def init_all():
    """
    Получает из базы новые id для
    - log_etl_run
    - log_etl_proc
    - log_etl_proc_table (для каждой таблицы в `MY_TABLES`)

    И записывает в XCOM такую структуру:

    ```json
    {
        "etl_run_id": 123,
        "etl_proc_id": 456,
        "all_tbl_ids": {
            "example_table1": {
                "mt_tbl_id": 0,
                "new_log_tbl_id": 7777
            },
        }
    }```
    """

    postgres = PostgresHook(postgres_conn_id=CONN_ID)
    with postgres.get_conn() as conn:
        with conn.cursor() as cur:

            etl_run_id = log_etl_run(act.get_new_id, cur=cur)
            etl_proc_id = log_etl_proc(
                act.get_new_id,
                {
                    'etl_run_id': etl_run_id,
                    'mt_etl_proc_id': '1',
                },
                cur=cur)
            table_ids = {}

            for table_name, table_params in MY_TABLES.items():
                table_ids[table_name] = {
                    'mt_tbl_id': table_params['mt_tbl_id'],
                    'new_log_tbl_id': log_etl_proc_table(
                        act.get_new_id,
                        {
                            'etl_proc_id': etl_proc_id,
                            'mt_tbl_id': table_params['mt_tbl_id'],
                        },
                        cur=cur)
                }

            to_save = {
                'etl_run_id': etl_run_id,
                'etl_proc_id': etl_proc_id,
                'all_tbl_ids': table_ids,
            }

    common.xcom_save(get_current_context(), to_save, with_log=True)


@task(task_id='finish_all_log_tables')
def finish_all():
    """
    Процесс, аналогичный `init_all`, только наоборот:
    получаем все id, которые сохраняли в xcom,
    записываем в базу успешное завершение
    """
    run_id, proc_id, all_tbl_ids = (
        common.get_params_from_xcom(
            get_current_context(),
            TASK_ID_FOR_PARAMS,
            ('etl_run_id', 'etl_proc_id', 'all_tbl_ids')
        ).values()
    )
    sql_params = {
        'etl_proc_id': proc_id,
        'etl_run_id': run_id,
    }

    postgres = PostgresHook(postgres_conn_id=CONN_ID)
    with postgres.get_conn() as conn:
        with conn.cursor() as cur:

            log_etl_run(act.finish, sql_params, cur=cur)
            log_etl_proc(act.finish, sql_params, cur=cur)

            for _, ids in all_tbl_ids.items():
                log_etl_proc_table(
                    act.finish,
                    {
                        **sql_params,
                        'new_log_tbl_id': ids['new_log_tbl_id'],
                        'mt_tbl_id': ids['mt_tbl_id'],
                    },
                    cur=cur,
                )


@dag(
    start_date=pendulum.local(2022, 1, 1),
    catchup=False,
    schedule=None,
    description=(
        "Пример DAG'а обработки таблиц с прямым использованием функций "
        "системы логирования utils.logs_meta.log_tables"
    ),
    tags=['example'],
    # параметр `clean_temp_tables` нужен для функции
    # `cleanup_temp_tables_task`, значение "True" - удалить временные таблицы
    params={"clean_temp_tables": True},
    default_args={
        'on_failure_callback': failure_function
    }
)
def manual_init_example():  # имя этой функции = dag_id

    actions = [
        'stage',
        # 'dq',
        # 'update',
        # 'insert'
    ]

    prev_action = init_all()

    for action in actions:
        # для каждого действия (на. stage > dq > update ...)

        with TaskGroup(f'{action}') as tg_action:

            for table_name, table_params in MY_TABLES.items():
                # для каждой таблицы и её параметров. например:
                # table_name = XXX
                # table_parms = {"mt_tbl_id": XXX, "my_param": "XXX"}

                prepare_task(action, table_name, table_params)

        prev_action >> tg_action
        prev_action = tg_action

    prev_action >> finish_all() >> clean_files.cleanup_temp_tables_task()


manual_init_example()
