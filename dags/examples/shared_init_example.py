""" по смыслу аналогичен dags/examples/manual_init_example.py
только тут используются более удобные методы
"""

import pendulum

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context

from utils import common    # просто общие полезные функции
# Airflow Task для удаление временных таблиц
from utils.clean_files import cleanup_temp_tables_task
# универсальные функции внутреннего логирования
from utils.logs_meta import shared
from utils.sql_operator import CustomSQLOperator

MY_TABLES = {   # список таблиц для обработки и их параметры
    'ex_table1': {  # только одна таблица для примера
        'mt_tbl_id': 123,   # id таблицы в схеме FIXME: это случайные цифры
        'my_param': 'param value'   # параметр для Jinja шаблона
    }
}
SQL_FILES_DIR = (       # директория, в которой находятся наши sql файлы
    common.get_sql_home_path() +
    # имя папки с sql файлами в идеале должно совпадать с dag_id
    '/dataflows/examples/manual_init_example'
)

# task_id - ид инициализирующего шага, в котором будут сохранены ид процессов в мете
TASK_ID_FOR_PARAMS = 'initialize_all'
CONN_ID = 'nds_dds'     # используемый Airflow Connection Id в одном месте
MT_ETL_PROC_ID = 1  # id типа для этого дага во внутренней системе логирования

# функцию можно задавать и без переменной, тут сделано для наглядности
_failure_function = shared.get_on_failure_function(
    tables_to_finish=('log_etl_run', 'log_etl_proc', 'log_etl_proc_table'),
    xcom_keys=('etl_run_id', 'etl_proc_id', 'all_tbl_ids')
)


@task()
def process_table(action: str, table_name: str, table_params: dict):
    """ Обработка таблицы """

    context = get_current_context()     # рекомендуемый метод получения контекста

    file_key = 'ERROR__file_key__'
    if action == 'stage':
        file_key = 'stage'

    sql_main_query_file_path = f"{table_name}/{table_name}_{file_key}.sql"

    template_vars = {
        'table_name': table_name,
        **table_params
    }

    # этот оператор выполняет обработку Jinja-шаблонов в
    # параметрах (params) и в SQL файлах, после чего выполняет SQL
    sql_operator = CustomSQLOperator(
        task_id=f'sql_op_{action}_{table_name}',
        handler=lambda cur: cur.rowcount,
        conn_id=CONN_ID,
        sql=sql_main_query_file_path,
        log_sql=True,
        params=template_vars,
        clean_file_check_path=sql_main_query_file_path
        # ^ не нужно отдельно ссылаться на файл <name>_clean.sql, тут путь к основному
    )
    rowcount = sql_operator.execute(context)

    log_progress(rowcount, action,
                 table_params['mt_tbl_id'], table_name, context)


def log_progress(rowcount: int, action: str, mt_tbl_id: int, tbl_name: str, context):
    """ Подготовка параметров и выполнение логирования прогресса
    по таблице (метод `log_etl_proc_table`) """
    from utils.logs_meta.log_tables import log_etl_proc_table, TableAction

    xparams = common.get_table_params_from_xcom(
        context=context,
        from_task_id=TASK_ID_FOR_PARAMS,
        tbl_name=tbl_name
    )

    if action not in ('update', 'insert', 'delete'):
        return  # логируем только с этими значениями

    log_params = {
        'dag_action': action,
        'table_name': tbl_name,
        'etl_proc_id': xparams['etl_proc_id'],
        'new_log_tbl_id': xparams['tbl_ids']['new_log_tbl_id'],
        # 'log_info': step_args.get('info'),
        'mt_tbl_id': mt_tbl_id,
        'affected_rows': rowcount
    }
    log_etl_proc_table(TableAction.progress, log_params, conn_id=CONN_ID)


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

    context = get_current_context()
    dag_params = context['params']

    my_tables = {
        table_name: {
            'mt_tbl_id': table_params['mt_tbl_id'],
        }
        for table_name, table_params in MY_TABLES.items()
    }
    log_ids = {
        # если run_id есть в параметрах - новый не будет создаваться
        'etl_run_id': dag_params.get('etl_run_id'),
        'etl_proc_id': None,
        'all_tbl_ids': my_tables
    }
    log_ids = shared.init_all(log_ids, context, MT_ETL_PROC_ID)
    common.xcom_save(context, log_ids, with_log=True)


@task(task_id='finish_all_log_tables')
def finish_all():
    """
    Процесс, аналогичный `init_all`, только наоборот:
    получаем все id, которые сохраняли в xcom,
    записываем в базу успешное завершение
    """
    log_etl_params = common.get_params_from_xcom(
        get_current_context(),
        TASK_ID_FOR_PARAMS,
        ('etl_run_id', 'etl_proc_id', 'all_tbl_ids')
    )
    shared.finish_all(
        log_etl_params=log_etl_params
    )


@dag(
    start_date=pendulum.local(2022, 1, 1),
    catchup=False,
    schedule=None,
    description=(
        "методы из utils.logs_meta.shared (см DAG Docs)"
    ),
    doc_md="""
    Пример DAG'а обработки таблиц с упрощённым использованием
    встроенной системы логирования.

    Кроме того в этом DAG'е используется оператор ``CustomSQLOperator``
    для выполнения SQL
    
    """,
    tags=['example'],
    template_searchpath=SQL_FILES_DIR,
    # параметр `clean_temp_tables` нужен для функции `cleanup_temp_tables_task`, значение "True" - удалить временные таблицы
    params={"clean_temp_tables": True},
    default_args={
        'on_failure_callback': _failure_function
    }
)
def shared_init_example():  # имя этой функции = dag_id

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

    prev_action >> finish_all() >> cleanup_temp_tables_task()


shared_init_example()
