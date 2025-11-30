""" Методы для работы с sql файлами для удаления временных таблиц

Раньше были только файлы с инструкциями DROP,
позже добавились TRUNCATE и т.д., по этому теперь везде слово "clean"
"""

from __future__ import annotations

import logging
from os.path import splitext
from typing import TYPE_CHECKING

from psycopg2 import errors
from psycopg2.errorcodes import UNDEFINED_TABLE

from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.common import get_file_content, get_rendered_file_content, xcom_save
from utils.logs_meta.functions import ensure_connection

if TYPE_CHECKING:
    from typing import Callable

    from psycopg2.extensions import cursor

    from airflow.utils.context import Context


DEFAULT_CONN_ID = 'nds_dds'

PARAM_NAME = 'clean_temp_tables'
FILES_XCOM_KEY = 'clean_files'
FILES_NAME_SUFFIX = '_clean'
LOG_PREFIX = '[clean-files]: '
LOG_FORMAT = "%(p)sВыполнение SQL [%(i)s/%(cnt)s]:\n%(sql)s%(nt)s%(dots)s"


def _exec_with_error_handler(cur: cursor, sql: str) -> bool:
    """ [внутренний метод]
    Выполняет запрос `sql` в курсоре `cur`, при успехе возвращает `True`

    Замалчивает ошибку, если таблица не существует и возвращает `False`
    """
    try:
        cur.execute(sql)
        cur.connection.commit()
        return True
    except errors.lookup(UNDEFINED_TABLE):  # таблица не существует
        cur.connection.commit()
    except Exception as e:
        raise Exception(
            f"{LOG_PREFIX}Ошибка при запуске:\n{sql}") from e
    return False


def _is_need_to_clean_all(context: Context):
    """ Возвращает значение `PARAM_NAME` из параметров DAG
     или False. """
    return ('params' in context
            and context['params'].get(PARAM_NAME, False))


@ensure_connection
def run_if_exist(file_path: str,
                 context: Context,
                 cur: cursor | None = None,
                 log_method: Callable | None = None,
                 jinja_args: dict | None = None):
    """
    Обработчик clean-файлов (файлов очистки)
    ---
    - Проверяет существование файла `<name>_clean.sql` (из `file_path`),
    если такой не найден - молча пропускает.
    - Обрабатывает как Jinja шаблон, если требуется.
    - Выполняет содержимое файла. Выводит сообщение при успехе,
    замалчивает ошибку, если таблицы нет.

    Args:
        file_path (str): путь к проверяемому файлу (это не clean-файл)

        log_method (Callable): метод для вывода сообщений

        jinja_args (dict): [не обязательный] аргументы для обработки
        содержимого файла, если это Jinja шаблон
    """

    if TYPE_CHECKING:
        assert cur

    file_name, file_extension = splitext(file_path)
    clean_file_name = file_name + FILES_NAME_SUFFIX + file_extension

    try:
        content = get_file_content(clean_file_name, context)
    except FileNotFoundError:
        return  # выходим если файл найти не удалось

    if "{{" in content:
        if not jinja_args:
            raise ValueError(f"Файл [{clean_file_name}] похож на Jinja-шаблон"
                             ", но 'jinja_args' пуст")
        sql = context['task'].render_template(
            content,
            context=jinja_args  # type: ignore (дальше там cast)
        )
    else:
        sql = content

    # проверяем наличие параметра PARAM_NAME в контексте DAG'а
    # если есть и его значение 'True', то сохраняет в xcom путь к
    # каждому файлу и jinja_args, если они были
    if _is_need_to_clean_all(context):
        x: dict[str, dict[str, str | dict]] = {
            FILES_XCOM_KEY: {
                'file_path': clean_file_name
            }
        }
        if jinja_args:
            x[FILES_XCOM_KEY]['jinja_args'] = jinja_args
        xcom_save(context, x)

    if _exec_with_error_handler(cur, sql):
        if not log_method:
            log_method = logging.getLogger('airflow.task').info
        log_method("%(p)sУспешно выполнен запрос:\n%(sql)s\n"
                   "из файла [%(file)s]",
                   {
                       'p': LOG_PREFIX,
                       'sql': sql,
                       'file': clean_file_name
                   })


@task(task_id='clean_all_temp_tables')
def cleanup_temp_tables_task(conn_id: str | None = None):
    """
    Выполняет все clean-файлы, которые были обработаны при выполнении
    этого DAG.

    Это выполняется только если в параметрах DAG есть
    `PARAM_NAME` и его значение 'True'
    """

    context = get_current_context()
    if not _is_need_to_clean_all(context):
        # у DAG нет параметров или функция вызвана, но не включена
        logging.getLogger('airflow.task').warning(
            "%(p)s\nЗапущена задача '%(task)s'\n"
            "но в параметрах DAG'а нет '%(param)s' со значением 'True'!\n"
            "видим следующие параметры:\n%(dag_params)s",
            {
                'p': LOG_PREFIX,
                'task': cleanup_temp_tables_task.function.__name__,
                'param': PARAM_NAME,
                'dag_params': context['params'],
            })
        return

    if not conn_id:  # поддержка id подключения из YAML файла
        conn_id = context.get('params', {}).get('DB_CONN_ID', DEFAULT_CONN_ID)

    all_tasks_ids = context['dag'].task_ids
    clean_files = context['ti'].xcom_pull(
        task_ids=all_tasks_ids, key=FILES_XCOM_KEY)

    clean_queries = set()
    for cf in clean_files:
        if 'jinja_args' in cf:
            sql = get_rendered_file_content(
                context, cf['file_path'], cf['jinja_args'])
        else:
            sql = get_file_content(cf['file_path'])
        clean_queries.add(sql)

    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    info = pg_hook.log.info
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            for i, statement in enumerate(clean_queries):
                info(LOG_FORMAT, {
                    'p': LOG_PREFIX,
                    'i': i + 1,
                    'cnt': len(clean_queries),
                    'nt': '\n' + '\t' * 6,
                    'sql': statement,
                    'dots': '...'
                })
                if _exec_with_error_handler(cur, statement):
                    info("^ готово")
                else:
                    info("^ таблицы не существует")
