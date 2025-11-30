"""
Тут находятся служебные функции для взаимодействия
с таблицами в БД, обёртки, проверки и геттеры из `dictionaries.py`.

Не рекомендуется импортировать этот код куда либо кроме модуля `logs_meta`
"""

from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING

from psycopg2 import sql
from psycopg2.extensions import cursor

from airflow.models import Variable
from airflow.models.connection import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.common import is_debug_mode
from utils.logs_meta import dictionaries

if TYPE_CHECKING:
    from airflow.utils.context import Context


DEFAULT_CONN_ID = 'nds_dds'


def ensure_connection(f):
    """
    Проверяет наличие курсора в аргументах,
    если не находит - создаёт новое подключение к БД

    Новое подключение будет использовать именованный аргумент `conn_id`,
    если он передан или `DEFAULT_CONN_ID`, определённого рядом с декоратором
    ---
    Таким образом у декорируемого метода всегда будет аргумент `cur` с
    курсором
    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        if kwargs.get('cur'):   # предполагаем, что курсор есть
            return f(*args, **kwargs)

        # иначе пробуем найти курсор в args:
        arg_cur_lst = [a for a in args if isinstance(a, cursor)]
        if arg_cur_lst:
            cur = arg_cur_lst[0]
            args = tuple(a for a in args if a != cur)
            kwargs['cur'] = cur
            return f(*args, **kwargs)   # и выполняем с курсором

        # если курсор не найден - создаём подключение и курсор
        conn_id = kwargs.pop('conn_id', get_db_conn_id())
        pg_hook = PostgresHook(postgres_conn_id=conn_id)

        # замена метода, чтоб не было "Using connection ID ..."
        pg_hook.get_connection = Connection.get_connection_from_secrets

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                return f(cur=cur, *args, **kwargs)

    return wrapper


def get_crc_from_run_id(context: Context):
    """ Возвращает `crc32` от `context['run_id']` """
    if not context:
        raise ValueError("Для получения 'run_id' необходим context.")

    import zlib
    rid = f"{context['dag']}@{context['run_id']}"
    return zlib.crc32(rid.encode('utf-8'))


def get_meta_schema(context: Context | None = None, default: str = 'meta') -> str:
    """ Возвращает значение `meta_schema` из параметров DAG
    или значение по умолчанию.

    По факту это заплатка для системы логирования,
    так как ещё не везде используются общие параметры.
    """
    if context:
        try:
            return context['params']['meta_schema']
        except KeyError:
            pass
    return default


def try_get_current_context() -> Context | None:
    """ Попытка получить context Airflow, в случае неудачи
    молча возвращает None """

    from airflow.exceptions import AirflowException
    from airflow.operators.python import get_current_context
    try:
        return get_current_context()
    except AirflowException:
        return None


def get_db_conn_id() -> bool:
    """ Возвращает имя подключения Airflow (`conn_id`) для логирования
    или значение по умолчанию из переменной `DEFAULT_CONN_ID` """

    context = try_get_current_context()
    return (
        (context['params'].get('LOG_CONN_ID') if context else None) or
        Variable.get('log_conn_id', DEFAULT_CONN_ID)
    )


def get_table_name_only(table: str):
    """ Возвращает только имя из `table`
    (если передано схема.имя, например) """

    return table.split('.', maxsplit=1)[-1].replace('"', '')


def get_table_iden(name: str):
    """ Возвращает безопасный идентификатор имени таблицы
        или схемы и имени таблицы"""

    if '.' in name:
        return sql.SQL('.').join(map(sql.Identifier, name.split('.')))
    return sql.Identifier(name)


def check_for_none(d: dict):
    """ Проверяет на наличие пустых значений
    при этом весь d может быть пуст """

    if not d:
        return
    for k, v in d.items():
        if v is None:
            raise ValueError(f"Ключ '{k}' имеет пустое значение")


# @ensure_connection    # убрал пока не используется отдельно
def change_table(table: str, action: str, cur: cursor,
                 sql_params: dict, where: dict | None = None,
                 upsert_attr: str | None = None):
    """
    Взаимодействие с указанной таблицей

    Args:
        table (str): имя таблицы (можно со схемой)
        action (str): действие, например `INSERT` или `UPDATE`
        sql_params (dict): пары имя столбца и значение (пример ниже)
        where (dict): [для UPDATE] предикаты `WHERE`, объединяются `AND`
        upsert_attr (str): [для UPSERT] имя атрибута для поиска
        cur (cursor): [не обязательный] текущий курсор

    Пример значений `sql_params` (предполагаем, что за раз изменяется только одна строка)
    ```json
    {
        "id": 111,
        "status_cd": "R",
        "begin_dttm": "2000-01-21 00:11:22.001",
        "finish_dttm": "NOW()"
    }
    ```
    """

    if is_debug_mode():
        check_for_none(sql_params)

    base_query = dictionaries.BASE_QUERIES.get(action)

    if not base_query:
        raise ValueError(f"Не удалось собрать запрос, значение 'action' = '{action}'")

    if action == 'INSERT':
        result_query = sql.SQL(base_query).format(
            table=get_table_iden(table),
            columns=_get_sql_map(sql.Identifier, sql_params.keys()),
            values=_get_sql_map(sql.Literal, sql_params.values())
        )

    elif action == 'UPDATE':
        if not where:
            raise ValueError("Мы ведь не хотим делать UPDATE без WHERE?")

        result_query = sql.SQL(base_query).format(
            table=get_table_iden(table),
            col_value_pairs=_get_sql_pairs(sql_params, for_set=True),
            where_sql=_get_sql_pairs(where, for_where=True)
        )

    elif action == 'UPSERT':
        if not upsert_attr:
            raise AttributeError("Для действия 'UPSERT' - необходим 'upsert_attr'")
        # копия, чтобы не менять sql_params
        local_sql_params = sql_params.copy()

        params_with_proc = local_sql_params.copy()
        local_sql_params.pop('etl_proc_id', None)   # FIXME: проверить зачем это
        insert_columns = [f"mt.{k}" for k in params_with_proc.keys()]

        result_query = sql.SQL(base_query).format(
            table=get_table_iden(table),
            upsert_attr=sql.Identifier(upsert_attr),
            table_columns=_get_sql_map(sql.Identifier, params_with_proc.keys()),
            insert_columns=_get_sql_map(get_table_iden, insert_columns),
            set_pairs=_get_sql_pairs(local_sql_params, for_set=True),
            update_select_pairs=_get_sql_pairs(local_sql_params, for_select=True),
            insert_select_pairs=_get_sql_pairs(params_with_proc, for_select=True)
        )

    _execute_query(cur, result_query)


@ensure_connection
def get_last_statuses(table_id_list: list,
                      context: Context | None = None,
                      cur: cursor | None = None):
    """ Возвращает список (`list[tuple]`) с данными
    по каждому id в `table_id_list`.
     Список полей см. в `dictionaries.TABLE_LAST_STATUS_QUERY`

    Args:
        table_id_list (list): список table_id из таблицы `log_etl_proc_table`

        context (Context): контекст Airflow (только для схемы "meta")

        cur (cursor): курсор, если не указан - откроется автоматом
    """

    assert cur

    meta_schema = get_meta_schema(context)
    table_name = f'{meta_schema}.log_etl_proc_table'

    base_query = dictionaries.TABLE_LAST_STATUS_QUERY
    result_query = sql.SQL(base_query).format(
        table=get_table_iden(table_name),
        table_ids=_get_sql_map(sql.Literal, table_id_list)
    )

    if TYPE_CHECKING:
        assert cur
        # заглушка для проверки типов, декоратор @ensure_connection
        # обеспечивает наличие cur

    _execute_query(cur, result_query)
    return cur.fetchall()


def get_nextval_for_sequence(seq_name: str, cur: cursor) -> int:
    """ Продвигает объект последовательности (`seq_name`) к следующему
    значению и возвращает это значение. """

    base_q = "select nextval({seq_name}::regclass);"
    query = sql.SQL(base_q).format(seq_name=sql.Literal(seq_name))
    _execute_query(cur, query)
    return cur.fetchone()[0]


def dict_safety_add(left_dict: dict, right_dict: dict, key: str, add_default: str | None = None):
    """ Если ключ `key` существует в `right_dict`,
    то этот ключ с его значением будет добавлен в `left_dict`

    Если `add_default` имеет значение, то оно будет записано
    в  `left_dict`
    """

    if right_dict.get(key):
        left_dict[key] = right_dict[key]
    else:
        if add_default is not None:
            left_dict[key] = add_default


def get_descr(key: str, is_action=False) -> str:
    """ Возвращает описание статуса или действия (при `is_action`)
    для ключа `key`.

    Поиск выполняется в

    `dictionaries.ACTION_DESCRIPTIONS` для `is_action = True`, или

    `dictionaries.STATUS_DESCRIPTIONS`
    """

    if is_action:
        msg_name = 'действия'
        descr_dict = dictionaries.ACTION_DESCRIPTIONS
    else:
        msg_name = 'статуса'
        descr_dict = dictionaries.STATUS_DESCRIPTIONS

    try:
        return descr_dict[key]
    except KeyError as e:
        raise KeyError(
            f"Не найдено описание для {msg_name} '{key}'") from e


def raise_not_implemented(func_name: str, action):
    """ Ошибка: такой action не реализован """
    msg = f"action '{action}' не реализован для функции '{func_name}'"
    raise NotImplementedError(msg)


def _execute_query(cur: cursor, query: str | bytes | sql.Composed):
    """ Единственный метод в системе логирования,
    где должен выполняться SQL (`cur.execute`) """

    match query:
        case str():
            q_str = query
        case bytes():
            q_str = query.decode("utf-8")
        case sql.Composed():
            q_str = query.as_string(cur.connection)
        case _:
            raise TypeError(f"Неожиданный тип параметра 'query': {type(query)}")

    _log_sql_query(q_str)

    # NOTE для отладки: print(query.as_string(cur.connection))
    cur.execute(query)


def _log_sql_query(query: str):
    """ Выводит в лог task переданный текст запроса,
    если в `Airflow Variables` есть переменная `debug_logs_meta`
    и её значение "`on`" """

    if not Variable.get('debug_logs_meta', None) == 'on':
        return

    import logging
    logger = logging.getLogger('airflow.task')

    log_lines = (
        "Выполнение SQL запроса в модуле logs_meta:",
        query.replace(' ' * 8, ' ' * 2)  # отступы: заменяем 8 пробелов на 2
    )
    logger.info('\n'.join(log_lines))


def _get_sql_map(c, l):
    """ Возвращает элементы `l` через запятую, обёрнутые с помощью `c`
    в виде SQL выражения """

    return sql.SQL(', ').join(map(c, l))


def _get_sql_pairs(d: dict, for_set=False, for_where=False, for_select=False):
    """ Обрабатывает dict, оборачивая ключи в `sql.Identifier`,
    а значения в `sql.Literal`.

    При `for_set`: возвращает пары ключ-значений через запятую
     (например `key1 = value1, key2 = value2`)

    При `for_where` возвращает пары ключ-значений, объединённые " AND ",
    заменяя значения None на "IS NULL"
     (например `key1 = value1 AND key2 IS NULL`)
    """

    def format_with_eq_sign(k, v):
        return sql.SQL('{} = {}').format(
            sql.Identifier(k),
            sql.Literal(v)
        )

    if for_set:
        return sql.SQL(', ').join([
            format_with_eq_sign(k, v) for k, v in d.items()
        ])

    if for_where:
        return sql.SQL(' AND ').join([
            format_with_eq_sign(k, v) if v else
            sql.SQL('{} IS NULL').format(sql.Identifier(k))
            for k, v in d.items()
        ])

    if for_select:
        return sql.SQL(', ').join([
            sql.SQL('{} AS {}').format(sql.Literal(v), sql.Identifier(k))
            for k, v in d.items()
        ])

    raise AttributeError("Либо for_set, либо for_where")
