"""
Универсальные функции инициализации, финализации, on_failure_callback
и другие, которые можно вызывать разными частями
для снижения количества повторяющегося кода
"""

from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from utils import common, gos_tech_api
from utils.logs_meta.functions import get_db_conn_id
from utils.logs_meta.log_tables import (
    TableAction,
    log_document_load,
    log_etl_proc,
    log_etl_proc_table,
    log_etl_run,
)

DB_LOG_INFO_MAX_LEN = 800


def try_get_sql_params(context: Context):
    """ Возвращает общие SQL параметры из `common.get_sql_params`.

    Или `{}` если не удалось и предупреждение в лог задачи. """

    try:
        return common.get_sql_params(context)
    except TypeError:
        context['ti'].log.warning("Не удалось получить sql параметры. "
                                  "В context['params']:\n%s",
                                  context['params'])
        return {}


def init_all(log_ids: dict, context: Context, mt_etl_proc_id: int = 0):
    """ Универсальная инициализация.

    Возвращает `log_ids`, где
    значения `None` будут заменены на новые id, а в `all_tbl_ids`, если
    передан - будут добавлены `new_log_tbl_id`

    Args:
        log_ids (dict): варианты значений:
        - `{'etl_run_id': None}`
        - `{'etl_run_id': None, 'etl_proc_id': None}`
        - `{'etl_run_id': None, 'etl_proc_id': None, 'all_tbl_ids': {x1, x2} }`
        - `{'etl_run_id': 1, 'etl_proc_id': None}`
        - `{'etl_run_id': 1, 'etl_proc_id': 1, 'all_tbl_ids': {x1, x2} }`

        где `x = { 'any_name': {'mt_tbl_id': 123} }`

        mt_etl_proc_id (int): id соответствующий этому DAG из таблицы
        meta.mt_etl_proc. Используется только для получения
        `etl_proc_id`, в остальных случаях не нужен.

    Returns:
    ---
    ```json
    {
        "etl_run_id": 123,
        "etl_proc_id": 456,
        "all_tbl_ids": {
            "nsi_n016": {
                "mt_tbl_id": 26,
                "new_log_tbl_id": 7777
            },
            "nsi_n017": {
                "mt_tbl_id": 27,
                "new_log_tbl_id": 8888
            },
        }
    }
    ```
    """
    gos_tech_api.send_dag_status('Запуск', context=context)

    action = TableAction.get_new_id
    conn_id = get_db_conn_id()

    pg_hook = PostgresHook(conn_id)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            sql_params = try_get_sql_params(context)

            if 'etl_run_id' in log_ids and log_ids['etl_run_id'] == None:
                log_ids['etl_run_id'] = log_etl_run(action, cur=cur)

            if 'etl_proc_id' in log_ids and log_ids['etl_proc_id'] == None:
                if not mt_etl_proc_id:
                    raise ValueError("Параметр mt_etl_proc_id необходим"
                                     " для получения etl_proc_id")

                log_ids['etl_proc_id'] = log_etl_proc(
                    action,
                    {
                        **sql_params,
                        'etl_run_id': log_ids['etl_run_id'],
                        'mt_etl_proc_id': mt_etl_proc_id,
                    },
                    cur=cur)

            if ('all_tbl_ids' in log_ids
                    and isinstance(log_ids['all_tbl_ids'], dict)):
                all_tbl_ids = log_ids['all_tbl_ids']
                for _, tbl_ids in all_tbl_ids.items():

                    # NOTE: тут выполняется поиск по ранее обработанным
                    # таблицам и если встречается уже заполненный
                    # 'new_log_tbl_id' для текущего 'mt_tbl_id',
                    # то повторной инициализации не будет,
                    # сохраняется имеющийся
                    tbl_ids['new_log_tbl_id'] = (
                        next((x['new_log_tbl_id']
                              for _, x in all_tbl_ids.items()
                              if x['mt_tbl_id'] == tbl_ids['mt_tbl_id']
                              and x.get('new_log_tbl_id')), None)
                        or
                        log_etl_proc_table(
                            action,
                            {
                                **sql_params,
                                'etl_proc_id': log_ids['etl_proc_id'],
                                'mt_tbl_id': tbl_ids['mt_tbl_id'],
                            },
                            cur=cur)
                    )

    return log_ids


def finish_all(log_etl_params: dict, action=TableAction.finish):
    """ Универсальная финализация.

    Записывает в таблицы
    `log_etl_run`, `log_etl_proc`, `log_etl_proc_table` статус,
    зависящий от `action`.

    Args:
        log_etl_params (dict): id'шники, используемые как параметры
        action (TableAction): применяемое действие (finish или failure)

    Example
    ---
    ```python
    finish_all(
        action=TableAction.finish,
        log_etl_params={
            'etl_run_id': 123,
            'etl_proc_id': 456,
            'all_tbl_ids': {
                'nsi_n016': {
                    'mt_tbl_id': 26,
                    'new_log_tbl_id': 7777
                },
                'nsi_n017': {
                    'mt_tbl_id': 27,
                    'new_log_tbl_id': 8888
                },
            }
        }
    )
    ```
    """

    finish_partial(
        tables_to_finish=('log_etl_run', 'log_etl_proc', 'log_etl_proc_table'),
        action=action,
        log_etl_params=log_etl_params,
    )


def finish_partial(tables_to_finish: tuple, log_etl_params: dict,
                   action=TableAction.finish, context=None):
    """ Универсальная финализация.
    Аналогична функции `finish_all`, только позволяет указать таблицы.

    Записывает в таблицы `tables_to_finish` статус,
    зависящий от `action`.

    Args:
        tables_to_finish (tuple): таблицы для обработки:
        `'log_etl_run', 'log_etl_proc', 'log_etl_proc_table'`
        action (TableAction): применяемое действие (finish или failure)
        log_etl_params (dict): id'шники, используемые как параметры

    Example
    ---
    ```python
    finish_all(
        tables_to_finish=('log_etl_proc','log_etl_proc_table'),
        action=TableAction.finish,
        log_etl_params={
            'etl_run_id': 123,
            'etl_proc_id': 456,
            'all_tbl_ids': {
                'nsi_n016': {
                    'mt_tbl_id': 26,
                    'new_log_tbl_id': 7777
                },
                'nsi_n017': {
                    'mt_tbl_id': 27,
                    'new_log_tbl_id': 8888
                },
            }
        }
    )
    ```
    При таком вызове будут обновлены только таблицы `log_etl_proc` и
    `log_etl_proc_table`, а `etl_run_id` в `log_etl_params` используется
    только как параметр (обязательный для обновления `log_etl_proc`)
    """

    def check_params(names: tuple):
        for name in names:
            if not (
                name in log_etl_params and
                log_etl_params[name]
            ):
                # raise AirflowFailException(f"Не найдено значение '{name}'")
                return False
        return True

    accepted_actions = [TableAction.finish, TableAction.failure]
    if action not in accepted_actions:
        raise AirflowFailException("Эта функция только для действий: "
                                   ' и '.join(map(str, accepted_actions)))

    status_for_api = ('Успешное завершение'
                      if action is TableAction.finish else
                      'Ошибка выполнения')
    gos_tech_api.send_dag_status(status_for_api, context=context)

    conn_id = get_db_conn_id()
    pg_hook = PostgresHook(conn_id)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:

            if ('log_etl_run' in tables_to_finish and
                    check_params(('etl_run_id',))):
                log_etl_run(action, log_etl_params, cur=cur, context=context)

            if ('log_etl_proc' in tables_to_finish and
                    check_params(('etl_proc_id', 'etl_run_id'))):
                log_etl_proc(action, log_etl_params, cur=cur, context=context)

            if ('log_etl_proc_table' in tables_to_finish and
                    check_params(('all_tbl_ids', 'etl_proc_id'))):
                for _, ids in log_etl_params['all_tbl_ids'].items():
                    log_etl_proc_table(
                        action,
                        {
                            **log_etl_params,
                            'new_log_tbl_id': ids['new_log_tbl_id'],
                            'mt_tbl_id': ids['mt_tbl_id'],
                        },
                        cur=cur,
                        context=context
                    )


def get_on_failure_function(tables_to_finish: tuple, xcom_keys: tuple,
                            with_document: bool = False):
    """ Возвращает универсальную функцию для обработки
    `on_failure_callback` с текущими параметрами, которая в свою очередь:
        Записывает в базу статус ошибки в таблицы:
        log_etl_run, log_etl_proc и в log_etl_proc_table для
        каждого id.

    Args:
        tables_to_finish (tuple): таблицы для обработки, например:
         `'log_etl_run', 'log_etl_proc', 'log_etl_proc_table'`

        xcom_keys (tuple): имена ключей с id таблиц, например:
        `('etl_run_id', 'etl_proc_id', 'all_tbl_ids')`

    обычно `exception` есть в `context`'е, если функция используется так:
    ```python
    default_args={
        'on_failure_callback': shared.get_on_failure_function(...)
    },
    ```
    """

    def native_failure_func(context):
        _shared_failure_func(context, tables_to_finish, xcom_keys, with_document)  # noqa

    return native_failure_func


def _shared_failure_func(context, tables_to_finish, xcom_keys, with_document):
    if context['params'].get('dry_run', False):
        logger = context['ti'].log
        logger.info('on-failure-функция: dry_run, выполнение пропускается')
        return

    log_etl_params = common.get_params_from_xcom_any_task(
        context, xcom_keys)

    # -1 используется для отладки, чтобы явно указать на тестовый запуск
    if None in log_etl_params.values() or -1 in log_etl_params.values():
        gos_tech_api.send_dag_status('Ошибка выполнения и on-failure-функции',
                                     context=context)
        import sys
        sys.tracebacklimit = 0
        # ^ убираем простыню, причины будут в предыдущей ошибке
        raise AirflowFailException(
            "Не можем выполнить failure_func, так как"
            " не все идентификаторы были получены."
            f" Вот что мы знаем: {log_etl_params}")

    if ex := context.get('exception'):
        log_info = repr(ex)
    else:
        log_info = "нет информации об ошибке"

    if len(log_info) > DB_LOG_INFO_MAX_LEN - 5:
        log_info = log_info[:DB_LOG_INFO_MAX_LEN - 5] + ' ...'
    log_etl_params['log_info'] = log_info

    finish_partial(
        tables_to_finish=tables_to_finish,
        action=TableAction.failure,
        log_etl_params=log_etl_params,
        context=context
    )

    if with_document:
        log_document_load(
            action=TableAction.failure,
            params={
                'etl_proc_id': log_etl_params['etl_proc_id'],
                'err_descr': log_info
            },
            context=context
        )
