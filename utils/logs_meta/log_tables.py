"""
Таблицы логов
---
В этом файле находятся методы, описывающие особенности каждой таблицы
логов. Один метод на одну таблицу.
Разные действия в рамках одной таблицы разделяются параметром `action:
TableAction`.

В этом файле НЕ должно быть подключений к БД,
вместо этого нужно использовать `change_table` из `functions`

Общие рекомендации и hint'ы:
---
Для необязательных параметров в `sql_params` можно использовать
`dict_safety_add()` (из utils.logs_meta.functions), например:
```python
lf.dict_safety_add(sql_params, params, 'log_info')
# если в params будет найден ключ 'log_info', то он будет добавлен в \
`sql_params`
```

В `finish`-действиях таблиц `log_etl_proc` и `log_etl_proc_table`
в поле `log_info` записывается пустая строка, если иное значение не
передано в `params`. Делается это для перезаписи прошлого значения.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

import utils.logs_meta.functions as lf
from airflow.exceptions import AirflowFailException
from utils.logs_meta import extra

if TYPE_CHECKING:
    from psycopg2.extensions import cursor
    from airflow.utils.context import Context


class TableAction(Enum):
    """ Общие действия для таблиц """

    get_new_id = 1
    """ Создание нового id (записывает в таблицу и возвращает числовым
    значением) """

    finish = 2
    """ Успешное завершение """

    progress = 3
    """ Обновление текущего статуса """

    failure = 4
    """ Завершение процесса с ошибкой """

    document_init = 5
    """ TODO: как обозвать?
    """


@lf.ensure_connection
def log_etl_run(action: TableAction, params: dict | None = None,
                cur: cursor | None = None,
                context: Context | None = None):
    """ Обязательные поля в `params` в зависимости от `action`:

    для `TableAction.get_new_id`:
    - без параметров!

    для `TableAction.finish` / `.failure`:
    - etl_run_id - идентификатор ETL процесса

    """

    assert cur

    if not context:
        context = lf.try_get_current_context()

    meta_schema = lf.get_meta_schema(context)
    table_name = f'{meta_schema}.log_etl_run'

    if action is TableAction.get_new_id:
        if not context:
            raise ContextNotFoundError()
        upsert_id = lf.get_crc_from_run_id(context)

        lf.change_table(
            table_name,
            'UPSERT',
            sql_params={
                'id': upsert_id,
                'status_cd': 'R',
                'begin_dttm': 'now()',
                'finish_dttm': None,
            },
            upsert_attr='id',
            cur=cur,
        )
        return upsert_id

    elif action in (TableAction.finish, TableAction.failure):
        sql_params = {
            'finish_dttm': 'now()'
        }

        if action is TableAction.finish:
            sql_params['status_cd'] = 'C'

        if action is TableAction.failure:
            sql_params['status_cd'] = 'E'

        assert params
        lf.change_table(
            table_name,
            'UPDATE',
            sql_params=sql_params,
            where={'id': params['etl_run_id']},
            cur=cur,
        )

    else:
        lf.raise_not_implemented(log_etl_run.__name__, action)


@lf.ensure_connection
def log_etl_proc(action: TableAction, params: dict, cur: cursor | None = None,
                 context: Context | None = None):
    """ Обязательные поля в `params` в зависимости от `action`:

    для `TableAction.get_new_id`:
    - mt_etl_proc_id - соответствующий этому DAG id из таблицы mt_etl_proc
    - etl_run_id - идентификатор ETL процесса
    - region_cd - [опционально] код периода в формате YYYYMM
    - period_cd - [опционально] код региона
    - log_info - [опционально] любая строка текста

    для `TableAction.finish` / `.failure`:
    - etl_run_id - идентификатор ETL процесса
    - etl_proc_id - Ссылка на Статус выполнения ETL процесса
    - log_info - [опционально] любая строка текста
    """

    assert cur

    if not context:
        context = lf.try_get_current_context()

    meta_schema = lf.get_meta_schema(context)
    table_name = f'{meta_schema}.log_etl_proc'

    if action is TableAction.get_new_id:
        if not context:
            raise ContextNotFoundError()
        upsert_id = lf.get_crc_from_run_id(context)
        status_cd = 'R'

        params = {
            **context['params'],
            **params
        }
        sql_params = {
            'id': upsert_id,
            'etl_proc_id': params['mt_etl_proc_id'],
            'etl_run_id': params['etl_run_id'],
            'status_cd': status_cd,
            'status_desc': lf.get_descr(status_cd),
            'begin_dttm': 'now()',
            'finish_dttm': None,
        }

        lf.dict_safety_add(sql_params, params, 'region_cd')
        lf.dict_safety_add(sql_params, params, 'period_cd')
        lf.dict_safety_add(sql_params, params, 'log_info')

        lf.change_table(
            table_name,
            'UPSERT',
            sql_params=sql_params,
            upsert_attr='id',
            cur=cur,
        )
        return upsert_id

    elif action in (TableAction.finish, TableAction.failure):
        sql_params = {}

        if action is TableAction.finish:
            status_cd = 'C'
            lf.dict_safety_add(sql_params, params, 'log_info', add_default='')
        else:
            status_cd = 'E'
            lf.dict_safety_add(sql_params, params, 'log_info')

        sql_params.update({
            'status_cd': status_cd,
            'status_desc': lf.get_descr(status_cd),
            'finish_dttm': 'now()',
        })

        lf.change_table(
            table_name,
            'UPDATE',
            sql_params=sql_params,
            where={
                'id': params['etl_proc_id'],
                'etl_run_id': params['etl_run_id']
            },
            cur=cur,
        )

    else:
        lf.raise_not_implemented(log_etl_proc.__name__, action)


@lf.ensure_connection
def log_etl_proc_table(action: TableAction, params: dict,
                       cur: cursor | None = None,
                       context: Context | None = None):
    """ `TableAction` `finish` и `failure` кроме
    `meta.log_etl_proc_table` так же обновляют соответствующую
    запись в `meta.log_table_status`

    В `params` следует передавать id таблиц так:
    - `mt_tbl_id` - постоянный `id` таблицы в системе
    (например для справочников `dict_id`)
    - `new_log_tbl_id` - сгенерированный `id`
    при вызове с `action=get_new_id`

    ---
    Обязательные поля в `params` в зависимости от `action`:

    для `TableAction.get_new_id`:
    - etl_proc_id - Ссылка на Статус выполнения ETL процесса
    - mt_tbl_id - постоянный id таблицы в системе
    - log_info - [опционально] любая строка текста
    - region_cd - [опционально][для log_table_status] код периода (YYYYMM)
    - period_cd - [опционально][для log_table_status] код региона

    для `TableAction.progress`:
    - etl_proc_id - Ссылка на Статус выполнения ETL процесса
    - new_log_tbl_id - id таблицы для логирования текущего процесса
    - table_name - имя таблицы для status_desc
    - dag_action - [stage|update|insert|delete] для поля status_desc
    - affected_rows - количество для [rec_upd_qnt|rec_ins_qnt|rec_del_qnt]

    для `TableAction.finish` / `.failure`:
    - etl_proc_id - Ссылка на Статус выполнения ETL процесса
    - mt_tbl_id - постоянный id таблицы в системе
    - new_log_tbl_id - id таблицы для логирования текущего процесса
    - log_info - [опционально] любая строка текста
    """

    assert cur

    if not context:
        context = lf.try_get_current_context()

    meta_schema = lf.get_meta_schema(context)
    table_name = f'{meta_schema}.log_etl_proc_table'

    if action is TableAction.get_new_id:
        if not context:
            raise ContextNotFoundError()
        new_log_tbl_id = (
            extra.search_in_log_etl_proc_table(
                table_id=params['mt_tbl_id'],
                log_etl_proc_id=params['etl_proc_id'],
                context=context,
                cur=cur)
            or
            lf.get_nextval_for_sequence(f"{meta_schema}.etl_proc_tbl_id_seq", cur)
        )
        status_cd = 'N'
        sql_params = {
            'id': new_log_tbl_id,
            'status_cd': status_cd,
            'status_desc': lf.get_descr(status_cd),
            'log_etl_proc_id': params['etl_proc_id'],
            'table_id': params['mt_tbl_id'],
            'rec_upd_qnt': 0,
            'rec_ins_qnt': 0,
            'rec_del_qnt': 0,
            'begin_dttm': 'now()',
        }
        lf.dict_safety_add(sql_params, params, 'log_info')

        lf.change_table(table_name, 'UPSERT', sql_params=sql_params,
                        upsert_attr='id', cur=cur)

        # ------------ далее для таблицы log_table_status --------------
        table_name = f"{meta_schema}.log_table_status"

        table_status_id = (
            extra.search_in_log_table_status(
                table_id=params['mt_tbl_id'],
                context=context,
                cur=cur)
            or
            lf.get_nextval_for_sequence(f"{meta_schema}.log_table_status_id_seq", cur)
        )
        # оставляем только параметры, существующие в log_table_status
        sql_params = {
            **{k: v for k, v in sql_params.items() if k in
               ('status_cd', 'status_desc', 'table_id')},
            'last_etl_proc_id': params['etl_proc_id'],
            'load_dttm': 'NOW()',
            'id': table_status_id
        }
        lf.dict_safety_add(sql_params, params, 'log_info')
        lf.dict_safety_add(sql_params, params, 'region_cd')
        lf.dict_safety_add(sql_params, params, 'period_cd')

        lf.change_table(table_name, 'UPSERT', sql_params=sql_params,
                        upsert_attr='table_id', cur=cur)

        return new_log_tbl_id

    elif action is TableAction.progress:
        raise KeyError("Больше не должно использоваться?")
        dag_action = params['dag_action']
        rows = params['affected_rows']
        sql_params = {
            'status_cd': 'R',
            'finish_dttm': 'now()',
        }

        keys = {
            'update': 'rec_upd_qnt',
            'insert': 'rec_ins_qnt',
            'delete': 'rec_del_qnt',
        }
        if key := keys.get(dag_action):
            sql_params[key] = rows

        descr = lf.get_descr(dag_action, is_action=True)
        sql_params['status_desc'] = f"{descr} {params['table_name']}"

        # log_info не пишем намеренно, актуализировалось 07.09.2023
        lf.change_table(
            table_name,
            'UPDATE',
            sql_params=sql_params,
            where={
                'id': params['new_log_tbl_id'],
                'log_etl_proc_id': params['etl_proc_id']
            },
            cur=cur,
        )

    elif action in (TableAction.finish, TableAction.failure):
        sql_params = {}

        if action is TableAction.finish:
            status_cd = 'C'
            lf.dict_safety_add(sql_params, params, 'log_info', add_default='')
        else:
            status_cd = 'E'
            lf.dict_safety_add(sql_params, params, 'log_info')

        sql_params.update({
            'status_cd': status_cd,
            'status_desc': lf.get_descr(status_cd),
        })

        lf.change_table(
            table_name,
            'UPDATE',
            sql_params={
                **sql_params,
                'finish_dttm': 'NOW()',
            },
            where={
                'id': params['new_log_tbl_id'],
                'log_etl_proc_id': params['etl_proc_id']
            },
            cur=cur,
        )
        lf.change_table(
            f'{meta_schema}.log_table_status',
            'UPDATE',
            sql_params={
                **sql_params,
                'load_dttm': 'NOW()',
                'last_etl_proc_id': params['etl_proc_id'],
            },
            where={
                'table_id': params['mt_tbl_id']
            },
            cur=cur,
        )

    else:
        lf.raise_not_implemented(log_etl_proc_table.__name__, action)


@lf.ensure_connection
def log_document_load(action: TableAction, params: dict,
                      cur: cursor | None = None,
                      context: Context | None = None):
    """ Обязательные поля в `params` в зависимости от `action`:

    для `TableAction.document_init`:
    - etl_proc_id - Ссылка на Статус выполнения ETL процесса
    - period_cd - код периода в формате YYYYMM
    - region_cd - код региона
    - ods_schema - ODS схема в БД

    для `TableAction.finish` / `.failure`:
    - etl_proc_id - Ссылка на Статус выполнения ETL процесса
    - err_descr - [опционально] описание ошибки

    """

    assert cur

    if not context:
        context = lf.try_get_current_context()

    meta_schema = lf.get_meta_schema(context)
    table_name = f'{meta_schema}.log_document_load'

    if action is TableAction.document_init:
        # 'ods_' + dag_params['region'] ?

        # отмечаем все записи в основной таблице (table_name)
        lf.change_table(
            table_name,
            'UPDATE',
            sql_params={
                'etl_status_cd': 'R',
                'etl_proc_id': params['etl_proc_id'],
                'update_dttm': 'NOW()',
                'etl_begin_dttm': 'NOW()'
            },
            where={
                'period_cd': params['period_cd'],
                'region_cd': params['region_cd'],
                'stream_status_cd': 'C',
                'etl_status_cd': None,
            },
            cur=cur,
        )

        # а так же обновляем по данным из {ods_schema}.zl_list

        extra.update_log_document_load(
            etl_proc_id=params['etl_proc_id'],
            ods_schema=params['ods_schema'],
            cur=cur
        )

    elif action in (TableAction.failure, TableAction.finish):
        sql_params = {}  # noqa

        if action == TableAction.finish:
            status_cd = 'C'
            lf.dict_safety_add(sql_params, params, 'err_descr', add_default='')
        else:
            status_cd = 'E'
            lf.dict_safety_add(sql_params, params, 'err_descr')

        sql_params['etl_status_cd'] = status_cd

        lf.change_table(
            table_name,
            'UPDATE',
            sql_params={
                **sql_params,
                'etl_finish_dttm': 'NOW()',
            },
            where={
                'etl_proc_id': params['etl_proc_id']
            },
            cur=cur,
        )

    else:
        lf.raise_not_implemented(log_etl_proc_table.__name__, action)


class ContextNotFoundError(AirflowFailException):
    def __init__(self) -> None:
        msg = "Не удалось получить Context!"
        super().__init__(msg)
