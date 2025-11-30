""" Функции для работы с таблицами в схеме meta """

from __future__ import annotations

from typing import TYPE_CHECKING

from psycopg2 import sql

import utils.logs_meta.functions as lf

if TYPE_CHECKING:
    from psycopg2.extensions import cursor

    from airflow.utils.context import Context


def search_in_log_etl_proc_table(table_id: int, log_etl_proc_id: int,
                                 context: Context, cur: cursor
                                 ) -> int | None:
    """ Пробудет найти комбинацию table_id и log_etl_proc_id в таблице
    {meta_schema}.log_etl_proc_table

    Возвращает идентификатор записи (id) или None

    Args:
        table_id (int): аналогично атрибуту в log_etl_proc_table
        log_etl_proc_id (int): аналогично атрибуту в log_etl_proc_table
    """
    meta_schema = lf.get_meta_schema(context)
    base_sql = """
        SELECT id
        FROM {meta_schema}.log_etl_proc_table
        WHERE log_etl_proc_id = {log_etl_proc_id}
        AND table_id = {table_id}
    """

    query = sql.SQL(base_sql).format(
        meta_schema=sql.Identifier(meta_schema),
        log_etl_proc_id=sql.Literal(log_etl_proc_id),
        table_id=sql.Literal(table_id)
    )
    return _exec_and_return_first_int_or_none(cur, query)


def search_in_log_table_status(table_id: int, context: Context, cur: cursor
                               ) -> int | None:
    """ Пробудет найти указанный table_id в таблице
    {meta_schema}.log_table_status

    Возвращает идентификатор записи (id) или None

    Args:
        table_id (int): аналогично атрибуту в log_table_status
    """
    meta_schema = lf.get_meta_schema(context)
    base_sql = """
        SELECT id
        FROM {meta_schema}.log_table_status
        WHERE table_id = {table_id}
    """

    query = sql.SQL(base_sql).format(
        meta_schema=sql.Identifier(meta_schema),
        table_id=sql.Literal(table_id)
    )
    return _exec_and_return_first_int_or_none(cur, query)


@lf.ensure_connection
def get_table_meta_id(schema_name: str, table_name: str,
                      cur: cursor | None = None,
                      context: Context | None = None) -> int | None:
    """ Возвращает id таблицы для логирования из таблицы
    {meta_schema}.mt_table """

    assert cur

    base_sql = """
        SELECT id
        FROM {meta_schema}.mt_table
        WHERE schema_cd = {schema_name}
        AND name = {table_name}
    """
    meta_schema = lf.get_meta_schema(context)

    query = sql.SQL(base_sql).format(
        meta_schema=sql.Identifier(meta_schema),
        schema_name=sql.Literal(schema_name),
        table_name=sql.Literal(table_name)
    )
    if TYPE_CHECKING:
        assert cur
        # заглушка для проверки типов, декоратор @ensure_connection
        # обеспечивает наличие cur
    return _exec_and_return_first_int_or_none(cur, query)


@lf.ensure_connection
def update_log_document_load(etl_proc_id: int, ods_schema: str,
                             cur: cursor | None = None,
                             context: Context | None = None):
    """ Обновляет в таблице `{meta_schema}.log_document_load` поля:
     - doc_cd = schet_code
     - doc_num = schet_nschet
     - doc_dt = schet_dschet

    на значения из `{ods_schema}.zl_list`

    Возвращает количество изменённых строк
    """

    assert cur

    base_sql = """
        UPDATE {meta_schema}.log_document_load d
        SET doc_cd = z.schet_code,
            doc_num = z.schet_nschet,
            doc_dt = z.schet_dschet
        FROM {ods_schema}.zl_list z
        WHERE z.id = d.doc_ods_id
        AND d.etl_proc_id = {etl_proc_id}
    """
    meta_schema = lf.get_meta_schema(context)

    query = sql.SQL(base_sql).format(
        meta_schema=sql.Identifier(meta_schema),
        ods_schema=sql.Identifier(ods_schema),
        etl_proc_id=sql.Literal(etl_proc_id)
    )

    lf._execute_query(cur, query)
    return cur.rowcount


def _exec_and_return_first_int_or_none(cur: cursor, query: sql.Composed):
    """ Выполняет переданный запрос на курсоре, возвращает значение
    первой ячейки первой строки, приведённое к целому числу или None.

    Args:
        cur (cursor): курсор
        query (sql.Composed): запрос
    """

    lf._execute_query(cur, query)
    return int(x[0]) if (x := cur.fetchone()) else None
