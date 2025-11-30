"""
В этом файле находятся только служебные данные системы логирования.
"""


BASE_QUERIES = {
    'INSERT': """
        INSERT INTO {table}
                ({columns})
        VALUES  ({values});
    """,
    'UPDATE': """
        UPDATE {table}
        SET {col_value_pairs}
        WHERE {where_sql};
    """,
    'UPSERT': """
        UPDATE {table}
        SET {set_pairs}
        FROM (
            SELECT {update_select_pairs}
        ) mt
        WHERE {table}.{upsert_attr} = mt.{upsert_attr} ;

        INSERT INTO {table}
            ({table_columns})
        SELECT
            {insert_columns}
        FROM (
            SELECT {insert_select_pairs}
        ) mt
        LEFT JOIN {table} lg ON lg.{upsert_attr} = mt.{upsert_attr}
        WHERE lg.{upsert_attr} IS NULL ;
    """
}

TABLE_LAST_STATUS_QUERY = """
SELECT DISTINCT ON (table_id)
  table_id::int,
  status_cd,
  finish_dttm,
  EXTRACT(epoch FROM NOW() - finish_dttm) / 3600 AS "diff_hours"
FROM {table} 
WHERE table_id IN ({table_ids}) 
  AND finish_dttm IS NOT NULL 
ORDER BY table_id, finish_dttm DESC
"""

STATUS_DESCRIPTIONS = {
    'R': "Процесс запущен",
    'C': "Процесс завершен",
    'E': "Процесс остановлен ошибкой",
    'N': "Инициализация дага",
}

ACTION_DESCRIPTIONS = {
    'stage':  "Создана промежуточная таблица",
    'update': "Обновлены строки в таблице",
    'insert': "Вставлены новые строки в таблицу",
    'delete': "Удалены строки в таблице",
}
