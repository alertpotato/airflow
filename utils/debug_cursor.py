import logging
import psycopg2


class DebugCursor(psycopg2.extensions.cursor):
    """ Пример:
    `with conn.cursor(cursor_factory=DebugCursor) as cur:` """

    def execute(self, query, **kwargs):
        task_logger = logging.getLogger('airflow.task')

        task_logger.info(
            "(НЕ)Выполняется запрос:\n%s\nargs: %s",
            query, kwargs)
        # super().execute(query, vars)
