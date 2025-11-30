from __future__ import annotations
from typing import TYPE_CHECKING
from airflow.exceptions import AirflowFailException

if TYPE_CHECKING:
    from psycopg2.extensions import cursor

CHECK_COLUMN = 'error_count'  # имя проверяемого столбца


class ResultChecker:
    def __init__(self, file_path: str):
        """
        Args:
            file_path (str): путь к sql файлу (используется только \
            в логировании)
        """
        self._file_path = file_path

    def handler_checker(self, cur: cursor) -> int:
        """ Проверяет наличие наличие указанного (в конструкторе)
        столбца в результатах запроса и его значение """

        names = [desc[0] for desc in cur.description]
        if CHECK_COLUMN not in names:
            self._raise_error(
                f"Нет столбца с таким именем, видим только: {names}")

        result = cur.fetchone()
        if not result:
            raise NotImplementedError("Тут нужна дополнительная обработка?")

        check_result = result[names.index(CHECK_COLUMN)]
        if check_result:    # значение столбца отличается от 0 или NULL
            self._raise_error(
                f"Проверка успешно обнаружила значение '{check_result}'")

        return self._successful_action()

    def _successful_action(self) -> int:
        ok_msg = self._get_message_text("Ошибок не обнаружено")

        import logging
        logging.getLogger('airflow.task').info(ok_msg)
        return -1

    def _get_message_text(self, message: str):
        lines = [
            "Проверка столбца с именем '{col_name}'",
            "В результатах выполнения файла [{file_path}]:",
            message
        ]
        msg = '\n'.join(lines)
        return msg.format(col_name=CHECK_COLUMN,
                          file_path=self._file_path)

    def _raise_error(self, message: str):
        err_msg = self._get_message_text(message)

        raise AirflowFailException(err_msg)
