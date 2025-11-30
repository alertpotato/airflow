""" Поддержка зависимостей таблиц: KMP-1532
Проверяет, что указанные (по table_id) таблицы были успешно обработаны
за последние n-часов.

секция в yaml: step['table_depends']
это может быть список table_id
или объект с полями:
    hours - количество часов для проверки условия
    list: - список table_id
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pendulum

from airflow.models import Variable
from utils.common import get_str_table
from utils.logs_meta.functions import get_last_statuses

if TYPE_CHECKING:
    from datetime import datetime

    from airflow.utils.context import Context


LOG_PREFIX = '[Зависимости таблиц]: '


def skip_task_if_table_depends_fail(step_args: dict, context: Context):
    """ [Зависимости таблиц: KMP-1532. Предполагается запуск из task!]
      Пропускает текущий `task` (`AirflowSkipException`), если:
     - в аргументах текущего шага есть `table_depends`
     - хотя бы одна из них не была загружена за последние n-часов
    """
    if not (td := step_args.get('table_depends')):
        return

    table_list: list
    hours = 0.0

    if isinstance(td, dict):
        table_list = td['list']
        hours = float(td.get('hours', 0))
    elif isinstance(td, list):
        table_list = td
    # типы должны быть проверены на этапе валидации (метод `syntax_validation`)

    result, messages = get_check_result(table_list, hours, context)
    log_message = '\n'.join(messages)

    if result:  # успешное завершение проверки
        context['ti'].log.info(log_message)

    else:   # проверка не пройдена
        context['ti'].log.error(log_message)
        # ^ отдельный вывод сообщения необходим потому, что ошибка
        # попадает в "Post task execution logs"

        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException(log_message.strip().splitlines()[0])


def get_check_result(table_id_list: list,
                     hours: float = 0.0,
                     context: Context | None = None):
    """
     - Запрашивает данные по таблицам из `table_id_list`,
     - проверяет на соблюдение условий (пока в теле функции),
     - возвращает результат проверки и сообщение

    Args:
        table_id_list (list): список table_id из таблицы `log_etl_proc_table`

        hours (int): за сколько часов, если нет - берётся из переменных

        context (Context): контекст Airflow (только для схемы "meta")

    Returns:
        - `(True, None)` - если проверка пройдена
        - `(False, list[str])` - если нет, тогда в списке - сообщения
    """

    statuses = get_last_statuses(table_id_list, context)
    tables_check = table_id_list.copy()

    if not hours:
        hours = float(Variable.get('table_depends.hours', 24))

    fail_tables = []
    for table_id, status, dt, diff_hours in statuses:
        if TYPE_CHECKING:
            assert isinstance(table_id, int)
            assert isinstance(status, str)
            assert isinstance(dt, datetime)
            assert isinstance(diff_hours, float)

        if status != 'C' or diff_hours > hours:
            pend_dt = pendulum.instance(dt)
            hum_diff = pend_dt.diff_for_humans(locale='ru')
            last_upd = f"{hum_diff} ({pend_dt.to_datetime_string()})"
            fail_tables.append((table_id, status, last_upd))
        tables_check.remove(table_id)
    msgs: list[str] = []
    if tables_check:
        msgs.extend([
            "Для следующих table_id не найдены данные:",
            ', '.join(map(str, tables_check)),
            '.'
        ])
    if fail_tables:
        headers = ('table_id', 'Статус', 'Последнее обновление')
        msgs.extend([
            "Следующие зависимые таблицы не прошли проверку:",
            str(get_str_table(fail_tables, headers)),
            '.'
        ])
    if msgs:
        msgs.insert(0,
                    f"{LOG_PREFIX}Проверка НЕ пройдена!\n"
                    f"Нет успешных выполнений за {hours} ч. "
                    "Выполнение шага пропускается. Детали:")
        return False, msgs
    return True, [f"{LOG_PREFIX}Проверка таблиц {table_id_list} пройдена успешно!"]


def syntax_validation(step: dict):
    """ Проверяет:
    - если есть ключ `table_depends`, то он должен сам быть списком \
    или содержать список.
    - если указан `hours` должен быть int или float

    Returns:
        - `(True, None)` - если проверка пройдена
        - `(False, list[str])` - если нет, тогда в списке - сообщения
    """

    if table_depends := step.get('table_depends'):
        table_list = None
        if isinstance(table_depends, dict):
            table_list = table_depends.get('list')
            if hours_r := table_depends.get('hours'):
                try:
                    float(hours_r)
                except ValueError:
                    return False, ["Ошибка синтаксиса ключа 'hours'"]
        elif isinstance(table_depends, list):
            table_list = table_depends

        if not (isinstance(table_list, list) and table_list):
            return False, ["Ошибка синтаксиса ключа 'table_depends'"]

    return True, None
