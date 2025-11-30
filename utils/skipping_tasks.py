"""
Модуль пропуска задач (по параметру DAG)
---
Метод `check_task` вызывает AirflowException, если:
- у в текущем контексте есть параметры DAG (`context['params']`)
- есть параметр по ключу `SKIP_TASKS_KEY`
- этот параметр ^ является списком
- в этом списке есть текущий task_id (из `context['task']`)

Статус задачи ставится 'success',
к TaskInstance добавляется сообщение о пропуске.

Предполагается вызов из:
```python
default_args={'pre_execute': skip_task}
```
В таком случае задача будет пропущена, не ломая логику запуска других.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.models import TaskInstance

if TYPE_CHECKING:
    from airflow.utils.context import Context


SKIP_TASKS_KEY = 'skip_tasks'


def print_dag_task_ids(context: Context):
    """ С какой-то версии Airflow - список задач DAG'а перестал
    выводиться, из-за чего задача печати вывода соответствующего списка
    стала критично важной для удобства использования модуля пропуска
    шагов.
    
    Лог-группы / Grouping of log lines (добавлено в 2.9.0)
    ---
    Этот метод расчитан на использование в `on_execute_callback`, а лог
    подготовки к запуску скрыт в лог-группе "Pre task execution logs".
    По этому в лог выводится сообщение закрытия группы и открытие новой
    в конце. 
    А так как этот метод должен вызываться прямо перед выполнением
    основной задачи (из документации), то новая группа открывается
    только чтобы закрыться на следующей строке.
    """

    linfo = context['ti'].log.info
    linfo("::endgroup::")

    task_ids = context['dag'].task_ids
    msgs = [
        "Список task_id текущего запуска,"
        f" подходящий для поля {SKIP_TASKS_KEY}:",
        *task_ids,
        '_' * 30
    ]
    linfo('\n'.join(msgs))
    linfo("::group::.")


def get_tasks_for_skip(context: Context) -> list[str] | None:
    """ Возвращает список из параметра `SKIP_TASKS_KEY` или `None`,
    если такого параметра нет в context """

    skip_tasks_param = context['params'].get(SKIP_TASKS_KEY)
    if skip_tasks_param and not isinstance(skip_tasks_param, list):
        raise KeyError(f"Параметр DAG '{SKIP_TASKS_KEY}' "
                       "должен быть списком (list).")
    return skip_tasks_param


def get_check_task_func():
    """ Возвращает обёртку для метода `check_task`, таким образом убирая
    лишний код из параметров DAG """

    def check_task_func(context):
        check_task(context)

    return check_task_func


def check_task(context: Context):
    """
    Основной метод, см. описание файла.

    Предполагается вызов из:
    ```python
    default_args={'pre_execute': skip_task}
    ```
    В таком случае задача будет пропущена, не ломая логику запуска других.
    """

    task_id = context['task'].task_id

    # map_index = context['ti'].map_index
    # args/kwargs = context['task'].op_kwargs / op_args

    skip_tasks_param = get_tasks_for_skip(context)

    if not skip_tasks_param:
        return  # нет такого параметра

    if task_id in skip_tasks_param:
        msg = f"Задача была пропущена, так как находится в списке '{SKIP_TASKS_KEY}'"
        ti = context['ti']
        if TYPE_CHECKING:
            assert isinstance(ti, TaskInstance)
        ti.set_state('success')
        ti.log.warning(msg)
        from utils.common import set_note
        set_note(msg, ti)
        raise AirflowException()
