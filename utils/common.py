""" Общие функции """

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Sequence

from airflow.models import TaskInstance, Variable
from airflow.utils.session import create_session

if TYPE_CHECKING:
    from logging import Logger

    from airflow.utils.context import Context
    from airflow.utils.state import DagRunState


def set_note(note: str, ti: TaskInstance):
    """ Устанавливает заметку (Task Instance Note) для указанного
    TaskInstance

    Args:
        note (str): текст заметки
        ti (TaskInstance): TaskInstance, например `context['ti']`
    """
    with create_session() as session:
        ctx = ti.get_template_context(session=session)
        dag_id = ctx["dag"].dag_id
        run_id = ctx["run_id"]
        ti = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == dag_id,
                TaskInstance.task_id == ti.task_id,
                TaskInstance.run_id == run_id,
                TaskInstance.map_index == ti.map_index,
            ).one()  # type: ignore
        )

        ti.note = note
        session.add(ti)


def last_dagrun_completely_successful(dag_id: str, log: Logger | bool = False):
    """ Проверяет последний запуск DAG (по `dag_id`)
     и если все задачи в нём отработали успешно - возвращает True,
     в обратном случае - False

    Args:
        dag_id (str): идентификатор DAG в Airflow

        log (Logger | bool): указанный логер будет использован для \
            вывода сообщений о найденных задачах. \
            Если значение `True` - берёт `getLogger('airflow.task')`

    Предполагается использование с `@task.short_circuit`:
    ```python
    @task.short_circuit()
    def check_last_dagrun_completely_successful(dag_id: str):
        from utils.common import last_dagrun_completely_successful

        return last_dagrun_completely_successful(dag_id, log=True)
    ```
    """
    from airflow.models import DagRun
    from airflow.utils.state import DagRunState, State

    if log == True:
        log = logging.getLogger('airflow.task')

    drs = sorted(
        # DagRun.find(dag_id='custom_sql_operator_tests',
        #             run_id='manual__2023-12-29T13:37:51.291787+00:00'),
        DagRun.find(dag_id=dag_id),
        key=lambda x: x.execution_date, reverse=True)  # type: ignore

    if not drs:
        if log:
            log.warning(f"Не найдено запусков для dag_id '{dag_id}'")
        return False
    last_dr = drs[0]
    del drs

    messages = [f"Проверяем DAG '{dag_id}', run_id: '{last_dr.run_id}'"]

    if last_dr.get_state() == DagRunState.SUCCESS:
        if log:
            messages.append("^ Этот DagRun завершён успешно!")
            log.info('\n'.join(messages))
        return True

    if log:
        from urllib.parse import quote

        from airflow.configuration import conf

        base_url = conf.get_mandatory_value("webserver", "BASE_URL")

        def get_line(ti) -> tuple[str, str, str]:
            return (
                ti.task_id,
                ti.state,
                f"{base_url}/dags/{ti.dag_id}/grid?root=&tab=logs"
                f"&dag_run_id={quote(ti.dag_run.run_id)}"
                f"&task_id={quote(ti.task_id)}"
            )
        dr_tis = sorted(last_dr.get_task_instances(),
                        key=lambda x: x.start_date)
        details = [get_line(ti) for ti in dr_tis if ti.state != State.SUCCESS]
        messages.extend([
            "^ Следующие задачи этого DagRun НЕ завершились успехом:",
            *get_str_table(details, ('task_id', 'state', 'url'), True)
        ])
        log.warning('\n'.join(messages))

    return False


def disable_myself_after(context: Context, runs_count: int,
                         only_state: DagRunState | None = None):
    """ Выключает текущий DAG, при достижении указанного количества
    запусков

    Args:
        context (Context): текущий контекст

        runs_count (int): отработает если у DAG было столько запусков
        (включая текущий)

        only_state (State): если указано - считает только запуски с этим
        статусом

    Returns:
        Возвращает True, если даг выключен и False в обратном случае
    """
    from airflow.models.dagrun import DagRun

    dag_id = context['dag'].dag_id
    if only_state:
        dag_runs_count = len(DagRun.find(dag_id=dag_id, state=only_state))
    else:
        dag_runs_count = len(DagRun.find(dag_id=dag_id))

    if dag_runs_count >= runs_count:
        from airflow.models.dag import DagModel
        dag = DagModel(dag_id=dag_id)
        dag.set_is_paused(is_paused=True)
        return True
    return False


def get_sql_params(context: Context, with_log=False) -> dict:
    """
    Возвращает параметры из `context['params']`, перечисленные в общих
      параметрах (`common_dag_params.PARAMS_DICT`) и добавляет:
    - `etl_run_id` - id этого ETL процесса (из XCOM)
    - `etl_proc_id` - id группы ETL процессов (из XCOM)
    - `period_cd` - код периода в формате YYYYMM (приведённый к строке)
    - `region_cd` - код региона (приведённый к строке)
    - `period_dt` - начало периода "YYYY-MM-01" из `period_cd` (если указан)
    - `ods_schema` - схемы ods, формируется из `region_cd` (если указан)
    - `temp_suffix` - окончание для временных таблиц (только если
        указаны `region_cd` и `period_cd`)

    Args:
        context (Context): контекст текущего DAG
        with_log (bool): Вывести в лог задачи полученные значения?
         по умолчанию - нет.
    """

    if 'params' not in context:
        raise KeyError("Параметры не обнаружены в переменной 'context'")

    dp = context['params']

    from utils.common_dag_params import PARAMS_DICT

    # параметры из DAG, перечисленные в общих
    result_params = {k: dp[k] for k in dp if k in PARAMS_DICT}

    # добавляем period, region и зависимые от них параметры, если заданы
    period = dp.get('period_cd')
    region = dp.get('region_cd')
    if period:
        result_params.update({
            'period_cd': str(period),
            'period_dt': f"{period[0:4]}-{period[4:6]}-01",
        })
    if region:
        result_params.update({
            'region_cd': str(region),
            'ods_schema': result_params.get('ods_schema') or f"ods_{region}"
        })
    if period and region:
        result_params.update({
            'temp_suffix': f"{region}_{period}_stage_1",
        })

    logs_meta_ids = get_params_from_xcom_any_task(
        context=context,
        keys=('etl_run_id', 'etl_proc_id')
    )
    result_params.update(logs_meta_ids)

    if with_log:
        log_it({
            "using SQL params": "",
            **result_params
        }, context['ti'].log.info)

    return result_params


def get_str_table(data: Sequence[tuple], headers: tuple | None = None,
                  as_list=False) -> list[str] | str:
    """ Возвращает таблицу в виде строки (или списка строк), которая
     будет содержать строки из data с учётом ширины строки (значения)

    Args:
        data (list[tuple]): исходные данные (список кортежей)

        headers (tuple): [опционально] заголовки

        as_list (bool): [опционально] вернуть в виде списка строк

    Пример использования:
    ```python
    data = [
        ('line1', 'val1'),
        ('very long line', 42)
    ]
    print(get_str_table(data=data, headers=('line name', 'value')))
    ```
    Результат:
    ```text
    | line name      | value |
    --------------------------
    | line1          | val1  |
    | very long line | 42    |
    ```
    """

    if not data:
        raise ValueError("Нет данных в 'data'!")
    data = list(data)

    if any([len(data[0]) != len(x) for x in data]):
        raise ValueError("Данные разного размера...")

    # подгоняем заголовки под данные, если они не совпадают
    if headers and (diff := len(data[0]) - len(headers)) != 0:
        headers = (*headers, *('',) * diff) if diff > 0 else headers[:diff]

    keys_with_len = ['|']
    lens_check = data + [headers] if headers else data
    for i in range(len(lens_check[0])):
        max_len = len(max([str(r[i]) for r in lens_check], key=len))
        keys_with_len.append(f"{{:<{max_len}}} |")
    result_pattern = ' '.join(keys_with_len)

    result_table = []
    if headers:
        h_str = result_pattern.format(*headers)
        result_table.extend([h_str, '-' * len(h_str)])

    result_table.extend([result_pattern.format(*map(str, r)) for r in data])

    return result_table if as_list else '\n'.join(result_table)


def log_it(to_log_dict: dict, method):
    """ Передаёт в `method` элементы `to_log_dict` построчно.
    Временная функция отладки, потом будет не нужна """
    method('\n' + '\n'.join(f"\t{k}: {v}" for k, v in to_log_dict.items()))


def is_debug_mode() -> bool:
    """ Возвращает `True` если переменная Airflow `debug-mode` имеет значение `on`
    или `False` в обратном случае """

    return Variable.get('debug-mode', None) == 'on'


def get_airflow_home_path():
    """ Возвращает путь к корневой директории Airflow.

    Значение должно хранится в переменной окружения `PYTHONPATH`,
    если такой нет - возвращается сохранённая строка """

    from os import environ
    pythonpath = environ.get('PYTHONPATH')
    return pythonpath or '/opt/airflow/dags/repo'


def get_sql_home_path():
    """ Должно указывать на репозиторий SQL.

    Значение должно хранится в переменной окружения `SQL_REPO_PATH`,
    если такой нет - возвращается сохранённая строка """

    from os import environ
    sql_repo_path = environ.get('SQL_REPO_PATH')
    return sql_repo_path or '/opt/airflow/sql/repo'


def get_dq_home_path():
    """ Должно указывать на репозиторий Data Quality.

    Значение должно хранится в переменной окружения `DQ_REPO_PATH`,
    если такой нет - возвращается сохранённая строка """

    from os import environ
    dq_repo_path = environ.get('DQ_REPO_PATH')
    return dq_repo_path or '/opt/airflow/data_quality/repo'


def xcom_save(context: Context, params_for_save: dict, with_log=False):
    """ Сохраняет переданные параметры в XCOM.
    (намеренно не используя Task Flow чтоб избежать стрелок на графе)

    Если `with_log=True` - так же записывает `params_for_save` в лог
    """

    ti = context['ti']
    for k, v in params_for_save.items():
        ti.xcom_push(k, v)
    if with_log:
        log_it({
            "saved to XCOM params": "",
            **params_for_save
        }, ti.log.info)


def get_params_from_xcom(context: Context,
                         from_task_id: str, keys: str | tuple) -> dict:
    """ Возвращает `dict` из параметров, переданных в `keys` """

    if isinstance(keys, str):
        return context['ti'].xcom_pull(task_ids=from_task_id, key=keys)
    elif isinstance(keys, tuple):
        return {key: context['ti'].xcom_pull(task_ids=from_task_id, key=key)
                for key in keys}


def get_params_from_xcom_any_task(context: Context, keys: str | tuple) -> dict:
    """ Возвращает `dict` из параметров, переданных в `keys`.
    Для каждого ключа будет взято первое значение среди всех задач """

    if isinstance(keys, str):
        return context['ti'].xcom_pull(key=keys)
    elif isinstance(keys, tuple):
        return {key: context['ti'].xcom_pull(key=key) for key in keys}


def get_table_params_from_xcom(context: Context, from_task_id: str, tbl_name: str) -> dict:
    """ Возвращает параметры из `XCOM` для одной таблицы,
    `dict` следующей структуры:

    (! предполагается, что id всех таблиц находятся в `all_tbl_ids`)
    ```json
    {
        "etl_proc_id": 123,
        "tbl_ids": {
            // тут будет то, что было передано при сохранении в xcom
            // значения ниже приведены для примера
            "mt_tbl_id": 26,
            "new_log_tbl_id": 7777
        },
    }
    ```
    """

    xparams = get_params_from_xcom(context, from_task_id,
                                   ('etl_proc_id', 'all_tbl_ids'))

    one_tbl_ids = xparams['all_tbl_ids'][tbl_name]

    return {
        'etl_proc_id': xparams['etl_proc_id'],
        'tbl_ids': one_tbl_ids
    }


def get_found_file_path(file_path: str,
                        search_paths: str | list[str] | None = None,
                        context: Context | None = None,
                        is_glob=False,
                        return_first=True) -> str | list[str]:
    """ Выполняет поиск файла `file_path` в каждой директории по порядку:
    - `search_paths` - аргумент этого метода
    - `dag.template_searchpath` - параметр DAG
    - `dag.folder` - директория файла DAG

    И возвращает первый найденный путь или `FileNotFoundError`

    Args:
        file_path (str): имя файла (может включать относительный путь)
        search_paths (list[str]): список путей для поиска
        context (Context): контекст Airflow для доступа к полям dag
        is_glob (bool): рассматривать `file_path` как шаблон glob
        return_first (bool): вернуть только первый результат или все.
        Это работает только с `is_glob`.

    Returns:
        str: существующий путь к файлу
    """

    from glob import glob
    from os.path import exists, join

    if not is_glob and exists(file_path):
        return file_path    # если передан существующий путь

    match search_paths:
        case list():
            all_search_paths = search_paths
        case str():
            all_search_paths = [search_paths]
        case _:
            all_search_paths = []

    if context:
        dag = context['dag']
        if dag.template_searchpath:
            if isinstance(dag.template_searchpath, str):
                all_search_paths.append(dag.template_searchpath)
            else:
                all_search_paths.extend(dag.template_searchpath)

        all_search_paths.append(dag.folder)

    for sp in all_search_paths:
        possible_path = join(sp, file_path)

        if is_glob:
            if return_first and (result := glob(possible_path)):
                return result[0]
            elif results := glob(possible_path, recursive=True):
                return results

        elif exists(possible_path):
            return possible_path
    msgs = [
        f"Файл не найден: `{file_path}`",
        "Поиск выполнялся в следующих директориях:",
        *all_search_paths,
    ]
    raise FileNotFoundError('\n'.join(msgs))


def get_file_content_old(path_to_file: str, context: Context | None = None):
    """ Возвращает содержимое файла. (без символа BOM)
    `FileNotFoundError` если файл найти не удалось.

    Поддерживает относительные пути: от расположения DAG'а и от свойства
    `template_searchpath`.

    Args:
        path_to_file (str): путь к файлу

        context (Context, optional): только для относительных путей
    """
    import codecs
    from os.path import exists, join

    def open_and_read(path: str):
        with open(path, mode='rb') as f:
            f_bytes = f.read()

        # убираем BOM символ
        if codecs.BOM_UTF8 in f_bytes:
            f_bytes = f_bytes.replace(codecs.BOM_UTF8, b'', 1)
        return f_bytes.decode('utf-8', 'ignore')

    if exists(path_to_file):
        return open_and_read(path_to_file)

    if context:
        dag = context['dag']
        search_paths = [dag.folder]
        if dag.template_searchpath:
            if isinstance(dag.template_searchpath, str):
                search_paths.append(dag.template_searchpath)
            else:
                search_paths.extend(dag.template_searchpath)

        for sp in search_paths:
            new_path = join(sp, path_to_file)
            if exists(new_path):
                return open_and_read(new_path)

    raise FileNotFoundError(f"Файл не найден: {path_to_file}")


def get_file_content(path_to_file: str, context: Context | None = None):
    """ Возвращает содержимое файла (UTF-8 без символа BOM)
    или `FileNotFoundError` если файл найти не удалось.

    Поддерживает относительные пути: от расположения DAG'а и от свойства
    `template_searchpath`.

    Args:
        path_to_file (str): путь к файлу

        context (Context, optional): только для относительных путей
    """
    import codecs

    exists_path = get_found_file_path(file_path=path_to_file, context=context)
    with open(exists_path, mode='rb') as f:  # type: ignore
        f_bytes = f.read()

        # убираем BOM символ
        if codecs.BOM_UTF8 in f_bytes:
            f_bytes = f_bytes.replace(codecs.BOM_UTF8, b'', 1)
        return f_bytes.decode('utf-8', 'ignore')


def get_rendered_file_content(context: Context, path_to_file: str, args) -> str:
    """ Возвращает содержимое файла, обработанное как шаблон Jinja """

    file_content = get_file_content(path_to_file=path_to_file, context=context)
    return context['task'].render_template(file_content, args)


def is_this_task_has_state(context: Context, task_id: str, state: str):
    """ Проверяем - находится ли задача в указанном статусе """

    ti = context['dag_run'].get_task_instance(task_id)
    return ti.current_state() == state  # type: ignore
