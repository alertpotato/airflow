from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.operators.python import get_current_context

from plugins.operators.dq_operator import PrepareSQLDataQualityOperator
from utils.common import (
    get_airflow_home_path,
    get_dq_home_path,
    get_found_file_path,
    get_params_from_xcom_any_task,
    get_rendered_file_content,
    get_sql_params,
)
from utils.skipping_tasks import SKIP_TASKS_KEY
from utils.sql_operator import CustomSQLOperator

DQ_METHODS_PATH = "sql/dq"
MAPPED_TASK_NAME_TEMPLATE = "Правило № {{task.x_params.dq_rule}}"


@task
def get_task_sql(tbl_name: str, queries: dict):
    return queries.get(tbl_name, [])


@task(task_id="get_dq_sqls")
def _get_dq_sqls(rules_id: list, dq_group: str) -> list[dict]:
    """(task!) Выполняет по очереди следующие действия:
    - находит файлы по каждому id в `rules_id` и рендерит их
    - оборачивает найденные файлы в dq-метод (найденный по `dq_group`)
    - возвращает список следующего формата:
    ```python
    [
      {
        'sql': "...", # sql запрос после рендеринга
        'x_params': {'dq_rule': 123} # номер правила из rules_id
      }
    ], ...
    ```

    Args:
        rules_id (list): список DQ rule_id
        dq_group (str): имя dq метода
        check_dtm (str): дата запуска проверки
    """
    import logging

    from airflow.models.variable import Variable

    ctx = get_current_context()
    logger = logging.getLogger("airflow.task")
    logger.info("Поиск файлов правил для списка id: %r", rules_id)

    rules_relative = Variable.get("dq_sql_rule_files_directory", "sql/RULES")
    main_dq_path = f"{get_dq_home_path()}/{rules_relative}"
    dq_methods_path = f"{get_airflow_home_path()}/{DQ_METHODS_PATH}"

    wrapped_rule_sqls: list[str] = []
    for rule_id in rules_id:
        try:
            dq_file = get_found_file_path(
                file_path=f"{rule_id}_*.sql", is_glob=True, search_paths=main_dq_path
            )
            logger.info("Для правила № %s найден: %s", rule_id, dq_file)

        except FileNotFoundError as ex:
            logger.error(str(ex).replace("\n", " | "))
            continue

        rule_file_params = {
            **get_sql_params(ctx),  # общие SQL параметры
            **ctx["params"],  # параметры уровня DAG'а
            **(get_params_from_xcom_any_task(ctx, keys="sql_vars") or {}),
            # ^ поддержка произвольных переменных, см: docs/tech.md#yaml-dag
        }
        # удалим список задач из параметров, если он есть
        rule_file_params.pop(SKIP_TASKS_KEY, None)

        # функция get_found_file_path по умолчанию возвращает первый
        # найденный файл, но проверка типов об этом не знает:
        assert isinstance(dq_file, str)
        _log_sql_and_params(dq_file, rule_file_params, logger)
        rendered = get_rendered_file_content(ctx, dq_file, {"params": rule_file_params})

        wrapper_params = {
            **rule_file_params,
            "rule_id": rule_id,
            "inner_query": rendered,
        }
        wrapper_path = f"{dq_methods_path}/{dq_group}/insert_wrapper.sql"
        wrapped_sql = get_rendered_file_content(
            ctx, wrapper_path, {"params": wrapper_params}
        )
        wrapped_rule_sqls.append(wrapped_sql)

        result_dict = [
            {"x_params": {"dq_rule": rule}, "sql": sql}
            for rule, sql in zip(rules_id, wrapped_rule_sqls)
        ]

    return result_dict  # type: ignore


def _log_sql_and_params(rule_file_path: str, params: dict, logger):
    """Вывод пути к файлу и доступных параметров"""

    if not Variable.get("debug_logs_dq", None) == "on":
        return

    log_msgs = [
        f"Обработка файла: {rule_file_path}",
        "-" * 60,
        "Параметры (доступные на стадии загрузки правил):",
        *[f"\t{k}: {v}" for k, v in params.items()],
        ".",
    ]
    logger.info("\n".join(log_msgs))


def create_dq_tasks(rules: list[int], tables: dict, dq_group: str, conn_id: str):
    """Создаёт задачи DQ для переданного шага YAML

    (! предполагается вызов из TaskGroup для шага DQ !)

    Args:
        rules (list[int]): Список rule_id правил DQ

        dq_group (str): метод DQ. Должен соответствовать имени директории
        в <корень репозитория Airflow>/`DQ_METHODS_PATH`

        conn_id (str): подключение для этого шага
    """

    from airflow.models.baseoperator import chain

    from utils.dds_variables import dds_dq_tables

    match dq_group:
        case 'erzl':
            sqls_list = _get_dq_sqls(rules, dq_group)

            tasks: list = [
                sqls_list,  # task создания этого списка, явная очерёдность
                CustomSQLOperator.partial(
                    task_id=f"dq_{dq_group}",
                    conn_id=conn_id,
                    map_index_template=MAPPED_TASK_NAME_TEMPLATE,
                ).expand_kwargs(sqls_list),
            ]

            return tasks

        case 'ffoms':
            # все уникальные таблицы yaml
            all_yaml_tables = set([v["mt_tbl_id"] for v in tables.values()])

            # все таблицы dds которые могут проверяться DQ
            all_dds_dq_tables = [dds_dq_tables[table_id]["table_name"] for table_id in all_yaml_tables if
                                 dds_dq_tables.get(table_id)]

            @task_group(group_id="pre_check_dq")
            def pre_check_dq():
                set_dq_running_status_task = CustomSQLOperator(
                    task_id="set_dq_running_status",
                    conn_id=conn_id,
                    sql="ldl_dq_set_running_status.sql"
                )

                clear_log_dq_err_task = CustomSQLOperator(
                    task_id="clear_log_dq_err",
                    conn_id=conn_id,
                    sql="clear_log_dq_err.sql"
                )

                set_dq_running_status_task >> clear_log_dq_err_task

            @task_group(group_id="check_dq")
            def check_dq():
                sqls_rules_list = PrepareSQLDataQualityOperator(
                    task_id='get_dq_sqls',
                    conn_id=conn_id,
                    dq_group=dq_group,
                    rules_ids=rules,
                )

                tasks_sql_list = []
                tasks_exec_rules_sql = []

                for table_name in all_dds_dq_tables:
                    task_sql_list = get_task_sql.override(task_id=f"get_sql_{table_name}")(table_name,
                                                                                           sqls_rules_list.output)

                    tasks_sql_list.append(task_sql_list)

                    exec_rules_sql = CustomSQLOperator.partial(
                        task_id=f"{table_name}",
                        conn_id=conn_id
                    ).expand(sql=task_sql_list)

                    tasks_exec_rules_sql.append(exec_rules_sql)

                chain(sqls_rules_list, tasks_sql_list, tasks_exec_rules_sql)

            @task_group(group_id="post_check_dq")
            def post_check_dq():
                set_dq_error_warning_status_task = CustomSQLOperator(
                    task_id="set_dq_error_warning_status",
                    conn_id=conn_id,
                    trigger_rule="none_failed",
                    sql="ldl_dq_set_error_warning_status.sql"
                )

                set_dq_completed_status_task = CustomSQLOperator(
                    task_id="set_dq_completed_status",
                    conn_id=conn_id,
                    trigger_rule="none_failed",
                    sql="ldl_dq_set_completed_status.sql"
                )

                set_dq_error_warning_status_task >> set_dq_completed_status_task

            return [pre_check_dq(), check_dq(), post_check_dq()]

        case _:
            sqls_list = _get_dq_sqls(rules, dq_group)

            tasks: list = [
                sqls_list,  # task создания этого списка, явная очерёдность
                CustomSQLOperator.partial(
                    task_id=f"dq_{dq_group}",
                    conn_id=conn_id,
                    map_index_template=MAPPED_TASK_NAME_TEMPLATE,
                ).expand_kwargs(sqls_list),
            ]

            return tasks
