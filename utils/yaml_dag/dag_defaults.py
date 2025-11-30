import logging

from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.utils.context import Context
from utils import common

TASK_ID_FOR_PARAMS = 'initialize_all'
TASK_ID_FOR_YAML_VARS = 'save_yaml_vars'

STEP_TASK_ID_PARAM = 'task_id'


def _parse_query_sqlglot(query):
    from sqlglot import exp, logger, parse_one

    logger.setLevel("ERROR")

    parsed_content = parse_one(query, read='postgres')

    # ищем insert, update, delete вложенные в cte
    ctes = parsed_content.find_all(exp.CTE)

    for cte in ctes:
        table = cte.find(exp.Table)

        match cte.this.key:
            case 'insert':
                result = ('rec_ins_qnt', table.name)
            case 'update':
                result = ('rec_upd_qnt', table.name)
            case 'delete':
                result = ('rec_del_qnt', table.name)
            case _:
                result = None

        return result

    table = parsed_content.find(exp.Table)

    match parsed_content.key:
        case 'insert':
            result = ('rec_ins_qnt', table.name)
        case 'update':
            result = ('rec_upd_qnt', table.name)
        case 'delete':
            result = ('rec_del_qnt', table.name)
        case _:
            result = None

    return result


def _parse_query_regexp(query):
    import re

    # insert
    insert_pattern = r'^insert\s+into\s+"?(?:\w*"?\.)?"?(\w*)"?'
    if match := re.search(insert_pattern, query, flags=re.IGNORECASE):
        return "rec_ins_qnt", match.group(1)

    # update
    update_pattern = r'^update\s+"?(?:\w*"?\.)?"?(\w*)"?'
    if match := re.search(update_pattern, query, flags=re.IGNORECASE):
        return "rec_upd_qnt", match.group(1)

    # delete
    delete_pattern = r'^delete\s+from\s+"?(?:\w*"?\.)?"?(\w*)"?'
    if match := re.search(delete_pattern, query, flags=re.IGNORECASE):
        return "rec_del_qnt", match.group(1)

    return None


def get_sql_parser():
    func_name = Variable.get('sql_parser', '_parse_query_regexp')

    return eval(func_name)


sql_parser = get_sql_parser()


def add_sql_params_file(context: Context, sql_scripts: list):
    """ Добавляет содержимое файла `pg_params.sql` в sql_scripts,
    если файл существует и выводит `info` сообщение, если нет."""

    params_file_name = "pg_params.sql"
    try:
        params_content = common.get_file_content(params_file_name, context)
        sql_scripts.append(params_content)
    except FileNotFoundError:
        context['ti'].log.info(
            "SQL параметры не были добавлены перед скриптом, так как файл"
            " [%s] не найден", params_file_name)


def get_op_handler(is_check: bool, step_args: dict):
    """ Возвращает обработчик для SQL-оператора (`CustomSQLOperator`)

    Если `is_check` - возвращается метод, проверяющий ошибки в этом столбце
        (в этом случае в `step_args` так же должен быть ключ `file`,
        для вывода ошибок);

    В противном случае возвращается метод получения количества строк
    (`cur.rowcount`)
    """

    if is_check:
        from utils.yaml_dag.result_checker import ResultChecker
        return ResultChecker(step_args['file']).handler_checker

    return lambda cur: (cur.rowcount, sql_parser(cur.query.decode()))


@task(task_id=TASK_ID_FOR_YAML_VARS)
def save_yaml_vars(yaml_vars: dict):
    common.xcom_save(get_current_context(), {'yaml_vars': yaml_vars}, True)


@task()
def process_step(step_args: dict):
    """ Обработка процесса """
    import pendulum

    from utils.sql_operator import CustomSQLOperator
    from utils.yaml_dag.table_depends import skip_task_if_table_depends_fail

    context = get_current_context()

    skip_task_if_table_depends_fail(step_args, context)

    # поддержка произвольных переменных, см: docs/tech.md#yaml-dag
    sql_vars = common.get_params_from_xcom_any_task(context, keys='sql_vars')

    sql_scripts = []
    add_sql_params_file(context, sql_scripts)

    main_content = common.get_file_content(step_args['file'], context)

    sql_scripts.append(main_content)
    sql_result_script = '\n'.join(sql_scripts)
    conn_id = step_args.get('conn_id') or context['params']['DB_CONN_ID']

    is_check = bool(step_args.get('check'))

    begin_dttm = pendulum.now()

    sql_operator = CustomSQLOperator(
        task_id=f"sql_op_{step_args[STEP_TASK_ID_PARAM]}",
        handler=get_op_handler(is_check, step_args),
        conn_id=conn_id,
        sql=sql_result_script,
        x_params=sql_vars or {},
        clean_file_check_path=step_args['file'],
        split_statements=True,
        return_last=False
    )
    output = sql_operator.execute(context)

    finish_dttm = pendulum.now()

    if context['params'].get('dry_run') or is_check:
        return  # остальное выполнять не нужно

    log_progress(output, step_args, context, begin_dttm, finish_dttm)


def log_progress(output: list[tuple], step_args: dict, context: Context, begin_dttm, finish_dttm):
    """ Подготовка параметров и выполнение логирования прогресса
    по таблице (метод `log_etl_proc_table`)

    """

    log_params = {}

    for row_count, parse_results in output:
        if parse_results is None:
            continue

        op, table_name = parse_results

        log_params.setdefault(
            table_name,
            {
                "rec_ins_qnt": 0,
                "rec_upd_qnt": 0,
                "rec_del_qnt": 0,
            }
        )

        log_params[table_name][op] += row_count

    if not log_params:
        return

    from airflow.models import Variable
    from utils.sql_operator import CustomSQLOperator

    log_sql = Variable.get('debug_logs_meta', None) == 'on'
    if log_sql:
        context['ti'].log.info("Выполнение SQL запроса в модуле logs_meta:")

    duration = finish_dttm.diff(begin_dttm).in_seconds()

    CustomSQLOperator(
        task_id="log_etl_proc_table",
        conn_id=context['params']['DB_CONN_ID'],
        sql="log_affected_rows.sql",
        log_sql=log_sql,
        x_params={
            "table_id": step_args["table_id"],
            "log_params": log_params,
            "begin_dttm": begin_dttm,
            "finish_dttm": finish_dttm,
            "duration": duration
        },
    ).execute(context)


@task(task_id=TASK_ID_FOR_PARAMS)
def init_all(yaml_path):
    """
    Получает из базы новые id сохраняет в XCOM.
    Структуру см. в описании `shared.init_all(...)`
    """
    from utils.logs_meta import shared
    from utils.skipping_tasks import get_tasks_for_skip
    from utils.yaml_dag.yaml_reader import YamlDagBuilder

    context = get_current_context()
    dag_params = context['params']

    builder = YamlDagBuilder(yaml_path)

    # поддержка модуля пропуска задач
    tasks_skip_param = get_tasks_for_skip(context) or []
    table_ids = {
        k: v for k, v in builder.get_all_table_ids(validate=True).items()
        if not any(k in tid for tid in tasks_skip_param)
    }  # не инициализируем таблицы для пропускаемых задач
    # NOTE: этот способ может отработать некорректно, если в DAG
    # есть несколько шагов, с одинаковым task_id, но валидация в методе
    # get_all_table_ids выведет предупреждение об этом в лог задачи.

    log_ids = {
        'etl_run_id': dag_params.get('etl_run_id'),
        'etl_proc_id': None,
        'all_tbl_ids': table_ids
    }
    if dag_params.get('dry_run', False):
        context['ti'].log.info('dry_run, только сохраняем id, без инициализации')
        common.xcom_save(context, log_ids, with_log=True)
        return  # NOTE: dry_run

    log_ids = shared.init_all(log_ids, context, dag_params['MT_ETL_PROC_ID'])
    common.xcom_save(context, log_ids, with_log=True)


@task(task_id='finish_all_log_tables')
def finish_all():
    from utils.logs_meta import shared

    context = get_current_context()
    if context['params'].get('dry_run', False):
        context['ti'].log.info('dry_run, не запускаем финализацию')
        return  # NOTE: dry_run

    log_etl_params = common.get_params_from_xcom(
        context,
        TASK_ID_FOR_PARAMS,
        ('etl_run_id', 'etl_proc_id', 'all_tbl_ids')
    )
    shared.finish_all(
        log_etl_params=log_etl_params,
    )
