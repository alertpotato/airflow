import pendulum

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils import common
from utils.dds_variables import nsi_dicts_all
from utils.logs_meta.log_tables import TableAction
from utils.logs_meta import shared
from utils.clean_files import run_if_exist, cleanup_temp_tables_task


NSI_DICTIONARY = nsi_dicts_all
SQL_PATH = common.get_airflow_home_path() + '/sql/dds_etl_dictionary'
TASK_ID_FOR_PARAMS = 'initialize_all'
CONN_ID = 'nds_dds'


def get_flat_nsis(source: dict) -> dict:
    res = {}
    for _, sv in source.items():
        for nk, nv in sv.items():
            res[nk] = nv
    return res


def get_group_tooltip(action: str, name: str) -> str:
    return f'Выполнение "{action}" для всех справочников [{name}]'


def etl_proc_failure_function(context):
    ''' Записывает в базу статус ошибки для
    - etl_run_id
    - etl_proc_id
    - ** и для каждой таблицы
    '''

    log_etl_params = common.get_params_from_xcom(
        context,
        TASK_ID_FOR_PARAMS,
        ('etl_run_id', 'etl_proc_id', 'all_tbl_ids')
    )
    log_etl_params['log_info'] = str(
        context.get('exception')
    ).partition('\n')[0].replace("'", '')

    shared.finish_all(
        conn_id=CONN_ID,
        log_etl_params=log_etl_params,
        action=TableAction.failure
    )


@task()
def process_nsi(action: str, nsi_name: str):
    """ Обработка справочника """

    from utils.logs_meta.log_tables import log_etl_proc_table

    context = get_current_context()
    xparams = common.get_table_params_from_xcom(
        context=context,
        from_task_id=TASK_ID_FOR_PARAMS,
        tbl_name=nsi_name)

    file_key = 'ERROR__file_key__'
    if action == 'stage':
        file_key = 'stage_1'
    if action == 'update':
        file_key = action
    if action == 'insert':
        file_key = action

    sql_params_file_path = SQL_PATH + "/pg_params.sql"
    sql_main_query_file_path = '/'.join([
        SQL_PATH,
        nsi_name,
        f"{nsi_name}_{file_key}.sql"
    ])

    params_content = common.get_file_content(sql_params_file_path)
    main_content = common.get_file_content(sql_main_query_file_path)
    # тут могла бы быть (ваша реклама) обработка шаблона
    # from airflow.utils.helpers import parse_template_string

    sql_result_script = '\n'.join([
        params_content,
        main_content,
    ])

    log_params = {
        'dag_action': action,
        'table_name': nsi_name,
        'new_log_tbl_id': xparams['tbl_ids']['new_log_tbl_id'],
        'etl_proc_id': xparams['proc_id'],
    }

    postgres = PostgresHook(postgres_conn_id=CONN_ID)
    common.log_it({
        "Current params": "",
        **log_params,
        'sql_params_file_path': sql_params_file_path,
        'sql_main_query_file_path': sql_main_query_file_path,
        "SQL query to execute": '\n' + sql_result_script
    }, postgres.log.info)

    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            run_if_exist(
                file_path=sql_main_query_file_path,
                context=context,
                cur=cur,
                log_method=postgres.log.info)

            cur.execute(sql_result_script)
            postgres.log.info(
                "Main query completed successfully, affected rows: %s",
                cur.rowcount)

            log_params['affected_rows'] = cur.rowcount
            log_etl_proc_table(TableAction.progress, log_params, cur=cur)


@task(task_id=TASK_ID_FOR_PARAMS)
def init_all():
    """
    Получает из базы новые id для
    - log_etl_run
    - log_etl_proc
    - log_etl_proc_table (для каждой таблицы в `NSI_DICTIONARY`)

    И записывает в XCOM такую структуру:

    ```json
    {
        "etl_run_id": 123,
        "etl_proc_id": 456,
        "all_tbl_ids": {
            "nsi_n016": {
                "mt_tbl_id": 26,
                "new_log_tbl_id": 7777
            },
            "nsi_n017": {
                "mt_tbl_id": 27,
                "new_log_tbl_id": 8888
            },
        }
    }```
    """

    from utils.logs_meta.log_tables import (
        log_etl_run,
        log_etl_proc,
        log_etl_proc_table
    )

    postgres = PostgresHook(postgres_conn_id=CONN_ID)
    with postgres.get_conn() as conn:
        with conn.cursor() as cur:

            etl_run_id = log_etl_run(TableAction.get_new_id, cur=cur)
            etl_proc_id = log_etl_proc(
                TableAction.get_new_id,
                {
                    'etl_run_id': etl_run_id,
                    'dag_type': 'dds_dictionary',
                },
                cur=cur)
            nsi_ids = {}
            flat_nsis = get_flat_nsis(NSI_DICTIONARY)
            for f_nsi_name, f_nsi_params in flat_nsis.items():
                nsi_ids[f_nsi_name] = {
                    'mt_tbl_id': f_nsi_params['dict_id'],
                    'new_log_tbl_id': log_etl_proc_table(
                        TableAction.get_new_id,
                        {
                            'etl_proc_id': etl_proc_id,
                            'mt_tbl_id': f_nsi_params['dict_id'],
                        },
                        cur=cur)
                }

            to_save = {
                'etl_run_id': etl_run_id,
                'etl_proc_id': etl_proc_id,
                'all_tbl_ids': nsi_ids,
            }

    common.xcom_save(get_current_context(), to_save, with_log=True)


@task(task_id='finish_all_log_tables')
def finish_all():
    log_etl_params = common.get_params_from_xcom(
        get_current_context(),
        TASK_ID_FOR_PARAMS,
        ('etl_run_id', 'etl_proc_id', 'all_tbl_ids')
    )
    shared.finish_all(
        conn_id=CONN_ID,
        log_etl_params=log_etl_params,
    )


@dag(
    start_date=pendulum.datetime(2022, 1, 1),
    catchup=False,
    schedule=None,
    description='Процесс обновления справочников',
    tags=['ЦМП', 'НСИ'],
    params={"clean_temp_tables": True},
    default_args={
        'on_failure_callback': etl_proc_failure_function
    }
)
def dds_etl_dictionary():

    actions = [
        'stage',
        # 'dq',
        'update',
        'insert'
    ]

    prev_action = init_all()

    for action in actions:
        # для каждого действия (на. stage > dq > update ...)

        with TaskGroup(f'{action}') as tg_action:
            prev_nsi_gr = None

            for group_name, nsi_object in NSI_DICTIONARY.items():
                # для каждой группы справочников. например:
                # group_name = "nsi_stage_0_dict"
                # nsi_object = {"XXX": {"dict_id": XXX, "dict_name": "XXX"},

                with TaskGroup(
                    group_id=f'{action}_for_{group_name}',
                    tooltip=get_group_tooltip(action, group_name)
                ) as tg:

                    for nsi_name, _ in nsi_object.items():
                        # для каждого справочника и его параметров. например:
                        # nsi_name = XXX
                        # nsi_parms = {"dict_id": XXX, "dict_name": "XXX"}

                        process_nsi.override(
                            task_id=f"{nsi_name}__{action}")(action, nsi_name)

                if prev_nsi_gr:
                    prev_nsi_gr >> tg
                prev_nsi_gr = tg
        prev_action >> tg_action
        prev_action = tg_action

    prev_action >> finish_all() >> cleanup_temp_tables_task()


dds_etl_dictionary()
