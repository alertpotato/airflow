import pendulum

from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.sensors.python import PythonSensor
from airflow.operators.python import get_current_context, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils import common
from utils import instance_watcher
from utils.logs_meta import shared
from utils.logs_meta.log_tables import TableAction
from utils.clean_files import run_if_exist, cleanup_temp_tables_task


CONN_ID = 'nds_dds'
TASK_ID_FOR_PARAMS = 'initialize_all'
WAITER_TASK_ID = 'wait_for_other_instance'
SQL_PATH = common.get_sql_home_path() + '/dataflows/dds_etl_dictionary'
MT_ETL_PROC_ID = 2
DAG_DESCRIPTION = '''
Процесс обновления справочников

Группы (верхний уровень) для параметра nsis - генерируются головным
дагом и хранятся в переменной Airflow: dds_etl_dictionary_keys

Пример параметров:
```json
{
    "etl_run_id": null,             # идентификатор ETL процесса.
    "clean_temp_tables": True,      # выполнять ли файлы очистки повторно в конце выполнения дага
    "nsis":                         # аналогично словарю nsi_dicts_all в utils/dds_variables.py
    {
        "nsi_stage_1_dict": {
            "nsi_n016": {
                "dict_id": 26,
                "dict_name": "..."
            },
            "nsi_n017": {
                "dict_id": 27,
                "dict_name": "..."
            },
            "nsi_n018": {
                "dict_id": 28,
                "dict_name": "..."
            },
            "nsi_n019": {
                "dict_id": 29,
                "dict_name": "..."
            },
            "nsi_n020": {
                "dict_id": 30,
                "dict_name": "..."
            }
        },
        "nsi_stage_2_dict": {},
        "nsi_stage_3_dict": {},
        "nsi_stage_4_dict": {},
        "nsi_stage_5_dict": {}
    },
}
```
'''


def get_group_tooltip(action: str, name: str) -> str:
    return f'Выполнение "{action}" для всех справочников [{name}]'


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
        'etl_proc_id': xparams['etl_proc_id'],
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
    Получает из базы новые id сохраняет в XCOM.
    Структуру см. в описании `shared.init_all(...)`
    """

    context = get_current_context()
    dag_params = context['params']

    my_tables = {}
    for _, sv in dag_params['nsis'].items():
        for nk, nv in sv.items():
            my_tables[nk] = {
                'mt_tbl_id': nv['dict_id']
            }
    log_ids = {
        'etl_run_id': dag_params['etl_run_id'],
        'etl_proc_id': None,
        'all_tbl_ids': my_tables
    }
    log_ids = shared.init_all(log_ids, context, MT_ETL_PROC_ID)
    common.xcom_save(context, log_ids, with_log=True)


@task(task_id='finish_all_log_tables')
def finish_all():
    log_etl_params = common.get_params_from_xcom(
        get_current_context(),
        TASK_ID_FOR_PARAMS,
        ('etl_run_id', 'etl_proc_id', 'all_tbl_ids')
    )
    shared.finish_partial(
        tables_to_finish=('log_etl_proc', 'log_etl_proc_table'),
        log_etl_params=log_etl_params,
    )


@task
def get_expandable_params(gr_name, action):
    """ Возвращает список, каждый элемент которого будет выглядеть так:
    `{'name': k, 'value': v, 'action': action}`

    из элементов группы `gr_name` из параметров, переданных в даг
    """

    context = get_current_context()
    return [{'action': action, 'nsi_name': k}
            for k, _ in context['params']['nsis'][gr_name].items()]


@dag(
    start_date=pendulum.datetime(2022, 1, 1),
    catchup=False,
    schedule=None,
    description=DAG_DESCRIPTION.splitlines()[1],
    doc_md=DAG_DESCRIPTION,
    tags=['ЦМП', 'НСИ'],
    params={"clean_temp_tables": True, "etl_run_id": None, "nsis": {}},
    default_args={
        'on_failure_callback': shared.get_on_failure_function(
            tables_to_finish=('log_etl_proc', 'log_etl_proc_table'),
            xcom_keys=('etl_run_id', 'etl_proc_id', 'all_tbl_ids'),
        ),
    },
)
def dds_etl_dictionary():

    check_active = BranchPythonOperator(
        task_id='check_active_dagruns',
        python_callable=instance_watcher.get_branch_callable(
            sensor_task_id=WAITER_TASK_ID,
            payload_task_id=TASK_ID_FOR_PARAMS
        )
    )

    wait_task = PythonSensor(
        task_id=WAITER_TASK_ID,
        python_callable=instance_watcher.get_sensor_callable(WAITER_TASK_ID),
        mode='poke',
        poke_interval=10,
        timeout=pendulum.duration(minutes=15).seconds
    )

    init_all_task = init_all()  # task_id = TASK_ID_FOR_PARAMS

    check_active >> [init_all_task, wait_task]

    actions = [
        'stage',
        'update',
        'insert'
    ]

    dict_keys: list = Variable.get(
        'dds_etl_dictionary_keys',
        deserialize_json=True,
        default_var=[]
    )

    prev_action = init_all_task

    for action in actions:
        # для каждого действия (на. stage > dq > update ...)

        with TaskGroup(f'{action}') as tg_action:
            prev_nsi_gr = None

            for group_name in dict_keys:
                # для каждой группы справочников. например:
                # group_name = "nsi_stage_0_dict"

                with TaskGroup(group_id=f'{action}_for_{group_name}',
                               tooltip=get_group_tooltip(action, group_name)
                               ) as tg:
                    exp_params = get_expandable_params(group_name, action)

                    process_nsi.expand_kwargs(exp_params)

                if prev_nsi_gr:
                    prev_nsi_gr >> tg
                prev_nsi_gr = tg
        prev_action >> tg_action
        prev_action = tg_action

    @task.short_circuit(trigger_rule='none_failed')
    def has_init():
        """ Проверяем - завершился ли успехом шаг инициализации """
        context = get_current_context()
        return common.is_this_task_has_state(
            context,
            TASK_ID_FOR_PARAMS,
            'success'
        )

    prev_action >> has_init() >> finish_all() >> cleanup_temp_tables_task()


dds_etl_dictionary()
