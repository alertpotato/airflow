import pendulum

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.logs_meta import shared
from utils import common
from utils.logs_meta.log_tables import TableAction
from utils.logs_meta.extra import get_table_meta_id

TASK_ID_FOR_PARAMS = 'init_all'
SQL_SCRIPT_PATH = common.get_sql_home_path() + '/dataflows/dds_etl_erzl'
CONN_ID = 'nds_dds'
TABLE_ERZL = ('erzl_insured', 'erzl_attached')
TARGET_SCHEMA = 'DDS'
MT_ETL_PROC_ID = 7


@task(task_id=TASK_ID_FOR_PARAMS)
def init_all():
    """ Инициализация логирования """

    context = get_current_context()
    dag_params = context['params']
    my_tables = {}
    pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            my_tables = {table: {'mt_tbl_id': get_table_meta_id(
                TARGET_SCHEMA, table, cur=cur)}
                for table in TABLE_ERZL}

    log_ids = {
        'etl_run_id': dag_params['etl_run_id'],
        'etl_proc_id': None,
        'all_tbl_ids': my_tables
    }

    # TODO: подумать о get_sql_params в init_all

    log_ids = shared.init_all(log_ids, context, MT_ETL_PROC_ID)
    common.xcom_save(context, log_ids, with_log=True)


@task
def table_action(table):
    """ Выполняет sql для одной таблицы """

    from utils.logs_meta.log_tables import log_etl_proc_table
    table_ids = common.get_table_params_from_xcom(
        context=get_current_context(),
        from_task_id=TASK_ID_FOR_PARAMS,
        tbl_name=table
    )
    log_params = {
        'dag_action': 'insert',
        'table_name': table,
        'new_log_tbl_id': table_ids['tbl_ids']['new_log_tbl_id'],
        'etl_proc_id': table_ids['etl_proc_id']
    }

    sql_file_path = '/'.join([SQL_SCRIPT_PATH, table,
                             table + '_insert' + '.sql'])
    sql_query = common.get_file_content(sql_file_path)
    pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_query)
            log_params['affected_rows'] = cur.rowcount
            log_etl_proc_table(TableAction.progress, log_params, cur=cur)


@task
def finish_all():
    """ Логгирует окончание """

    log_etl_params = common.get_params_from_xcom(
        context=get_current_context(),
        from_task_id=TASK_ID_FOR_PARAMS,
        keys=('etl_run_id', 'etl_proc_id', 'all_tbl_ids'))

    shared.finish_all(
        log_etl_params=log_etl_params,
    )


@dag(
    dag_id='dds_etl_erzl',
    start_date=pendulum.datetime(2023, 3, 17, 2, 0, 0),
    schedule='@daily',
    catchup=False,
    description='Загрузка таблиц ЕРЗЛ',
    tags=['dds', 'erzl'],
    render_template_as_native_obj=True,
    template_searchpath=SQL_SCRIPT_PATH,
    default_args={
        'on_failure_callback': shared.get_on_failure_function(
            tables_to_finish=('log_etl_run', 'log_etl_proc',
                              'log_etl_proc_table'),
            xcom_keys=('etl_run_id', 'etl_proc_id', 'all_tbl_ids'),
        )
    },
    params={'etl_run_id': -1, 'period_dt': None}
)
def dds_erzl():

    tasks = {}
    with TaskGroup("insert", tooltip='Заполнение ддс') as group_execute:
        for table in TABLE_ERZL:
            tasks[table] = table_action.override(task_id=table)(table)
        [tasks['erzl_insured'], tasks['erzl_attached']]

    init_all() >> group_execute >> finish_all()


dds_erzl()
