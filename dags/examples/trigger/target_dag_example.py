import pendulum
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from utils import common
from utils.logs_meta import shared

TASK_ID_FOR_PARAMS = 'init_all'
MT_ETL_PROC_ID = 7

default_args = {
    'on_failure_callback': shared.get_on_failure_function(
        tables_to_finish=('log_etl_proc', 'log_etl_proc_table'),
        xcom_keys=('etl_run_id', 'etl_proc_id', 'all_tbl_ids'),
    )
}


@task(task_id=TASK_ID_FOR_PARAMS)
def init_all():
    context = get_current_context()
    dag_params = context['params']

    log_ids = {
        'etl_run_id': dag_params['etl_run_id'],
        'etl_proc_id': None,
    }
    log_ids = shared.init_all(log_ids, context, 1)
    common.xcom_save(context, log_ids, with_log=True)


@task(task_id='finish_all_log_tables')
def finish_all():
    log_etl_params = common.get_params_from_xcom(
        get_current_context(),
        TASK_ID_FOR_PARAMS,
        ('etl_run_id', 'etl_proc_id')
    )
    shared.finish_partial(
        ('log_etl_proc', 'log_etl_proc_table'),
        log_etl_params)


@task
def print_params():
    context = get_current_context()
    log_lines = [">>> params:"]
    params = context.get('params')
    if params:
        log_lines.extend([f"\t{k}: {v}" for k, v in params.items()])
    else:
        log_lines.append("Параметры не обнаружены")

    print('\n'.join(log_lines))


@dag(
    start_date=pendulum.local(2023, 8, 28),
    schedule=None,
    catchup=False,
    tags=['example'],
    default_args=default_args,
    params={'etl_run_id': 0}
)
def target_dag_example():

    with TaskGroup("process_groups") as process_groups_tg:
        print_params()

    process_groups_tg
    init_all() >> process_groups_tg >> finish_all()


target_dag_example()
