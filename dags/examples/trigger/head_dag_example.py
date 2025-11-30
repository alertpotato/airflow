import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils import common
from utils.logs_meta.log_tables import log_etl_run, TableAction as act


DAG_DESCRIPTION = '''
Пример головного DAG'а
'''

INIT_TASK_ID = 'init_run_id'


@task(task_id=INIT_TASK_ID)
def init_run_id():
    context = get_current_context()
    dag_run = context.get('dag_run')
    if dag_run and dag_run.external_trigger:
        print("Это ручной запуск, пропускаем инициализацию, `etl_run_id`: 0")
        init_vars = {'etl_run_id': 0}
    else:
        run_id = log_etl_run(act.get_new_id, context=context)
        init_vars = {'etl_run_id': run_id}

    common.xcom_save(get_current_context(), init_vars, with_log=True)


@task
def finish_etl_run():
    context = get_current_context()
    dag_run = context.get('dag_run')
    if dag_run and dag_run.external_trigger:
        print("Это ручной запуск, пропускаем финализацию, `etl_run_id`: 0")
    else:
        p = common.get_params_from_xcom(context, INIT_TASK_ID, ('etl_run_id',))
        log_etl_run(act.finish, p)


@dag(
    schedule='@once',
    start_date=pendulum.local(2023, 8, 28),
    catchup=False,
    tags=['example'],
    # params={},
    description=DAG_DESCRIPTION.splitlines()[1],
    doc_md=DAG_DESCRIPTION
)
def head_dag_example():

    conf = {
        'etl_run_id':
            "{{ ti.xcom_pull("
            f"task_ids='{INIT_TASK_ID}', key='etl_run_id'"
            ") }}",
    }

    trigger_op = TriggerDagRunOperator(
        task_id='target_dag_trigger',
        trigger_dag_id='target_dag_example',
        wait_for_completion=True,
        poke_interval=5,
        max_active_tis_per_dag=1,
        conf=conf
    )

    (
        init_run_id() >>
        trigger_op >>
        finish_etl_run()
    )


head_dag_example()
