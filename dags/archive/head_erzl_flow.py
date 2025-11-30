import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils import common
from utils.logs_meta.log_tables import TableAction as act, log_etl_run


DB_CONN_ID = 'nds_dds'

DAG_DESCRIPTION = '''
Головной даг ЕРЗЛ

Jira: [KMP-923](https://jira.element-lab.ru/browse/KMP-923)
'''


@task
def init_run_id():
    # p = {'etl_run_id': log_etl_run(act.get_new_id, conn_id=DB_CONN_ID)}
    p = {'etl_run_id': -1}  # NOTE: на время тестов
    common.xcom_save(get_current_context(), p, with_log=True)


@task
def finish_etl_run():
    run_id = get_current_context()['ti'].xcom_pull(key='etl_run_id')
    if run_id == -1:
        print("Finish test etl_run_id: -1")
        return  # NOTE: на время тестов
    log_etl_run(act.finish, {'etl_run_id': run_id}, conn_id=DB_CONN_ID)


@task
def get_period_dt():
    ''' "логика создания параметра period_dt" '''

    return get_current_context()['params']['period_dt']


@dag(
    schedule=None,
    start_date=pendulum.datetime(2022, 1, 1),
    catchup=False,
    # tags=['dev'],
    params={'period_dt': None},
    description=DAG_DESCRIPTION.split('\n\n', maxsplit=1)[0],
    doc_md=DAG_DESCRIPTION
)
def head_erzl_flow():

    # period = get_period_dt()
    conf = {
        'etl_run_id': "{{ ti.xcom_pull(task_ids='init_run_id', key='etl_run_id') }}",
        'period_dt': "{{ params.period_dt }}"
    }

    @task
    def trigger_dds_etl_erzl(conf_for_trigger):
        TriggerDagRunOperator(
            task_id='run_dds_etl_erzl',
            trigger_dag_id='dds_etl_erzl',
            wait_for_completion=True,
            poke_interval=5,
            max_active_tis_per_dag=1,
            conf=conf_for_trigger
        ).execute(get_current_context())

    @task
    def trigger_dds_etl_erzl_dma(conf_for_trigger):
        TriggerDagRunOperator(
            task_id='run_dds_etl_erzl_dma',
            trigger_dag_id='dds_etl_erzl_dma',
            wait_for_completion=True,
            poke_interval=5,
            max_active_tis_per_dag=1,
            conf=conf_for_trigger
        ).execute(get_current_context())

    init_run_id() \
        >> trigger_dds_etl_erzl(conf) >> trigger_dds_etl_erzl_dma(conf) \
        >> finish_etl_run()


head_erzl_flow()
