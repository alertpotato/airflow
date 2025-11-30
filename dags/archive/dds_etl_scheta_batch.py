import pendulum

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import ParamValidationError


PROCESSING_DAG_ID = 'dds_etl_scheta'
LIST_PARAM_NAME = 'process_list'

DAG_DESCRIPTION = '''
Пакетная загрузка счетов по комбинациям "регион/период".
Вызывает выполнение DAG'а `%s` для каждого элемента,
переданного в параметре `%s`.

Пример параметров:
``` json
{
    "%s": [
        {"period": "202201", "region": "71"},
        {"period": "202202", "region": "71"},
    ]
}
```
Jira: https://jira.element-lab.ru/projects/KMP/issues/KMP-171
''' % (PROCESSING_DAG_ID, LIST_PARAM_NAME, LIST_PARAM_NAME)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2022, 1, 1),
    catchup=False,
    tags=['УС ОМС', 'СЧЕТА'],
    params={LIST_PARAM_NAME: []},
    description=DAG_DESCRIPTION.splitlines()[1],
    doc_md=DAG_DESCRIPTION
)
def dds_etl_scheta_batch():

    @task()
    def check_args(**context) -> list:
        """
        Проверка параметров
        """

        params = context['params']
        if LIST_PARAM_NAME in params and isinstance(params[LIST_PARAM_NAME], list):
            return params[LIST_PARAM_NAME]
        else:
            message = f'''
            Параметр {LIST_PARAM_NAME} не соответствует требованиям:
            получены параметры: {params}
            '''
            raise ParamValidationError(message)

    args_list = check_args()

    TriggerDagRunOperator.partial(
        task_id="process_list",
        trigger_dag_id=PROCESSING_DAG_ID,
        wait_for_completion=True,
        poke_interval=5,  # как часто проверять результат
        max_active_tis_per_dag=1
    ).expand(conf=args_list)


dds_etl_scheta_batch_dag = dds_etl_scheta_batch()
