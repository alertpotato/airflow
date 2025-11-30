from airflow.datasets import Dataset  # , DatasetAlias
from airflow.datasets.metadata import Metadata
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.utils.context import Context

example_ds_item = Dataset("example_ds_item")
example_ds_finish = Dataset("example_ds_finish")


@task(outlets=[example_ds_item])
def producer_task(period_cd, region_cd):
    # context = get_current_context()
    # context["outlet_events"][example_ds_item].extra = {
    #     'period': period_cd,
    #     'region': region_cd
    # }
    yield Metadata(example_ds_item, {
        'period': period_cd,
        'region': region_cd
    })


@dag(
    dag_display_name="эксперименты с наборами данных",
    start_date=None,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['example']
)
def ds_producer_dag():
    f = EmptyOperator(task_id='finish_task', outlets=[example_ds_finish])

    pairs = [{"period_cd": 190101 + i,
              "region_cd": 42 * (i+1)} for i in range(5)]

    for pair in pairs:
        name = '_'.join(str(v) for v in pair.values())
        producer_task.override(task_id=name)(**pair) >> f


ds_producer_dag()
