from pendulum import datetime

from airflow.datasets import Dataset
# from airflow.datasets import DatasetAlias
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context

example_ds_item = Dataset("example_ds_item")
example_ds_finish = Dataset("example_ds_finish")


@task
def print_data():
    context = get_current_context()
    triggering_dataset_events = context["triggering_dataset_events"]
    for dataset, event_list in triggering_dataset_events.items():
        print(f"DataSet: {dataset}")
        for event in event_list:
            print(f"\t{event.id=}, {event.extra=}, {event.source_run_id=}")


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=[example_ds_item, example_ds_finish],  # по умолчанию тут "AND"
    catchup=False,
    tags=['example']
)
def ds_consumer_dag():

    EmptyOperator(task_id="empty_task") >> print_data()


ds_consumer_dag()
