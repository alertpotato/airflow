from __future__ import annotations

import pendulum
from airflow.datasets import Dataset
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

dataset_batch_head_scheta_success = Dataset("batch_head_scheta_success")

with DAG(
        dag_id="dma_trigger_cpp_rakurs",
        catchup=False,
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        schedule=[dataset_batch_head_scheta_success],
        tags=["dataset", "dma", "cpp"],
) as dag:
    @task
    def update_run_config(triggering_dataset_events=None, **contex):
        dag_run = contex["dag_run"]

        if not dag_run.conf and dag_run.run_type == 'dataset_triggered':
            conf = {}

            for dataset, events_list in triggering_dataset_events.items():
                for event in events_list:
                    etl_run_id = event.extra["etl_run_id"]
                    conf[etl_run_id] = {
                        "region_cd": event.extra.get("region_cd"),
                        "period_cd": event.extra.get("period_cd")
                    }

            dag_run.conf = conf


    trigger_cpp_task = TriggerDagRunOperator(
        task_id='run_dma_cpp_rakurs',
        trigger_dag_id='dma_cpp_rakurs',
        wait_for_completion=False,
    )

    update_run_config() >> trigger_cpp_task
