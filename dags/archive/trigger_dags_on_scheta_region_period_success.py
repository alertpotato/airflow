from __future__ import annotations

import pendulum, json
from airflow.datasets import Dataset
from airflow.models.dag import DAG
from airflow.decorators import task, task_group
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

dataset_scheta_region_period_success = Dataset("scheta_region_period_success")


@task
def update_run_config(triggering_dataset_events=None):
    context = get_current_context()
    dag_run = context["dag_run"]

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


@task
def get_params_for_risk_vzaim_lek():
    context = get_current_context()
    conf = context["dag_run"].conf

    periods = [
        {
            "clean_temp_tables": True,
            "period_cd": v["period_cd"]
        } for _, v in conf.items()
    ]

    return periods


with DAG(
        dag_id="trigger_dags_on_scheta_region_period_success",
        catchup=False,
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        schedule=[dataset_scheta_region_period_success],
        tags=["dataset"],
) as dag:
    update_run_config_task = update_run_config()


    @task_group(group_id="risk_vzarm_lek")
    def risk_vzaim_lek_tg():
        params_risk_vzaim_lek = get_params_for_risk_vzaim_lek()

        trigger_risk_vzaim_lek_task = TriggerDagRunOperator.partial(
            task_id='run_risk_vzaim_lek',
            trigger_dag_id='risk_vzaim_lek',
            wait_for_completion=False,
        ).expand(conf=params_risk_vzaim_lek)


    update_run_config_task >> risk_vzaim_lek_tg()
