""" Функции для ожидания других экземпляров DAG'а

Возможно это стоит переписать на кастомный оператор.

Example
---
```python
WAITER_TASK_ID = 'wait_for_other_instance'

choice = BranchPythonOperator(
    task_id='wait_or_do_something',
    python_callable=instance_watcher.get_branch_callable(
        sensor_task_id=WAITER_TASK_ID,
        payload_task_id='do_work'
    )
)

wait_task = PythonSensor(
    task_id=WAITER_TASK_ID,
    python_callable=instance_watcher.get_sensor_callable(WAITER_TASK_ID),
    mode='poke',
    poke_interval=10,
    timeout=pendulum.duration(minutes=15).seconds
)

end = EmptyOperator(task_id='dag_finish', trigger_rule='none_failed')

choice >> [do_work(), wait_task] >> end
```
"""

from airflow.operators.python import get_current_context as _get_current_context
from airflow.models.dagrun import DagRun
from airflow.utils.state import State


def get_branch_callable(sensor_task_id: str, payload_task_id: str):
    def what_to_do_next():
        """ Возвращает task_id следующего действия """

        dag_run_id = _get_current_context()['dag_run'].id
        active_dag_runs = _get_active_dag_runs()
        first_active = min(active_dag_runs, key=lambda d: d.id)
        if len(active_dag_runs) > 1 and first_active.id != dag_run_id:
            return sensor_task_id
        return payload_task_id

    return what_to_do_next


def get_sensor_callable(sensor_task_id: str):
    def sensor_func():
        return _other_instance_complete(sensor_task_id)
    return sensor_func


def _get_active_dag_runs():
    """ Возвращает другие экземпляры запусков этого DAG'а
     в статусе `RUNNING` """

    context = _get_current_context()
    dag_id = context['dag'].dag_id
    return DagRun.find(dag_id=dag_id, state=State.RUNNING)


def _find_dag_run_id(sensor_task_id: str):
    """ Возвращает `dag_run.id` выполняющегося в данный момент
    экземпляра этого DAG'а, у которого есть активные задачи
    (кроме задач с `task_id == WAITER_TASK_ID`)
    """
    for dag_run in _get_active_dag_runs():
        if any(1 for ti in dag_run.get_task_instances(state='running')
                if ti.task_id != sensor_task_id):
            return dag_run.id
    from airflow.exceptions import AirflowSkipException
    raise AirflowSkipException(
        "Не найдены активные экземпляры этого DAG. "
        "Обычно это означает, что такой экземпляр только что завершился")


def _other_instance_complete(sensor_task_id: str) -> bool:
    context = _get_current_context()
    id_key = 'pending_dag_run_id'
    pending_dag_run_id = context['ti'].xcom_pull(key=id_key)

    if not pending_dag_run_id:
        pending_dag_run_id = _find_dag_run_id(sensor_task_id)
        context['ti'].xcom_push(id_key, pending_dag_run_id)

    dag_id = context['dag'].dag_id
    target_dag_run = [dr for dr in DagRun.find(
        dag_id=dag_id) if dr.id == pending_dag_run_id][0]

    if target_dag_run.state == 'success':
        # успешное завершение ожидания
        return True
    elif target_dag_run.state == 'failed':
        # ошибка, если отслеживаемый dag_run завершился ошибкой
        from airflow.exceptions import AirflowFailException
        raise AirflowFailException(
            "Другой экземпляр этого DAG'а завершился с ошибкой. "
            f"Вот что мы о нём знаем:\n{target_dag_run}")

    return False
