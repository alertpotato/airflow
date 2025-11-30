import pendulum

from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from utils import common
from utils.yaml_dag import yaml_reader

builder = yaml_reader.YamlDagBuilder(
    'dataflows/examples/yaml_dag_example/yaml_dag_example.yaml')


@task
def create_sql_vars():
    sql_vars = {
        'var1': 'value1',
        'var2': 'super value',
    }
    common.xcom_save(get_current_context(), {'sql_vars': sql_vars}, True)


@task
def new_task(arg: str = "'без аргумента'"):
    print(f"новая задача: {arg}")



@task.short_circuit
def to_start_or_not_to_start():
    return True


@task
def the_end():
    print("Th-th-th-that's all folks!")

@dag(
    start_date=pendulum.local(2023, 7, 1),
    tags=['example'],
    **builder.get_dag_args()
)
def yaml_dag_example():

    @task_group
    def pre_processing_group():
        new_task("перед выполнением")
        create_sql_vars()

    builder.create_tasks(
        pre_init_task=to_start_or_not_to_start(),
        pre_processing_task=pre_processing_group(),
        post_processing_task=new_task.override(
            task_id='another_task', trigger_rule='all_done')(),
        after_all_task=the_end()
    )


yaml_dag_example()
