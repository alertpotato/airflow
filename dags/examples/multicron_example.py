import pendulum

from airflow.decorators import dag, task
from multicron_plugin import MultiCronTimetable
from airflow.operators.python import get_current_context


@task
def print_current_dt_and_next_runs():
    context = get_current_context()
    dag = context['dag']

    next_runs = []
    last_interval = dag.get_run_data_interval(context['dag_run'])
    tz = pendulum.now().timezone_name
    for _ in range(5):
        if not (dr_info := dag.next_dagrun_info(last_interval)):
            break
        last_interval = dr_info.data_interval
        next_runs.append(dr_info[0].in_timezone(tz))

    local_format = '%H:%M:%S %d.%m.%Y'
    msgs = [
        '-' * 3,
        f">>> Сейчас: {pendulum.now().strftime(local_format)}",
        ">>> Планируемые запуски:"
    ]
    for nr in next_runs:
        msgs.append(f"\t{nr.strftime(local_format)}")

    print('\n'.join(msgs))


@dag(
    start_date=pendulum.local(2023, 11, 30),
    # schedule="11,12,13 16 * * *",
    # schedule=MultiCronTimetable(["11,12,13 16 * * *"]),
    # schedule=MultiCronTimetable(["0-10/5 * * * *", "15-20/2 * * * *"]),
    schedule=MultiCronTimetable(["*/20 3 8 * *", "0 12 * * MON,TUE"]),
    # schedule=MultiCronTimetable("@once"),
    catchup=False,
    tags=['example'],
)
def multicron_example():
    print_current_dt_and_next_runs()


multicron_example()
