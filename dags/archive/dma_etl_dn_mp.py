import logging
import pendulum

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup

from utils import dds_functions

DDS_SCHEMA = "dds"
DMA_SCHEMA = "dma"

TARGET_TABLE = "dn_mp"

DMA_CONN_ID = "nds_dma"

RELATIVE_DATAMARTS_PATH = 'sql/dma'
SQL_SCRIPT_PATH = "/opt/airflow/sql/dma/dn_mp"
# SQL_SCRIPTS_PATH = (
#         dds_functions.get_ariflow_home_path() +
#         '/' +
#         RELATIVE_DATAMARTS_PATH
# )

TASK_ID_FOR_PARAMS = "log_init.save_params"


def log_action(action, tbl_name, rowcount):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    params = get_proc_id_and_tbl_id_from_xcom(
        context=get_current_context(),
        from_task_id=TASK_ID_FOR_PARAMS,
        table_name=tbl_name)

    if action == "stage":
        sql = f"update meta.log_etl_proc_table set status_cd='R', finish_dttm =now(),status_desc='Создание промежуточных таблиц' " \
              f"where id={params['tbl_id']} and log_etl_proc_id = {params['proc_id']}"
    elif action == "insert":
        sql = f"update meta.log_etl_proc_table set status_cd='R',finish_dttm =now(),status_desc='Вставлены новые строки в таблицу {tbl_name}',rec_ins_qnt={rowcount} " \
              f"where id = {params['tbl_id']} and log_etl_proc_id = {params['proc_id']}"
    elif action == "delete":
        sql = f"update meta.log_etl_proc_table set status_cd='R',finish_dttm =now(),status_desc='Удалены строки в таблице {tbl_name}',rec_del_qnt={rowcount} " \
              f"where id = {params['tbl_id']} and log_etl_proc_id = {params['proc_id']}"

    postgres = PostgresHook(postgres_conn_id=DMA_CONN_ID)
    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


def get_action_files(path, action):
    import glob
    import os
    files = glob.glob(f"{path}/{TARGET_TABLE}_{action}*.sql")

    return sorted([os.path.basename(x).split('.')[0] for x in files])


def get_proc_id_and_tbl_id_from_xcom(context, from_task_id: str, table_name: str) -> dict:
    """ возвращает `proc_id` и `tbl_id` из `XCOM` """

    ti = context['ti']

    run_id = ti.xcom_pull(
        task_ids=from_task_id, key='run_id')

    proc_id = ti.xcom_pull(
        task_ids=from_task_id, key='proc_id')

    tbl_ids = ti.xcom_pull(
        task_ids=from_task_id, key='tbl_ids')

    tbl_id = tbl_ids[table_name]

    return {
        "run_id": run_id,
        "proc_id": proc_id,
        "tbl_id": tbl_id
    }


def etl_proc_failure_function(context):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    params = get_proc_id_and_tbl_id_from_xcom(
        context=context,
        from_task_id=TASK_ID_FOR_PARAMS,
        table_name=TARGET_TABLE)

    dag_error = str(context.get('exception')).partition(
        '\n')[0].replace("'", '')

    postgres = PostgresHook(postgres_conn_id=DMA_CONN_ID)
    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"update meta.log_etl_proc_table set status_cd='E',finish_dttm=now(),status_desc='Процесс остановлен ошибкой',log_info='{dag_error}' where id = {params['tbl_id']} and log_etl_proc_id = {params['proc_id']};")
            cur.execute(
                f"update meta.log_etl_run set status_cd='E',finish_dttm=now() where id = {params['run_id']};")
            cur.execute(
                f"update meta.log_etl_proc set status_cd='E',finish_dttm=now(),status_desc='Процесс остановлен ошибкой',log_info='{dag_error}' where id = {params['proc_id']} and etl_run_id = {params['run_id']};")


@task
def get_new_etl_tbl_id(table_name, etl_proc_id):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id=DMA_CONN_ID)
    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            # Логирование начала процесса

            sql = "INSERT INTO meta.log_etl_proc_table (id, log_etl_proc_id, table_id, status_cd, status_desc, rec_ins_qnt, rec_upd_qnt, rec_del_qnt, begin_dttm, finish_dttm, log_info) " \
                  "SELECT " \
                  " nextval('meta.etl_proc_tbl_id_seq'::regclass) AS id" \
                  f",{etl_proc_id} AS log_etl_proc_id" \
                  ",t.id AS table_id" \
                  ",'N' AS status_cd" \
                  ",'Инициализация дага' AS status_desc" \
                  ",0 AS rec_ins_qnt" \
                  ",0 AS rec_upd_qnt" \
                  ",0 AS rec_del_qnt" \
                  ",now() AS begin_dttm" \
                  ",NULL AS finish_dttm" \
                  ",'' AS log_info " \
                  "FROM meta.mt_table AS t " \
                  f"WHERE t.name = '{table_name}' " \
                  f"RETURNING id"

            cur.execute(sql)
            return int(cur.fetchone()[0])


@task
def close_etl_run_and_proc_id():
    """ Записывает в таблицы `meta.log_etl_proc` и `log_etl_run`  """

    from airflow.providers.postgres.hooks.postgres import PostgresHook

    params = get_proc_id_and_tbl_id_from_xcom(
        context=get_current_context(),
        from_task_id=TASK_ID_FOR_PARAMS,
        table_name=TARGET_TABLE)

    result = f"close_etl_run_and_proc_id for etl_proc_id={params['proc_id']}, etl_run_id={params['run_id']}"
    postgres = PostgresHook(postgres_conn_id=DMA_CONN_ID)
    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"update meta.log_etl_proc set status_cd='C',finish_dttm =now(),status_desc='Процесс завершен' "
                f"where id = {params['proc_id']} and etl_run_id = {params['run_id']};")
            cur.execute(
                f"update meta.log_etl_run set status_cd='C',finish_dttm =now() where id = {params['run_id']};")
            return 'SUCCESS ' + result


@task
def close_etl_tbl_id():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    params = get_proc_id_and_tbl_id_from_xcom(
        context=get_current_context(),
        from_task_id=TASK_ID_FOR_PARAMS,
        table_name=TARGET_TABLE)

    postgres = PostgresHook(postgres_conn_id=DMA_CONN_ID)
    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            # Логирование конца процесса
            sql_string = f"update meta.log_etl_proc_table set status_cd='C',finish_dttm =now(),status_desc='Процесс завершен' where id = {params['tbl_id']} and log_etl_proc_id = {params['proc_id']};"
            logging.info(f"Execution sql: {sql_string}")
            cur.execute(sql_string)


@task
def dn_mp_execute_sql(actions, **kwargs):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id=DMA_CONN_ID)
    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            for action in actions:
                sql_string = str(kwargs["templates_dict"][action])
                logging.info(f"Execution sql: {sql_string}")
                cur.execute(sql_string)
                logging.info(f"Rows affected: {cur.rowcount}")

                log_action(action, TARGET_TABLE, cur.rowcount)


@dag(
    "dma_etl_dn_mp",
    start_date=pendulum.datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={"start_dt": None, "end_dt": None, "tfoms_id": 0},
    render_template_as_native_obj=True,
    description='Процесс наполнения витрины данных dn_mp',
    template_searchpath=SQL_SCRIPT_PATH,
    on_failure_callback=etl_proc_failure_function,
    tags=["dma"]
)
def dma_etl_mp():
    with TaskGroup("log_init", tooltip='Инициализация лога') as log_init_group:
        etl_run_id = dds_functions.get_new_etl_run_id.override(
            task_id="run_initialize")()
        etl_proc_id = dds_functions.get_new_etl_proc_id.override(
            task_id="etl_initialize")(etl_run_id)
        etl_tbl_id = get_new_etl_tbl_id.override(
            task_id="tbl_initialize")(TARGET_TABLE, etl_proc_id)

        save_params_task = dds_functions.save_params({
            "run_id": etl_run_id,
            "proc_id": etl_proc_id,
            "tbl_ids": {TARGET_TABLE: etl_tbl_id}

        })

        etl_run_id >> etl_proc_id >> etl_tbl_id >> save_params_task

    prev_task = None
    with TaskGroup("stage", tooltip='Сборка временных таблиц') as stage_group:
        files = get_action_files(SQL_SCRIPT_PATH, "stage")

        for file in files:
            task_stage = dn_mp_execute_sql.override(task_id=file,
                                                    templates_dict={
                                                        "stage": f"{file}.sql"},
                                                    templates_exts=[".sql"])(["stage"])

            if prev_task:
                prev_task.set_downstream(task_stage)

            prev_task = task_stage

    task_insert = dn_mp_execute_sql.override(task_id="dn_mp_insert", templates_dict={
        "delete": "dn_mp_delete.sql",
        "insert": "dn_mp_insert.sql"
    },
        templates_exts=[".sql"])(["delete", "insert"])

    with TaskGroup("log_finalize", tooltip='Закрытие лога') as log_finalize_group:
        finalize_tbl = close_etl_tbl_id.override(
            task_id='tbl_finalize')()

        finalize_run_proc = close_etl_run_and_proc_id.override(
            task_id='etl_finalize_and_run_finalize')()

        finalize_run_proc.set_upstream(finalize_tbl)

    log_init_group >> stage_group >> task_insert >> log_finalize_group


dma_etl_mp()
