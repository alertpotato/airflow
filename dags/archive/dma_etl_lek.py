import logging

import pendulum
from airflow.decorators import (
    dag,
    task,
)
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup

from utils import dds_functions

DMA_CONN_ID = "nds_dma"
DDS_CONN_ID = "nds_dds"

SQL_SCRIPT_PATH = "/opt/airflow/sql/dma/lek"
TASK_ID_FOR_PARAMS = "log_init.save_params"

TARGET_TABLES = {
    'lek_sh_2_sh': {
        'delete': 'lek_sh_2_sh/lek_sh_2_sh_delete.sql',
        'insert': 'lek_sh_2_sh/lek_sh_2_sh_insert.sql',
    },
    'lek_pls_ds_par_tmp01': {
        'insert': 'lek_pls_ds_par_tmp01/lek_pls_ds_par_tmp01_insert.sql',
    },
    'lek_ds_npolis_sh_current': {
        'delete': 'lek_ds_npolis_sh_current/lek_ds_npolis_sh_current_delete.sql',
        'insert': 'lek_ds_npolis_sh_current/lek_ds_npolis_sh_current_insert.sql',
    },
    'lek_ds_npolis_sh': {
        'delete': 'lek_ds_npolis_sh/lek_ds_npolis_sh_delete.sql',
        'insert': 'lek_ds_npolis_sh/lek_ds_npolis_sh_insert.sql',
    },
    'lek_ds_npolis_sh_current_upgraded': {
        'delete': 'lek_ds_npolis_sh_current_upgraded/lek_ds_npolis_sh_current_upgraded_delete.sql',
        'insert': 'lek_ds_npolis_sh_current_upgraded/lek_ds_npolis_sh_current_upgraded_insert.sql',
    },
    'lek_ds_npolis_sh_upgraded': {
        'delete': 'lek_ds_npolis_sh_upgraded/lek_ds_npolis_sh_upgraded_delete.sql',
        'insert': 'lek_ds_npolis_sh_upgraded/lek_ds_npolis_sh_upgraded_insert.sql',
    },
    'lek_ds_sh_cnt_advanced': {
        'delete': 'lek_ds_sh_cnt_advanced/lek_ds_sh_cnt_advanced_delete.sql',
        'insert': 'lek_ds_sh_cnt_advanced/lek_ds_sh_cnt_advanced_insert.sql',
    },
    'lek_ds_npolis_sh_predictions_grouped': {
        'delete': 'lek_ds_npolis_sh_predictions_grouped/lek_ds_npolis_sh_predictions_grouped_delete.sql',
        'insert': 'lek_ds_npolis_sh_predictions_grouped/lek_ds_npolis_sh_predictions_grouped_insert.sql',
    },
    'lek_predictions_with_prices': {
        'delete': 'lek_predictions_with_prices/lek_predictions_with_prices_delete.sql',
        'insert': 'lek_predictions_with_prices/lek_predictions_with_prices_insert.sql',
    },
    'lek_predictions_with_prices_grouped': {
        'delete': 'lek_predictions_with_prices_grouped/lek_predictions_with_prices_grouped_delete.sql',
        'insert': 'lek_predictions_with_prices_grouped/lek_predictions_with_prices_grouped_insert.sql',
    },
}


def get_proc_id_and_tbl_id_from_xcom(context, from_task_id: str, table_name: str) -> dict:
    """ возвращает `proc_id` и `tbl_id` из `XCOM` """
    ti = context['ti']
    run_id = ti.xcom_pull(task_ids=from_task_id, key='run_id')
    proc_id = ti.xcom_pull(task_ids=from_task_id, key='proc_id')
    tbl_ids = ti.xcom_pull(task_ids=from_task_id, key='tbl_ids')
    tbl_id = tbl_ids[table_name]
    return {
        "run_id": run_id,
        "proc_id": proc_id,
        "tbl_id": tbl_id,
    }


def log_action(action, tbl_name, rowcount):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    params = get_proc_id_and_tbl_id_from_xcom(
        context=get_current_context(),
        from_task_id=TASK_ID_FOR_PARAMS,
        table_name=tbl_name,
    )

    if action == "insert":
        sql = f"""
            update meta.log_etl_proc_table 
            set status_cd='R',
                finish_dttm =now(),
                status_desc='Вставлены новые строки в таблицу {tbl_name}',
                rec_ins_qnt={rowcount}
            where id = {params['tbl_id']} 
            and log_etl_proc_id = {params['proc_id']}
        """
    elif action == "delete":
        sql = f"""
            update meta.log_etl_proc_table
            set status_cd='R',
                finish_dttm =now(),
                status_desc='Удалены строки в таблице {tbl_name}',
                rec_del_qnt={rowcount}
            where id = {params['tbl_id']}
            and log_etl_proc_id = {params['proc_id']}
        """

    postgres = PostgresHook(postgres_conn_id=DMA_CONN_ID)
    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


@task
def get_new_etl_tbl_id(table_name, etl_proc_id):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id=DDS_CONN_ID)
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
def close_etl_run_and_proc_id(target_table):
    """ Записывает в таблицы `meta.log_etl_proc` и `log_etl_run`  """

    from airflow.providers.postgres.hooks.postgres import PostgresHook

    params = get_proc_id_and_tbl_id_from_xcom(
        context=get_current_context(),
        from_task_id=TASK_ID_FOR_PARAMS,
        table_name=target_table)

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
def close_etl_tbl_id(target_table):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    params = get_proc_id_and_tbl_id_from_xcom(
        context=get_current_context(),
        from_task_id=TASK_ID_FOR_PARAMS,
        table_name=target_table)

    postgres = PostgresHook(postgres_conn_id=DMA_CONN_ID)
    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            # Логирование конца процесса
            sql_string = f"""
                update meta.log_etl_proc_table
                set status_cd='C',
                    finish_dttm =now(),
                    status_desc='Процесс завершен'
                where id = {params['tbl_id']}
                and log_etl_proc_id = {params['proc_id']};
            """
            logging.info(f"Execution sql: {sql_string}")
            cur.execute(sql_string)


def etl_proc_failure_function(context):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    params = get_proc_id_and_tbl_id_from_xcom(
        context=context,
        from_task_id=TASK_ID_FOR_PARAMS,
        table_name='lek_predictions_with_prices_grouped')

    dag_error = str(context.get('exception')).partition(
        '\n')[0].replace("'", '')

    postgres = PostgresHook(postgres_conn_id=DMA_CONN_ID)
    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                    update meta.log_etl_proc_table 
                    set status_cd='E',
                        finish_dttm=now(),
                        status_desc='Процесс остановлен ошибкой',
                        log_info='{dag_error}' 
                    where id = {params['tbl_id']} 
                    and log_etl_proc_id = {params['proc_id']};"""
            )
            cur.execute(
                f"update meta.log_etl_run set status_cd='E',finish_dttm=now() where id = {params['run_id']};")
            cur.execute(
                f"""
                    update meta.log_etl_proc 
                    set status_cd='E',
                        finish_dttm=now(),
                        status_desc='Процесс остановлен ошибкой',
                        log_info='{dag_error}' 
                    where id = {params['proc_id']} 
                    and etl_run_id = {params['run_id']};"""
            )


@task
def lek_execute_sql(target_table, actions, **kwargs):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id=DMA_CONN_ID)
    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            for action in actions:
                sql_string = str(kwargs["templates_dict"][action])
                logging.info(f"Execution sql: {sql_string}")
                cur.execute(sql_string)
                logging.info(f"Rows affected: {cur.rowcount}")

                log_action(action, target_table, cur.rowcount)


@dag(
    'dma_etl_lek',
    start_date=pendulum.datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={},
    description='Загрузка витрины лекарств',
    tags=['dma', 'lek'],
    render_template_as_native_obj=True,
    template_searchpath=SQL_SCRIPT_PATH,
    on_failure_callback=etl_proc_failure_function,
    orientation='TB',
)
def wf_dag() -> None:
    with TaskGroup("log_init", tooltip='Инициализация лога') as log_init_group:
        etl_run_id = dds_functions.get_new_etl_run_id.override(
            task_id="run_initialize")()
        etl_proc_id = dds_functions.get_new_etl_proc_id.override(
            task_id="etl_initialize")(etl_run_id)
        tbl_ids = {
            target_table: get_new_etl_tbl_id.override(
                task_id="tbl_initialize")(target_table, etl_proc_id)
            for target_table in TARGET_TABLES
        }
        dds_functions.save_params({
            "run_id": etl_run_id,
            "proc_id": etl_proc_id,
            "tbl_ids": tbl_ids,
        })

    with TaskGroup('sql_queries', tooltip='Расчет витрины лекарств') as calc_lek_mart_group:
        from airflow.utils.helpers import chain
        sql_query_tasks = []
        for table_name, templates_dict in TARGET_TABLES.items():
            sql_query_tasks.append(
                lek_execute_sql.override(
                    task_id=table_name,
                    templates_dict=templates_dict,
                    templates_exts=[".sql"],
                )(table_name, templates_dict),
            )
        chain(*sql_query_tasks)

    with TaskGroup("log_finalize", tooltip='Закрытие лога') as log_finalize_group:
        for target_table in TARGET_TABLES:
            finalize_tbl = close_etl_tbl_id.override(
                task_id='tbl_finalize')(target_table)
            finalize_run_proc = close_etl_run_and_proc_id.override(
                task_id='etl_finalize_and_run_finalize')(target_table)
            finalize_run_proc.set_upstream(finalize_tbl)

    log_init_group >> calc_lek_mart_group >> log_finalize_group


wf_dag()
