import pendulum
from os import path
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowFailException
from airflow.models.baseoperator import chain

from utils import common, common_dag_params
from utils.logs_meta.log_tables import (
    TableAction,
    log_etl_proc_table
)
from utils.logs_meta import shared
from utils.dds_variables import scheta_tables
from utils.clean_files import run_if_exist, cleanup_temp_tables_task
from utils.sql_operator import CustomSQLOperator

# STAGE_EXCLUDE = ['schet_src_ids', 'sl_src_ids', 'z_sl_src_ids']
STAGE_EXCLUDE = ['schet_src_ids', 'sl_src_ids', 'z_sl_src_ids', 'doc_ods_id_for_ins']
DQ_EXCLUDE = ['master_person_identification', 'schet_src_ids', 'sl_src_ids', 'z_sl_src_ids', 'doc_ods_id_for_ins',
              'z_sl_stg', 'zap_patient_stg', 'head_zl_list', 'zl_list_schet_nd']
INSERT_EXCLUDE = ['master_person_identification', 'head_zl_list', 'z_sl_stg', 'zap_patient_stg', 'zl_list_schet_nd']
SCHETA = scheta_tables

AIRFLOW_HOME = common.get_airflow_home_path()

SCHETA_SQL_PATH = common.get_sql_home_path() + '/dataflows/dds_etl_scheta'

DQ_RULES_SQL_PATH = common.get_dq_home_path() + '/sql'
DQ_SCRIPTS_SQL_PATH = f'{AIRFLOW_HOME}/sql/dq'
META_SCRIPTS_SQL_PATH = f'{AIRFLOW_HOME}/sql/meta'

TMPL_SEARCH_PATH = []
TMPL_SEARCH_PATH.append(path.join(DQ_RULES_SQL_PATH))
TMPL_SEARCH_PATH.append(path.join(DQ_SCRIPTS_SQL_PATH))
TMPL_SEARCH_PATH.append(path.join(META_SCRIPTS_SQL_PATH))

MT_ETL_PROC_ID = 3
TASK_ID_FOR_PARAMS = 'initialize_all'

DDS_CONN_ID = 'nds_dds'
DQ_CONN_ID = 'nds_dq'

DATA_BASE = "nds"

SCHEMA_STAGE = "stage"

DAG_DESCRIPTION = 'Процесс нормализации таблиц счетов мед помощи'


@task
def delete_duplicate_data():
    params_file_path = SCHETA_SQL_PATH + '/pg_params.sql'
    main_file_path = SCHETA_SQL_PATH + '/delete_duplicate_data.sql'
    params_sql = common.get_file_content(params_file_path)
    main_sql = common.get_file_content(main_file_path)
    sql_result_script = params_sql + '\n' + main_sql

    sql_operator = CustomSQLOperator(
        task_id='delete_duplicate_data_sql',
        conn_id=DDS_CONN_ID,
        sql=sql_result_script,
        split_statements=True,
    )
    sql_operator.execute(get_current_context())


@task
def table_processing(action: str, tbl_name: str):
    context = get_current_context()

    file_key = None
    if action == 'stage':
        file_key = 'stage_1'
    if action == 'insert':
        file_key = 'insert'

    sql_params_file_path = SCHETA_SQL_PATH + '/pg_params.sql'
    sql_main_file_path = '/'.join([
        SCHETA_SQL_PATH,
        tbl_name,
        f"{tbl_name}_{file_key}.sql"
    ])

    sql_params_query = common.get_file_content(sql_params_file_path, context)
    sql_main_query = common.get_file_content(sql_main_file_path, context)

    sql_result_script = sql_params_query + '\n' + sql_main_query

    sql_operator = CustomSQLOperator(
        task_id=f'sql_op_{action}_{tbl_name}',
        handler=lambda cur: cur.rowcount,
        conn_id=DDS_CONN_ID,
        sql=sql_result_script,
        params={'table_name': tbl_name},
        clean_file_check_path=sql_main_file_path
    )
    rowcount = sql_operator.execute(context)

    ids = common.get_table_params_from_xcom(
        context=context,
        from_task_id=TASK_ID_FOR_PARAMS,
        tbl_name=tbl_name
    )
    log_params = {
        'dag_action': action,
        'table_name': tbl_name,
        'new_log_tbl_id': ids['tbl_ids']['new_log_tbl_id'],
        'etl_proc_id': ids['etl_proc_id'],
        'affected_rows': rowcount
    }
    log_etl_proc_table(TableAction.progress, log_params, conn_id=DDS_CONN_ID)


@task.short_circuit
def verify_params():
    context = get_current_context()
    sql_params = common.get_sql_params(context)
    check_list = [
        {
            'query': """
            select 1
            where '{{params.ods_schema}}' in (
                select table_schema from information_schema.tables
            )
            """,
            'ex_message': (
                f"Не найдена 'ods_schema' = '{sql_params['ods_schema']}'"
            )
        },
        {
            'query': """
            select 1
            where '{{params.period_cd}}'in (
                select '20'|| substring(zglv_filename from '_(.{4})') from {{params.ods_schema}}.zl_list_zglv)
            """,
            'ex_message': (
                f"Не найден 'period_cd' = '{sql_params['period_cd']}'"
            )
        },
    ]

    for i, check in enumerate(check_list):
        cop = CustomSQLOperator(
            task_id=f'verify_params_{i + 1}',
            handler=lambda cur: bool(cur.fetchone()),
            conn_id=DDS_CONN_ID,
            sql=check['query'],
        )
        if not cop.execute(context):
            raise AirflowFailException(
                check['ex_message'] +
                " (см. параметры DAG'а и метод common.get_sql_params)")

    return True


@task(task_id=TASK_ID_FOR_PARAMS)
def init_all():
    """
    Получает из базы новые id сохраняет в XCOM.
    Структуру см. в описании `shared.init_all(...)`
    """

    context = get_current_context()
    dag_params = context['params']

    my_tables = {
        s_name: {
            'mt_tbl_id': s_value['tbl_id']
        }
        for s_name, s_value in SCHETA.items()
    }
    log_ids = {
        **common.get_sql_params(context),
        'etl_run_id': dag_params['etl_run_id'],
        'etl_proc_id': None,
        'all_tbl_ids': my_tables,
    }
    log_ids = shared.init_all(log_ids, context, MT_ETL_PROC_ID)
    common.xcom_save(context, log_ids, with_log=True)


@task(task_id='finish_all_log_tables')
def finish_all():
    log_etl_params = common.get_params_from_xcom(
        get_current_context(),
        TASK_ID_FOR_PARAMS,
        ('etl_run_id', 'etl_proc_id', 'all_tbl_ids')
    )
    shared.finish_partial(
        tables_to_finish=('log_etl_proc', 'log_etl_proc_table'),
        log_etl_params=log_etl_params,
    )


def find_dq_sql_file(find_path, rule_id, rule_group):
    import glob
    import os
    os.chdir(f"{find_path}/{rule_group}/")
    for file in glob.glob(f"{rule_id}_*.sql"):
        return file


@task
def get_ffoms_sql(tbl_name: str, queries: dict):
    return queries.get(tbl_name, [])


@task
def prepare_ref_sql(tbl_name: str, table_id: int, rules: list, columns: list, etl_proc_id, **context):
    log_etl_params = common.get_sql_params(context)

    result = []

    for rule in rules:
        rule_id = rule[0]
        rule_group = rule[1]

        if not columns:
            continue

        file_name = find_dq_sql_file(DQ_RULES_SQL_PATH, rule[0], rule[1])

        params = {
            "stage_schema": log_etl_params["stage_schema"],
            "temp_suffix": log_etl_params["temp_suffix"],
            "table_name": tbl_name,
            "columns": columns
        }

        sql = common.get_rendered_file_content(context, f"{DQ_RULES_SQL_PATH}/{rule_group}/{file_name}",
                                               {"params": params})

        query_params = {
            "table_id": table_id,
            "rule_id": rule_id,
            "query_check": sql,
            "etl_proc_id": etl_proc_id
        }

        final_sql = common.get_rendered_file_content(context, f"{DQ_SCRIPTS_SQL_PATH}/insert_log_dq_err.sql",
                                                     {"params": query_params})

        result.append(final_sql)

    return result


@task
def prepare_ffoms_rules_sql(dq_rules, etl_proc_id, **context):
    import logging

    log_etl_params = common.get_sql_params(context)

    result = {}

    for rule in dq_rules:
        rule_id = rule[0]
        rule_group = rule[1]
        table_id = rule[2]
        table_name = rule[3]
        type_d = rule[4]
        file_ver = rule[5]

        logging.info(f"Ищем файл правил: {DQ_RULES_SQL_PATH}/{rule_group}/{rule_id}_*.sql")

        file_name = find_dq_sql_file(DQ_RULES_SQL_PATH, rule_id, rule_group)

        if file_name:
            logging.info(f"Найден файл правил: {DQ_RULES_SQL_PATH}/{rule_group}/{file_name}")
        else:
            AirflowFailException(f"Не найден файл правил для rule_id: {DQ_RULES_SQL_PATH}/{rule_group}/{rule_id}_*.sql")

        sql_dq_params = {
            "stage_schema": log_etl_params["stage_schema"],
            "dds_schema": log_etl_params["dds_schema"],
            "temp_suffix": log_etl_params["temp_suffix"],
            "where_dq_condition": f"t.doc_ods_id in (select doc_ods_id from meta.log_document_load "
                                  f"where etl_proc_id = {etl_proc_id} and position(file_type_cd in '{type_d}')>0 and file_ver = '{file_ver}')"
        }

        sql_dq = common.get_rendered_file_content(context, f"{DQ_RULES_SQL_PATH}/{rule_group}/{file_name}",
                                                  {"params": sql_dq_params})

        query_params = {
            "table_id": table_id,
            "rule_id": rule_id,
            "query_check": sql_dq,
            "etl_proc_id": etl_proc_id
        }

        final_sql = common.get_rendered_file_content(context, f"{DQ_SCRIPTS_SQL_PATH}/insert_log_dq_err.sql",
                                                     {"params": query_params})

        if table_name not in result.keys():
            result[table_name] = []

        result[table_name].append(final_sql)

    return result


@task
def prepare_dq_report_sql(report_tables, etl_proc_id, **context):
    log_etl_params = common.get_sql_params(context)

    mapping = {
        0: {
            "parent_el": None,
            "parent_el_id": None
        },
        # b_diag
        99: {
            "parent_el": "ZL_LIST/ZAP/Z_SL/SL/SL_ID",
            "parent_el_id": "sl_id"
        },
        # b_prot
        100: {
            "parent_el": "ZL_LIST/ZAP/Z_SL/SL/SL_ID",
            "parent_el_id": "sl_id"
        },
        # cons
        101: {
            "parent_el": "ZL_LIST/ZAP/Z_SL/SL/SL_ID",
            "parent_el_id": "sl_id"
        },
        # ins_policy
        102: {
            "parent_el": None,
            "parent_el_id": None
        },
        # ksg_kpg
        103: {
            "parent_el": "ZL_LIST/ZAP/Z_SL/SL/SL_ID",
            "parent_el_id": "sl_id"
        },
        # ksg_kpg_crit
        104: {
            "parent_el": "ZL_LIST/ZAP/Z_SL/SL/SL_ID",
            "parent_el_id": "sl_id"
        },
        # lek_dose
        105: {
            "parent_el": "ZL_LIST/ZAP/Z_SL/SL/SL_ID",
            "parent_el_id": "sl_id"
        },
        # lek_pr
        106: {
            "parent_el": "ZL_LIST/ZAP/Z_SL/SL/SL_ID",
            "parent_el_id": "sl_id"
        },
        # med_dev
        108: {
            "parent_el": "ZL_LIST/ZAP/Z_SL/SL/SL_ID",
            "parent_el_id": "sl_id"
        },
        # napr
        109: {
            "parent_el": "ZL_LIST/ZAP/Z_SL/SL/SL_ID",
            "parent_el_id": "sl_id"
        },
        # naz
        110: {
            "parent_el": "ZL_LIST/ZAP/Z_SL/SL/SL_ID",
            "parent_el_id": "sl_id"
        },
        # onk_sl
        115: {
            "parent_el": "ZL_LIST/ZAP/Z_SL/SL/SL_ID",
            "parent_el_id": "sl_id"
        },
        # onk_usl
        116: {
            "parent_el": "ZL_LIST/ZAP/Z_SL/SL/SL_ID",
            "parent_el_id": "sl_id"
        },
        # person
        118: {
            "parent_el": "ZL_LIST/ZAP/N_ZAP",
            "parent_el_id": "id_pac"
        },
        # sank
        124: {
            "parent_el": "ZL_LIST/ZAP/Z_SL/IDCASE",
            "parent_el_id": "case_b_id"
        },
        # schet
        125: {
            "parent_el": "ZL_LIST/SCHET/NSCHET",
            "parent_el_id": "nschet"
        },
        # sl
        127: {
            "parent_el": "ZL_LIST/ZAP/Z_SL/SL/SL_ID",
            "parent_el_id": "id"
        },
        # sl_code_mes1
        128: {
            "parent_el": "ZL_LIST/ZAP/Z_SL/SL/SL_ID",
            "parent_el_id": "sl_id"
        },
        # sl_ds
        129: {
            "parent_el": "ZL_LIST/ZAP/Z_SL/SL/SL_ID",
            "parent_el_id": "sl_id"
        },
        # sl_koef
        130: {
            "parent_el": "ZL_LIST/ZAP/Z_SL/SL/SL_ID",
            "parent_el_id": "sl_id"
        },
        # usl
        136: {
            "parent_el": "ZL_LIST/ZAP/Z_SL/SL/SL_ID",
            "parent_el_id": "sl_id"
        },
        # z_sl
        137: {
            "parent_el": "ZL_LIST/ZAP/Z_SL/IDCASE",
            "parent_el_id": "case_b_id"
        },

    }

    result = []

    for item in report_tables:
        table_id = item[0]
        table_name = item[1]

        params = {
            "etl_proc_id": etl_proc_id,
            "temp_suffix": log_etl_params["temp_suffix"],
            "stage_schema": log_etl_params["stage_schema"],
            "table_id": table_id,
            "table_name": table_name,
            "parent_el": mapping[table_id]["parent_el"],
            "parent_el_id": mapping[table_id]["parent_el_id"]
        }

        report_sql = common.get_rendered_file_content(context,
                                                      f"{DQ_SCRIPTS_SQL_PATH}/generate_report_all.sql",
                                                      {"params": params})

        result.append(report_sql)

    return result


@dag(
    start_date=pendulum.datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    # max_active_runs=1,
    default_args={
        'on_failure_callback': shared.get_on_failure_function(
            tables_to_finish=('log_etl_proc', 'log_etl_proc_table'),
            xcom_keys=('etl_run_id', 'etl_proc_id', 'all_tbl_ids'),
            with_document=True
        )
    },
    params=common_dag_params.PARAMS_DICT,
    template_searchpath=TMPL_SEARCH_PATH,
    description=DAG_DESCRIPTION,
    doc_md=DAG_DESCRIPTION + common_dag_params.get_md_description(),
    tags=['УС ОМС', 'СЧЕТА']
)
def dds_etl_scheta_dq_test():
    set_etl_running_status_task = CustomSQLOperator(
        task_id="set_etl_running_status",
        conn_id=DQ_CONN_ID,
        sql="ldl_etl_set_running_status.sql",
        split_statements=True
    )

    tasks_list_s = []
    with TaskGroup("stage", tooltip='Сборка временных таблиц') as stage_group:
        for config_name, _ in SCHETA.items():
            if config_name in STAGE_EXCLUDE:
                continue

            task_stage = table_processing.override(
                task_id=config_name + '_stage')(
                action='stage',
                tbl_name=config_name)

            tasks_list_s.append(task_stage)
        chain(*tasks_list_s)

    tasks_list_i = []
    with TaskGroup("insert", tooltip='Заполнение ддс') as insert_group:
        for config_name, _ in SCHETA.items():
            if config_name in INSERT_EXCLUDE:
                continue

            task_insert = table_processing.override(
                task_id=config_name + '_insert')(
                action='insert',
                tbl_name=config_name)
            tasks_list_i.append(task_insert)
        chain(*tasks_list_i)

    with TaskGroup("dq", tooltip="Контроль качества данных") as dq_group:
        set_dq_running_status_task = CustomSQLOperator(
            task_id="set_dq_running_status",
            conn_id=DQ_CONN_ID,
            sql="ldl_dq_set_running_status.sql"
        )

        clear_log_dq_err_task = CustomSQLOperator(
            task_id="clear_log_dq_err",
            conn_id=DQ_CONN_ID,
            sql="clear_log_dq_err.sql"
        )

        task_list = []

        # with TaskGroup("check_ref", tooltip="Проверка ссылочных полей") as dq_reference_group:
        #     get_ref_rules_task = CustomSQLOperator(
        #         task_id="get_ref_rules",
        #         conn_id=DQ_CONN_ID,
        #         sql="get_ref_rules.sql",
        #         do_xcom_push=True
        #     )

        #     for table_name, value in SCHETA.items():
        #         table_id = value["tbl_id"]

        #         column_list_task = CustomSQLOperator(
        #             task_id=f"get_columns_{table_name}",
        #             conn_id=DQ_CONN_ID,
        #             sql="get_column_list.sql",
        #             do_xcom_push=True,
        #             params={
        #                 "table_name": table_name
        #             }
        #         )
        #         column_list_task.set_upstream(get_ref_rules_task)

        #         sql_list_task = prepare_ref_sql.override(task_id=f"get_sql_{table_name}")(table_name, table_id,
        #                                                                                   get_ref_rules_task.output,
        #                                                                                   column_list_task.output,
        #                                                                                   "{{ ti.xcom_pull(key='etl_proc_id', task_ids='initialize_all') }}")
        #         sql_list_task.set_upstream(column_list_task)

        #         check_ref_task = CustomSQLOperator.partial(
        #             task_id=f"check_ref_{table_name}",
        #             conn_id=DQ_CONN_ID,
        #         ).expand(sql=sql_list_task)
        #         # check_ref_task.set_upstream(sql_list_task)

        #         task_list.append(check_ref_task)

        #     get_ref_rules_task >> task_list

        with TaskGroup("check_ffoms", tooltip="Проверка данных по правилам ФФОМС") as dq_ffoms_group:
            get_ffoms_rules_task = CustomSQLOperator(
                task_id="get_ffoms_rules",
                conn_id=DQ_CONN_ID,
                sql="get_ffoms_rules.sql",
                do_xcom_push=True,
                trigger_rule="none_failed"
            )

            prepare_ffoms_sql_task = prepare_ffoms_rules_sql(get_ffoms_rules_task.output,
                                                             "{{ ti.xcom_pull(key='etl_proc_id', task_ids='initialize_all') }}")
            prepare_ffoms_sql_task.set_upstream(get_ffoms_rules_task)

            task_list.clear()
            for table_name in SCHETA.keys():
                sql_list_task = get_ffoms_sql.override(task_id=f"get_sql_{table_name}")(table_name,
                                                                                        prepare_ffoms_sql_task)
                sql_list_task.set_upstream(prepare_ffoms_sql_task)

                check_ffoms_task = CustomSQLOperator.partial(
                    task_id=f"{table_name}",
                    conn_id=DDS_CONN_ID,
                ).expand(sql=sql_list_task)
                check_ffoms_task.set_upstream(sql_list_task)

                task_list.append(check_ffoms_task)

        set_dq_error_warning_status_task = CustomSQLOperator(
            task_id="set_dq_error_warning_status",
            conn_id=DQ_CONN_ID,
            trigger_rule="none_failed",
            sql="ldl_dq_set_error_warning_status.sql"
        )

        set_dq_completed_status_task = CustomSQLOperator(
            task_id="set_dq_completed_status",
            conn_id=DQ_CONN_ID,
            sql="ldl_dq_set_completed_status.sql"
        )

        set_dq_running_status_task >> clear_log_dq_err_task >> dq_ffoms_group \
        >> set_dq_error_warning_status_task >> set_dq_completed_status_task

    with TaskGroup("report_dq", "Создание отчета об ошибках") as report_dq_group:
        clear_report_oms_log_task = CustomSQLOperator(
            task_id="clear_report_oms_log",
            conn_id=DQ_CONN_ID,
            sql="clear_report_oms_log.sql"
        )

        get_err_tables_task = CustomSQLOperator(
            task_id="get_err_tables",
            conn_id=DQ_CONN_ID,
            sql="get_err_tables.sql"
        )

        prepare_dq_report_sql_task = prepare_dq_report_sql(get_err_tables_task.output,
                                                           "{{ ti.xcom_pull(key='etl_proc_id', task_ids='initialize_all') }}")

        generate_report_task = CustomSQLOperator.partial(
            task_id="generate_report",
            conn_id=DQ_CONN_ID
        ).expand(sql=prepare_dq_report_sql_task)

        clear_report_oms_log_task >> get_err_tables_task >> prepare_dq_report_sql_task >> generate_report_task

        # del_dupls_task = delete_duplicate_data.override()

    set_etl_finish_status_task = CustomSQLOperator(
        task_id="set_etl_finish_status",
        conn_id=DQ_CONN_ID,
        sql="ldl_etl_set_finish_status.sql"
    )

    # verify_params() >> init_all() >> stage_group >> logical_dq_group \
    # >> report_dq_group >> finish_all() >> cleanup_temp_tables_task()  # >> del_dupls_task() >> insert_group

    verify_params() >> init_all() >> set_etl_running_status_task \
    >> stage_group >> dq_group >> delete_duplicate_data() >> insert_group >> report_dq_group >> set_etl_finish_status_task


dds_etl_scheta_dq_test()