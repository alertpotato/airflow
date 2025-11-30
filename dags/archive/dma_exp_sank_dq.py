from __future__ import annotations
from typing import TYPE_CHECKING

import pendulum
from os import path
from airflow.decorators import dag, task, task_group
from utils.yaml_dag import dag_defaults
from utils import common, common_dag_params
from utils.logs_meta import shared
from airflow.operators.python import get_current_context
from utils.sql_operator import CustomSQLOperator
from airflow.exceptions import AirflowFailException
from utils.common_dag_params import PARAMS_DICT

if TYPE_CHECKING:
    from airflow.utils.context import Context

DDS_CONN_ID = 'nds_dds'
DQ_CONN_ID = 'nds_dq'

AIRFLOW_HOME = common.get_airflow_home_path()

DQ_RULES_SQL_PATH = common.get_dq_home_path() + '/sql'
DQ_SCRIPTS_SQL_PATH = f'{AIRFLOW_HOME}/sql/dq'

META_SCRIPTS_SQL_PATH = f'{AIRFLOW_HOME}/sql/meta'
SANK_SCRIPTS_SQL_PATH = f'{common.get_sql_home_path()}/dataflows/dds_etl_expertise'
YAML_PATH = f'{common.get_sql_home_path()}/dataflows/dds_etl_expertise/exp_sank.yaml'

TMPL_SEARCH_PATH = []
TMPL_SEARCH_PATH.append(path.join(DQ_RULES_SQL_PATH))
TMPL_SEARCH_PATH.append(path.join(DQ_SCRIPTS_SQL_PATH))
TMPL_SEARCH_PATH.append(path.join(META_SCRIPTS_SQL_PATH))
TMPL_SEARCH_PATH.append(path.join(SANK_SCRIPTS_SQL_PATH))

TASK_ID_FOR_PARAMS = 'initialize_all'
MT_ETL_PROC_ID = 29

SANK = {
    "sank": {"tbl_id": 124, "tbl_name": ""},
    "sank_exp": {"tbl_id": 712, "tbl_name": ""},
    "sank_src_ids": {"tbl_id": 767, "tbl_name": ""},
    "src01_exp_zap": {"tbl_id": 768, "tbl_name": ""},
    "kmp_result_exp": {"tbl_id": 769, "tbl_name": ""},
}


def get_sql_params(context: Context, with_log=False) -> dict:
    """
    Возвращает параметры из `context['params']`, перечисленные в общих
      параметрах (`common_dag_params.PARAMS_DICT`) и добавляет:
    - `etl_run_id` - id этого ETL процесса (из XCOM)
    - `etl_proc_id` - id группы ETL процессов (из XCOM)
    - `period_cd` - код периода в формате YYYYMM (приведённый к строке)
    - `region_cd` - код региона (приведённый к строке)
    - `period_dt` - начало периода "YYYY-MM-01" из `period_cd` (если указан)
    - `ods_schema` - схемы ods, формируется из `region_cd` (если указан)
    - `temp_suffix` - окончание для временных таблиц (только если
        указаны `region_cd` и `period_cd`)

    Args:
        context (Context): контекст текущего дага
        with_log (bool): Вывести в лог задачи полученные значения?
         По-умолчанию - нет.
    """

    if 'params' not in context:
        raise KeyError("Параметры не обнаружены в переменной 'context'")

    dp = context['params']

    # параметры из DAG, перечисленные в общих
    result_params = {k: dp[k] for k in dp if k in PARAMS_DICT}

    # добавляем period, region и зависимые от них параметры, если заданы
    period = dp.get('period_cd')
    region = dp.get('region_cd')
    if period:
        result_params.update({
            'period_cd': str(period),
            'period_dt': f"{period[0:4]}-{period[4:6]}-01",
        })
    if region:
        result_params.update({
            'region_cd': str(region),
            'ods_schema': result_params.get('ods_schema') or f"ods_{region}"
        })

    result_params['temp_suffix'] = dp['temp_suffix']

    logs_meta_ids = common.get_params_from_xcom_any_task(
        context=context,
        keys=('etl_run_id', 'etl_proc_id')
    )
    result_params.update(logs_meta_ids)

    if with_log:
        common.log_it({
            "using SQL params": "",
            **result_params
        }, context['ti'].log.info)

    return result_params


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
        for s_name, s_value in SANK.items()
    }
    log_ids = {
        **common.get_sql_params(context),
        'etl_run_id': dag_params['etl_run_id'],
        'etl_proc_id': None,
        'all_tbl_ids': my_tables,
    }
    log_ids = shared.init_all(log_ids, context, MT_ETL_PROC_ID)
    common.xcom_save(context, log_ids, with_log=True)


@task(task_id='finish_all_log_tables', trigger_rule="none_failed")
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
def prepare_ffoms_rules_sql(dq_rules, etl_proc_id, **context):
    import logging

    log_etl_params = get_sql_params(context)

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
def get_ffoms_sql(tbl_name: str, queries: dict):
    return queries.get(tbl_name, [])


@task
def prepare_dq_report_sql(report_tables, etl_proc_id, **context):
    from airflow.models import Variable

    log_etl_params = common.get_sql_params(context)

    mapping = Variable.get("dq_report_mapping", {}, True)

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
            "parent_el": mapping[str(table_id)]["parent_el"],
            "parent_el_id": mapping[str(table_id)]["parent_el_id"]
        }

        report_sql = common.get_rendered_file_content(context,
                                                      f"{DQ_SCRIPTS_SQL_PATH}/generate_report_all.sql",
                                                      {"params": params})

        result.append(report_sql)

    return result


@dag(
    start_date=pendulum.datetime(2023, 12, 20, 16, 0, 0, tz='Europe/Moscow'),
    params=common_dag_params.PARAMS_DICT,
    schedule_interval=None,
    catchup=False,
    tags=['exp', 'dds', 'dma', 'dq', 'ЦМП'],
    template_searchpath=TMPL_SEARCH_PATH,
)
def dma_exp_sank_dq():
    set_etl_running_status_task = CustomSQLOperator(
        task_id="set_etl_running_status",
        conn_id=DDS_CONN_ID,
        sql=f"ldl_etl_set_running_status_exp.sql",
        split_statements=True
    )

    @task_group
    def stage():
        exp_load_tmp_task = CustomSQLOperator(
            task_id="01_exp_load_tmp",
            conn_id=DDS_CONN_ID,
            sql="01_exp_load_tmp.sql",
            clean_file_check_path="01_exp_load_tmp.sql"
        )

        exp_ins_tmp_task = CustomSQLOperator(
            task_id="02_exp_ins_tmp",
            conn_id=DDS_CONN_ID,
            sql="02_exp_ins_tmp.sql",
            clean_file_check_path="02_exp_ins_tmp.sql"
        )

        exp_sank_tmp_task = CustomSQLOperator(
            task_id="03_exp_sank_tmp",
            conn_id=DDS_CONN_ID,
            sql="03_exp_sank_tmp.sql",
            clean_file_check_path="03_exp_sank_tmp.sql"
        )

        exp_sank_exp_task = CustomSQLOperator(
            task_id="04_exp_sank_exp_tmp",
            conn_id=DDS_CONN_ID,
            sql="04_exp_sank_exp_tmp.sql",
            clean_file_check_path="04_exp_sank_exp_tmp.sql"
        )

        exp_load_tmp_task >> exp_ins_tmp_task \
        >> exp_sank_tmp_task >> exp_sank_exp_task

    @task_group
    def dq():
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

        @task_group
        def check_ffoms():
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

            task_list = []

            for table_name in SANK.keys():
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

        set_dq_running_status_task >> clear_log_dq_err_task >> check_ffoms() >> set_dq_error_warning_status_task >> set_dq_completed_status_task

    exp_delete_duplicate_task = CustomSQLOperator(
        task_id="05_exp_delete_duplicate",
        conn_id=DDS_CONN_ID,
        sql="05_exp_delete_duplicate.sql",
        clean_file_check_path="05_exp_delete_duplicate.sql"
    )

    @task_group
    def dds():
        sank_task = CustomSQLOperator(
            task_id="06_sank",
            conn_id=DDS_CONN_ID,
            sql="06_sank.sql"
        )

        sank_exp_task = CustomSQLOperator(
            task_id="07_sank_exp",
            conn_id=DDS_CONN_ID,
            sql="07_sank_exp.sql"
        )

        src01_zl_list_task = CustomSQLOperator(
            task_id="08_src01_zl_list",
            conn_id=DDS_CONN_ID,
            sql="08_src01_zl_list.sql"
        )

        src01_exp_zap_task = CustomSQLOperator(
            task_id="09_src01_exp_zap",
            conn_id=DDS_CONN_ID,
            sql="09_src01_exp_zap.sql"
        )

        sank_src_ids_task = CustomSQLOperator(
            task_id="10_sank_src_ids",
            conn_id=DDS_CONN_ID,
            sql="10_sank_src_ids.sql"
        )

        z_sl_task = CustomSQLOperator(
            task_id="11_z_sl",
            conn_id=DDS_CONN_ID,
            sql="11_z_sl.sql",
            clean_file_check_path="11_z_sl.sql"
        )

        src01_zl_list_del_task = CustomSQLOperator(
            task_id="12_src01_zl_list_del",
            conn_id=DDS_CONN_ID,
            sql="12_src01_zl_list_del.sql"
        )

        kmp_result_exp_task = CustomSQLOperator(
            task_id="13_kmp_result_exp",
            conn_id=DDS_CONN_ID,
            sql="13_kmp_result_exp.sql"
        )

        sank_task >> sank_exp_task >> src01_zl_list_task >> src01_exp_zap_task >> \
        sank_src_ids_task >> z_sl_task >> src01_zl_list_del_task >> kmp_result_exp_task

    @task_group
    def report_dq():
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

    init_all() >> set_etl_running_status_task >> stage() >> dq() >> exp_delete_duplicate_task >> \
    dds() >> report_dq() >> dag_defaults.finish_all.override(trigger_rule="none_failed")()


dma_exp_sank_dq()
