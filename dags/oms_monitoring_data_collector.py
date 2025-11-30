import pendulum
from os import path
from airflow.decorators import task, dag, task_group
from airflow.operators.python import get_current_context
from utils.sql_operator import CustomSQLOperator
from utils import common
from airflow.datasets import Dataset

DDS_CONN_ID = "nds_us_dma"

AIRFLOW_HOME = common.get_airflow_home_path()
MONITORING_SCRIPTS_SQL_PATH = f'{AIRFLOW_HOME}/sql/mon'

TMPL_SEARCH_PATH = []
TMPL_SEARCH_PATH.append(path.join(MONITORING_SCRIPTS_SQL_PATH))

dataset_etl_fd_ms_oms_success = Dataset("etl_fd_ms_oms_success")
dataset_etl_fd_erzl_oms_success = Dataset("etl_fd_erzl_oms_success")

@task
def get_log_etl_proc_id(ti=None, triggering_dataset_events=None):
    result_list = []

    for dataset, events_list in triggering_dataset_events.items():
        for event in events_list:
            result_list.append(event.extra["log_etl_proc_id"])

    ti.xcom_push(key="log_etl_proc_id", value=', '.join(str(i) for i in result_list))


@dag(
    start_date=pendulum.datetime(2024, 7, 1),
    schedule=(dataset_etl_fd_ms_oms_success | dataset_etl_fd_erzl_oms_success),
    template_searchpath=TMPL_SEARCH_PATH,
    tags=["mon"]
)
def oms_monitoring_data_collector():
    get_log_etl_proc_id_task = get_log_etl_proc_id()

    insert_monitoring_data_task = CustomSQLOperator(
        task_id="insert_monitoring_data",
        conn_id=DDS_CONN_ID,
        sql="insert_monitoring_data.sql",
        split_statements=True,
        x_params={
            "log_etl_proc_id": "{{ ti.xcom_pull(task_ids='get_log_etl_proc_id', key='log_etl_proc_id') }}"
        }
    )

    @task_group()
    def etl_fd_ms_oms():
        w_inv_mcare_f_task = CustomSQLOperator(
            task_id="w_inv_mcare_f",
            conn_id=DDS_CONN_ID,
            sql="insert_monitoring_fact_data.sql",
            split_statements=True,
            x_params={
                "log_etl_proc_id": "{{ ti.xcom_pull(task_ids='get_log_etl_proc_id', key='log_etl_proc_id') }}",
                "fact_table_name": "w_inv_mcare_f",
                "error_table_name": "err_inv_mcare_fs",
                "table_id": 828
            }
        )

        w_cpp_mcf_f_task = CustomSQLOperator(
            task_id="w_cpp_mcf_f",
            conn_id=DDS_CONN_ID,
            sql="insert_monitoring_fact_data.sql",
            split_statements=True,
            x_params={
                "log_etl_proc_id": "{{ ti.xcom_pull(task_ids='get_log_etl_proc_id', key='log_etl_proc_id') }}",
                "fact_table_name": "w_cpp_mcf_f",
                "error_table_name": "err_cpp_mcf_fs",
                "table_id": 838
            }
        )

        w_inv_mconsult_f_task = CustomSQLOperator(
            task_id="w_inv_mconsult_f",
            conn_id=DDS_CONN_ID,
            sql="insert_monitoring_fact_data.sql",
            split_statements=True,
            x_params={
                "log_etl_proc_id": "{{ ti.xcom_pull(task_ids='get_log_etl_proc_id', key='log_etl_proc_id') }}",
                "fact_table_name": "w_inv_mconsult_f",
                "error_table_name": "err_inv_mconsult_fs",
                "table_id": 825
            }
        )

        w_inv_mservice_f_task = CustomSQLOperator(
            task_id="w_inv_mservice_f",
            conn_id=DDS_CONN_ID,
            sql="insert_monitoring_fact_data.sql",
            split_statements=True,
            x_params={
                "log_etl_proc_id": "{{ ti.xcom_pull(task_ids='get_log_etl_proc_id', key='log_etl_proc_id') }}",
                "fact_table_name": "w_inv_mservice_f",
                "error_table_name": "err_inv_mservice_fs",
                "table_id": 830
            }
        )

        w_inv_mservice_onko_f_task = CustomSQLOperator(
            task_id="w_inv_mservice_onko_f",
            conn_id=DDS_CONN_ID,
            sql="insert_monitoring_fact_data.sql",
            split_statements=True,
            x_params={
                "log_etl_proc_id": "{{ ti.xcom_pull(task_ids='get_log_etl_proc_id', key='log_etl_proc_id') }}",
                "fact_table_name": "w_inv_mservice_onko_f",
                "error_table_name": "err_inv_mservice_onko_fs",
                "table_id": 1033
            }
        )

        w_inv_onk_diagnostic_f_task = CustomSQLOperator(
            task_id="w_inv_onk_diagnostic_f",
            conn_id=DDS_CONN_ID,
            sql="insert_monitoring_fact_data.sql",
            split_statements=True,
            x_params={
                "log_etl_proc_id": "{{ ti.xcom_pull(task_ids='get_log_etl_proc_id', key='log_etl_proc_id') }}",
                "fact_table_name": "w_inv_onk_diagnostic_f",
                "error_table_name": "err_inv_onk_diagnostic_fs",
                "table_id": 1034
            }
        )

        w_inv_ref_treatment_f_task = CustomSQLOperator(
            task_id="w_inv_ref_treatment_f",
            conn_id=DDS_CONN_ID,
            sql="insert_monitoring_fact_data.sql",
            split_statements=True,
            x_params={
                "log_etl_proc_id": "{{ ti.xcom_pull(task_ids='get_log_etl_proc_id', key='log_etl_proc_id') }}",
                "fact_table_name": "w_inv_ref_treatment_f",
                "error_table_name": "err_inv_ref_treatment_fs",
                "table_id": 1044
            }
        )

        w_inv_sanction_f_task = CustomSQLOperator(
            task_id="w_inv_sanction_f",
            conn_id=DDS_CONN_ID,
            sql="insert_monitoring_fact_data.sql",
            split_statements=True,
            x_params={
                "log_etl_proc_id": "{{ ti.xcom_pull(task_ids='get_log_etl_proc_id', key='log_etl_proc_id') }}",
                "fact_table_name": "w_inv_sanction_f",
                "error_table_name": "err_inv_sanction_fs",
                "table_id": 1035
            }
        )

        w_refusal_onko_f_task = CustomSQLOperator(
            task_id="w_refusal_onko_f",
            conn_id=DDS_CONN_ID,
            sql="insert_monitoring_fact_data.sql",
            split_statements=True,
            x_params={
                "log_etl_proc_id": "{{ ti.xcom_pull(task_ids='get_log_etl_proc_id', key='log_etl_proc_id') }}",
                "fact_table_name": "w_refusal_onko_f",
                "error_table_name": "err_refusal_onko_fs",
                "table_id": 992
            }
        )

        w_inv_sheme_pay_f_task = CustomSQLOperator(
            task_id="w_inv_sheme_pay_f",
            conn_id=DDS_CONN_ID,
            sql="insert_monitoring_fact_data.sql",
            split_statements=True,
            x_params={
                "log_etl_proc_id": "{{ ti.xcom_pull(task_ids='get_log_etl_proc_id', key='log_etl_proc_id') }}",
                "fact_table_name": "w_inv_sheme_pay_f",
                "error_table_name": "err_inv_sheme_pay_fs",
                "table_id": 1042
            }
        )

        w_inv_invalid_f_task = CustomSQLOperator(
            task_id="w_inv_invalid_f",
            conn_id=DDS_CONN_ID,
            sql="insert_monitoring_fact_data.sql",
            split_statements=True,
            x_params={
                "log_etl_proc_id": "{{ ti.xcom_pull(task_ids='get_log_etl_proc_id', key='log_etl_proc_id') }}",
                "fact_table_name": "w_inv_invalid_f",
                "error_table_name": "err_inv_invalid_fs",
                "table_id": 1048
            }
        )

        w_cpp_dop_f_task = CustomSQLOperator(
            task_id="w_cpp_dop_f",
            conn_id=DDS_CONN_ID,
            sql="insert_monitoring_fact_data.sql",
            split_statements=True,
            x_params={
                "log_etl_proc_id": "{{ ti.xcom_pull(task_ids='get_log_etl_proc_id', key='log_etl_proc_id') }}",
                "fact_table_name": "w_cpp_dop_f",
                "error_table_name": "err_cpp_dop_bk_fs",
                "table_id": 1178
            }
        )

        w_inv_mcare_f_task >> w_cpp_mcf_f_task >> w_inv_mconsult_f_task >> \
        w_inv_mservice_f_task >> w_inv_mservice_onko_f_task >> w_inv_onk_diagnostic_f_task >> \
        w_inv_ref_treatment_f_task >> w_inv_sanction_f_task >> w_refusal_onko_f_task >> \
        w_inv_sheme_pay_f_task >> w_inv_invalid_f_task >> w_cpp_dop_f_task

    @task_group()
    def etl_fd_erzl_oms():
        w_erzl_insured_f_task = CustomSQLOperator(
            task_id="w_erzl_insured_f",
            conn_id=DDS_CONN_ID,
            sql="insert_monitoring_fact_data.sql",
            split_statements=True,
            x_params={
                "log_etl_proc_id": "{{ ti.xcom_pull(task_ids='get_log_etl_proc_id', key='log_etl_proc_id') }}",
                "fact_table_name": "w_erzl_insured_f",
                "error_table_name": "err_erzl_ins_bk_fs",
                "table_id": 659
            }
        )

        w_erzl_attached_f_task = CustomSQLOperator(
            task_id="w_erzl_attached_f",
            conn_id=DDS_CONN_ID,
            sql="insert_monitoring_fact_data.sql",
            split_statements=True,
            x_params={
                "log_etl_proc_id": "{{ ti.xcom_pull(task_ids='get_log_etl_proc_id', key='log_etl_proc_id') }}",
                "fact_table_name": "w_erzl_attached_f",
                "error_table_name": "err_erzl_att_bk_fs",
                "table_id": 660
            }
        )

        w_erzl_insured_f_task >> w_erzl_attached_f_task

    get_log_etl_proc_id_task >> [insert_monitoring_data_task, etl_fd_ms_oms(), etl_fd_erzl_oms()]


oms_monitoring_data_collector()
