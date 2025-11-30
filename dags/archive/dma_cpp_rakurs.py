import pendulum
from os import path
from airflow.decorators import dag, task, task_group
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils.sql_operator import CustomSQLOperator
from utils import common, common_dag_params
from utils.logs_meta.log_tables import TableAction as act, log_etl_run
from airflow.operators.python import get_current_context

DB_CONN_ID = 'nds_dds'
DMA_CONN_ID = 'nds_dma'

AIRFLOW_HOME = common.get_airflow_home_path()

CPP_SCRIPTS_SQL_PATH = f'{AIRFLOW_HOME}/sql/cpp'

TMPL_SEARCH_PATH = []
TMPL_SEARCH_PATH.append(path.join(CPP_SCRIPTS_SQL_PATH))


@task
def init_run_id():
    etl_run_id = log_etl_run(act.get_new_id, conn_id=DB_CONN_ID)
    p = {
        'etl_run_id': etl_run_id,
        'temp_suffix_run_id': f'{etl_run_id}_tmp'

    }

    common.xcom_save(get_current_context(), p, with_log=True)


@task
def get_last_update_dttm(**context):
    import logging
    result = CustomSQLOperator(
        task_id="get_last_update_dttm",
        conn_id=DMA_CONN_ID,
        sql="00_get_last_update_dttm.sql",
        log_sql=True,
    ).execute(context)

    logging.info(f"\n{common.get_str_table(result, headers=('Параметр', 'Значение'))}")

    for row in result:
        context['ti'].xcom_push(key=row[0], value=row[1])


@task
def finish_etl_run():
    run_id = get_current_context()['ti'].xcom_pull(key='etl_run_id')
    log_etl_run(act.finish, {'etl_run_id': run_id}, conn_id=DB_CONN_ID)


@dag(
    start_date=pendulum.datetime(2024, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={},
    params={
        "form_schet_b_id": "sch.code || '~' || sch.region_cd",
        "dma_schema": "dma",
        "dma_calc_schema": "dma_calc"
    },
    template_searchpath=TMPL_SEARCH_PATH,
    tags=["dma", "cpp"]
)
def dma_cpp_rakurs():
    init_run_id_task = init_run_id()

    @task_group(tooltip="Создание таблиц инкремента", group_id="Создание_инкремента")
    def create_increment():
        get_last_update_dttm_task = get_last_update_dttm()

        cpp_patient_src_ids_task = CustomSQLOperator(
        #cpp_ldl_task = CustomSQLOperator(
            #task_id="cpp_ldl",
            task_id="cpp_patient_src_ids",
            conn_id=DMA_CONN_ID,
            #sql="cpp_ldl.sql",
            sql="01_2_cpp_patient_src_ids.sql",
            log_sql=True,
            split_statements=True,
            x_params={
                #"cpp_ldl": "{{ ti.xcom_pull(key='cpp_ldl') }}",
                "cpp_patient_src_ids": "{{ ti.xcom_pull(key='cpp_patient_src_ids') }}",
                "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}"
            }
        )

        cpp_ldl_task = CustomSQLOperator(
            task_id="cpp_ldl",
            conn_id=DMA_CONN_ID,
            sql="01_1_cpp_ldl.sql",
            log_sql=True,
            split_statements=True,
            x_params={
                "cpp_ldl": "{{ ti.xcom_pull(key='cpp_ldl') }}",
                "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}"
            }
        )

        cpp_doc_mpi_task = CustomSQLOperator(
            task_id="cpp_doc_mpi",
            conn_id=DMA_CONN_ID,
            sql="01_cpp_doc_mpi_inc.sql",
            log_sql=True,
            split_statements=True,
            x_params={
                "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}"
            }
        )

        cpp_hist_mpi_task = CustomSQLOperator(
            task_id="cpp_hist_mpi",
            conn_id=DMA_CONN_ID,
            sql="02_cpp_hist_mpi.sql",
            log_sql=True,
            split_statements=True,
            x_params={
                "cpp_hist_mpi": "{{ ti.xcom_pull(key='cpp_hist_mpi') }}",
                "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}"
            }
        )

        cpp_master_person_upd_task = CustomSQLOperator(
            task_id="cpp_master_person_upd",
            conn_id=DMA_CONN_ID,
            sql="03_cpp_master_person_upd.sql",
            log_sql=True,
            split_statements=True,
            x_params={
                "cpp_load": "{{ ti.xcom_pull(key='cpp_load') }}",
                "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}"
            }
        )

        cpp_master_person_inc_task = CustomSQLOperator(
            task_id="cpp_master_person_inc",
            conn_id=DMA_CONN_ID,
            sql="04_cpp_master_person_inc.sql",
            log_sql=True,
            split_statements=True,
            x_params={
                "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}"
            }
        )

        cpp_hist_sch_task = CustomSQLOperator(
            task_id="cpp_hist_sch",
            conn_id=DMA_CONN_ID,
            sql="05_cpp_hist_sch.sql",
            log_sql=True,
            split_statements=True,
            x_params={
                "cpp_hist_sch": "{{ ti.xcom_pull(key='cpp_hist_sch') }}",
                "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}"
            }
        )

        cpp_schet_inc_task = CustomSQLOperator(
            task_id="cpp_schet_inc",
            conn_id=DMA_CONN_ID,
            sql="06_cpp_schet_inc.sql",
            log_sql=True,
            split_statements=True,
            x_params={
                "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}"
            }
        )

        # rakurs_med_flg_inc_tast = CustomSQLOperator(
        #     task_id="rakurs_med_flg_inc",
        #     conn_id=DMA_CONN_ID,
        #     sql="07_rakurs_med_flg_inc.sql",
        #     log_sql=True,
        #     split_statements=True,
        #     x_params={
        #         "cpp_mrf": "{{ ti.xcom_pull(key='cpp_mrf') }}",
        #         "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}"
        #     }
        # )

        rakurs_master_person_inc_task = CustomSQLOperator(
            task_id="rakurs_master_person_inc",
            conn_id=DMA_CONN_ID,
            sql="08_rakurs_master_person_inc.sql",
            log_sql=True,
            split_statements=True,
            x_params={
                "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}",
                "cpp_mrf": "{{ ti.xcom_pull(key='cpp_mrf') }}"
            }
        )
        """
        get_last_update_dttm_task >> [cpp_patient_src_ids_task, cpp_ldl_task, cpp_hist_mpi_task, cpp_master_person_upd_task] >> \
        cpp_doc_mpi_task >> cpp_master_person_inc_task >> cpp_hist_sch_task >> cpp_schet_inc_task >> rakurs_med_flg_inc_tast >> \
        rakurs_master_person_inc_task
        """
        get_last_update_dttm_task >> [cpp_patient_src_ids_task, cpp_ldl_task, cpp_hist_mpi_task, cpp_master_person_upd_task] >> \
        cpp_doc_mpi_task >> cpp_master_person_inc_task >> cpp_hist_sch_task >> cpp_schet_inc_task >> rakurs_master_person_inc_task
        
    trigger_cpp_task = TriggerDagRunOperator(
        task_id="run_dma_etl_cpp_inc",
        trigger_dag_id="dma_etl_cpp_inc",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=90,
        max_active_tis_per_dag=5,
        trigger_run_id="{{ run_id }}_{{ ti.xcom_pull(key='etl_run_id') }}",
        conf={
            "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}",
            "cpp_load": "{{ ti.xcom_pull(key='cpp_load') }}"
        }
    )

    cpp_table_mpi_upd_task = CustomSQLOperator(
        task_id="cpp_table_mpi_upd",
        conn_id=DMA_CONN_ID,
        sql="09_cpp_table_mpi_upd.sql",
        log_sql=True,
        x_params={
            "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}",
        },
    )

    last_update_dttm_upd_task = CustomSQLOperator(
        task_id="Обновление_last_update_dttm",
        conn_id=DMA_CONN_ID,
        sql="10_last_update_dttm_upd.sql",
        log_sql=True,
        x_params={
            "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}",
        },
    )

    @task_group(tooltip="Обновление витрин рисков", group_id="Обновление_ракурсов_1")
    def dma_rakurs1_tg():
        trigger_rakurs_card = TriggerDagRunOperator(
            task_id='dma_rakurs_card_inc',
            trigger_dag_id='dma_rakurs_card_inc',
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=5,
            max_active_tis_per_dag=5,
            trigger_rule="none_failed",
            trigger_run_id="{{ run_id }}_{{ ti.xcom_pull(key='etl_run_id') }}",
            conf={
                "etl_run_id": "{{ ti.xcom_pull(key='etl_run_id') }}",
                "temp_suffix_run_id": "{{ ti.xcom_pull(key='etl_run_id') }}_tmp"
            },
            doc="Загрузка_витрин_ракурса_кардиолог"
        )

        trigger_dma_rakurs_smp = TriggerDagRunOperator(
            task_id='dma_rakurs_smp_inc',
            trigger_dag_id='dma_rakurs_smp_inc',
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=5,
            max_active_tis_per_dag=5,
            trigger_rule="none_failed",
            trigger_run_id="{{ run_id }}_{{ ti.xcom_pull(key='etl_run_id') }}",
            conf={
                "etl_run_id": "{{ ti.xcom_pull(key='etl_run_id') }}",
                "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}"
            },
            doc="Загрузка_витрин_ракурса_СМП"
        )

        trigger_rakurs_sc_urg = TriggerDagRunOperator(
            task_id='dma_rakurs_sc_urg_inc',
            trigger_dag_id='dma_rakurs_sc_urg_inc',
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=5,
            max_active_tis_per_dag=5,
            trigger_rule="none_failed",
            trigger_run_id="{{ run_id }}_{{ ti.xcom_pull(key='etl_run_id') }}",
            conf={
                "etl_run_id": "{{ ti.xcom_pull(key='etl_run_id') }}",
                "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}"
            },
            doc="Загрузка_витрин_ракурса_врач_экстр_госп"
        )

        trigger_dma_rakurs_surg = TriggerDagRunOperator(
            task_id='dma_rakurs_surg_inc',
            trigger_dag_id='dma_rakurs_surg_inc',
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=5,
            max_active_tis_per_dag=5,
            trigger_rule="none_failed",
            trigger_run_id="{{ run_id }}_{{ ti.xcom_pull(key='etl_run_id') }}",
            conf={
                "etl_run_id": "{{ ti.xcom_pull(key='etl_run_id') }}",
                "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}"
            },
            doc="Загрузка_витрин_ракурса_хирург"
        )

    @task_group(group_id="Обновление_ракурсов_2")
    def dma_rakurs2_tg():
        trigger_dma_rakurs_terapevt_inc = TriggerDagRunOperator(
            task_id='dma_rakurs_terapevt_inc',
            trigger_dag_id='dma_rakurs_terapevt_inc',
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=5,
            max_active_tis_per_dag=5,
            trigger_rule="none_failed",
            trigger_run_id="{{ run_id }}_{{ ti.xcom_pull(key='etl_run_id') }}",
            conf={
                "dma_schema": "{{ params['dma_schema'] }}",
                "etl_run_id": "{{ ti.xcom_pull(key='etl_run_id') }}",
                "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}"
            }
        )

        trigger_dma_rakurs_onk_inc = TriggerDagRunOperator(
            task_id='dma_rakurs_onk_inc',
            trigger_dag_id='dma_rakurs_onk_inc',
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=5,
            max_active_tis_per_dag=5,
            trigger_rule="none_failed",
            trigger_run_id="{{ run_id }}_{{ ti.xcom_pull(key='etl_run_id') }}",
            conf={
                "dma_schema": "{{ params['dma_schema'] }}",
                "etl_run_id": "{{ ti.xcom_pull(key='etl_run_id') }}",
                "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}"
            }
        )

        trigger_dma_rakurs_kids_incr = TriggerDagRunOperator(
            task_id='dma_rakurs_kids_incr',
            trigger_dag_id='dma_rakurs_kids_incr',
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=5,
            max_active_tis_per_dag=5,
            trigger_rule="none_failed",
            trigger_run_id="{{ run_id }}_{{ ti.xcom_pull(key='etl_run_id') }}",
            conf={
                "dma_schema": "{{ params['dma_schema'] }}",
                "etl_run_id": "{{ ti.xcom_pull(key='etl_run_id') }}",
                "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}"
            }
        )

        trigger_dma_rakurs_dent_incr = TriggerDagRunOperator(
            task_id='dma_rakurs_dent_incr',
            trigger_dag_id='dma_rakurs_dent_incr',
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=5,
            max_active_tis_per_dag=5,
            trigger_rule="none_failed",
            trigger_run_id="{{ run_id }}_{{ ti.xcom_pull(key='etl_run_id') }}",
            conf={
                "dma_schema": "{{ params['dma_schema'] }}",
                "etl_run_id": "{{ ti.xcom_pull(key='etl_run_id') }}",
                "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}"
            }
        )

    drop_increment_task = CustomSQLOperator(
        task_id="drop_increment_tables",
        conn_id=DMA_CONN_ID,
        sql="drop_increment_tables.sql",
        log_sql=True,
        x_params={
            "temp_suffix_run_id": "{{ ti.xcom_pull(key='temp_suffix_run_id') }}",
        },
    )

    init_run_id_task >> create_increment() >> trigger_cpp_task >> cpp_table_mpi_upd_task >> \
    last_update_dttm_upd_task >> dma_rakurs1_tg() >> dma_rakurs2_tg() >> drop_increment_task >> finish_etl_run()


dag = dma_cpp_rakurs()
