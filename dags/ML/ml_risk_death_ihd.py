from datetime import datetime
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator


DESCRIPTION = '''
Модель риска смерти от ИБС

**Параметры:**

- `ml_model_tag` - тэг docker образа ML модели (если не указан, то берётся значение ml_model_tag из переменных airflow)
- `command_path` - базовый путь для всех скриптов внутри docker контейнера

---

Задача: [jira]\
-

Вики: [wiki.element-lab]\
(https://wiki.element-lab.ru/pages/viewpage.action?pageId=55772433)
'''


# airflow connection с информацией о docker registry из которого docker операторы забирают docker образы для запуска
DOCKER_CONNECTION_ID = 'ml_docker_registry'
DOCKER_IMAGE_NAME = "{{ conn.ml_docker_registry.host }}/%s:{{ params.ml_model_tag or var.value.ml_model_tag }}"
# TODO: разобраться почему Jinja шаблон {{ var.value.ml_docker_runner_url }} не взлетает
DOCKER_RUNNER_URL = Variable.get(key="ml_docker_runner_url")


# переменные окружения для использования внутри docker контейнера
DOCKER_CONTAINER_ENV_VARIABLES = {
    "PREDICT_PD": "{{ params.predict_pd }}",
    "DEMO": "{{ params.demo }}",
    "REGIONS": "{{ params.regions }}",
    "TRAIN": "{{ params.train }}",
    "NDS_CONN_STRING": "{{ conn.ml_nds_cmp.get_uri() }}",
    "NDS_US_CONN_STRING": "{{ conn.ml_nds_us.get_uri() }}",
    "ENDPOINT_URL": "{{ conn.ml_s3.get_extra_dejson().get('endpoint_url') }}",
    "AWS_ACCESS_KEY_ID": "{{ conn.ml_s3.login }}",
    "AWS_SECRET_ACCESS_KEY": "{{ conn.ml_s3.get_password() }}"
}


# название ML модели, оно же является названием docker образа (без учета тэга)
ML_MODEL_NAME = 'model_death_ihd'


# этапы расчета ML алгоритма
TASKS = [
    'raw_data_collection',
    'features_collection',
    'features_preprocess',
    'model_train',
    'model_run'
]


@dag(
    schedule_interval=None,
    start_date=datetime(2023, 8, 1),
    catchup=False,
    tags=["ml", "datascience"],
    default_args={"owner": "ml"},
    description=DESCRIPTION.splitlines()[1],
    doc_md=DESCRIPTION,
    render_template_as_native_obj=True,
    params={
        'ml_model_tag': None,
        'command_path': '/home/app/airflow_tasks',
        "predict_pd":   Param(default="", type="string"),
        "demo":         Param(default="false", type="string",),
        'regions':      Param(default="", type="string"),
        "train":        Param(default="false", type="string",),
    }
)
def ml_risk_death_ihd():

    tasks = []

    # создаем docker операторы(задачи) для каждого этапа ML алгоритма
    for task in TASKS:
        docker_operator=DockerOperator(
            task_id=task,
            image=DOCKER_IMAGE_NAME % ML_MODEL_NAME,
            command=["python3", "{{ params.command_path }}/%s/main.py" % task],
            docker_conn_id=DOCKER_CONNECTION_ID,
            docker_url=DOCKER_RUNNER_URL,
            environment=DOCKER_CONTAINER_ENV_VARIABLES,
            force_pull=True,      # всегда загружаем docker образ заново
            auto_remove="force",  # удаляем docker контейнер после запуска
            tty=True,
            mount_tmp_dir=False   # убираем монтирование временной директории в docker контейнер
        )
        tasks.append(docker_operator)

    chain(*tasks)


ml_risk_death_ihd()
