from datetime import datetime
from airflow.kubernetes.secret import Secret
from airflow.decorators import dag  # , task
from airflow.models.baseoperator import chain
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


DESCRIPTION = '''
Распределение случаев оказания медицинской помощи по видам и условиям в рамках ПГГ

**Параметры:**

- `MODEL_TAG` - если не указан, то берётся значение MODEL_TAG из переменных Airflow
- `COMMAND_PATH` - базовый путь для всех скриптов
- `DELETE_POD` - значение параметра `is_delete_operator_pod`

---

Задача: [jira]\
(https://jira.element-lab.ru/browse/KMP-1466)

Вики: [wiki.element-lab]\
(https://wiki.element-lab.ru/pages/viewpage.action?pageId=55772026)
'''

KUBERNETES_CONN_ID = 'kubernetes_ML_conn_id'

IMAGE = ("{{ var.value.DOCKER_REGISTRY }}/%s:"
         "{{ params.MODEL_TAG or var.value.MODEL_TAG }}")

K8S_POD_OPERATOR_PARAMS = {
    "namespace": "model",
    "image": IMAGE % 'model_pgg_rules',
    "image_pull_policy": "Always",
    "image_pull_secrets": "docker-registry",
    "startup_timeout_seconds": 100,
    "get_logs": True,
    "is_delete_operator_pod": "{{ params.DELETE_POD }}",
    "in_cluster": False,
    "do_xcom_push": False,
    "tolerations": [
        {
            "effect": "NoExecute",
            "key": "node.kubernetes.io/not-ready",
            "operator": "Exists",
            "tolerationSeconds": 300,
        },
        {
            "effect": "NoExecute",
            "key": "node.kubernetes.io/unreachable",
            "operator": "Exists",
            "tolerationSeconds": 300,
        },
        {
            "effect": "NoSchedule",
            "key": "dedicated",
            "operator": "Equal",
            "value": "datascience",
        },
    ],
    "secrets": [Secret(deploy_type="env", deploy_target=None, secret="ml-model-secret")],
}

PODS = {    # task_id / name / label : file path
    'process_raw_data': 'process_pgg_rules/main.py',
}


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
        'MODEL_TAG': None,
        'COMMAND_PATH': '/home/app/airflow_tasks',
        'DELETE_POD': True,
    }
)
def ml_model_pgg_rules():
    chain(*[
        KubernetesPodOperator(
            task_id=pod,
            name=pod,
            kubernetes_conn_id=KUBERNETES_CONN_ID,
            cmds=["python3", "{{ params.COMMAND_PATH }}/%s" % PODS[pod]],
            labels={"app": pod, "owner": "ml"},
            # container_resources=k8s.V1ResourceRequirements(
            #    requests={"memory": "XGi", "cpu": X},
            #    limits={"memory": "XGi", "cpu": X},
            # ),
            **K8S_POD_OPERATOR_PARAMS,
        )
        for pod in PODS])


ml_model_pgg_rules()
