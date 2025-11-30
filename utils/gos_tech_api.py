from datetime import datetime
from re import compile

import requests

from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.utils.session import create_session

API_CONN_ID = 'audit-api'
LOG_PREFIX = 'ГосТех - Аудит:'
HOST_NAME_REGEX = compile(r'\/\/([^\/]+)')


def _get_conn():
    return BaseHook.get_connection(API_CONN_ID)


def _get_user_who_triggered_dag(dag_id: str) -> tuple[str, str | None]:
    """ Возвращает кортеж `(логин, полное имя пользователя)` из
    последнего запуска DAG'а с указанным `dag_id`.

    Если имя пользователя не указано - возвращается `(логин, None)`
    """

    from airflow.models.log import Log

    with create_session() as session:
        triggered_by: str = (
            session.query(Log.owner)
            # type: ignore
            .filter(Log.dag_id == dag_id, Log.event == 'trigger')
            .order_by(Log.dttm.desc())  # type: ignore
            .limit(1)
            .scalar()
        )
    # событие 'trigger' есть только при ручном запуске.
    if not triggered_by:
        triggered_by = 'airflow-scheduler'
    splitted_user = triggered_by.split('(')
    user_login = splitted_user[0].strip()
    user_name = splitted_user[1].strip(')') if len(splitted_user) > 1 else None
    return user_login, user_name


def _send_event(event_name: str,
                params: list,
                user_login: str,
                user_name: str | None = None,
                tags: list | None = None
                ):
    from airflow.configuration import conf

    conn = _get_conn()
    api_endpoint = f"{conn.host}/rn/foms-gisoms/event"

    airflow_url = conf.get_mandatory_value("webserver", "BASE_URL")
    host_name = (x.group(1) if (x := HOST_NAME_REGEX.search(airflow_url))
                 else "id_not_found")

    headers = {
        'X-Node-ID': host_name
    }

    payload = {
        "createdAt": int(datetime.utcnow().timestamp()*1e3),    # 16!
        "metamodelVersion": conn.schema,
        "module": "Airflow",
        "name": event_name,
        "params": params,
        "userNode": airflow_url,
        "userLogin": user_login,
        # "session": "", # ?
    }
    if user_name:
        payload['userName'] = user_name
    if tags:
        payload['tags'] = tags

    extra_args = conn.extra_dejson or {}
    headers.update(extra_args.pop('headers', {}))

    return requests.post(
        **extra_args,
        url=api_endpoint,
        headers=headers,
        json=payload,
    )


def _is_disabled():
    return Variable.get('audit_enabled', None) == 'off'


def send_dag_status(status: str, context=None):
    """ Отправляет переданный `status` в виде события 'dag_trigger'.

    При вызове из task передавать `context` не обязательно """

    if _is_disabled():
        return

    if not context:
        context = get_current_context()

    user_login, user_name = _get_user_who_triggered_dag(context['dag'].dag_id)

    params = [
        {
            "name": "dag_id",
            "value": context['dag'].dag_id
        },
        {
            "name": "run_id",
            "value": context['run_id']
        },
        {
            "name": "status",
            "value": status
        },
    ]

    logger = context['ti'].log
    try:
        response = _send_event(
            event_name='dag_trigger',
            params=params,
            user_login=user_login,
            user_name=user_name,
            tags=context['dag'].tags
        )

        if response.ok:
            logger.info(
                f"{LOG_PREFIX} успешно отправлено событие 'dag_trigger', "
                f"{status=}, {user_login=}, response_code={response.status_code}")
        else:
            logger.warning(
                f"{LOG_PREFIX} не удалось отправить событие! "
                f"{status=}, {user_login=}, "
                f"response_code={response.status_code}, reason={response.reason}")
    except Exception as ex:
        # тут намеренно не выводится traceback
        logger.error(f"{LOG_PREFIX} ошибка при попытке отправки события: {ex}")
