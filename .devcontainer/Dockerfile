ARG AIRFLOW_VERSION=2.5.0
ARG PYTHON_VERSION=3.10
ARG AIRFLOW_UID=50000

# указанное сочетание версии Airflow и Python должно существовать в https://hub.docker.com/r/apache/airflow/tags
# например: apache/airflow:2.5.0-python3.10
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
ARG AIRFLOW_UID

USER root

# mysql и mssql не используется
RUN rm -f /etc/apt/sources.list.d/{mysql.list,mssql-release.list}

# права на sudo для пользователя airflow
RUN usermod -aG sudo -u ${AIRFLOW_UID} airflow && echo 'airflow ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# пред-настройки для git
COPY .devcontainer/cfg/preset.gitconfig /tmp/
# установка git и других пакетов
RUN apt update && apt install -y git

# настройка bash prompt: цвета и текущая ветка. можно закомментить, если не нужно
COPY .devcontainer/setup_scripts/ /tmp/
RUN bash /tmp/bash_prompt_setup.sh airflow

USER airflow
# служебные переменные окружения
ENV SHELL=/bin/bash \
    USER_NAME=airflow

RUN PDIR=$(pip cache dir); [[ ! -w $PDIR ]] && { sudo rm -rf $(dirname "$PDIR") && mkdir -p $PDIR; }
COPY requirements.txt /
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-no-providers-${PYTHON_VERSION}.txt"
RUN pip install \
    --disable-pip-version-check \
    --upgrade-strategy only-if-needed \
    --constraint "${CONSTRAINT_URL}" \
    -r /requirements.txt
