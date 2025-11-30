#!/bin/bash

PASS_FILE=~/.pgpass
if [ ! -f $PASS_FILE ]; then
#       hostname:port:database:username:password
    echo "airflow-db:5432:${POSTGRES_DB}:${POSTGRES_USER}:${POSTGRES_PASSWORD}" > $PASS_FILE
    chmod 0600 $PASS_FILE
fi


SAVED_FILE=airflow_debug_dag_id.tmp
if [ ! -f $SAVED_FILE ]
then
    # если нет такого файла - предполагаем, что мы только запустили отладчик
    echo "$1" >> $SAVED_FILE
    echo "dag_id '$1' сохранён в файл [$SAVED_FILE], этот dag_run так же будет удалён при следующем запуске"
    DAG_ID=$1
else
    # если же файл найден - предполагаем, что это уже второй вызов скрипта
    DAG_ID=$(cat $SAVED_FILE)
    rm $SAVED_FILE
fi
if [[ "$1" == "$DAG_ID" ]]; then
    PREDICT="= '$1'"
else
    PREDICT="IN ('$1', '$DAG_ID')"
fi
psql \
    -h airflow-db \
    -d airflow \
    -U airflow \
    -w -c \
    "DELETE FROM dag_run \
    WHERE dag_id $PREDICT \
    AND queued_at IS NULL" \
    && echo "^ удалено строк, где dag_id $PREDICT    # база Airflow, таблица 'dag_run'"

# актуализировалось на версии Airflow 2.7.3
# записи, которые появлялись при запуске через команду airflow dags test <dag_id>
# появлялись в базе с полем queued_at = NULL, по этому такие удаляем
