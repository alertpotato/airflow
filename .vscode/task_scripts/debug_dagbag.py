"""
Вызывает те же методы, то CLI: `airflow dags reserialize`
Позволяет полноценно отлаживать процесс загрузки DAG

from airflow.cli.commands.dag_command import dag_reserialize
"""

from airflow.models import DagBag

dagbag = DagBag()
dagbag.sync_to_db()
