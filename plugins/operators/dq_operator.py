from __future__ import annotations

import logging
from typing import Any

from typing import TYPE_CHECKING, Sequence, Any
from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
from airflow.providers.common.sql.operators.sql import BaseSQLOperator
from airflow.providers.common.sql.hooks.sql import DbApiHook, fetch_all_handler, return_single_query_results
from utils import common
from utils.skipping_tasks import SKIP_TASKS_KEY

from utils.common import (
    get_airflow_home_path,
    get_dq_home_path,
    get_found_file_path,
    get_rendered_file_content,
    get_sql_params,
    get_params_from_xcom_any_task
)

if TYPE_CHECKING:
    import jinja2
    from airflow.utils.context import Context
    from collections.abc import Mapping

AIRFLOW_HOME = common.get_airflow_home_path()

DQ_SCRIPTS_SQL_PATH = f'{AIRFLOW_HOME}/sql/dq'
DQ_RULES_SQL_PATH = f"{common.get_dq_home_path()}/sql"


class BaseDQOperator(BaseSQLOperator):
    template_fields: Sequence[str] = ("conn_id", "x_params")
    template_ext: Sequence[str] = (".sql", ".json")
    template_fields_renderers = {"sql": "sql", "x_params": "json"}
    ui_color = "#bdabff"

    _jinja_processed = False
    _processed_params: dict = {}
    _params_for_print: dict[str, dict] = {}

    def __init__(self,
                 dq_group: str,
                 x_params: Mapping = {},
                 **kwargs):
        self.x_params = x_params
        self.dq_group = dq_group

        super().__init__(**kwargs)

    def render_template_fields(self, context: Context,
                               jinja_env: jinja2.Environment | None = None
                               ) -> None:

        # Jinja окружение, которое будет передано в метод базового класса
        if not jinja_env:
            # подстановка текущего DAG, для запуска из Python-кода
            if not self.get_dag():
                self.dag = context['dag']
            jinja_env = self.get_template_env()

        # сборка параметров в порядке приоритетов:
        if not self._processed_params:

            # обработка jinja-шаблонов в параметрах
            rendered_params = self.render_template(
                self.x_params, context, jinja_env, set())

            params_no_none = {k: v for k, v in rendered_params.items()
                              if v not in (None, 'None')}

            dag_params = context['params']  # параметры уровня DAG'а
            # и удалим список задач из параметров, если он есть
            dag_params.pop(SKIP_TASKS_KEY, None)

            common_params = get_sql_params(context)

            # кроме прочего - последовательность наложения параметров:
            params_sources = {
                'общие параметры': common_params,
                'параметры DAG': dag_params,
                'параметры оператора': params_no_none
            }

            for k, v in params_sources.items():
                for pk, pv in v.items():
                    self._params_for_print[pk] = {'val': pv, 'src': k}
                    self._processed_params[pk] = pv

            self.x_params = self._processed_params  # только для UI

        # вызов метода базового класса.
        # заменяем параметры в контексте для базового класса
        context['params'] = self._processed_params  # type: ignore
        super().render_template_fields(context, jinja_env=jinja_env)

        self._jinja_processed = True


class PrepareSQLDataQualityOperator(BaseDQOperator):
    template_fields: Sequence[str] = ("where_dq_condition",)
    template_ext: Sequence[str] = (".sql", ".json")
    template_fields_renderers = {"where_dq_condition": "sql"}

    def __init__(self,
                 rules_ids: list,
                 where_dq_condition: str | None = None,
                 **kwargs):
        self.rules_ids = rules_ids
        self.where_dq_condition = where_dq_condition

        super().__init__(**kwargs)

    def find_dq_sql_file(self, find_path, rule_id, rule_group):
        import glob
        import os
        os.chdir(f"{find_path}/{rule_group}/")
        for file in glob.glob(f"{rule_id}_*.sql"):
            return file

    def execute(self, context: Context) -> Any:
        group_cd = []

        match self.dq_group:
            case 'ffoms':
                pass
            case 'ref':
                pass
            case 'erzl':
                group_cd.append('ERZL')

        # получаем правила из meta
        logging.info("Получение активных правил...")

        file_sql = f"{DQ_SCRIPTS_SQL_PATH}/{self.dq_group}/get_rules.sql"

        sql_params = context["params"]
        sql_params["group_cd"] = ','.join(map("'{0}'".format, group_cd))
        sql_params["rules"] = self.rules_ids

        logging.debug(f"sql params: {sql_params}")

        sql = get_rendered_file_content(context, file_sql, {"params": context["params"]})

        hook = self.get_db_hook()

        dq_rules = hook.run(
            sql=sql,
            handler=fetch_all_handler,
            # autocommit=self.autocommit,
            # parameters=self.parameters,
            # return_last=self.return_last,
            # **extra_kwargs,
        )

        if not dq_rules:
            return {}

        logging.info("Подготовка скриптов по правилам...")

        sql_params = {
            **get_sql_params(context),  # общие SQL параметры
            **context['params'],  # параметры уровня DAG'а
            **(get_params_from_xcom_any_task(context, keys='sql_vars') or {})
        }

        logging.debug(f"sql params: {sql_params}")

        result = {}

        if dq_rules:
            for rule in dq_rules:
                rule_id = rule[0]
                rule_group = rule[1]
                table_id = rule[2]
                table_name = rule[3]
                type_d = rule[4]
                file_ver = rule[5]
                err_status_cd = rule[6]

                logging.info(f"Ищем файл правил: {DQ_RULES_SQL_PATH}/{rule_group}/{rule_id}_*.sql")

                file_name = self.find_dq_sql_file(DQ_RULES_SQL_PATH, rule_id, rule_group)

                if file_name:
                    logging.info(f"Найден файл правил: {DQ_RULES_SQL_PATH}/{rule_group}/{file_name}")
                else:
                    AirflowFailException(
                        f"Не найден файл правил для rule_id: {DQ_RULES_SQL_PATH}/{rule_group}/{rule_id}_*.sql")

                sql_params = {**self.x_params}

                where_dq_condition = ""

                if self.where_dq_condition:
                    where_dq_condition = self.where_dq_condition
                else:
                    match self.dq_group:
                        case 'ffoms':
                            where_dq_condition = f"t.doc_ods_id in (select doc_ods_id from meta.log_document_load where etl_proc_id = {sql_params['etl_proc_id']} and position(file_type_cd in '{type_d}')>0 and file_ver = '{file_ver}')"
                        case 'ref':
                            where_dq_condition = True
                        case 'erzl':
                            where_dq_condition = True

                sql_params["where_dq_condition"] = where_dq_condition

                logging.debug(f"{sql_params=}")
                logging.debug(f"Рендеринг правила: {rule_id}")

                sql_dq = get_rendered_file_content(context, f"{DQ_RULES_SQL_PATH}/{rule_group}/{file_name}",
                                                   {"params": sql_params})

                logging.debug(f"SQL правила: {sql_dq}")
                logging.debug(f"Рендеринг финального sql: {rule_id}")

                sql_params["table_id"] = table_id
                sql_params["rule_id"] = rule_id
                sql_params["query_check"] = sql_dq
                sql_params["err_status_cd"] = err_status_cd

                logging.debug(f"{sql_params=}")

                final_sql = get_rendered_file_content(context,
                                                      f"{DQ_SCRIPTS_SQL_PATH}/{self.dq_group}/insert_wrapper.sql",
                                                      {"params": sql_params})

                logging.debug(f"{final_sql=}")

                if table_name not in result.keys():
                    result[table_name] = []

                result[table_name].append(final_sql)

        return result


class ReportDataQualityOperator(BaseDQOperator):
    pass
