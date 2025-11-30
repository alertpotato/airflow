from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Mapping, MutableMapping, Sequence, Dict, Optional
from airflow.providers.common.sql.operators.sql import BaseSQLOperator
from airflow.exceptions import AirflowException
from utils.common import get_sql_params
from utils.sql_operator import CustomSQLOperator

if TYPE_CHECKING:
    import jinja2


class CreateTmpTable(CustomSQLOperator):
    ui_color = "#edcdaa"

    def __init__(self,
                 schema: str,
                 table_name: str,
                 temp_prefix: str | None = "",
                 temp_suffix: str | None = "",
                 drop_if_exists: bool = False,
                 **kwargs
                 ) -> None:
        super().__init__(**kwargs)
        self.schema = schema
        self.table_name = table_name
        self.temp_prefix = temp_prefix
        self.temp_suffix = temp_suffix
        self.drop_if_exists = drop_if_exists

    def _generate_table_name(self):
        return f"{self.temp_prefix}{self.table_name}{self.temp_suffix}"

    def execute(self, context):
        if not self._jinja_processed:
            self.render_template_fields(context)

        tmp_table_name = self._generate_table_name()

        if self.drop_if_exists:
            drop_sql = f"DROP TABLE IF EXISTS {self.schema}.{tmp_table_name};"
        else:
            drop_sql = ""

        create_sql = f"CREATE TABLE {self.schema}.{tmp_table_name} AS "

        if isinstance(self.sql, str):
            self.sql = f"{drop_sql}\n" \
                       f"{create_sql}\n" \
                       f"{self.sql}"
        else:
            raise AirflowException(
                "Supported only string SQL statements."
                "Please turn your SQL statement into a text statement."
            )

        self.render_template(self.sql, context)

        if self.log_sql:
            self._log_me(context)
    
        hook = self.get_db_hook()

        output = hook.run(
            sql=self.sql,
            autocommit=False
        )

        return output
