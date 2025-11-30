from __future__ import annotations

from airflow.providers.common.sql.operators.sql import BaseSQLOperator
from airflow.exceptions import AirflowException
from utils.sql_operator import CustomSQLOperator


class TmpTableOperator(BaseSQLOperator):
    def __init__(self,
                 schema: str,
                 table_name: str,
                 temp_prefix: str | None = "",
                 temp_suffix: str | None = "",
                 **kwargs
                 ) -> None:
        super().__init__(**kwargs)
        self.schema = schema
        self.table_name = table_name
        self.temp_prefix = temp_prefix
        self.temp_suffix = temp_suffix

    def _generate_table_name(self):
        return f"{self.temp_prefix}_{self.table_name}_{self.temp_suffix}"


class CreateTmpTable(TmpTableOperator, CustomSQLOperator):
    ui_color = "#edcdaa"

    def __init__(self,
                 drop_if_exists: bool = False,
                 **kwargs
                 ) -> None:
        super().__init__(**kwargs)
        self.drop_if_exists = drop_if_exists

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


class DropTmpTable(TmpTableOperator):
    ui_color = "#4dd9d3"

    def __init__(self,
                 **kwargs
                 ) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        sql = f"DROP TABLE IF EXISTS {self.schema}.{self._generate_table_name()};"

        self.log.info("Executing: %s", sql)
        hook = self.get_db_hook()

        output = hook.run(
            sql=sql,
            autocommit=False
        )
        if not self.do_xcom_push:
            return None

        return output
