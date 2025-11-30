from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Mapping, MutableMapping, Sequence, Dict, Optional, Any
from airflow.providers.common.sql.operators.sql import BaseSQLOperator


class DropTmpTable(BaseSQLOperator):
    ui_color = "#4dd9d3"

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
        return f"{self.temp_prefix}{self.table_name}{self.temp_suffix}"

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
