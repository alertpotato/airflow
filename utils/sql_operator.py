from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from airflow.models.connection import Connection
from airflow.models.param import ParamsDict
from airflow.providers.common.sql.hooks.sql import return_single_query_results
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from utils.clean_files import FILES_NAME_SUFFIX as CLEAN_FILE_SUFFIX
from utils.clean_files import run_if_exist as _clean_files_run_if_exist
from utils.common import get_sql_params, get_str_table
from utils.skipping_tasks import SKIP_TASKS_KEY

if TYPE_CHECKING:
    from collections.abc import Mapping

    import jinja2
    from airflow.utils.context import Context


LOG_LINE = '-' * 60


class CustomSQLOperator(SQLExecuteQueryOperator):
    """Выполняет SQL файл или текст запроса

    параметры SQL
    ---
    оператор автоматически добавляет параметры из
    - функции `common.get_sql_params` (в т.ч. `etl_run_id` и `etl_proc_id`)
    - свойств DAG'а
    - аргумента `x_params` (исключая `None` значения)

    и именно в такой последовательности. Это означает, что в любом месте
    можно переопределить стандартные параметры, а в аргументах
    оператора - параметры из свойств DAG'а.

    Про `None` значения: параметры оператора со значением `None` не
    перезаписывают вышестоящие параметры с тем же именем.

    Args:
        task_id: идентификатор задачи (сущность Airflow)

        conn_id: идентификатор подключения к БД (сущность Airflow)

        sql (str, list[str]): текст SQL запроса или путь к файлу.
            Расширение файла должно быть '.sql'.
            Путь должен быть относительным от расположения файла DAG'а
            или от значения `template_searchpath` из свойств DAG'а

        x_params (dict): (optional) параметры, которые будут
            доступны в Jinja шаблонах через "`{{ params.x }}`"
            Так же реализована поддержка свойства params для обратной
            совместимости, но не рекомендуется пользоваться им в новом коде

        log_sql (bool): выводить текст выполняемого запроса и параметры
            в лог задачи. (по умолчанию: True)

        clean_file_check_path (str): (optional) путь к основному
            sql файлу, для проверки на наличие `*_clean.sql` файла.
            Если "clean-файл" будет найден - он будет выполнен перед `sql`.
            ВНИМАНИЕ! Это НЕ путь к файлу с окончанием `*_clean.sql`.

        dry_run (bool): (optional) Не выполнять SQL запросы. Не мешает всему
            остальному, например - обработке шаблонов и логированию.
            Так же может быть указан в параметрах DAG
            (по умолчанию: False)

        autocommit (bool): (optional) если True - каждая команда будет
            фиксироваться автоматически. (по умолчанию: False).

        handler (func): (optional) функция применяемая к курсору.
            например вернуть количество строк: `lambda cur: cur.rowcount`
            (по умолчанию: fetch_all_handler).

        split_statements (bool): (optional) разделяет несколько выражений на
            отдельные шаги. (по умолчанию: False)

        return_last (bool): (optional) возвращать результат только
            последнего выражения (по умолчанию: True).
            вернуть все можно только с `split_statements=True`

        show_return_value_in_logs (bool): (optional) выводить результат
            выполнения SQL в лог задачи. Не рекомендуется для больших
            объёмов данных. (по умолчанию: False).

    в этом операторе работают jinja-шаблоны вместе с task-mapping,
    хотя не должны:
    https://airflow.apache.org/docs/apache-airflow/2.9.3/authoring-and-scheduling/dynamic-task-mapping.html#how-do-templated-fields-and-mapped-arguments-interact
    (актуально на 2.9.3)
    """

    template_fields: Sequence[str] = ("conn_id", "sql", "x_params")
    template_ext: Sequence[str] = (".sql", ".json")
    template_fields_renderers = {"sql": "sql", "x_params": "json"}
    ui_color = "#abc3ff"
    x_params: Mapping

    _jinja_processed = False
    _processed_params: dict
    """ Тут хранятся обработанные параметры
    (т.е. после рендеринга Jinja шаблонов и добавления общих параметров) """

    _params_for_print: dict[str, dict]
    """ параметры с источником (для вывода в лог задачи), формат:
    ```json
    {
        "param_name": {"val": "param value", "src": "param level"}
    }
    ```
    """

    def __init__(
            self,
            x_params: Mapping | None = None,
            log_sql: bool = True,
            clean_file_check_path: str | None = None,
            dry_run: bool | None = None,
            **kwargs
    ) -> None:
        self._processed_params = {}
        self._params_for_print = {}

        super().__init__(**kwargs)

        self.clean_file_check_path = clean_file_check_path
        self.log_sql = log_sql
        self.dry_run_or_none = dry_run
        # ^ dry_run_or_none - чтобы не закрывать метод dry_run от предков.

        try:    # Airflow валится в ошибку при обращении к свойству...
            has_dag = hasattr(self, 'dag')
        except Exception:
            has_dag = False

        # поддержка старого свойства 'params',
        if p := kwargs.get('params'):
            if has_dag:
                # с защитой от наслоения параметров DAG
                p = {k: v for k, v in p.items()
                     if v.value != self.dag.params.get(k)}
            x_params = {
                **ParamsDict(p).dump(),    # type: ignore
                **(x_params or {})
            }
        self.x_params = x_params or {}

    def execute(self, context):
        """ Выполняет SQL и возвращает значение.

        Перед запуском обрабатываются Jinja шаблоны.
        """

        # если в оператор переданы параметры и обработка Jinja ранее
        # не запускалась - запускаем принудительно:
        if not self._jinja_processed:
            self.render_template_fields(context)

        if self.log_sql:
            self._log_me(context)

        hook = self.get_db_hook()   # тут будет вызван get_connection

        # замена метода, чтоб не было "Using connection ID ..."
        # airflow/hooks/base.py:73
        hook.get_connection = Connection.get_connection_from_secrets

        if self.split_statements is not None:
            extra_kwargs = {"split_statements": self.split_statements}
        else:
            extra_kwargs = {}

        if self._get_dry_run(context):
            return

        if self.clean_file_check_path:
            self._clean_files_check(context)

        hook._run_command = self._run_command   # type: ignore
        output = hook.run(
            sql=self.sql,
            autocommit=self.autocommit,
            parameters=None,
            handler=self.handler if self.do_xcom_push else None,
            return_last=self.return_last,
            **extra_kwargs,
        )

        if not self.do_xcom_push:
            return None
        if return_single_query_results(self.sql, self.return_last, self.split_statements):
            # For simplicity, we pass always list as input to _process_output, regardless if
            # single query results are going to be returned, and we return the first element
            # of the list in this case from the (always) list returned by _process_output
            return self._process_output([output], hook.descriptions)[-1]  # type: ignore
        return self._process_output(output, hook.descriptions)  # type: ignore

    def render_template_fields(self, context: Context,
                               jinja_env: jinja2.Environment | None = None
                               ) -> None:
        """ Обрабатывает Jinja-шаблоны в параметрах и в переданном sql.

        Не требуется запускать вручную, если в оператор переданы `x_params`.
        """

        # # Jinja окружение, которое будет передано в метод базового класса
        if not jinja_env:
            # подстановка текущего DAG, для запуска из Python-кода
            if not self.get_dag():
                self.dag = context['dag']
            jinja_env = self.get_template_env()

        if not self._params_for_print:
            merged_params = self._get_merged_params(context)
            # тут будет выполнена только частичная обработка шаблонов:
            rendered_params = super().render_template(
                merged_params, context, jinja_env, set())
            context["params"].update(rendered_params)

        super().render_template_fields(context, jinja_env)

        # к этому моменту предполагаем, что обработка шаблонов завершена
        for k, v in self._params_for_print.items():
            pv = self.x_params.get(k) or v['val']
            self._params_for_print[k]['val'] = pv
            self._processed_params[k] = pv

        self._jinja_processed = True

    def render_template(
            self,
            content: Any,
            context: Context,
            jinja_env: jinja2.Environment | None = None,
            seen_oids: set[int] | None = None,
    ) -> Any:
        """
        Заплатка для обработки шаблонов и параметров в mapped-task'ах:
        
        Необходимо добавить в контекст все расширенные параметры,
        так как стандартная обработка таких задач не вызывает 
        `render_template_fields` (для mapped-task - отдельный метод) """

        if not self._params_for_print:
            merged_params = self._get_merged_params(context)
            context["params"].update(merged_params)
        elif self._processed_params:
            context["params"].update(self._processed_params)

        return super().render_template(
            content,
            context,
            jinja_env,
            seen_oids
        )

    def _get_merged_params(self, context: Context):
        """ Возвращает весь набор внутренних параметров
        (включая параметры оператора, общие и от DAG),

        заодно собирая дерево `_params_for_print` (используется для печати)

        Порядок наложения параметров:
        - общие параметры
        - параметры уровня DAG'а
        - параметры оператора
        - 'etl_run_id' и 'etl_proc_id' из XCOM

        это означает, что (например) в параметрах оператора можно
        переопределить параметры DAG'а 
        """

        merged_params = {}

        # сборка параметров в порядке приоритетов:

        dag_params = dict(context['params'])  # параметры уровня DAG'а
        common_params = get_sql_params(context)  # общие

        # если в XCOM нашлись не пустые значения от текущего запуска
        xcom_values = {k: v for k, v in common_params.items()
                       if k in ['etl_run_id', 'etl_proc_id'] and v}

        # кроме прочего - последовательность СЛИЯНИЯ параметров:
        params_sources = {
            'общие параметры': common_params,
            'параметры DAG': dag_params,
            'параметры оператора': self.x_params,
            'этот запуск': xcom_values
        }

        if self._params_for_print:
            self.log.warning("повторное создание _params_for_print!\n%s",
                             self._params_for_print)

        for src, src_params in params_sources.items():
            for pk, pv in src_params.items():
                if pk == SKIP_TASKS_KEY:
                    continue    # убираем список задач из другого модуля
                self._params_for_print[pk] = {'val': pv, 'src': src}
                merged_params[pk] = pv
        return merged_params

    def _log_me(self, context: Context):
        log_msgs = [f"{CustomSQLOperator.__name__} - Подготовка:",]

        if self._params_for_print:
            tuples = [(k, v['val'], v['src'])
                      for k, v in self._params_for_print.items()]
            sort_order = (  # сортировка ПЕЧАТИ, не путать со слиянием
                'этот запуск',
                'параметры оператора',
                'параметры DAG',
                'общие параметры'
            )
            tuples.sort(key=lambda x: (sort_order.index(x[2]), x[0]))
            headers = ('Имя', 'Значение', 'Источник')
            log_msgs.extend([
                LOG_LINE,
                "Параметры:",
                LOG_LINE,
                *get_str_table(data=tuples, headers=headers, as_list=True),
                LOG_LINE
            ])

        dr = self._get_dry_run(context)
        execute_message = ('Выполнение SQL' if not dr else
                           "SQL НЕ будет выполнен! (`dry_run=True`)")

        if isinstance(self.sql, list):
            log_msgs.append(f"{execute_message} ({len(self.sql)} шт):")
            if dr:
                queries = []
                for i, statement in enumerate(self.sql):
                    queries.append(f"> {i + 1}:\n{statement}\n")
                log_msgs.extend(queries)
        elif self.split_statements:
            log_msgs.append(
                f"{execute_message}"
                " (каждый запрос отдельно - `split_statements=True`):"
            )
            if dr:
                log_msgs.append(f"\n{self.sql}\n")
        else:
            log_msgs.append(f"{execute_message}:")
            if dr:
                log_msgs.append(f"\n{self.sql}\n")
        log_msgs.append(LOG_LINE)

        if self.clean_file_check_path:
            if self._get_dry_run(context):
                log_msgs.append(
                    f"Указан путь для поиска `{CLEAN_FILE_SUFFIX}.sql` файла "
                    "(с `dry_run=True` он не выполняется)"
                    f": [{self.clean_file_check_path}]"
                )
            else:
                log_msgs.append(
                    f"Сначала выполняется `{CLEAN_FILE_SUFFIX}.sql` файл для "
                    f"[{self.clean_file_check_path}], если такой существует."
                )
            log_msgs.append(LOG_LINE)

        self.log.info('\n'.join(log_msgs))

    def _clean_files_check(self, context: Context):
        _clean_files_run_if_exist(
            file_path=self.clean_file_check_path,
            context=context,
            jinja_args={'params': self._processed_params},
            conn_id=self.conn_id,  # type: ignore (для декоратора)
            log_method=self.log.info
        )

    def _get_dry_run(self, context: Context) -> bool:
        """ Возвращает значение параметра `dry_run` по приоритету:
        - главнее всех параметр оператора
        - если он не указан: используется параметр DAG'а
        - если не указан и он - возвращается `False`
        (значение по умолчанию)
        """

        if self.dry_run_or_none is not None:
            return self.dry_run_or_none
        if context['params'].get('dry_run', None):
            return context['params']['dry_run']
        else:
            return False

    def _run_command(self, cur, sql_statement, parameters):
        """ Переопределение метода из
        airflow/providers/common/sql/hooks/sql.py
        для изменения сообщения, выводимого в лог.

        Код основан на версии 2.6.3, обновить при необходимости.
        """
        if self.log_sql:
            self.log.info("Выполнение запроса:\n%s\n...⏳", sql_statement)

        # убрана версия с parameters
        cur.execute(sql_statement)

        if self.log_sql:
            rowcount = cur.rowcount if cur.rowcount > 0 else "нет"
            self.log.info("\t^ Готово, затронуто строк: %(rowcount)s\n%(line)s",
                          dict(rowcount=rowcount, line=LOG_LINE))
