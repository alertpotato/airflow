"""
Загрузка YAML структуры для формирования DAG

класс YamlDagBuilder:
    Экземпляр хранит информацию о загруженном yaml файле и содержит методы
    для взаимодействия с ним.

    Любое обращение к yaml файлу автоматически загружает его во
    внутреннее поле экземпляра, далее используется кешированное значение.

    Raises:
        FileNotFoundError: yaml файл не найден

        YAMLError: ошибка синтаксиса YAML

        ValidationError: ошибка в синтаксисе описания DAG в yaml файле

        TypeError: ошибка типа значения
"""
from __future__ import annotations

from pathlib import Path
from re import compile as re_compile
from typing import TYPE_CHECKING, Callable

from oyaml import SafeLoader, YAMLError, safe_load

from airflow.models import Variable as AirflowVariable
from airflow.models.baseoperator import BaseOperator, chain
from airflow.utils.helpers import GROUP_KEY_REGEX, KEY_REGEX
from airflow.utils.task_group import TaskGroup
from utils.clean_files import cleanup_temp_tables_task
from utils.common import get_airflow_home_path, get_file_content, get_sql_home_path
from utils.common_dag_params import PARAMS_DICT, get_md_description
from utils.logs_meta.shared import get_on_failure_function
from utils.skipping_tasks import check_task, print_dag_task_ids
from utils.yaml_dag import dag_defaults, tags, validations
from utils.yaml_dag.dq_tasks import create_dq_tasks
from utils.yaml_dag.validations import BuilderError, ElemType

if TYPE_CHECKING:
    from re import Pattern
    from typing import Any

STEPS_KEY = 'process_steps'
VARIABLES_KEY = 'variables'
DEFAULT_DQ_CONN_ID = 'nds_dq'

COLORS = {
    ElemType.parallel_group: '#ce99e0',
    ElemType.sequential_group: '#84ebb0',
    'yaml_step': '#ffe0c0',
    # 'dq_group': '?'    # TODO: добавить?
}

# максимальная длина имени шага и группы - скопировано из Airflow:
# airflow/utils/helpers.py
STEP_NAME_MAX_LENGTH = 250
GROUP_NAME_MAX_LENGTH = 200

STEP_NAME_REPLACE_REGEX = re_compile(r'[^\w.-]+')
GROUP_NAME_REPLACE_REGEX = re_compile(r'[^\w-]+')
NON_TEXT_PREFIX_REGEX = re_compile(r'^[^А-яA-z0-9_]*')

LONG_NAME_ENDING = '_l'


class YamlDagBuilder():
    _yaml_obj: dict[str, Any] = {}
    _file_path: str
    _first_step_processed = False

    _vals: validations.Validations
    """ экземпляр класса Validations, для запуска проверок с контекстом """

    def __init__(self, file_path: str):
        """
        Args:
            file_path (str): путь к yaml файлу (от корня SQL репозитория)
        """
        self._file_path = file_path
        self._vals = validations.Validations(self)

    def get_dag_args(self, default_args: dict | None = None):
        """ Возвращает набор настроек для конструктора DAG:

        - `schedule`: расписание запуска в формате cron
        - `doc_md`: описание из yaml или из readme.md (из каталога dag'а)
        - `description`: первая непустая строка из описания
        - `params`: сведённые параметры из функции `get_params_for_dag`
        - `default_args`: настройки для задач, в т.ч. `on_failure_callback`
        - `template_searchpath`: путь к файлам DAG (директория yaml)
        - `render_template_as_native_obj`: `True`

        Если передан default_args - он будет добавлен ПОСЛЕ параметров
        модуля yaml_dag
        """

        yaml_obj = self.get_yaml_object()
        yaml_dir = Path(get_sql_home_path(), self._file_path).parent
        descr_path = Path(yaml_dir, 'README.md')

        display_name = None

        doc_md = None
        description = None

        dag_section: dict = yaml_obj.get('dag') or {}

        if name_str := dag_section.get('name'):
            display_name = NON_TEXT_PREFIX_REGEX.sub(
                "", name_str.strip().splitlines()[0], 1)

        if descr := dag_section.get('description'):
            doc_md = descr
        elif descr_path.exists():
            doc_md = get_file_content(descr_path.as_posix())

        if doc_md:
            doc_md += '\n---\n' + get_md_description()
            description = NON_TEXT_PREFIX_REGEX.sub(
                "", doc_md.strip().splitlines()[0], 1)

        cron = self._vals.get_valid_cron(dag_section.get('cron'))

        pool = dag_section.get('pool', 'default_pool')

        # недокументированная фича:
        # доп. путь в template_searchpath, а нужно ли?
        searchpaths = [yaml_dir.as_posix()]
        if extra_path := dag_section.get('extra_path'):
            searchpaths.append(extra_path)

        # каталог с служебными скриптами логирования
        searchpaths.append(f"{get_airflow_home_path()}/sql/log")

        default_args_for_dag = {
            'pre_execute': _get_hidden_func(check_task),
            'on_failure_callback': get_on_failure_function(
                tables_to_finish=('log_etl_proc', 'log_etl_proc_table'),
                xcom_keys=('etl_run_id', 'etl_proc_id', 'all_tbl_ids'),
            ),
            'pool': pool,
            'trigger_rule': 'none_failed',
            **(default_args or {})
        }

        return {
            'dag_display_name': display_name,
            'schedule': cron,
            'doc_md': doc_md,
            'description': description,
            'params': self.get_params_for_dag(),
            'default_args': default_args_for_dag,
            'template_searchpath': searchpaths,
            'render_template_as_native_obj': True,
        }

    def get_variables(self) -> dict:
        """ Возвращает только секцию переменных из указанного файла """
        return self.get_yaml_object()[VARIABLES_KEY]

    def get_params_for_dag(self):
        """ Возвращает сведённые параметры в таком порядке:
        - общие параметры из `utils.common_dag_params.PARAMS_DICT`
        - все переменные из yaml файла, секции `VARIABLES_KEY`
        """

        return {
            **PARAMS_DICT,
            **self.get_variables(),
        }

    def get_task_list(self):
        """ Возвращает список задач из подготовленных task_id (
        функцией `self.prepare_step_name`).

        Предполагается, что этот метод может вызываться вне `task`,
          поэтому тут метод `self.get_all_table_ids` вызывается без
          валидации """

        return list(self.get_all_table_ids(
            steps_without_table_id=True).keys())

    def get_all_table_ids(self, validate=False, steps_without_table_id=False):
        """ Возвращает table_id всех шагов из YAML файла.

        Args:
            validate: выполнять ли проверку синтаксиса описания шага.
            Такая проверка имеет смысл только при выполнении этого
            кода внутри task (например на инициализации),
            а во время генерации DAG - её лучше отключать.

            steps_without_table_id: так как прежде всего этот метод
            служит для формирования списка шагов с их table_id -
            шаги без такого ключа не возвращаются.
            Эта опция не учитывается валидацией!

        Returns:
        ```json
        {
            "{step_task_id}": {
                "mt_tbl_id": 123
            },
            // ...
        }
        ```
        где:
        `step_task_id` = значению поля `step`, если есть или `file`
        (в значении так же заменяются недопустимые символы на '_')

        а цифры `123` для примера - это `step['table_id']`

        Выводится предупреждение, если в YAML файле найдены 2 разных
        значения для одного `step_task_id`
        """

        def update_with_check(step: dict, result: dict):
            """ Обновляет `result`, добавляя туда `step` в формате для
            инициализации.

            Выполняет валидацию шага, если `validate=True`
            """

            if validate:
                self._vals.validate_step_syntax(step)
                self._vals.check_step_uniqueness(
                    step, result, prepare_step_task_id)

            if step.get('table_id', -1) == -1:
                if not steps_without_table_id:
                    return
                step['table_id'] = -1

            step_task_id = prepare_step_task_id(
                step.get('step') or step.get('file'))  # type: ignore
            entry = {step_task_id: {"mt_tbl_id": step['table_id']}}
            result.update(entry)

        def process_steps(group, result_dict):
            match self._vals.get_elem_type(group):
                case ElemType.step:
                    update_with_check(group, result_dict)
                case ElemType.sequential_group | ElemType.parallel_group:
                    _, gr_steps = _get_first_key_value(group)
                    for step in gr_steps:
                        process_steps(step, result_dict)
                case _:
                    raise BuilderError(
                        group, "получение всех table_id", self._file_path)

        yaml_obj = self.get_yaml_object()
        result_dict = {}

        for elem in yaml_obj[STEPS_KEY]:
            process_steps(elem, result_dict)

        return result_dict

    def get_yaml_object(self) -> dict[str, Any]:
        """ Загружает файл структуры DAG.
        Путь указывается от корня SQL репозитория.

        Возвращает объект со всем содержимым файла."""

        if self._yaml_obj:
            return self._yaml_obj

        f_path = Path(get_sql_home_path(), self._file_path)
        if not f_path.exists():
            raise FileNotFoundError(f"Не найден файл: [{f_path}]")

        for t in [tags.PARALLEL_TAG, tags.SEQUENTIAL_TAG]:
            SafeLoader.add_constructor(t, tags.tagged_step_constructor)

        try:
            with open(f_path, 'r', encoding='utf-8') as file:
                self._yaml_obj = safe_load(file)

                # тут запускается проверка базовых сущностей,
                # чтобы упростить обнаружение проблем далее
                self._vals.validate_yaml_root()
                self._vals.validate_yaml_variables()
                return self._yaml_obj

        except YAMLError as ex:
            # тут намеренно обрезается бесполезный traceback
            import sys
            sys.tracebacklimit = 0
            msg = ["Синтаксическая ошибка YAML файла"]
            if hasattr(ex, 'problem_mark'):
                msg.extend([
                    f_path.as_posix(),
                    f"{ex.problem}, line {ex.problem_mark.line + 1}, "  # type: ignore
                    f"column {ex.problem_mark.column + 1}"  # type: ignore
                ])
            raise YAMLError('\n'.join(msg)) from None

    def create_tasks(self,
                     pre_init_task: Any = None,
                     pre_processing_task: Any = None,
                     post_processing_task: Any = None,
                     after_all_task: Any = None,
                     init_task=dag_defaults.init_all,
                     process_step_task=dag_defaults.process_step,
                     finish_task=dag_defaults.finish_all):
        """ Генерирует задачи Airflow из YAML файла `yaml_path`.
        И возвращает их в виде списка задач.

        Используется последовательность (внимание на разницу в обращении):

        `pre_init_task` >> `init_task(yaml_path)` >> `pre_processing_task` \
        >> `'все задачи из yaml'` >> `post_processing_task` >> `finish_task()` \
        >> `cleanup_temp_tables_task()` >> `after_all_task`

        Все задачи можно переопределить на свои собственные.
        В задачу init_task передаётся аргумент `yaml_path`

        Примеры смотреть в `dags/examples/yaml_dag_example.py`

        У этого метода нет обязательных аргументов.

        Args:
            pre_init_task: вызванный Task или Task_Group
            pre_processing_task: вызванный Task или Task_Group
            post_processing_task: вызванный Task или Task_Group
            init_task: ссылка на (@task)метод инициализации (в \
                аргументы передаётся `yaml_path`)
            process_step_task: ссылка на (@task)метод обработки шага. \
                В аргументы передаётся dict, содержащий весь шаг и \
                дополнительный ключ ['task_id']. По этому ключу можно \
                получить mt_tbl_id из стандартного `init_task`
            finish_task: ссылка на (@task)метод финализации \
                (без аргументов)
        """

        yaml_obj = self.get_yaml_object()

        init_result = init_task.override(
            on_execute_callback=_get_hidden_func(print_dag_task_ids)
        )(self._file_path)

        result_tasks: list = [init_result]

        if pre_init_task:
            result_tasks.insert(0, pre_init_task)

        if pre_processing_task:
            result_tasks.append(pre_processing_task)

        # корневая группа всегда список и выполняется последовательно
        steps = [self._get_yaml_steps(e, process_step_task)
                 for e in yaml_obj[STEPS_KEY]]
        chain(*steps)
        result_tasks.extend(steps)

        if post_processing_task:
            result_tasks.append(post_processing_task)

        result_tasks.extend([finish_task(), cleanup_temp_tables_task()])

        if after_all_task:
            result_tasks.append(after_all_task)

        chain(*result_tasks)
        return result_tasks

    def _get_yaml_steps(self, group: dict, process_step_task):
        """ Рекурсивный обработчик шагов YAML файла.

        Принимает группу, которая может состоять из шагов или вложенных
        групп.

        Args:
            group (dict): группа шагов, представленная в виде dict

            process_step_task (task): обработчик для выполнения шага

        Returns:
            PythonOperator (из `process_step_task`) или TaskGroup

        Raises:
            BuilderError: не удалось определить тип шага
        """
        match self._vals.get_elem_type(group):

            case ElemType.step:
                el = self._create_step(process_step_task, group)

            case ElemType.sequential_group | ElemType.parallel_group as t:
                gr_name, gr_items = _get_first_key_value(group)
                group_id = prepare_group_id(gr_name)
                with TaskGroup(group_id=group_id, tooltip=str(gr_name)) as el:
                    tasks = []
                    # уверенность, что тут list приходит от get_elem_type
                    for item in gr_items:
                        step = self._get_yaml_steps(item, process_step_task)
                        tasks.append(step)
                    if t is ElemType.sequential_group:
                        chain(*tasks)

                    if color := COLORS.get(t):
                        el.ui_color = color
            case _:
                raise BuilderError(group, "построение задач", self._file_path)

        return el

    def _create_step(self, process_step_task, step: dict) -> BaseOperator | TaskGroup:
        # sure_key - результат поиска 'file' или 'dq_group' в ключах step
        # точнее фильтр не нужен, так как это не валидация
        sure_key = set(['file', 'dq_group']).intersection(step).pop()
        # step_name - имя шага до обработки
        step_name = step.get('step') or step[sure_key]
        step_task_id = prepare_step_task_id(step_name)
        step['task_id'] = step_task_id

        step_task_args = {
            'task_id': step_task_id,
            'task_display_name': step_name,
            'doc': step.get('info'),
        }

        if ds_uri := step.get('dataset'):
            from airflow import Dataset
            step_task_args['outlets'] = [Dataset(ds_uri)]

        # к первому шагу trigger_rule не добавляется, не смотря на YAML
        if self._first_step_processed:
            if (tr := step.get('trigger_rule')):
                step_task_args['trigger_rule'] = tr
        else:
            self._first_step_processed = True

        match sure_key:
            case 'file':
                step_op = process_step_task.override(**step_task_args)(step)
                step_op.operator.ui_color = COLORS.get('yaml_step')
                return step_op
            case 'dq_group':
                return self._create_dq_step(step, step_task_args, process_step_task)
            case _:
                raise BuilderError(
                    step, "создание задач, выбор типа", self._file_path)

    def _create_dq_step(self, step: dict, task_args: dict, process_step_task):
        # подключение DQ из YAML или значение по умолчанию
        dq_conn_id: str = (
            step.get('conn_id') or
            self.get_variables().get('DQ_CONN_ID') or
            AirflowVariable.get('dq_conn_id', DEFAULT_DQ_CONN_ID)
        )

        with TaskGroup(group_id=task_args['task_id'],
                       tooltip=task_args['doc']) as dq_group:
            dq_tasks = create_dq_tasks(
                rules=step['rules'],
                tables=self.get_all_table_ids(),
                dq_group=step['dq_group'],
                conn_id=dq_conn_id)

            pre_dq, post_dq = step.get('pre_dq'), step.get('post_dq')
            pre_post_files = [x for x in (pre_dq, post_dq) if x]

            for file in pre_post_files:
                dq_tasks.insert(
                    0 if file is pre_dq else len(dq_tasks),
                    process_step_task.override(  # как обычный sql-шаг
                        task_id=prepare_step_task_id(file))({**step, 'file': file})
                )

            chain(*dq_tasks)

        return dq_group


def _get_hidden_func(func: Callable):
    """ Возвращает обёртку для переданной функции, таким образом убирая
    лишний код из параметров DAG.

    Предполагается, что функция будет принимать context
    """

    def _temp_func(context):
        func(context)

    return _temp_func


def _get_first_key_value(obj: dict):
    # альт, работает так же: (k, v), = obj.items()
    name: str = next(iter(obj))
    return name, obj[name]


def prepare_step_task_id(name: str):
    return _get_prepared_name(name=name,
                              max_len=STEP_NAME_MAX_LENGTH,
                              check_rx=KEY_REGEX,
                              repl_rx=STEP_NAME_REPLACE_REGEX)


def prepare_group_id(name: str):
    return _get_prepared_name(name=name,
                              max_len=GROUP_NAME_MAX_LENGTH,
                              check_rx=GROUP_KEY_REGEX,
                              repl_rx=GROUP_NAME_REPLACE_REGEX)


def _get_prepared_name(name: str, max_len: int,
                       check_rx: Pattern,
                       repl_rx: Pattern):
    if isinstance(name, tags.TaggedGroup):
        _, name = name  # распаковка TaggedStep, который tuple(tag, name)

    if not isinstance(name, str):
        raise TypeError("Имя шага должно быть строкой! Видим: "
                        f"значение '{name}' ({type(name)})")

    if len(name) > max_len:
        ending_len = len(LONG_NAME_ENDING)
        name = name[0:max_len - ending_len] + LONG_NAME_ENDING

    if not check_rx.match(name):
        name = repl_rx.sub('_', name)

    return name
