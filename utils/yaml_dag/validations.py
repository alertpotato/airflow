""" Сюда вынесен весь код, относящийся к валидации YAML
и проверке синтаксиса YamlDagBuilder """

import logging
from enum import Enum

from croniter import croniter
from multicron_plugin import MultiCronTimetable

from airflow.exceptions import AirflowFailException
from utils.yaml_dag import tags
from utils.yaml_dag.table_depends import \
    syntax_validation as table_depends_syntax_validation


class ElemType(Enum):
    """ Тип элемента YAML файла. Используется при чтении и анализе файла. """

    step = 0
    """ Шаг, предполагающий выполнение SQL файла """

    sequential_group = 1
    """ Группа (шаги выстраиваются последовательно c помощью `chain`) """

    parallel_group = 2
    """ Параллельная группа (создаётся TaskGroup с параллельными шагами) """

    error = 111
    """ Не удалось определить тип элемента """


class ValidationError(AirflowFailException):
    def __init__(self, element, messages: str | list[str], anchor: str = "",
                 file: str | None = None):
        """ Убирает traceback и добавляет оформление к сообщению:
        ```python
        "Валидация YAML не пройдена:",
        *messages,
        f"Тут: {repr(element)}",
        get_readme_link(),
        "."
        ```

        Args:
            element: элемент в разметке, на котором произошла ошибка

            messages (str | list[str]): сообщение или несколько строк

            anchor (str): [не обязательный] якорь для ссылки на справку
        по умолчанию используется значение из метода `get_readme_link`
        """
        import sys

        # так как это валидация - traceback не нужен
        sys.tracebacklimit = 0

        if isinstance(messages, str):
            messages = [messages]

        result_messages = [
            "Валидация YAML не пройдена:",
            *messages,
            f"Тут: {repr(element)}"
        ]

        if file:
            result_messages.append(f"Файл: {file}")

        args = {'ending': anchor} if anchor else {}
        result_messages.extend([get_readme_link(**args), '.'])

        super().__init__('\n'.join(result_messages))


class BuilderError(ValidationError):
    def __init__(self, element, messages: str | list[str], file, anchor: str = ""):
        """ Ошибка в генераторе DAG из YAML, не обработан тип шага. 

        Вызывает ValidationError, с сообщением:
        `"Не обработан тип шага на стадии: " + message`
        """

        if isinstance(messages, str):
            messages = [messages]

        messages[0] = "Не обработан тип шага на стадии: " + messages[0]

        super().__init__(element, messages, anchor, file)


class Validations():

    def __init__(self, builder) -> None:
        """ в конструктор класса передаётся экземпляр YamlDagBuilder """
        self._builder = builder

    def validate_dag_args(self, description_file_path):
        """ Проверяет секцию 'dag' в yaml-файле на корректность заполнения """

        dag = self._builder._yaml_obj.get('dag')
        if not dag:
            return  # может не быть секции 'dag' ?

        if dag.get('description'):
            return

        # есть секция, нет описания
        if description_file_path.exists():
            return

        # есть секция, нет описание и файл не существует
        raise ValidationError(
            dag,
            [
                "Секция 'dag' не соответствует синтаксису.",
                "Не указан ключ 'description' (в yaml файле, в секции 'dag')",
                "и не найден файл 'README.md' по адресу:",
                description_file_path.as_posix()
            ],
            file=self._builder._file_path
        )

    def validate_step_syntax(self, step: dict):
        """ Проверяет указанный шаг на соответствие синтаксису.

        Args:
            step (dict): шаг целиком

        Raises:
            ValidationError: если шаг не соответствует синтаксису
        """

        # базовые проверки:

        match step:
            case {'file': str(), 'table_id': int()}:
                # pass    # корректный файл с table_id
                self._check_table_id_is_zero(step)

            case {'file': str(), 'check': True}:
                pass    # корректный файл с проверкой

            case {'dq_group': str(), 'rules': list()}:
                pass    # корректный dq шаг

            case _:
                raise ValidationError(
                    step,
                    "Элемент не соответствует ни одному из возможных "
                    "описаний шага",
                    file=self._builder._file_path)

        # базовые проверки закончены, дальше стандартизированные модули:
        check_list = [
            table_depends_syntax_validation,
        ]
        error_messages = []
        for validation_method in check_list:
            check_result, check_msgs = validation_method(step)
            if check_result is False:
                error_messages.extend(check_msgs or [
                    "Проверка не пройдена, "
                    f"модуль '{validation_method.__name__}'"])
        if error_messages:
            raise ValidationError(step, error_messages,
                                  file=self._builder._file_path)

    def validate_yaml_variables(self):
        """ Проверяет обязательные переменные (секция `variables`).

        Отсутствие секции `variables` приведёт к ошибке KeyError.
        """

        yaml_variables = self._builder._yaml_obj['variables']

        required = [
            'DB_CONN_ID',
            'MT_ETL_PROC_ID'
        ]

        for var in required:
            if var not in yaml_variables:
                raise ValidationError(
                    "обязательные переменные",
                    f"не найдена переменная '{var}'",
                    "#описание-основных-блоков",
                    file=self._builder._file_path
                )

    def validate_yaml_root(self):
        """ Проверяет наличие основных блоков шагов в yaml файле """

        yaml_obj = self._builder._yaml_obj
        valid_root_elements = [
            ('dag', dict, False),
            ('variables', dict, True),
            ('process_steps', list, True),
        ]

        for elem_name, elem_type, required in valid_root_elements:
            if (
                (required and elem_name not in yaml_obj)
                or
                ((f := yaml_obj.get(elem_name)) and not isinstance(f, elem_type))
            ):
                raise ValidationError(
                    "основные блоки YAML файла",
                    f"блок {elem_name} не обнаружен или имеет некорректный тип",
                    "#описание-основных-блоков",
                    file=self._builder._file_path
                )

    def _check_table_id_is_zero(self, step: dict):
        if step['table_id'] == 0:
            logging.getLogger('airflow.task').warning(
                "⚠ Значение 'table_id' = 0 может использоваться "
                "только во время разработки DAG.\nТакие шаги как:\n%s\n"
                "не будут инициализированы системой логирования и "
                "могут некорректно работать.", repr(step))

    def get_elem_type(self, elem) -> ElemType:
        """ Возвращает тип элемента из ElemType.

        Для шагов тут достаточно проверки наличия ключей, так как
        валидация типов осуществляется в методе `validate_step_syntax`

        Если не удалось определить - возвращает `ElemType.error`
        """

        match elem:
            case {'file': _} | {'dq_group': _}:
                return ElemType.step
        # если не совпали ключи - сопоставляем первый элемент:
        match next(iter(elem.items())):
            case tags.TaggedGroup((tags.PARALLEL_TAG, str())), list():
                return ElemType.parallel_group
            case tags.TaggedGroup((tags.SEQUENTIAL_TAG, str())), list():
                return ElemType.sequential_group

            # case str(), list():  # NOTE: вариант группы по умолчанию
            #     return ElemType.sequential_group

        return ElemType.error

    def check_step_uniqueness(self, step: dict, result: dict, get_name_method):
        """ Проверяет, что сочетание имени шага
        (значение 'step' или 'file') и значение 'table_id' уникальны
        для этого DAG.

        Если нет - пишет warning в лог.

        Args:
            step (dict): шаг целиком

            result (dict): проверенные ранее шаги, куда добавляется этот
        """

        table_id = step.get('table_id')
        if not table_id:
            return

        if table_id in [v['mt_tbl_id'] for _, v in result.items()]:
            logging.getLogger('airflow.task').warning(
                "⚠ table_id %(table_id)s встречается более одного раза!\n"
                "Повтор был обнаружен в этом шаге:\n%(step)s",
                {
                    'table_id': table_id,
                    'step': step,
                }
            )

    def get_valid_cron(self, cron: None | str | list[str]):
        """ Возвращает `cron`, если синтаксис прошёл проверку.
        ValidationError если нет. """

        match cron:
            case None | "@once" | "@continuous":
                return cron  # эти значения обрабатываются самим Airflow
            case str() if croniter.is_valid(cron):
                return MultiCronTimetable(cron)
            case list() if all(croniter.is_valid(x) for x in cron):
                return MultiCronTimetable(cron)
            case _:
                raise ValidationError(
                    cron,
                    "расписание 'cron' имеет некорректный синтаксис",
                    file=self._builder._file_path)


def get_readme_link(ending: str = "#описание-возможных-ключей-шага"):
    return (
        "( см: "
        "https://git.element-lab.ru/oms-management-services/"
        "etl/sql/-/blob/master/README.md"
        + ending +
        " )"
    )
