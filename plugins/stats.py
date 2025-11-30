import logging
from re import compile
from statsd import StatsClient


# допустимые символы в явном виде
REGEX_CHARS_RANGE = r'a-zA-Z0-9_'

CHECK_REGEX = compile(r"^[" + REGEX_CHARS_RANGE + r"]+$")
REPLACE_REGEX = compile(r"[^" + REGEX_CHARS_RANGE + r"]+")

STAT_MAX_LENGTH = 250
LONG_NAME_ENDING = '_TLDR'

_SYMBOLS = (
    u"абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ",
    u"abvgdeejzijklmnoprstufhzcss_y_euaABVGDEEJZIJKLMNOPRSTUFHZCSS_Y_EUA"
)
TRANSLATE_MAPPING = {ord(a): ord(b) for a, b in zip(*_SYMBOLS)}
""" да, это простейшая транслитерация. Такая применяется для упрощения """


def custom_stat_name_handler(stat_name: str, max_length=STAT_MAX_LENGTH) -> str:
    """
    Validate the StatsD stat name.

    Apply changes when necessary and return the transformed stat name.
    """
    if not isinstance(stat_name, str):
        raise TypeError("Имя шага должно быть строкой! Видим: "
                        f"значение '{stat_name}' ({type(stat_name)})")

    if len(stat_name) > max_length:
        stat_name = (
            stat_name[0:max_length - len(LONG_NAME_ENDING)]
            + LONG_NAME_ENDING
        )

    if CHECK_REGEX.match(stat_name):
        return stat_name

    return REPLACE_REGEX.sub('_', stat_name.translate(TRANSLATE_MAPPING))


# class CustomStatsClient(StatsClient):
#     def _send(self, data: str):
#         """Send data to statsd."""
#         # оригинал: data.encode(encoding='ascii')
#         # план Б: data.encode(encoding='ascii', errors='backslashreplace')
#         try:
#             self._sock.sendto(data.encode(encoding='utf-8'), self._addr)
#         except Exception as ex:
#             log = logging.getLogger('airflow.task')
#             log.error("Ошибка при отправке метрики! Данные: \"%s\", ошибка:",
#                       data, exc_info=ex)
#             pass    # логгер временный
