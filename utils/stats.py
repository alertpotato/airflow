from re import compile


# допустимые символы в явном виде
REGEX_CHARS_RANGE = r'A-Za-zА-Яа-я.-'

CHECK_REGEX = compile(r"^[" + REGEX_CHARS_RANGE + r"]+$")
REPLACE_REGEX = compile(r"[^" + REGEX_CHARS_RANGE + r"]+")

STAT_MAX_LENGTH = 250
LONG_NAME_ENDING = '_TLDR'


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
    return REPLACE_REGEX.sub('_', stat_name)
