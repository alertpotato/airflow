from subprocess import run

GIT_CONFIG_TRANSFER_PATH = "/opt/airflow/.devcontainer/cfg/git_host.tmp"
""" Этот путь указан в README.md, там ожидаются имя/email с хоста """

GIT_LOG_PREFIX = "Git config"


def parse_file_and_set_config(data: str):
    git_conf = dict(tuple(x.split(maxsplit=1) for x in data.splitlines()))
    # print_message(**git_conf)
    name_and_mail = ["user.name", "user.email"]
    # если в конфиге оказалось всё, что нам нужно
    results = []
    if all(x in git_conf for x in name_and_mail):
        for c in name_and_mail:
            result = run(["git", "config", "--global", c, git_conf[c]])
            results.append(result)
    if len(results) == len(name_and_mail) and all(x.returncode == 0 for x in results):
        message("[УСПЕХ] имя и email установлены из файла: {file}")
    else:
        message("[ОШИБКА] имя и email НЕ НАЙДЕНЫ в файле: {file}")


def message(text: str):
    print(
        ("{pref}: " + text).format(pref=GIT_LOG_PREFIX, file=GIT_CONFIG_TRANSFER_PATH)
    )


try:
    with open(GIT_CONFIG_TRANSFER_PATH, encoding="utf-8") as f:
        git_host_data = f.read()

    if git_host_data:
        parse_file_and_set_config(git_host_data)

except FileNotFoundError:
    message("[ОШИБКА] не найден файл: {file}")
except Exception as ex:
    pass  # пока и другие ошибки тут не важны
