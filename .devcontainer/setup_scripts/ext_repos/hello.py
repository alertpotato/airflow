script_path = ".devcontainer/setup_scripts/ext_repos/get.py"

msg = """
{line}
Подключение репозиториев SQL и Data Quality.

Для выполнения всех действий в автоматическом режиме -
выполнить следующую строку в новом терминале контейнера:
python {path} --auto
{line}

Вывести команды для ручного режима:
python {path} --manual
""".format(line="*" * 50, path=script_path)

print(msg)
