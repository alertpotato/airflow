import argparse
import os
import re
from subprocess import getoutput, run

REPOS = {
    "SQL_REPO_PATH": "oms-management-services/etl/sql.git",
    "DQ_REPO_PATH": "oms-management-services/etl/data-quality.git",
}
""" Относительные пути репозиториев и переменные окружения """

CODE_WORKSPACE_FILE = ".vscode/airflow.code-workspace"
CODE_OPEN_WORKSPACE_CMD = ["code", "-r", CODE_WORKSPACE_FILE]

_repos_names = "SQL и Data Quality"
LOG_PREFIX = f"Подключение репозиториев {_repos_names}:"

MESSAGE_TEMPLATE = """
{line}

{pref}
{{text}}

{line}
""".format(line="*" * 50, pref=LOG_PREFIX)


def print_help(cmds: list[list]):
    cmds.append(CODE_OPEN_WORKSPACE_CMD)
    cmds_for_print = "\n".join([" ".join(x) for x in cmds])
    print(MESSAGE_TEMPLATE.format(text="\n" + cmds_for_print))


def exec_it(cmds: list[list], is_script=False):
    af_home = os.environ.get('AIRFLOW_HOME')
    ext_repos = f"{af_home}/_ext_repos"
    if not af_home or not os.path.isdir(af_home):
        raise KeyError("Не найден 'AIRFLOW_HOME'")
    if os.path.isdir(ext_repos):
        if os.listdir(ext_repos):
            raise FileExistsError(f"Директория '{ext_repos}' не пуста!")

    if not is_script:
        cmds_for_print = "\n".join(
            [" ".join(x) for x in [*cmds, CODE_OPEN_WORKSPACE_CMD]]
        )

        print(
            MESSAGE_TEMPLATE.format(
                text=f"Будут выполнены следующие команды:\n\n{cmds_for_print}"
            )
        )

        try:
            input("Нажмите любую клавишу для продолжения или CTRL+C для отмены...")
        except KeyboardInterrupt:
            print("\t(выполнение отменено)")
            exit()

    for cmd in cmds:
        run(cmd, check=True)

    # Если check не вызвал ошибку:
    print("[УСПЕХ], открываем workspace...")
    run(CODE_OPEN_WORKSPACE_CMD)


parser = argparse.ArgumentParser(
    description=f"Скрипт формирует команды для клонирования репозиториев {_repos_names}, "
    "после чего выводит их в консоль или сразу же выполняет. "
    "Так же добавляется команда открытия workspace для VS Code"
)
# fmt: off
parser.add_argument("--auto", help="Выполнить команды сразу же", action="store_true")
parser.add_argument("--manual", help="Вывести команды в консоль (без выполнения)", action="store_true")
parser.add_argument("--script", help="Не спрашивать подтверждения при --auto", action="store_true")
# fmt: on


args = parser.parse_args()
if (x := {args.auto, args.manual}) and x == {True, True} or x == {False, False}:
    parser.print_help()
    exit(0)

try:
    parsing_pattern = re.compile(r"(?P<pr>http.+):\S+:(?P<t>\w+)@(?P<h>[^\/]+)\/\S+")
    git_output = getoutput("git remote -v")
    match = parsing_pattern.search(git_output)
    if match:
        protocol = match.group("pr")
        token = match.group("t")
        host = match.group("h")

        commands = []
        for repo_name, repo_path in REPOS.items():
            env_value = os.environ[repo_name]
            git_clone_command = [
                "git",
                "clone",
                "--depth=30",
                f"{protocol}://oauth2:{token}@{host}/{repo_path}",
                env_value,
            ]
            commands.append(git_clone_command)
        if len(commands) == len(REPOS):
            if args.auto:
                exec_it(commands, args.script)
            else:
                print_help(commands)
        else:
            print(LOG_PREFIX, "[ОШИБКА] Не удалось получить команды")
    else:
        print(LOG_PREFIX, "[ОШИБКА] Не удалось получить токен", git_output)
except Exception as ex:
    print(LOG_PREFIX, "[ОШИБКА] при выполнении скрипта", __name__, ":\n", repr(ex))
