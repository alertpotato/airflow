"""
Удалить все слитые ветки (merged) во всех репозиториях этого контейнера
Репы: основная, SQL, DQ (задаются в скрипте).
Переключает во всех репах на основную ветку (например master), удаляет все ветки, которые уже были слиты в основную.
"""

import logging
import sys
from os import chdir, environ
from subprocess import DEVNULL, check_call, getoutput

ENVS = [
    'SQL_REPO_PATH',
    'DQ_REPO_PATH'
]

REPOS = []

GET_MAIN_BRANCH = "git remote | xargs git remote show | sed -n '/HEAD branch/s/.*: //p'"


if len(sys.argv) > 1 and (arg := sys.argv[1]):
    REPOS.append(arg)
else:
    raise ValueError("Скрипту должен передаваться путь к основной репе")

for env in ENVS:
    if p := environ.get(env):
        REPOS.append(p)
    else:
        logging.warning("Не найден путь репозитория для '%s'", env)


def process_all_repos():
    for repo in REPOS:
        print(f"\n\tрепозиторий: '{repo}' ...", end='')
        chdir(repo)
        main_branch = getoutput(GET_MAIN_BRANCH)
        current = getoutput('git branch --show-current')

        if not current == main_branch:
            checkout_result = getoutput(f"git checkout {main_branch}")
            if f"Switched to branch '{main_branch}'" not in checkout_result:
                raise ValueError(f"Неожиданный ответ: {checkout_result}")

            check_call("git pull --rebase", shell=True, stdout=DEVNULL)

        merged_list = getoutput("git branch --merged").splitlines()
        if len(merged_list) == 1:
            print(' нет веток для удаления.')
            continue
        elif len(merged_list) == 0:
            raise ValueError("Что-то пошло не так...")
        else:
            merged_list = [x.strip() for x in merged_list if x != f"* {main_branch}"]
            print(f" ветки к удалению: {merged_list}")

        for branch in merged_list:
            check_call(f"git branch -d {branch}", shell=True)


print('\n'.join([
    "Запуск очистки слитых веток для следующий репозиториев:",
    *REPOS
]))

process_all_repos()

print("Очистка завершена.")
