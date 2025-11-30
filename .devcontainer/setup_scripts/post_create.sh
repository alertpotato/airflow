#!/usr/bin/env bash


# добавляем настройки git под vscode
# и убираем последствия некорректной автоматизации, копирующей конфиг с хоста
gitconfig() {
    cd /home/airflow

    if [ -f .gitconfig ]; then
        awk '/credential|name|email/' .gitconfig > /tmp/user.gitconfig
    fi

    sudo install -m 644 -o airflow -g root /tmp/preset.gitconfig .gitconfig

    if [ -f /tmp/user.gitconfig ]; then
        cat /tmp/user.gitconfig >> .gitconfig

    fi
}

gitconfig

cd /opt/airflow/.devcontainer/setup_scripts
python git_config.py
python ext_repos/hello.py
