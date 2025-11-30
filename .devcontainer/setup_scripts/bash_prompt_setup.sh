#!/usr/bin/env bash

# Тут есть блоки кода для zsh, но эта оболочка не установлена в контейнер. можно добавить при желании

set -e

USERNAME="$1"

# afdev bash and OMZ themes - partly inspired by https://github.com/ohmyzsh/ohmyzsh/blob/master/themes/robbyrussell.zsh-theme
afdev_bash="$(cat \
<<'EOF'

# afdev_bash bash prompt theme
source /tmp/git-prompt.sh

__bash_prompt() {
    local userpart='`export XIT=$? \
        && echo -n "\[\033[0;32m\]\u " \
        && [ "$XIT" -ne "0" ] && echo -n "\[\033[1;31m\]➜" || echo -n "\[\033[0m\]➜"`'
    local gitbranch='`\
        if [ "$(git config --get afdev_bash-theme.hide-status 2>/dev/null)" != 1 ]; then \
            export BRANCH=$(__git_ps1 "%s"); \
            if [ "${BRANCH}" != "" ]; then \
                echo -n "\[\033[0;36m\](\[\033[1;31m\]${BRANCH}" \
                && if git ls-files --others --exclude-standard --directory --no-empty-directory --error-unmatch \
                    -- ':/*' >/dev/null 2>/dev/null; then \
                        echo -n " \[\033[1;33m\]✗"; \
                fi \
                && echo -n "\[\033[0;36m\]) "; \
            fi; \
        fi`'
    local lightblue='\[\033[1;34m\]'
    local removecolor='\[\033[0m\]'
    PS1="${userpart} ${lightblue}\w ${gitbranch}${removecolor}\$ "
    unset -f __bash_prompt
}
__bash_prompt

EOF
)"

afdev_zsh="$(cat \
<<'EOF'
# Afdev zsh prompt theme
__zsh_prompt() {
    local prompt_username
    prompt_username="%n"
    PROMPT="%{$fg[green]%}${prompt_username} %(?:%{$reset_color%}➜ :%{$fg_bold[red]%}➜ )" # User/exit code arrow
    PROMPT+='%{$fg_bold[blue]%}%(5~|%-1~/…/%3~|%4~)%{$reset_color%} ' # cwd
    PROMPT+='$([ "$(git config --get afdev-theme.hide-status 2>/dev/null)" != 1 ] && git_prompt_info)' # Git status
    PROMPT+='%{$fg[white]%}$ %{$reset_color%}'
    unset -f __zsh_prompt
}
ZSH_THEME_GIT_PROMPT_PREFIX="%{$fg_bold[cyan]%}(%{$fg_bold[red]%}"
ZSH_THEME_GIT_PROMPT_SUFFIX="%{$reset_color%} "
ZSH_THEME_GIT_PROMPT_DIRTY=" %{$fg_bold[yellow]%}✗%{$fg_bold[cyan]%})"
ZSH_THEME_GIT_PROMPT_CLEAN="%{$fg_bold[cyan]%})"
__zsh_prompt

EOF
)"


# ** Shell customization section **
if [ "${USERNAME}" = "root" ]; then 
    user_rc_path="/root"
else
    user_rc_path="/home/${USERNAME}"
fi

# Add RC snippet and custom bash prompt
if [ "${RC_SNIPPET_ALREADY_ADDED}" != "true" ]; then
    echo "${afdev_bash}" >> "${user_rc_path}/.bashrc"
    echo 'export PROMPT_DIRTRIM=4' >> "${user_rc_path}/.bashrc"
    if [ "${USERNAME}" != "root" ]; then
        echo "${afdev_bash}" >> "/root/.bashrc"
        echo 'export PROMPT_DIRTRIM=4' >> "/root/.bashrc"
    fi
    chown ${USERNAME}:0 "${user_rc_path}/.bashrc"
    RC_SNIPPET_ALREADY_ADDED="true"
fi

# Optionally install and configure zsh and Oh My Zsh!
if [ "${INSTALL_ZSH}" = "true" ]; then
    if ! type zsh > /dev/null 2>&1; then
        apt_get_update_if_needed
        apt-get install -y zsh
    fi
    if [ "${ZSH_ALREADY_INSTALLED}" != "true" ]; then
        ZSH_ALREADY_INSTALLED="true"
    fi

    # Adapted, simplified inline Oh My Zsh! install steps that adds, defaults to a afdev theme.
    # See https://github.com/ohmyzsh/ohmyzsh/blob/master/tools/install.sh for official script.
    oh_my_install_dir="${user_rc_path}/.oh-my-zsh"
    if [ ! -d "${oh_my_install_dir}" ] && [ "${INSTALL_OH_MYS}" = "true" ]; then
        template_path="${oh_my_install_dir}/templates/zshrc.zsh-template"
        user_rc_file="${user_rc_path}/.zshrc"
        umask g-w,o-w
        mkdir -p ${oh_my_install_dir}
        git clone --depth=1 \
            -c core.eol=lf \
            -c core.autocrlf=false \
            -c fsck.zeroPaddedFilemode=ignore \
            -c fetch.fsck.zeroPaddedFilemode=ignore \
            -c receive.fsck.zeroPaddedFilemode=ignore \
            "https://github.com/ohmyzsh/ohmyzsh" "${oh_my_install_dir}" 2>&1
        echo -e "$(cat "${template_path}")\nDISABLE_AUTO_UPDATE=true\nDISABLE_UPDATE_PROMPT=true" > ${user_rc_file}
        sed -i -e 's/ZSH_THEME=.*/ZSH_THEME="afdev"/g' ${user_rc_file}

        mkdir -p ${oh_my_install_dir}/custom/themes
        echo "${afdev_zsh}" > "${oh_my_install_dir}/custom/themes/afdev.zsh-theme"
        # Shrink git while still enabling updates
        cd "${oh_my_install_dir}"
        git repack -a -d -f --depth=1 --window=1
        # Copy to non-root user if one is specified
        if [ "${USERNAME}" != "root" ]; then
            cp -rf "${user_rc_file}" "${oh_my_install_dir}" /root
            chown -R ${USERNAME}:0 "${user_rc_path}"
        fi
    fi
fi
