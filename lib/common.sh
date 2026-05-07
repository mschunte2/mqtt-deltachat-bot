#!/bin/bash
# Shared shell helpers for mqtt-bot scripts. Sourced (not executed) by
# start-mqtt-bot.sh, init-from-backup.sh, setup-mosquitto.sh.
#
# Contract: caller must `cd "$(dirname "$0")"` before sourcing so that
# relative paths (`./.env/env`, `./venv/bin/activate`) resolve correctly.

# Load environment from .env/env into the current shell, exported.
load_env() {
    local env_file=".env/env"
    if [ ! -f "$env_file" ]; then
        echo "Missing $env_file. Copy ./env.example to .env/env and edit." >&2
        exit 1
    fi
    set -a
    # shellcheck disable=SC1090
    source "$env_file"
    set +a
}

# Activate the project venv, creating it on first use.
activate_venv() {
    if [ ! -d ./venv ]; then
        echo "venv not found; creating ./venv ..." >&2
        python3 -m venv ./venv
        ./venv/bin/pip -q install --upgrade pip
        ./venv/bin/pip -q install deltachat2 deltabot-cli deltachat-rpc-server 'paho-mqtt>=2.0' appdirs
    fi
    # shellcheck disable=SC1091
    source ./venv/bin/activate
}
