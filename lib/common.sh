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
    # .env/env holds MQTT_PASS, and .env/ may also hold a Delta Chat
    # profile backup — the bot's entire identity. Both were
    # group/world-readable on the live host (0664 / 0644), so any other
    # local account could read them. Tighten on every load rather than
    # only at setup time, so a fresh `git clone` + editor round-trip
    # can't quietly widen them again.
    chmod 0700 .env 2>/dev/null || true
    chmod 0600 "$env_file" 2>/dev/null || true
    find .env -maxdepth 1 -type f -name '*.tar' -exec chmod 0600 {} + 2>/dev/null || true

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
