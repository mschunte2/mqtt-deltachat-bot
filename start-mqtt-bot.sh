#!/bin/bash
# Start the mqtt-bot service. Loads .env/env so child processes inherit
# secrets/config (bot.py reads MQTT_*, ALLOWED_CHATS, etc. from os.environ).
set -e
cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./lib/common.sh
load_env
activate_venv
exec python3 -m bot serve --logging "${LOG_LEVEL:-info}"
