#!/bin/bash
# Install and configure Mosquitto for mqtt-bot use.
#
# - apt installs mosquitto + mosquitto-clients (idempotent)
# - drops /etc/mosquitto/conf.d/mqtt-bot.conf binding to 127.0.0.1 with
#   password auth (re-runs cleanly; only writes if content differs)
# - creates the bot user via mosquitto_passwd
# - enables and (re)starts the mosquitto service
#
# Usage:
#   sudo ./setup-mosquitto.sh
#
# Reads MQTT_USER, MQTT_PASS, MQTT_PORT from .env/env. The Plug M Gen 3
# connects to this broker over the LAN, so the listener is reachable
# from the LAN interface in addition to localhost — see LAN_LISTENER.

set -e
cd "$(dirname "$0")"

if [ "$EUID" -ne 0 ]; then
    echo "Run as root: sudo $0" >&2
    exit 1
fi

# shellcheck disable=SC1091
source ./lib/common.sh
load_env

: "${MQTT_USER:?MQTT_USER must be set in .env/env}"
: "${MQTT_PASS:?MQTT_PASS must be set in .env/env}"
: "${MQTT_PORT:=1883}"

if ! command -v mosquitto >/dev/null 2>&1; then
    echo "Installing mosquitto + mosquitto-clients ..." >&2
    apt-get update -q
    apt-get install -y mosquitto mosquitto-clients
fi

PASSWD_FILE="/etc/mosquitto/mqtt-bot.passwd"
CONF_FILE="/etc/mosquitto/conf.d/mqtt-bot.conf"

# Listener bound to all interfaces so LAN-attached Shelly plugs can reach it.
# (Localhost-only would also work if you tunnel via the plug's cloud, but the
#  whole point here is a self-hosted setup.)
NEW_CONF=$(cat <<EOF
# Managed by setup-mosquitto.sh — overwritten on re-run.
# Persistence is left to the distro's main mosquitto.conf (defaults are fine).
listener $MQTT_PORT 0.0.0.0
allow_anonymous false
password_file $PASSWD_FILE
EOF
)

if [ ! -f "$CONF_FILE" ] || [ "$(cat "$CONF_FILE")" != "$NEW_CONF" ]; then
    echo "Writing $CONF_FILE ..." >&2
    printf '%s\n' "$NEW_CONF" > "$CONF_FILE"
    chmod 0644 "$CONF_FILE"
fi

# Create or update the bot user.
if [ ! -f "$PASSWD_FILE" ]; then
    touch "$PASSWD_FILE"
    chown mosquitto:mosquitto "$PASSWD_FILE" 2>/dev/null || true
    chmod 0600 "$PASSWD_FILE"
fi
echo "Setting password for $MQTT_USER in $PASSWD_FILE ..." >&2
mosquitto_passwd -b "$PASSWD_FILE" "$MQTT_USER" "$MQTT_PASS"

systemctl enable --now mosquitto >/dev/null
systemctl restart mosquitto

echo "Mosquitto ready on port $MQTT_PORT for user $MQTT_USER." >&2
echo "Quick check:" >&2
echo "  mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -u $MQTT_USER -P '<pass>' -t test -m hi" >&2
echo "  mosquitto_sub -h 127.0.0.1 -p $MQTT_PORT -u $MQTT_USER -P '<pass>' -t test" >&2
