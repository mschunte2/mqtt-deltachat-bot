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
ACL_FILE="/etc/mosquitto/mqtt-bot.acl"
CONF_FILE="/etc/mosquitto/conf.d/mqtt-bot.conf"

# Per-role credentials. Previously a single account was created and the
# plugs had to reuse the bot's, which meant any plug (or anything that
# had read a plug's stored credential) could publish to command topics
# and forge status. MQTT_DEVICE_USER/PASS are optional: set them in
# .env/env and configure the plugs with those instead.
: "${MQTT_DEVICE_USER:=}"
: "${MQTT_DEVICE_PASS:=}"

# Listener bound to all interfaces so LAN-attached Shelly plugs can reach it.
# (Localhost-only would also work if you tunnel via the plug's cloud, but the
#  whole point here is a self-hosted setup.)
NEW_CONF=$(cat <<EOF
# Managed by setup-mosquitto.sh — overwritten on re-run.
# Persistence is left to the distro's main mosquitto.conf (defaults are fine).
#
# NOTE: this listener faces the LAN, and MQTT CONNECT carries the
# username and password in cleartext on every (re)connect. On an
# untrusted network add a TLS listener (cafile/certfile/keyfile) and
# point the plugs at it. The ACL file below is what stops an
# authenticated client from driving relays directly, bypassing the
# bot's ALLOWED_CHATS gate entirely.
listener $MQTT_PORT 0.0.0.0
allow_anonymous false
password_file $PASSWD_FILE
acl_file $ACL_FILE
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
# Password on stdin, not argv: `mosquitto_passwd -b <file> <user> <pass>`
# puts the secret in /proc/*/cmdline, readable by any local user for the
# lifetime of the process.
if mosquitto_passwd --help 2>&1 | grep -q -- "-S"; then
    printf '%s' "$MQTT_PASS" | mosquitto_passwd -S "$PASSWD_FILE" "$MQTT_USER"
else
    # Older mosquitto_passwd has no stdin mode. Fall back, but say so.
    echo "note: this mosquitto_passwd has no stdin mode (-S); the password" >&2
    echo "      will be briefly visible in the process list." >&2
    mosquitto_passwd -b "$PASSWD_FILE" "$MQTT_USER" "$MQTT_PASS"
fi

# Topic ACLs. Without these, ANY client that can authenticate may
# publish to a command topic (switching a relay with no reference to
# ALLOWED_CHATS) or publish a forged status/switch:0 that drives twin
# state, chat events, samples_raw and rule evaluation — including
# tripping a real "off if idle" rule with fake zero-power readings.
#
# The bot needs to write commands and read status; a device needs the
# reverse. Topic prefixes come from devices.json.
DEVICE_PREFIXES=$(python3 - <<'PYEOF'
import json, sys
try:
    doc = json.load(open("devices.json"))
except Exception:
    sys.exit(0)
for d in doc.get("devices", []):
    p = str(d.get("topic_prefix", "")).strip().rstrip("/")
    if p:
        print(p)
PYEOF
)

{
    echo "# Managed by setup-mosquitto.sh — overwritten on re-run."
    echo ""
    echo "user $MQTT_USER"
    if [ -z "$DEVICE_PREFIXES" ]; then
        echo "# devices.json unreadable at setup time — falling back to"
        echo "# unrestricted access for the bot account. Re-run this script"
        echo "# once devices.json is in place to narrow it."
        echo "topic readwrite #"
    else
        while read -r prefix; do
            [ -n "$prefix" ] || continue
            echo "topic write ${prefix}/command/#"
            echo "topic read  ${prefix}/status/#"
            echo "topic read  ${prefix}/online"
        done <<< "$DEVICE_PREFIXES"
    fi
    if [ -n "$MQTT_DEVICE_USER" ]; then
        echo ""
        echo "user $MQTT_DEVICE_USER"
        while read -r prefix; do
            [ -n "$prefix" ] || continue
            echo "topic read  ${prefix}/command/#"
            echo "topic write ${prefix}/status/#"
            echo "topic write ${prefix}/online"
        done <<< "$DEVICE_PREFIXES"
    fi
} > "$ACL_FILE"
chown root:mosquitto "$ACL_FILE" 2>/dev/null || true
chmod 0640 "$ACL_FILE"
echo "Wrote $ACL_FILE" >&2

if [ -z "$MQTT_DEVICE_USER" ]; then
    echo "" >&2
    echo "WARNING: MQTT_DEVICE_USER is unset, so the plugs must keep using" >&2
    echo "  the bot's own credential — which the ACL grants publish rights" >&2
    echo "  on command topics. Set MQTT_DEVICE_USER/MQTT_DEVICE_PASS in" >&2
    echo "  .env/env, re-run this script, and reconfigure each plug to" >&2
    echo "  separate the two roles." >&2
fi

# Create the device account too, when configured.
if [ -n "$MQTT_DEVICE_USER" ] && [ -n "$MQTT_DEVICE_PASS" ]; then
    echo "Setting password for $MQTT_DEVICE_USER ..." >&2
    if mosquitto_passwd --help 2>&1 | grep -q -- "-S"; then
        printf '%s' "$MQTT_DEVICE_PASS" \
            | mosquitto_passwd -S "$PASSWD_FILE" "$MQTT_DEVICE_USER"
    else
        mosquitto_passwd -b "$PASSWD_FILE" "$MQTT_DEVICE_USER" "$MQTT_DEVICE_PASS"
    fi
fi

systemctl enable --now mosquitto >/dev/null
systemctl restart mosquitto

echo "Mosquitto ready on port $MQTT_PORT for user $MQTT_USER." >&2
echo "Quick check:" >&2
echo "  mosquitto_pub -h 127.0.0.1 -p $MQTT_PORT -u $MQTT_USER -P '<pass>' -t test -m hi" >&2
echo "  mosquitto_sub -h 127.0.0.1 -p $MQTT_PORT -u $MQTT_USER -P '<pass>' -t test" >&2
