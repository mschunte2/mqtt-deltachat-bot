#!/bin/bash
# Render systemd-unit/deltabot.service.template into /etc/systemd/system/
# and enable+start it. Bot identity (BOT_NAME) is read from .env/env so
# multiple bot instances on one host get distinct unit names.
#
# Usage:  sudo ./install-systemd-unit.sh
#         sudo ./install-systemd-unit.sh -y     # non-interactive (no prompts)

set -e
cd "$(dirname "$0")"

if [ "$EUID" -ne 0 ]; then
    echo "Run as root: sudo $0" >&2
    exit 1
fi

YES=0
[ "${1:-}" = "-y" ] && YES=1

# Load .env/env to get BOT_NAME without touching env-file format.
# shellcheck disable=SC1091
source ./lib/common.sh
load_env

: "${BOT_NAME:?BOT_NAME must be set in .env/env}"

# BOT_NAME comes from an operator-written env file and is interpolated
# into a unit PATH under /etc/systemd/system and into sed replacement
# text below. A `/` would write outside the intended directory; `|`,
# `&` or a newline would corrupt or inject directives into a root-owned
# unit that systemd then starts. Device names are already constrained
# by config.NAME_RE; hold BOT_NAME to the same standard.
if ! printf '%s' "$BOT_NAME" | grep -Eq '^[A-Za-z0-9][A-Za-z0-9_-]{0,31}$'; then
    echo "BOT_NAME=${BOT_NAME@Q} is not a safe unit name." >&2
    echo "Use letters, digits, underscore and dash only (max 32)." >&2
    exit 1
fi

WORKING_DIR=$(pwd)
RUN_USER="${SUDO_USER:-$USER}"
DESCRIPTION="${BOT_NAME}"
UNIT_NAME="deltabot-${BOT_NAME}.service"
UNIT_BASENAME="deltabot-${BOT_NAME}"
DEST="/etc/systemd/system/${UNIT_NAME}"
TEMPLATE="systemd-unit/deltabot.service.template"

# The unit's ReadWritePaths must cover the bot's state directory, which
# appdirs resolves to ~/.config/<BOT_NAME> for the RUN_USER.
RUN_HOME=$(getent passwd "$RUN_USER" | cut -d: -f6)
: "${RUN_HOME:?could not resolve home directory for ${RUN_USER}}"
STATE_DIR="${RUN_HOME}/.config/${BOT_NAME}"

if [ ! -f "$TEMPLATE" ]; then
    echo "missing template: $TEMPLATE" >&2
    exit 1
fi

# Confirm overwrite if dest exists.
if [ -f "$DEST" ] && [ "$YES" -eq 0 ]; then
    read -rp "$DEST exists. Overwrite? [y/N] " ans
    case "$ans" in [yY]*) ;; *) echo "aborted." >&2; exit 1 ;; esac
fi

# Render placeholders. Values are escaped for sed's replacement side
# (`|` is the delimiter; `&` and `\` are special there) so a path
# containing any of them can't corrupt the rendered unit.
sed_escape() { printf '%s' "$1" | sed -e 's/[\\&|]/\\&/g'; }

sed -e "s|@DESCRIPTION@|$(sed_escape "$DESCRIPTION")|g" \
    -e "s|@USER@|$(sed_escape "$RUN_USER")|g" \
    -e "s|@WORKING_DIR@|$(sed_escape "$WORKING_DIR")|g" \
    -e "s|@STATE_DIR@|$(sed_escape "$STATE_DIR")|g" \
    -e "s|@UNIT_BASENAME@|$(sed_escape "$UNIT_BASENAME")|g" \
    "$TEMPLATE" > "$DEST"
chmod 0644 "$DEST"

# Fail loudly rather than installing a half-rendered unit.
if grep -q '@[A-Z_]*@' "$DEST"; then
    echo "unsubstituted placeholder(s) left in $DEST:" >&2
    grep -o '@[A-Z_]*@' "$DEST" | sort -u >&2
    rm -f "$DEST"
    exit 1
fi
echo "wrote $DEST"

# Hardening directives are worthless if the unit doesn't parse. Catch
# a typo here rather than after enable.
if ! systemd-analyze verify "$DEST" 2>/dev/null; then
    echo "warning: systemd-analyze reported issues with $DEST" >&2
fi

# Make sure the runner is executable + venv exists. This is the spot
# where install-time fails are friendliest to the user — surface them
# now rather than in journalctl after enable.
if [ ! -x "${WORKING_DIR}/start-mqtt-bot.sh" ]; then
    chmod +x "${WORKING_DIR}/start-mqtt-bot.sh"
fi

# Quick config validation as the run user (not root) so file perms match prod.
sudo -u "${RUN_USER}" bash -c "cd '${WORKING_DIR}' && \
    test -f .env/env && test -f devices.json" || {
    echo "warning: .env/env or devices.json missing — bot will refuse to start" >&2
}

systemctl daemon-reload
systemctl enable "${UNIT_NAME}" >/dev/null
systemctl restart "${UNIT_NAME}"

echo ""
echo "${UNIT_NAME} enabled and started."
echo "  status: systemctl status ${UNIT_NAME}"
echo "  logs:   journalctl -u ${UNIT_NAME} -f"
