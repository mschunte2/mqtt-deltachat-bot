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

WORKING_DIR=$(pwd)
RUN_USER="${SUDO_USER:-$USER}"
DESCRIPTION="${BOT_NAME}"
UNIT_NAME="deltabot-${BOT_NAME}.service"
DEST="/etc/systemd/system/${UNIT_NAME}"
TEMPLATE="systemd-unit/deltabot.service.template"

if [ ! -f "$TEMPLATE" ]; then
    echo "missing template: $TEMPLATE" >&2
    exit 1
fi

# Confirm overwrite if dest exists.
if [ -f "$DEST" ] && [ "$YES" -eq 0 ]; then
    read -rp "$DEST exists. Overwrite? [y/N] " ans
    case "$ans" in [yY]*) ;; *) echo "aborted." >&2; exit 1 ;; esac
fi

# Render placeholders.
sed -e "s|@DESCRIPTION@|${DESCRIPTION}|g" \
    -e "s|@USER@|${RUN_USER}|g" \
    -e "s|@WORKING_DIR@|${WORKING_DIR}|g" \
    "$TEMPLATE" > "$DEST"
chmod 0644 "$DEST"
echo "wrote $DEST"

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
