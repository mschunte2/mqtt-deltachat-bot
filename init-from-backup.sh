#!/bin/bash
# Initialise the bot's Delta Chat account from a backup tar in .env/.
#
# Idempotent: only runs the import if the account dir is empty AND a tar
# exists. Safe to run on every deploy.
#
# Pre-reqs:
#   - .env/env populated (BOT_NAME)
#   - exactly one *.tar in .env/  (the Delta Chat profile backup)

set -e
cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./lib/common.sh
load_env

: "${BOT_NAME:?BOT_NAME must be set in .env/env}"

ACCOUNT_DIR="$HOME/.config/$BOT_NAME"

mapfile -t TARS < <(find ./.env -maxdepth 1 -type f -name '*.tar' 2>/dev/null | sort)

if [ "${#TARS[@]}" -eq 0 ]; then
    echo "No backup tar found in .env/. Nothing to import." >&2
    exit 0
fi

if [ "${#TARS[@]}" -gt 1 ]; then
    echo "Multiple .tar files in .env/ -- refusing to guess. Keep exactly one:" >&2
    printf '  %s\n' "${TARS[@]}" >&2
    exit 1
fi

BACKUP="${TARS[0]}"

if [ -d "$ACCOUNT_DIR" ] && [ -n "$(ls -A "$ACCOUNT_DIR" 2>/dev/null)" ]; then
    echo "Account dir $ACCOUNT_DIR already populated; skipping import." >&2
    exit 0
fi

echo "Importing $BACKUP into $ACCOUNT_DIR ..." >&2
activate_venv
mkdir -p "$ACCOUNT_DIR"
python3 -c "
import sys
from deltachat_rpc_server import deltachat_rpc_server  # noqa: F401
from deltachat2 import Bot, IOTransport, Rpc

with Rpc(accounts_dir=r'''$ACCOUNT_DIR''') as rpc:
    accid = rpc.add_account()
    rpc.import_backup(accid, r'''$BACKUP''', None)
    print('imported account id', accid, file=sys.stderr)
"
echo "Done. Move or remove $BACKUP from .env/ if you do not want to retry on next deploy." >&2
