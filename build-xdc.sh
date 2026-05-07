#!/bin/bash
# Package each device class's webxdc app as <class>.xdc.
#
# Usage:
#   ./build-xdc.sh                       # build every devices/*/app
#   ./build-xdc.sh devices/shelly_plug   # build one class
#
# Layout per class:
#   devices/<class>/app/                 # source (HTML/JS/CSS + manifest.toml)
#   devices/<class>/<class>.xdc          # output (gitignored)
#
# Plain zip — a .xdc is just a zip with index.html + manifest at the root.

set -euo pipefail

build_one() {
    local class_dir="${1%/}"
    local class_name app_src out
    class_name=$(basename "$class_dir")
    app_src="$class_dir/app"
    out="$class_dir/$class_name.xdc"

    if [[ ! -d "$app_src" ]]; then
        echo "  $class_name: no app/ dir, skipping" >&2
        return 0
    fi
    for f in index.html main.js style.css manifest.toml; do
        if [[ ! -f "$app_src/$f" ]]; then
            echo "  $class_name: missing $f in $app_src, skipping" >&2
            return 0
        fi
    done

    local stage
    stage=$(mktemp -d)
    trap 'rm -rf "$stage"' RETURN

    cp "$app_src/"*.{html,js,css,toml,svg,png} "$stage/" 2>/dev/null || true
    if [[ -d "$app_src/public" ]]; then
        cp -r "$app_src/public/." "$stage/"
    fi

    rm -f "$out"
    # Resolve the dest to an absolute path BEFORE we cd into the stage; -m
    # tolerates the file not existing yet (we're about to create it).
    local out_abs
    out_abs=$(realpath -m "$out")
    (cd "$stage" && zip -qrX "$out_abs" .)
    echo "  $class_name -> $out ($(stat -c%s "$out") bytes)"
}

if [[ $# -eq 1 ]]; then
    build_one "$1"
else
    cd "$(dirname "$0")"
    for d in devices/*/; do
        build_one "$d"
    done
fi
