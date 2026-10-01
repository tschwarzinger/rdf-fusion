#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_SVG="${SCRIPT_DIR}/logo.svg"
LARGE_PNG="${SCRIPT_DIR}/logo.png"
THUMB_PNG="${SCRIPT_DIR}/logo-thumbnail.png"
WEB_LARGE_PNG="${SCRIPT_DIR}/../pages/static/images/birdie.png"
WEB_THUMB_PNG="${SCRIPT_DIR}/../pages/static/favicon.png"

resvg -w 1024 "$INPUT_SVG" "$LARGE_PNG"
resvg -w 128 "$INPUT_SVG" "$THUMB_PNG"
resvg -w 128 "$INPUT_SVG" "$WEB_LARGE_PNG"
resvg -w 128 "$INPUT_SVG" "$WEB_THUMB_PNG"
