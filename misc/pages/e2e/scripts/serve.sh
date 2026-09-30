#!/usr/bin/env bash
# Build the frontend assets and start a local static server for Playwright E2E.
#
# The playground is served at http://127.0.0.1:${E2E_PORT}/playground/ with the
# Hugo baseURL overridden to this server's origin (no /rdf-fusion sub-path).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PAGES_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PORT="${E2E_PORT:-8089}"

# 1. Compile Svelte/Bootstrap/FontAwesome into static/generated/
npm run build --prefix "${PAGES_DIR}"

# 2. Build the static site with a local baseURL (root-relative asset paths).
hugo --minify --source "${PAGES_DIR}" --baseURL "http://127.0.0.1:${PORT}/"

# 3. Serve the generated site.
cd "${PAGES_DIR}"
node e2e/scripts/static-server.mjs public "${PORT}"
