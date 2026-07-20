#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
exec node --expose-gc "$SCRIPT_DIR/vfs-lifecycle-leak-torture.mjs" "$@"
