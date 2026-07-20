#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
exec node "$repo_root/sandbox/scripts/vfs-virtiofs-git-ref-stress.mjs" "$@"
