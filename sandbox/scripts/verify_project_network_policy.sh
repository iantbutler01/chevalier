#!/usr/bin/env bash
set -euo pipefail
sandbox_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixture_dir="$(mktemp -d)"
trap 'rm -rf "$fixture_dir"' EXIT
CHEVALIER_NETWORK_FIXTURE_DIR="$fixture_dir" cargo test --manifest-path "$sandbox_root/Cargo.toml" -p vmd --lib export_project_network_fixture -- --ignored
cp "$sandbox_root/scripts/network-policy/verify.py" "$fixture_dir/verify.py"
docker build -t chevalier-network-policy-test "$sandbox_root/scripts/network-policy"
docker run --rm --cap-add NET_ADMIN -v "$fixture_dir:/fixture:ro" chevalier-network-policy-test
