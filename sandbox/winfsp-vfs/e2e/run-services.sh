#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
sandbox_dir=$(cd -- "$script_dir/../.." && pwd)
repo_root=$(cd -- "$sandbox_dir/.." && pwd)
architecture=${OPENBRACKET_WINFSP_ARCHITECTURE:-arm64}
case "$architecture" in
  arm64)
    default_image="$sandbox_dir/windows-image/output/windows-11-iot-enterprise-ltsc-2024-arm64-services"
    ;;
  amd64)
    default_image="$sandbox_dir/windows-image/output/windows-11-enterprise-25h2-amd64-services"
    ;;
  *)
    echo "OPENBRACKET_WINFSP_ARCHITECTURE must be arm64 or amd64" >&2
    exit 1
    ;;
esac
image=${OPENBRACKET_WINFSP_IMAGE:-"$default_image"}
test_root=$(mktemp -d "${TMPDIR:-/tmp}/openbracket-winfsp-services.XXXXXX")
data_dir="$test_root/vmd"
storage_root="$test_root/storage"
status_file="$test_root/unused-result.json"
gateway_token=$(openssl rand -hex 32)
result_token=$(openssl rand -hex 32)
owner="windows-service-smoke"
scope="runs/$(openssl rand -hex 8)/workspace"
gateway_port=63359
result_port=18099
vmd_port=18062
host_pid=
vmd_pid=
success=0

cleanup() {
  if [[ -n ${vmd_pid:-} ]] && kill -0 "$vmd_pid" >/dev/null 2>&1; then
    kill "$vmd_pid" >/dev/null 2>&1 || true
    wait "$vmd_pid" >/dev/null 2>&1 || true
  fi
  if [[ -n ${host_pid:-} ]] && kill -0 "$host_pid" >/dev/null 2>&1; then
    kill "$host_pid" >/dev/null 2>&1 || true
    wait "$host_pid" >/dev/null 2>&1 || true
  fi
  if [[ $success == 1 ]]; then
    rm -rf "$test_root"
  else
    printf 'Windows service smoke diagnostics retained at %s\n' "$test_root" >&2
  fi
}
trap cleanup EXIT INT TERM

mkdir -m 700 "$data_dir" "$storage_root"
test -f "$image/image-manifest.json"

if [[ $(uname -s) == Darwin ]]; then
  (cd "$sandbox_dir" && cargo build -p vmd --no-default-features --features macos-fskit --bin vmd)
else
  (cd "$sandbox_dir" && cargo build -p vmd --bin vmd)
fi
(cd "$repo_root/ts" && npm run build:native:debug && npm run build:ts)
(cd "$repo_root/ts-sandbox" && npm run build:debug)

OPENBRACKET_WINFSP_GATEWAY_TOKEN="$gateway_token" \
OPENBRACKET_WINFSP_RESULT_TOKEN="$result_token" \
OPENBRACKET_WINFSP_STORAGE_ROOT="$storage_root" \
OPENBRACKET_WINFSP_STATUS_FILE="$status_file" \
OPENBRACKET_WINFSP_OWNER="$owner" \
OPENBRACKET_WINFSP_SCOPE="$scope" \
OPENBRACKET_WINFSP_GATEWAY_PORT="$gateway_port" \
OPENBRACKET_WINFSP_RESULT_PORT="$result_port" \
OPENBRACKET_WINFSP_HOTPATCH_ARTIFACT="${OPENBRACKET_WINFSP_HOTPATCH_ARTIFACT:-}" \
  node "$script_dir/host.mjs" >"$test_root/host.log" 2>&1 &
host_pid=$!

CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN="$gateway_token" \
  "$sandbox_dir/target/debug/vmd" \
  --listen "127.0.0.1:$vmd_port" \
  --data-dir "$data_dir" \
  --qemu-run-as-uid "$(id -u)" \
  --qemu-run-as-gid "$(id -g)" \
  --disable-node-registry \
  --disable-control-bus >"$test_root/vmd.log" 2>&1 &
vmd_pid=$!

for _ in {1..300}; do
  nc -z 127.0.0.1 "$gateway_port" >/dev/null 2>&1 && nc -z 127.0.0.1 "$vmd_port" >/dev/null 2>&1 && break
  kill -0 "$host_pid" >/dev/null 2>&1
  kill -0 "$vmd_pid" >/dev/null 2>&1
  sleep 0.1
done
nc -z 127.0.0.1 "$gateway_port"
nc -z 127.0.0.1 "$vmd_port"

OPENBRACKET_WINFSP_VMD_ENDPOINT="http://127.0.0.1:$vmd_port" \
OPENBRACKET_WINFSP_ARCHITECTURE="$architecture" \
OPENBRACKET_WINFSP_IMAGE="$image" \
OPENBRACKET_WINFSP_GATEWAY_ENDPOINT="http://127.0.0.1:$gateway_port/internal/chevalier/vfs/$owner" \
OPENBRACKET_WINFSP_SCOPE="$scope" \
OPENBRACKET_WINFSP_STORAGE_PATH="$storage_root/$scope/service-smoke.txt" \
OPENBRACKET_WINFSP_VMD_DATA_DIR="$data_dir" \
OPENBRACKET_WINFSP_ACCEPTANCE_RECEIPT="$image/service-acceptance.json" \
OPENBRACKET_WINFSP_HOTPATCH_ARTIFACT="${OPENBRACKET_WINFSP_HOTPATCH_ARTIFACT:-}" \
OPENBRACKET_WINFSP_HOTPATCH_URL="${OPENBRACKET_WINFSP_HOTPATCH_URL:-http://10.0.2.2:$result_port/hotpatch}" \
OPENBRACKET_WINFSP_HOTPATCH_TOKEN="$result_token" \
OPENBRACKET_WINFSP_PHASE_ENDPOINT="http://127.0.0.1:$result_port/phase" \
OPENBRACKET_WINFSP_PHASE_TOKEN="$result_token" \
OPENBRACKET_WINFSP_HOTPATCH_DEBUG="${OPENBRACKET_WINFSP_HOTPATCH_DEBUG:-}" \
  node "$script_dir/service-smoke.mjs"

jq -e '.phase == "success"' "$status_file" >/dev/null
test "$(cat "$storage_root/$scope/online.txt")" = "online-published"
test "$(cat "$storage_root/$scope/renamed.txt")" = "renamed-online"
test "$(cat "$storage_root/$scope/offline-renamed.txt")" = "offline-durable"
test ! -e "$storage_root/$scope/delete-me.txt"
test ! -e "$storage_root/$scope/REPO"
if find "$storage_root/$scope/repo/.git" -type f -name '*.lock' -print -quit | grep -q .; then
  echo "published Git state retained a lock file" >&2
  exit 1
fi

if [[ -z ${OPENBRACKET_WINFSP_HOTPATCH_ARTIFACT:-} ]]; then
  manifest="$image/image-manifest.json"
  jq '
    .status = "sealed-local-acceptance"
    | .proved = ((.proved + [
        "first-boot-service-registration",
        "chevalier-services-live-vm",
        "authenticated-command-execution",
        "native-file-rpc",
        "winfsp-create-write-flush-read-delete-basic-info",
        "git-init-add-commit-fsck",
        "git-lock-rename-stress",
        "guest-vfs-wal-recovery",
        "gateway-publication-drain",
        "fresh-identity-uniqueness-matrix",
        "same-node-state-disk-restart"
      ]) | unique)
    | .notProved = [.notProved[] | select(. != "first-boot-service-registration" and . != "chevalier-services-live-vm" and . != "guest-vfs-wal-recovery" and . != "gateway-publication-drain" and . != "fresh-identity-uniqueness-matrix" and . != "product-runtime")]
  ' "$manifest" >"$manifest.partial"
  mv "$manifest.partial" "$manifest"
fi

success=1
