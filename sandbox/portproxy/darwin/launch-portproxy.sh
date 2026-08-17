#!/bin/sh
set -eu

RUNTIME_ROOT="/Library/Application Support/Chevalier"
ENV_FILE="$RUNTIME_ROOT/etc/portproxy.env"

if [ -f "$ENV_FILE" ]; then
  set -a
  . "$ENV_FILE"
  set +a
fi

exec "$RUNTIME_ROOT/bin/portproxy" \
  --server \
  --rpc-bind-address 127.0.0.1 \
  --rpc-port 13338 \
  --server-addr 127.0.0.1:13337
