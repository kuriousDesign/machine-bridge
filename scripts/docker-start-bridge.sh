#!/usr/bin/env bash

set -euo pipefail

target_uid="$(printenv UID 2>/dev/null || true)"
target_gid="$(printenv GID 2>/dev/null || true)"
worktree_dir="/workspaces/machine-bridge"
sdk_dir="/workspaces/machine-sdk"
node_modules_dir="${worktree_dir}/node_modules"

if [[ -z "$target_uid" ]]; then
    target_uid="$(id -u node)"
fi

if [[ -z "$target_gid" ]]; then
    target_gid="$(id -g node)"
fi

install -d -m 775 "$node_modules_dir"
chown -R "${target_uid}:${target_gid}" "$node_modules_dir"

exec env HOME=/home/node setpriv \
    --reuid="$target_uid" \
    --regid="$target_gid" \
    --init-groups \
    sh -lc "cd '$worktree_dir' && npm install && npm --prefix '$sdk_dir' run build && npm run build && npm start"