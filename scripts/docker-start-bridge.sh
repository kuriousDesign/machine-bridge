#!/usr/bin/env bash

set -euo pipefail

target_uid="$(printenv UID 2>/dev/null || true)"
target_gid="$(printenv GID 2>/dev/null || true)"
worktree_dir="/workspaces/machine-bridge"
sdk_dir="/workspaces/machine-sdk"
node_modules_dir="${worktree_dir}/node_modules"
deps_stamp_file="${node_modules_dir}/.deps-hash"

if [[ -z "$target_uid" ]]; then
    target_uid="$(id -u node)"
fi

if [[ -z "$target_gid" ]]; then
    target_gid="$(id -g node)"
fi

install -d -m 775 "$node_modules_dir"
chown -R "${target_uid}:${target_gid}" "$node_modules_dir"

sdk_hash_input() {
    local target_dir="$1"

    if [[ ! -d "$target_dir" ]]; then
        return
    fi

    find "$target_dir" -type f -print0 \
        | sort -z \
        | xargs -0 sha256sum
}

current_hash="$({
    sha256sum "$worktree_dir/package.json" "$worktree_dir/package-lock.json" "$sdk_dir/package.json"
    sdk_hash_input "$sdk_dir/src"
    sdk_hash_input "$sdk_dir/scripts"
    sdk_hash_input "$sdk_dir/styles"
} | sha256sum | cut -d ' ' -f1)"

saved_hash="$(cat "$deps_stamp_file" 2>/dev/null || true)"

exec env HOME=/home/node setpriv \
    --reuid="$target_uid" \
    --regid="$target_gid" \
    --init-groups \
    sh -lc "cd '$sdk_dir' && npm run build && cd '$worktree_dir' && if [[ ! -d node_modules/@kuriousdesign/machine-sdk ]] || [[ '$current_hash' != '$saved_hash' ]]; then npm install && printf '%s\n' '$current_hash' > '$deps_stamp_file'; fi && npm run build && npm start"

# exec env HOME=/home/node setpriv \
#     --reuid="$target_uid" \
#     --regid="$target_gid" \
#     --init-groups \
#     sh -lc "cd '$worktree_dir' && npm install && npm --prefix '$sdk_dir' run build && npm run build && npm start"