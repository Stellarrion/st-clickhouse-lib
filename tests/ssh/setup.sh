#!/usr/bin/env bash
set -euo pipefail

OUT="${1:-$(pwd)/tests/ssh/.generated}"
mkdir -p "$OUT"

KEY="$OUT/id_ed25519"
PUB="$KEY.pub"
USERS="$OUT/ssh_users.xml"

if [ ! -f "$KEY" ]; then
  ssh-keygen -q -t ed25519 -N "" -C "st-clickhouse-ci" -f "$KEY"
fi

read -r KEY_TYPE KEY_BASE64 _ < "$PUB"

cat > "$USERS" <<EOF
<clickhouse>
  <users>
    <ssh_user>
      <profile>default</profile>
      <quota>default</quota>
      <networks>
        <ip>::/0</ip>
      </networks>
      <ssh_keys>
        <ssh_key>
          <type>${KEY_TYPE}</type>
          <base64_key>${KEY_BASE64}</base64_key>
        </ssh_key>
      </ssh_keys>
    </ssh_user>
  </users>
</clickhouse>
EOF

echo "$KEY"
