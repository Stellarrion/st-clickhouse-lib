#!/usr/bin/env bash
# Generate self-signed CA + server cert for ClickHouse TLS testing.
# Creates: ca.crt, server.crt, server.key, client.crt, client.key
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
OUT="${1:-$DIR/certs}"
mkdir -p "$OUT"

# ── CA ────────────────────────────────────────────────────────
openssl req -x509 -nodes -new -sha256 \
  -days 365 \
  -newkey rsa:2048 \
  -keyout "$OUT/ca.key" \
  -out "$OUT/ca.crt" \
  -subj "/CN=ClickHouse Test CA" \
  2>/dev/null

# ── Server cert ───────────────────────────────────────────────
openssl req -nodes -new -sha256 \
  -newkey rsa:2048 \
  -keyout "$OUT/server.key" \
  -out "$OUT/server.csr" \
  -subj "/CN=clickhouse.local" \
  2>/dev/null

cat > "$OUT/server.ext" <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage=digitalSignature,nonRepudiation,keyEncipherment,dataEncipherment
subjectAltName=@alt_names
[alt_names]
DNS.1=clickhouse.local
DNS.2=localhost
IP.1=127.0.0.1
EOF

openssl x509 -req -sha256 \
  -days 365 \
  -in "$OUT/server.csr" \
  -CA "$OUT/ca.crt" \
  -CAkey "$OUT/ca.key" \
  -CAcreateserial \
  -extfile "$OUT/server.ext" \
  -out "$OUT/server.crt" \
  2>/dev/null

# ── Client cert ───────────────────────────────────────────────
openssl req -nodes -new -sha256 \
  -newkey rsa:2048 \
  -keyout "$OUT/client.key" \
  -out "$OUT/client.csr" \
  -subj "/CN=clickhouse-client" \
  2>/dev/null

openssl x509 -req -sha256 \
  -days 365 \
  -in "$OUT/client.csr" \
  -CA "$OUT/ca.crt" \
  -CAkey "$OUT/ca.key" \
  -CAcreateserial \
  -out "$OUT/client.crt" \
  2>/dev/null

# Clean up CSRs and ext
rm -f "$OUT/"*.csr "$OUT/"*.ext "$OUT/"*.srl
chmod 644 "$OUT/server.crt" "$OUT/server.key" "$OUT/ca.crt"

cat > "$OUT/tls.xml" <<EOF
<clickhouse>
  <tcp_port_secure>9440</tcp_port_secure>
  <openSSL>
    <server>
      <certificateFile>/etc/clickhouse-server/server.crt</certificateFile>
      <privateKeyFile>/etc/clickhouse-server/server.key</privateKeyFile>
      <verificationMode>none</verificationMode>
      <loadDefaultCAFile>false</loadDefaultCAFile>
      <cacheSessions>false</cacheSessions>
      <disableProtocols>sslv2,sslv3</disableProtocols>
      <preferServerCiphers>true</preferServerCiphers>
    </server>
  </openSSL>
</clickhouse>
EOF

echo "✅ TLS certs generated in $OUT"
echo "   ca.crt       — CA certificate (trust anchor)"
echo "   server.crt   — Server certificate"
echo "   server.key   — Server private key"
echo "   tls.xml      — ClickHouse TLS config fragment"
echo "   client.crt   — Client certificate (mTLS)"
echo "   client.key   — Client private key (mTLS)"
echo ""
echo "To start ClickHouse with TLS:"
echo "  docker run -d --name ch-tls \\"
echo "    -p 9440:9440 \\"
echo "    -v $OUT/server.crt:/etc/clickhouse-server/server.crt:ro \\"
echo "    -v $OUT/server.key:/etc/clickhouse-server/server.key:ro \\"
echo "    -v $OUT/tls.xml:/etc/clickhouse-server/config.d/tls.xml:ro \\"
echo "    clickhouse/clickhouse-server:26.4"
