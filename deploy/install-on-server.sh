#!/usr/bin/env bash
# Run as root on the relay host (e.g. after: ssh root@relay2.seidenwege.com).
# Installs orbitdb-relay under systemd with Kubo-disjoint ports 28190–28193.
set -euo pipefail

NODE_MIN_MAJOR=22
INSTALL_ROOT=/opt/orbitdb-relay
DATA_DIR=/var/lib/orbitdb-relay
ENV_FILE=/etc/default/orbitdb-relay
UNIT_DST=/etc/systemd/system/orbitdb-relay.service

die() { echo "error: $*" >&2; exit 1; }

[[ "$(id -u)" -eq 0 ]] || die "run as root"

if ! command -v node >/dev/null 2>&1; then
  die "Node.js not found; install Node ${NODE_MIN_MAJOR}+ first (e.g. NodeSource or distro packages)"
fi

major="$(node -p "process.versions.node.split('.')[0]" 2>/dev/null || echo 0)"
[[ "${major}" -ge "${NODE_MIN_MAJOR}" ]] || die "Node >= ${NODE_MIN_MAJOR} required, found $(node --version 2>/dev/null || echo unknown)"

if ! command -v npm >/dev/null 2>&1; then
  die "npm not found; install npm alongside Node"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -f "${SCRIPT_DIR}/orbitdb-relay.service" ]] || die "missing ${SCRIPT_DIR}/orbitdb-relay.service (run from repo clone)"

mkdir -p "${INSTALL_ROOT}" "${DATA_DIR}"

if ! id orbitdb-relay &>/dev/null; then
  useradd --system --home "${DATA_DIR}" --create-home --shell /usr/sbin/nologin orbitdb-relay
fi
chown -R orbitdb-relay:orbitdb-relay "${DATA_DIR}"

tee "${INSTALL_ROOT}/package.json" >/dev/null <<'EOF'
{
  "private": true,
  "type": "module",
  "dependencies": {
    "orbitdb-relay": "^0.9.1"
  }
}
EOF
chown -R orbitdb-relay:orbitdb-relay "${INSTALL_ROOT}"
if command -v runuser >/dev/null 2>&1; then
  runuser -u orbitdb-relay -- env HOME="${DATA_DIR}" bash -lc "cd '${INSTALL_ROOT}' && npm install --omit=dev"
elif command -v sudo >/dev/null 2>&1; then
  sudo -u orbitdb-relay env HOME="${DATA_DIR}" bash -lc "cd '${INSTALL_ROOT}' && npm install --omit=dev"
else
  die "need runuser or sudo to run npm install as orbitdb-relay"
fi

PUBLIC_IP=""
if command -v curl >/dev/null 2>&1; then
  PUBLIC_IP="$(curl -4 -fsS --max-time 5 https://ifconfig.me/ip 2>/dev/null || true)"
fi
if [[ -z "${PUBLIC_IP}" ]] && command -v dig >/dev/null 2>&1; then
  # Best-effort: query OpenDNS for "myip" (may not work on all networks)
  PUBLIC_IP="$(dig +short myip.opendns.com @resolver1.opendns.com 2>/dev/null | head -1 || true)"
fi

if [[ -f "${ENV_FILE}" ]]; then
  echo "keeping existing ${ENV_FILE} (remove it first to regenerate)"
else
  cp "${SCRIPT_DIR}/orbitdb-relay.env.example" "${ENV_FILE}"
  chmod 640 "${ENV_FILE}"
  chown root:orbitdb-relay "${ENV_FILE}"
  if [[ -n "${PUBLIC_IP}" ]]; then
    sed -i.bak "s|^# VITE_APPEND_ANNOUNCE=.*|VITE_APPEND_ANNOUNCE=/ip4/${PUBLIC_IP}/tcp/28191,/ip4/${PUBLIC_IP}/tcp/28192/ws,/ip4/${PUBLIC_IP}/udp/28193/webrtc-direct|" "${ENV_FILE}" || true
    rm -f "${ENV_FILE}.bak"
    echo "set VITE_APPEND_ANNOUNCE using detected IPv4: ${PUBLIC_IP} — verify in ${ENV_FILE}"
  else
    echo "could not detect public IPv4; edit VITE_APPEND_ANNOUNCE in ${ENV_FILE}"
  fi
fi

cp "${SCRIPT_DIR}/orbitdb-relay.service" "${UNIT_DST}"
systemctl daemon-reload
systemctl enable orbitdb-relay
systemctl restart orbitdb-relay

echo ""
echo "Done. Check: systemctl status orbitdb-relay"
echo "Logs: journalctl -u orbitdb-relay -f"
echo "Multiaddrs: curl -sS http://127.0.0.1:28190/multiaddrs | jq ."
