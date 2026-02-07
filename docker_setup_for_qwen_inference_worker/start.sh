#!/usr/bin/env bash
#set -euo pipefail
set -u

echo "[init] starting camoufox container"

export USER="${USER:-root}"
export HOME="/root"
export PATH="/opt/venv/bin:${PATH}"

VNC_DISPLAY="${VNC_DISPLAY:-3}"
VNC_GEOMETRY="${VNC_GEOMETRY:-1920x1080}"
VNC_DEPTH="${VNC_DEPTH:-24}"
VNC_PASSWORD="${VNC_PASSWORD:-}"

SOCKS_LISTEN="${SOCKS_LISTEN:-127.0.0.1:1080}"
SOCKS_UPSTREAM="${SOCKS_UPSTREAM:-}"

export DISPLAY=":${VNC_DISPLAY}"

mkdir -p /root/.vnc

rm -f "/tmp/.X${VNC_DISPLAY}-lock" || true
rm -f "/tmp/.X11-unix/X${VNC_DISPLAY}" || true

# ---- VNC password ----
if [[ -n "${VNC_PASSWORD}" ]]; then
  printf '%s' "${VNC_PASSWORD}" | vncpasswd -f >/root/.vnc/passwd
  chmod 600 /root/.vnc/passwd
fi

# ---- START VNC ----
echo "[vnc] starting VNC server"

chmod +x /root/.vnc/xstartup || true

vncserver -kill ":${VNC_DISPLAY}" >/dev/null 2>&1 || true
vncserver ":${VNC_DISPLAY}" \
  -geometry "${VNC_GEOMETRY}" \
  -depth "${VNC_DEPTH}" \
  -ac


# ---- START pproxy ----
#echo "[pproxy] binary: $(which pproxy)"
#echo "[pproxy] listen: ${SOCKS_LISTEN}"

#if [[ -n "${SOCKS_UPSTREAM}" ]]; then
#  echo "[pproxy] upstream: ${SOCKS_UPSTREAM}"
#  pproxy -l "socks5://${SOCKS_LISTEN}" -r "${SOCKS_UPSTREAM}" --daemon
#else
#  echo "[pproxy] direct mode (via tun0)"
#  pproxy -l "socks5://${SOCKS_LISTEN}" --daemon
#fi

nohup env NO_AT_BRIDGE=1 DISPLAY=":${VNC_DISPLAY}" /data/scripts/clipboard-daemon > /var/log/clipboard-daemon.log 2>&1 < /dev/null & disown
nohup env QWEN_ATTACH_RESTART_RETRIES=5 DISPLAY=":${VNC_DISPLAY}" /opt/venv/bin/python /data/scripts/qwen_server.py > /var/log/qwen_server.log 2>&1 < /dev/null & disown



echo "[init] container started successfully"

tail -f /dev/null
