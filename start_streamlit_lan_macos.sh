#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
APP="$APP_DIR/app.py"
PORT="${PORT:-8501}"
LOG="$APP_DIR/streamlit_lan.log"
ERR_LOG="$APP_DIR/streamlit_lan.err.log"

if [[ -x "$APP_DIR/.venv/bin/python" ]]; then
  PYTHON="$APP_DIR/.venv/bin/python"
else
  PYTHON="$(command -v python3)"
fi

if [[ ! -f "$APP" ]]; then
  echo "Missing Streamlit app: $APP" >&2
  exit 1
fi

LAN_IPS="$(
  for dev in en0 en1 en2; do
    ipconfig getifaddr "$dev" 2>/dev/null || true
  done
  route get default 2>/dev/null | awk '/interface:/{print $2}' | while read -r dev; do
    ipconfig getifaddr "$dev" 2>/dev/null || true
  done
  ifconfig 2>/dev/null | awk '/inet / && $2 !~ /^127\\./ {print $2}'
)" 
LAN_IPS="$(echo "$LAN_IPS" | awk 'NF && !seen[$0]++')"

if lsof -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Port $PORT is already listening. If this is your Streamlit app, open one of:"
  while IFS= read -r ip; do
    [[ -n "$ip" ]] && echo "  http://$ip:$PORT"
  done <<< "$LAN_IPS"
  exit 0
fi

echo "Starting Streamlit for LAN access on 0.0.0.0:$PORT..."
echo "After it starts, open from another device on the same Wi-Fi/LAN:"
while IFS= read -r ip; do
  [[ -n "$ip" ]] && echo "  http://$ip:$PORT"
done <<< "$LAN_IPS"

echo "$(date -Iseconds) Starting Streamlit LAN server on 0.0.0.0:$PORT." >> "$LOG"
cd "$APP_DIR"
nohup "$PYTHON" -m streamlit run "$APP" \
  --server.address 0.0.0.0 \
  --server.port "$PORT" \
  --server.headless true \
  >> "$LOG" 2>> "$ERR_LOG" &
