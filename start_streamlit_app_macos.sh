#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
APP="$APP_DIR/app.py"
PORT="${PORT:-8501}"
LOG="$APP_DIR/streamlit_startup.log"
ERR_LOG="$APP_DIR/streamlit_startup.err.log"

if [[ -x "$APP_DIR/.venv/bin/python" ]]; then
  PYTHON="$APP_DIR/.venv/bin/python"
else
  PYTHON="$(command -v python3)"
fi

if [[ ! -f "$APP" ]]; then
  echo "Missing Streamlit app: $APP" >&2
  exit 1
fi

if lsof -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "$(date -Iseconds) Streamlit already listening on port $PORT; skip startup." >> "$LOG"
  exit 0
fi

echo "$(date -Iseconds) Starting Streamlit on 127.0.0.1:$PORT." >> "$LOG"
cd "$APP_DIR"
nohup "$PYTHON" -m streamlit run "$APP" \
  --server.address 127.0.0.1 \
  --server.port "$PORT" \
  --server.headless true \
  >> "$LOG" 2>> "$ERR_LOG" &

echo "Started. Open http://localhost:$PORT"
