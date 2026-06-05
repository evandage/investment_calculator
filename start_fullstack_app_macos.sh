#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="$APP_DIR/.venv/bin/python"

if [[ ! -x "$PYTHON" ]]; then
  PYTHON="python3"
fi

cd "$APP_DIR"

if ! lsof -iTCP:8010 -sTCP:LISTEN >/dev/null 2>&1; then
  nohup "$PYTHON" -m uvicorn backend.main:app --host 0.0.0.0 --port 8010 \
    > "$APP_DIR/backend_startup.log" 2> "$APP_DIR/backend_startup.err.log" &
fi

cd "$APP_DIR/frontend"
if ! lsof -iTCP:5173 -sTCP:LISTEN >/dev/null 2>&1; then
  nohup sh -c "npm install && npm run dev -- --host 0.0.0.0 --port 5173" \
    > "$APP_DIR/frontend_startup.log" 2> "$APP_DIR/frontend_startup.err.log" &
fi

echo "Backend:  http://127.0.0.1:8010"
echo "Frontend: http://127.0.0.1:5173"
