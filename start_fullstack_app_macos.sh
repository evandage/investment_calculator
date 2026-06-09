#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_PORT="${BACKEND_PORT:-8010}"
FRONTEND_PORT="${FRONTEND_PORT:-5173}"
BACKEND_SESSION="investment-calculator-backend"
FRONTEND_SESSION="investment-calculator-frontend"
ACTION="${1:-start}"

log() {
  printf "[investment-app] %s\n" "$*"
}

lan_ip() {
  ipconfig getifaddr en0 2>/dev/null \
    || ipconfig getifaddr en1 2>/dev/null \
    || hostname -I 2>/dev/null | awk '{print $1}' \
    || printf "127.0.0.1"
}

port_pid() {
  lsof -tiTCP:"$1" -sTCP:LISTEN 2>/dev/null || true
}

ensure_python() {
  if [[ ! -x "$APP_DIR/.venv/bin/python" ]]; then
    log "Creating Python venv..."
    python3 -m venv "$APP_DIR/.venv"
  fi
  PYTHON="$APP_DIR/.venv/bin/python"
  if ! "$PYTHON" -c "import fastapi, uvicorn, futu" >/dev/null 2>&1; then
    log "Installing Python dependencies..."
    "$PYTHON" -m pip install -r "$APP_DIR/requirements.txt"
  fi
}

find_node() {
  if [[ -n "${NODE:-}" && -x "${NODE:-}" ]]; then
    NODE_BIN="$NODE"
  elif command -v node >/dev/null 2>&1; then
    NODE_BIN="$(command -v node)"
  elif [[ -x "$HOME/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node" ]]; then
    NODE_BIN="$HOME/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node"
  elif [[ -x "/Applications/Codex.app/Contents/Resources/node" ]]; then
    NODE_BIN="/Applications/Codex.app/Contents/Resources/node"
  else
    echo "Node.js is required. Install it from https://nodejs.org or set NODE=/path/to/node" >&2
    exit 1
  fi
}

ensure_frontend_deps() {
  find_node
  if [[ ! -d "$APP_DIR/frontend/node_modules" ]]; then
    if ! command -v npm >/dev/null 2>&1; then
      echo "frontend/node_modules is missing and npm was not found. Install Node.js/npm, then rerun this script." >&2
      exit 1
    fi
    log "Installing frontend dependencies..."
    (cd "$APP_DIR/frontend" && npm install)
  fi
}

run_detached() {
  local session="$1"
  local command="$2"
  if command -v screen >/dev/null 2>&1; then
    screen -S "$session" -X quit >/dev/null 2>&1 || true
    screen -dmS "$session" /bin/zsh -lc "$command"
  else
    nohup /bin/zsh -lc "$command" >/dev/null 2>&1 &
  fi
}

start_backend() {
  ensure_python
  if [[ -n "$(port_pid "$BACKEND_PORT")" ]]; then
    log "Backend already listening on port $BACKEND_PORT."
    return
  fi
  : > "$APP_DIR/backend_startup.log"
  : > "$APP_DIR/backend_startup.err.log"
  run_detached "$BACKEND_SESSION" "cd '$APP_DIR'; exec '$PYTHON' -m uvicorn backend.main:app --host 0.0.0.0 --port '$BACKEND_PORT' >> '$APP_DIR/backend_startup.log' 2>> '$APP_DIR/backend_startup.err.log'"
  log "Backend starting on port $BACKEND_PORT..."
}

start_frontend() {
  ensure_frontend_deps
  if [[ -n "$(port_pid "$FRONTEND_PORT")" ]]; then
    log "Frontend already listening on port $FRONTEND_PORT."
    return
  fi
  : > "$APP_DIR/frontend_startup.log"
  : > "$APP_DIR/frontend_startup.err.log"
  run_detached "$FRONTEND_SESSION" "cd '$APP_DIR/frontend'; exec '$NODE_BIN' ./node_modules/vite/bin/vite.js --host 0.0.0.0 --port '$FRONTEND_PORT' >> '$APP_DIR/frontend_startup.log' 2>> '$APP_DIR/frontend_startup.err.log'"
  log "Frontend starting on port $FRONTEND_PORT..."
}

stop_port() {
  local port="$1"
  local pid
  pid="$(port_pid "$port")"
  if [[ -n "$pid" ]]; then
    log "Stopping port $port: $pid"
    kill $pid 2>/dev/null || true
  fi
}

stop_app() {
  stop_port "$BACKEND_PORT"
  stop_port "$FRONTEND_PORT"
  screen -S "$BACKEND_SESSION" -X quit >/dev/null 2>&1 || true
  screen -S "$FRONTEND_SESSION" -X quit >/dev/null 2>&1 || true
}

status_app() {
  local ip
  ip="$(lan_ip)"
  if [[ -n "$(port_pid "$BACKEND_PORT")" ]]; then
    log "Backend:  running at http://127.0.0.1:$BACKEND_PORT"
  else
    log "Backend:  stopped"
  fi
  if [[ -n "$(port_pid "$FRONTEND_PORT")" ]]; then
    log "Frontend: running at http://127.0.0.1:$FRONTEND_PORT"
    log "LAN URL:  http://$ip:$FRONTEND_PORT"
  else
    log "Frontend: stopped"
  fi
}

case "$ACTION" in
  start)
    start_backend
    start_frontend
    sleep 2
    status_app
    ;;
  stop)
    stop_app
    ;;
  restart)
    stop_app
    sleep 1
    start_backend
    start_frontend
    sleep 2
    status_app
    ;;
  status)
    status_app
    ;;
  *)
    echo "Usage: $0 [start|stop|restart|status]" >&2
    exit 2
    ;;
esac
