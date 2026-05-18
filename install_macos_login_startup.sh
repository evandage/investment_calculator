#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$APP_DIR/start_streamlit_app_macos.sh"
PLIST_DIR="$HOME/Library/LaunchAgents"
PLIST="$PLIST_DIR/com.investment-calculator.streamlit.plist"
LOG="$APP_DIR/streamlit_launchd.log"
ERR_LOG="$APP_DIR/streamlit_launchd.err.log"

if [[ ! -f "$SCRIPT" ]]; then
  echo "Missing startup script: $SCRIPT" >&2
  exit 1
fi

mkdir -p "$PLIST_DIR"
chmod +x "$SCRIPT"

cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.investment-calculator.streamlit</string>
  <key>ProgramArguments</key>
  <array>
    <string>$SCRIPT</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>WorkingDirectory</key>
  <string>$APP_DIR</string>
  <key>StandardOutPath</key>
  <string>$LOG</string>
  <key>StandardErrorPath</key>
  <string>$ERR_LOG</string>
</dict>
</plist>
EOF

echo "Created LaunchAgent:"
echo "  $PLIST"
echo
echo "It will run next time you log in."
echo "To enable immediately without rebooting, run:"
echo "  launchctl bootstrap gui/$(id -u) \"$PLIST\""
echo
echo "To remove later, run:"
echo "  launchctl bootout gui/$(id -u) \"$PLIST\""
echo "  rm \"$PLIST\""
