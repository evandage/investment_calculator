#!/bin/bash
cd "$(dirname "$0")" || exit 1
python3 ./launch_app.py
result=$?
if [ "$result" -ne 0 ]; then
  echo "Launch failed. See the message above."
  read -r -p "Press Enter to close..." _
fi
exit "$result"
