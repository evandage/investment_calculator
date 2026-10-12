"""The two local, passwordless portfolio profiles."""

import json
import os
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATE_FILE = ROOT / "profiles.json"
PROFILE_DIR = ROOT / "profiles"
PROFILES = {"evan": "Evan", "nave": "Nave"}
_STATE_LOCK = threading.RLock()


def active_user_id() -> str:
    with _STATE_LOCK:
        try:
            value = json.loads(STATE_FILE.read_text(encoding="utf-8")).get("active_user_id")
        except (OSError, ValueError, AttributeError):
            value = None
    return value if value in PROFILES else "evan"


def profile_path(legacy_path: Path, user_id: str | None = None) -> Path:
    user = user_id or active_user_id()
    if user not in PROFILES:
        raise ValueError("unknown profile")
    return legacy_path if user == "evan" else PROFILE_DIR / user / legacy_path.name


def switch_profile(user_id: str) -> str:
    if user_id not in PROFILES:
        raise ValueError("unknown profile")
    with _STATE_LOCK:
        temporary = STATE_FILE.with_suffix(".json.tmp")
        temporary.write_text(json.dumps({"active_user_id": user_id}, indent=2), encoding="utf-8")
        os.replace(temporary, STATE_FILE)
    return user_id
