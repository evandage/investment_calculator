"""The two local, passwordless portfolio profiles."""

import json
import os
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATE_FILE = ROOT / "profiles.json"
PROFILE_DIR = ROOT / "profiles"
PROFILES = {"evan": "Evan", "nave": "Nave"}
_STATE_LOCK = threading.RLock()
_REQUEST_USER = ContextVar[str | None]("request_user", default=None)


@contextmanager
def use_profile(user_id: str):
    if user_id not in PROFILES:
        raise ValueError("unknown profile")
    token = _REQUEST_USER.set(user_id)
    try:
        yield
    finally:
        _REQUEST_USER.reset(token)


def active_user_id() -> str:
    if user_id := _REQUEST_USER.get():
        return user_id
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
