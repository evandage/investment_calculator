"""Update the checkout, open Futu OpenD, and start the platform's app script."""
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent


def opend_ready():
    host = os.environ.get("FUTU_OPEND_HOST", "127.0.0.1") or "127.0.0.1"
    try:
        port = int(os.environ.get("FUTU_OPEND_PORT", "11111"))
        with socket.create_connection((host, port), timeout=1):
            return True
    except (OSError, ValueError):
        return False


def find_opend():
    configured = os.environ.get("FUTU_OPEND_PATH")
    if configured:
        return Path(configured).expanduser()
    if sys.platform == "win32":
        candidates = [Path(os.environ.get("APPDATA", "")) / "Futu_OpenD" / "Futu_OpenD.exe"]
        try:
            import winreg
            for root in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):
                for view in (winreg.KEY_WOW64_64KEY, winreg.KEY_WOW64_32KEY):
                    try:
                        with winreg.OpenKey(root, r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall", 0, winreg.KEY_READ | view) as key:
                            for index in range(winreg.QueryInfoKey(key)[0]):
                                with winreg.OpenKey(key, winreg.EnumKey(key, index)) as item:
                                    try:
                                        if winreg.QueryValueEx(item, "DisplayName")[0] == "Futu_OpenD":
                                            candidates.append(Path(winreg.QueryValueEx(item, "DisplayIcon")[0].strip('"')))
                                    except OSError:
                                        pass
                    except OSError:
                        pass
        except ImportError:
            pass
    else:
        candidates = [Path("/Applications/Futu_OpenD.app"), Path.home() / "Applications/Futu_OpenD.app"]
    return next((path for path in candidates if path.exists()), None)


def main():
    if sys.platform not in ("win32", "darwin"):
        raise RuntimeError("Only Windows and macOS have app startup scripts.")
    print("Updating repository (fast-forward only)...", flush=True)
    subprocess.run(["git", "pull", "--ff-only"], cwd=ROOT, check=True)

    if opend_ready():
        print("OpenD port is already reachable.", flush=True)
    else:
        path = find_opend()
        if path and path.exists():
            print(f"Starting OpenD: {path}", flush=True)
            if sys.platform == "darwin":
                subprocess.run(["open", str(path)], check=True)
            else:
                subprocess.Popen([str(path)], cwd=path.parent)
            for _ in range(10):
                if opend_ready():
                    break
                time.sleep(1)
            if not opend_ready():
                print("OpenD is not reachable yet. Complete login or verification in its window.", flush=True)
        else:
            print("OpenD GUI was not found. Set FUTU_OPEND_PATH to its executable or .app and retry.", flush=True)

    print("Starting application...", flush=True)
    if sys.platform == "win32":
        command = ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ROOT / "start_fullstack_app.ps1")]
    else:
        command = ["bash", str(ROOT / "start_fullstack_app_macos.sh")]
    subprocess.run(command, cwd=ROOT, check=True)


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"Launch failed: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
