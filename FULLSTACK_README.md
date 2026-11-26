# Fullstack Dashboard

The application uses a FastAPI backend with a Vite/React frontend.

## Architecture

- Backend: FastAPI on `http://127.0.0.1:8010`
- Frontend: Vite React on `http://127.0.0.1:5173`
- Profiles: choose Evan or Nave when opening the app. Click the account avatar in the top-right corner to return to the picker. Each tab sends its own profile with its requests. Profile selection is passwordless, so anyone with access to the app can choose either account.
- Data files: Evan keeps the existing root JSON files. Nave's holdings, balances, satellite universe and targets, swing positions, and closed satellite P&L live under `profiles/nave/`. Trade, FX, budget, history, and audit files retain their separate user-keyed records.
- Market data: Futu OpenD first, Tencent/Sina/Eastmoney fallback

## One-click launch

On Windows, double-click `launch_app.cmd`. On macOS, run `chmod +x launch_app.command` once and then double-click it. The launcher runs `git pull --ff-only`, starts the Futu OpenD GUI if its configured port is not reachable, and calls the existing startup script for the current system. Windows requires the existing `.venv`; macOS requires `python3`.

If the pull fails (for example, due to local changes or divergent history), startup stops so the Git issue can be resolved first. OpenD may require you to log in or complete verification in its window; the app starts even if OpenD is unavailable and can use its fallback market data sources. Set `FUTU_OPEND_PATH` to the GUI executable on Windows or the `.app` on macOS if automatic discovery fails. `FUTU_OPEND_HOST` and `FUTU_OPEND_PORT` control the connection check, matching the backend configuration.

## Windows

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
cd frontend
npm.cmd install
cd ..
.\start_fullstack_app.ps1
```

If PowerShell blocks scripts, run the same backend/frontend commands manually:

```powershell
.\.venv\Scripts\python.exe -m uvicorn backend.main:app --host 127.0.0.1 --port 8010
cd frontend
npm.cmd run dev -- --host 127.0.0.1 --port 5173
```

## macOS

```bash
python3 -m pip install -r requirements.txt
cd frontend
npm install
cd ..
chmod +x ./start_fullstack_app_macos.sh
./start_fullstack_app_macos.sh
```

## Phone Access

On the same LAN, open:

```text
http://<computer-lan-ip>:5173
```

For cross-network access, use Tailscale or ZeroTier instead of exposing the ports directly.
