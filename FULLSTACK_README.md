# Fullstack Dashboard

The application uses a FastAPI backend with a Vite/React frontend.

## Architecture

- Backend: FastAPI on `http://127.0.0.1:8010`
- Frontend: Vite React on `http://127.0.0.1:5173`
- Profiles: use the top-right selector to switch between Evan and Nave. This is a local profile switch without passwords. The active profile is shared by all open tabs and devices using this server.
- Data files: Evan keeps the existing root JSON files. Nave's holdings, balances, satellite universe and targets, swing positions, and closed satellite P&L live under `profiles/nave/`. Trade, FX, budget, history, and audit files retain their separate user-keyed records.
- Market data: Futu OpenD first, Tencent/Sina/Eastmoney fallback

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
