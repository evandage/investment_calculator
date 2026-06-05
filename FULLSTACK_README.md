# Fullstack Dashboard

This branch adds a cross-platform frontend/backend version while keeping the existing Streamlit app.

## Architecture

- Backend: FastAPI on `http://127.0.0.1:8010`
- Frontend: Vite React on `http://127.0.0.1:5173`
- Data files: reuses `holdings.json`, `balances.json`, and `monthly_budget_usage.json`
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
