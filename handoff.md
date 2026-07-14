# Investment Calculator Handoff

## Runtime rule

This project runs as a full-stack app:

- Backend: FastAPI on port `8010`
- Frontend: Vite on port `5173`
- After changing backend or frontend code, restart the services before asking the user to verify the result.

## Windows

From the project root in PowerShell:

```powershell
.\start_fullstack_app.ps1
```

This script stops anything listening on ports `8010` and `5173`, then starts both services in the background. It uses the Windows Python virtual environment and writes logs to:

- `backend_startup.log`
- `backend_startup.err.log`
- `frontend_startup.log`
- `frontend_startup.err.log`

## macOS

From the project root in Terminal:

```bash
chmod +x ./start_fullstack_app_macos.sh
./start_fullstack_app_macos.sh restart
```

The script restarts both services and reports their status. Logs are written to the same four `*_startup.log` files.

## Verification

After restart, verify:

- Frontend: `http://127.0.0.1:5173`
- Backend: `http://127.0.0.1:8010/api/health`

On macOS, use `./start_fullstack_app_macos.sh status` to check service status. On Windows, inspect the startup logs if a service does not load.

## Important

Use the `.ps1` restart flow on Windows and the `.sh` restart flow on macOS. Do not ask the user to repeat this context for each code change; restart the appropriate services as part of the handoff.
