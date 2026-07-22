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

## Portfolio history and exact anchors

- The trade ledger before an exact holdings anchor is intentionally allowed to be incomplete. Do not ask the user to reconstruct every old trade when a trusted dated holdings anchor exists.
- A `holdings` adjustment with reason `exact_holdings_anchor_reconciliation` is the authoritative position state on its `effective_date`. Rebuild later snapshots by replaying trades and later adjustments from that anchor; do not use incomplete earlier trades to overwrite it.
- The holdings page edits quantity and average cost inline. Its “保存并设为新锚点” action sends the complete holdings table to `PUT /api/holdings`; every successful save must record the exact-anchor adjustment above and invalidate history from the anchor date.
- A `balances` adjustment with `reconstruct_from_date` is the cash anchor. Historical EOD cash inside that window is reconstructed by reversing or replaying dated trades from the nearest trusted anchor. When several balance anchors share an effective date, use the latest `recorded_at` value.
- Historical balance reconstruction covers both cash and realized P&L. Reverse a later sell's `realized_pnl` when valuing a date before that sale, and add it from the sale date onward. `total_pnl_cny` is unrealized holdings P&L + FX P&L + dated realized USD/CNY P&L.
- Confirming or deleting trades must record a new cash anchor and invalidate performance history from the earliest affected trade date. Monthly bought amounts are budget controls only and are not a holdings/cash ledger.
- Cash balance and cash principal are separate fields: `cash_usd` / `cash_cny` are spendable balances, while `cash_cost_basis_usd` / `cash_cost_basis_cny` are the residual principal basis. On a sale, add only the sold lot's cost basis to cash basis and put the difference into realized P&L; on a buy, subtract the purchase amount from cash basis. Cash basis may be negative after realized gains are reinvested. Total-return basis and cumulative cash FX P&L must use cash basis, never raw cash balance.
- Legacy balances without cash-basis fields are migrated as `cash - realized - attributed dividends` (USD) and `cash - realized` (CNY). A manual balance edit moves only the net principal delta into cash basis, so simultaneously adding sale proceeds and realized P&L does not double count the profit.
- `holding_daily_*` and `security_daily_*` are trade-aware securities P&L. `total_daily_*` is the adjacent change in cumulative total P&L and includes securities, FX, cash and realized P&L. Never overwrite one with the other.
- `symbol_market_pct` (and the legacy `symbol_daily_pct` alias) is pure close-to-close market return. `symbol_position_pct` is the user's trade-aware return for that symbol. Same-day buys and sells can make these values differ legitimately.
- The cumulative chart's “total assets daily weighted” tooltip uses `total_daily_pnl_cny` / `total_daily_pnl_pct`. Position attribution and contribution views use the security/position fields.
- Before rebuilding historical snapshots after a data repair, keep a timestamped backup of `portfolio_history.json` and `portfolio_snapshot_ledger.json`.
