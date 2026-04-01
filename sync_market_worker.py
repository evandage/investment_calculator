import argparse
import os
import time
from datetime import datetime, timezone

from chart_boards import configure_market_storage, sync_symbol_bars


DEFAULT_SYMBOLS = ["VOO", "QQQ", "TLT", "510300.SS"]


def _db_conf_from_env() -> dict[str, str] | None:
    url = str(os.environ.get("SUPABASE_URL", "")).strip().rstrip("/")
    key = str(os.environ.get("SUPABASE_KEY", "")).strip()
    if not url or not key:
        return None
    return {"url": url, "key": key}


def _run_once(symbols: list[str]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    print(f"[sync-worker] start {now} symbols={symbols}", flush=True)
    for sym in symbols:
        try:
            out = sync_symbol_bars(sym)
            print(f"[sync-worker] {sym} done: {out}", flush=True)
        except Exception as e:
            print(f"[sync-worker] {sym} failed: {e}", flush=True)
    print("[sync-worker] cycle done", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Periodically sync market bars to Supabase.")
    parser.add_argument("--interval", type=int, default=60, help="Sync interval seconds (default: 60)")
    parser.add_argument(
        "--symbols",
        type=str,
        default=",".join(DEFAULT_SYMBOLS),
        help="Comma separated symbols (default: VOO,QQQ,TLT,510300.SS)",
    )
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    args = parser.parse_args()

    conf = _db_conf_from_env()
    if not conf:
        print("[sync-worker] missing SUPABASE_URL or SUPABASE_KEY", flush=True)
        return 2

    symbols = [x.strip() for x in args.symbols.split(",") if x.strip()]
    if not symbols:
        symbols = DEFAULT_SYMBOLS[:]

    configure_market_storage(conf, read_only=False)

    if args.once:
        _run_once(symbols)
        return 0

    iv = max(30, int(args.interval))
    print(f"[sync-worker] running loop interval={iv}s", flush=True)
    while True:
        started = time.time()
        _run_once(symbols)
        sleep_s = max(1.0, iv - (time.time() - started))
        time.sleep(sleep_s)


if __name__ == "__main__":
    raise SystemExit(main())

