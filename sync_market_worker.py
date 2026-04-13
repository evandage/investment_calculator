import argparse
import os
import time
from datetime import datetime, time as dtime, timedelta, timezone
from zoneinfo import ZoneInfo

from chart_boards import configure_market_storage, sync_symbol_bars


DEFAULT_SYMBOLS = ["VOO", "QQQ", "TLT", "IEI", "510300.SS", "510500.SS"]
_TZ_SH = ZoneInfo("Asia/Shanghai")
_TZ_NY = ZoneInfo("America/New_York")
_OPEN_BUF_MIN = max(0, int(os.environ.get("MARKET_OPEN_BUFFER_MINUTES", "5")))


def _in_any_window(now_local: datetime, windows: list[tuple[dtime, dtime]], *, buffer_minutes: int = 0) -> bool:
    now_t = now_local.time()
    if buffer_minutes > 0:
        now_t = (now_local + timedelta(minutes=0)).time()
    for start, end in windows:
        st = datetime.combine(now_local.date(), start, tzinfo=now_local.tzinfo)
        ed = datetime.combine(now_local.date(), end, tzinfo=now_local.tzinfo)
        if buffer_minutes > 0:
            st = st - timedelta(minutes=buffer_minutes)
            ed = ed + timedelta(minutes=buffer_minutes)
        if st.time() <= now_t <= ed.time():
            return True
    return False


def _is_symbol_session_open(symbol: str, now_utc: datetime) -> bool:
    # A股（含午休）
    if symbol.endswith(".SS") or symbol.endswith(".SZ"):
        now_sh = now_utc.astimezone(_TZ_SH)
        if now_sh.weekday() >= 5:
            return False
        windows = [(dtime(9, 30), dtime(11, 30)), (dtime(13, 0), dtime(15, 0))]
        return _in_any_window(now_sh, windows, buffer_minutes=_OPEN_BUF_MIN)

    # 美股常规时段（自动处理夏令时）
    now_ny = now_utc.astimezone(_TZ_NY)
    if now_ny.weekday() >= 5:
        return False
    windows = [(dtime(9, 30), dtime(16, 0))]
    return _in_any_window(now_ny, windows, buffer_minutes=_OPEN_BUF_MIN)


def _db_conf_from_env() -> dict[str, str] | None:
    url = str(os.environ.get("SUPABASE_URL", "")).strip().rstrip("/")
    key = str(os.environ.get("SUPABASE_KEY", "")).strip()
    if not url or not key:
        return None
    return {"url": url, "key": key}


def _run_once(symbols: list[str]) -> None:
    now_utc = datetime.now(timezone.utc)
    now = now_utc.isoformat()
    active = [sym for sym in symbols if _is_symbol_session_open(sym, now_utc)]
    print(f"[sync-worker] start {now} symbols={symbols}", flush=True)
    print(f"[sync-worker] active symbols by market session: {active}", flush=True)
    if not active:
        print("[sync-worker] no active market session, skip this cycle", flush=True)
        print("[sync-worker] cycle done", flush=True)
        return
    for sym in active:
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
        help="Comma separated symbols (default: VOO,QQQ,TLT,IEI,510300.SS,510500.SS)",
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

