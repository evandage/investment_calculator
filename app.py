import re
import json
from pathlib import Path
from datetime import datetime
from typing import Any

import requests
import streamlit as st

# 拉取失败时的回退价（与常见区间一致）
_FALLBACK = {
    "VOO": 400.0,
    "TLT": 90.0,
    "GLD": 180.0,
    "510300.SS": 4.0,
    "510500.SS": 6.0,
}

_TICKERS = {
    "voo": "VOO",
    "tlt": "TLT",
    "gld": "GLD",
    "hs300": "510300.SS",  # 华泰柏瑞沪深300ETF
    "zz500": "510500.SS",  # 南方中证500ETF
}

_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
}

# (connect, read) 秒；避免长时间挂死
_HTTP_TIMEOUT = (5, 15)

# 美股：腾讯财经批量接口；失败则用新浪全球行情
_QQ_US = {"VOO": "usVOO", "TLT": "usTLT", "GLD": "usGLD"}
_SINA_GB = {"VOO": "gb_voo", "TLT": "gb_tlt", "GLD": "gb_gld"}
_SINA_CN = {"510300.SS": "sh510300", "510500.SS": "sh510500"}

# A 股 ETF：东方财富 push2（secid：沪 1.xxxxxx）
_EM_CN_SEC = {"510300.SS": "1.510300", "510500.SS": "1.510500"}
_HOLDINGS_FILE = Path(__file__).with_name("holdings.json")
_BALANCE_FILE = Path(__file__).with_name("balances.json")
_ASSET_META = {
    "VOO": {"label": "VOO", "currency": "USD"},
    "TLT": {"label": "TLT", "currency": "USD"},
    "GLD": {"label": "GLD", "currency": "USD"},
    "510300.SS": {"label": "沪深300ETF", "currency": "CNY"},
    "510500.SS": {"label": "中证500ETF", "currency": "CNY"},
}
_TARGET_WEIGHTS = {
    "VOO": 0.4,
    "TLT": 0.2,
    "GLD": 0.1,
    "510300.SS": 0.2,
    "510500.SS": 0.1,
}


def _fetch_fx_from_erapi() -> float | None:
    url = "https://open.er-api.com/v6/latest/USD"
    r = requests.get(url, timeout=_HTTP_TIMEOUT, headers=_REQUEST_HEADERS)
    j = r.json()
    rates = j.get("rates", {})
    cny = rates.get("CNY")
    if cny is None:
        return None
    fx = float(cny)
    return fx if fx > 0 else None


def _fetch_fx_from_qq() -> float | None:
    r = requests.get(
        "http://qt.gtimg.cn/q=USDCNY",
        timeout=_HTTP_TIMEOUT,
        headers=_REQUEST_HEADERS,
    )
    r.encoding = "gbk"
    m = re.search(r'="([^"]*)"', r.text)
    if not m:
        return None
    parts = m.group(1).split("~")
    if len(parts) < 4:
        return None
    fx = float(parts[3])
    return fx if fx > 0 else None


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_usdcny_rate_meta() -> dict[str, str | float]:
    fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for source, fn in (
        ("ER-API", _fetch_fx_from_erapi),
        ("腾讯 USDCNY", _fetch_fx_from_qq),
    ):
        try:
            fx = fn()
            if fx is not None:
                return {"fx": fx, "source": source, "fetched_at": fetched_at}
        except Exception:
            pass
    return {"fx": 6.9, "source": "Fallback(6.9)", "fetched_at": fetched_at}


def _fetch_usdcny_rate() -> float:
    return float(_fetch_usdcny_rate_meta()["fx"])


def _parse_qq_us_response(text: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for line in text.replace("\n", "").split(";"):
        line = line.strip()
        if not line.startswith("v_us"):
            continue
        m = re.match(r'v_(us[A-Za-z]+)="([^"]*)"', line)
        if not m:
            continue
        code, body = m.group(1), m.group(2)
        parts = body.split("~")
        if len(parts) > 3:
            try:
                p = float(parts[3])
                if p > 0:
                    out[code] = p
            except ValueError:
                continue
    return out


def _fetch_qq_us() -> dict[str, float]:
    url = "http://qt.gtimg.cn/q=" + ",".join(_QQ_US.values())
    r = requests.get(url, timeout=_HTTP_TIMEOUT, headers=_REQUEST_HEADERS)
    r.encoding = "gbk"
    raw = _parse_qq_us_response(r.text)
    return {sym: raw[qc] for sym, qc in _QQ_US.items() if qc in raw}


def _fetch_sina_gb(list_code: str) -> float | None:
    url = "https://hq.sinajs.cn/list=" + list_code
    r = requests.get(
        url,
        timeout=_HTTP_TIMEOUT,
        headers={
            **_REQUEST_HEADERS,
            "Referer": "https://finance.sina.com.cn/",
        },
    )
    r.encoding = "gbk"
    m = re.search(r'="([^"]*)"', r.text)
    if not m or not m.group(1).strip():
        return None
    parts = m.group(1).split(",")
    if len(parts) < 2:
        return None
    try:
        p = float(parts[1])
        return p if p > 0 else None
    except ValueError:
        return None


def _fetch_sina_cn(list_code: str) -> float | None:
    url = "https://hq.sinajs.cn/list=" + list_code
    r = requests.get(
        url,
        timeout=_HTTP_TIMEOUT,
        headers={
            **_REQUEST_HEADERS,
            "Referer": "https://finance.sina.com.cn/",
        },
    )
    r.encoding = "gbk"
    m = re.search(r'="([^"]*)"', r.text)
    if not m or not m.group(1).strip():
        return None
    parts = m.group(1).split(",")
    if len(parts) < 4:
        return None
    try:
        p = float(parts[3])
        return p if p > 0 else None
    except ValueError:
        return None


def _fetch_eastmoney(secid: str) -> float | None:
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    params = {
        "secid": secid,
        "fields": "f43,f57",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "invt": "2",
        "fltt": "2",
    }
    r = requests.get(url, params=params, timeout=_HTTP_TIMEOUT, headers=_REQUEST_HEADERS)
    j = r.json()
    d = j.get("data")
    if not d:
        return None
    v = d.get("f43")
    if v is None:
        return None
    x = float(v)
    if x > 100:
        x /= 1000.0
    return x if x > 0 else None


@st.cache_data(ttl=120, show_spinner=False)
def _fetch_spot_prices_meta() -> dict[str, object]:
    out: dict[str, float] = {}
    source_by_symbol: dict[str, str] = {}
    fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        for sym, p in _fetch_qq_us().items():
            out[sym] = p
            source_by_symbol[sym] = "腾讯美股"
    except Exception:
        pass

    for sym in ("VOO", "TLT", "GLD"):
        if sym not in out:
            try:
                p = _fetch_sina_gb(_SINA_GB[sym])
                if p is not None:
                    out[sym] = p
                    source_by_symbol[sym] = "新浪全球"
            except Exception:
                pass

    for sym, secid in _EM_CN_SEC.items():
        try:
            p = _fetch_eastmoney(secid)
            if p is not None:
                out[sym] = p
                source_by_symbol[sym] = "东方财富"
        except Exception:
            pass

    symbols = list(_TICKERS.values())
    for sym in ("510300.SS", "510500.SS"):
        if sym not in out:
            try:
                p = _fetch_sina_cn(_SINA_CN[sym])
                if p is not None:
                    out[sym] = p
                    source_by_symbol[sym] = "新浪A股"
            except Exception:
                pass

    prices = {sym: out.get(sym, _FALLBACK[sym]) for sym in symbols}
    for sym in symbols:
        source_by_symbol.setdefault(sym, "Fallback")
    return {"prices": prices, "source_by_symbol": source_by_symbol, "fetched_at": fetched_at}


def _fetch_spot_prices() -> dict[str, float]:
    return dict(_fetch_spot_prices_meta()["prices"])


def _default_holdings() -> dict[str, dict[str, float]]:
    return {
        sym: {"shares": 0.0, "avg_cost": float(_FALLBACK[sym])}
        for sym in _ASSET_META.keys()
    }


def _load_holdings() -> dict[str, dict[str, float]]:
    if not _HOLDINGS_FILE.exists():
        return _default_holdings()
    try:
        data = json.loads(_HOLDINGS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _default_holdings()

    holdings = _default_holdings()
    for sym in holdings:
        item = data.get(sym, {})
        shares = item.get("shares", 0.0)
        avg_cost = item.get("avg_cost", holdings[sym]["avg_cost"])
        try:
            holdings[sym]["shares"] = max(0.0, float(shares))
            holdings[sym]["avg_cost"] = max(0.0, float(avg_cost))
        except (TypeError, ValueError):
            continue
    return holdings


def _save_holdings(holdings: dict[str, dict[str, float]]) -> None:
    payload = {
        sym: {
            "shares": float(max(0.0, item["shares"])),
            "avg_cost": float(max(0.0, item["avg_cost"])),
        }
        for sym, item in holdings.items()
    }
    _HOLDINGS_FILE.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _merge_buy(
    holding: dict[str, float], add_shares: float, add_price: float
) -> dict[str, float]:
    old_shares = float(holding.get("shares", 0.0))
    old_cost = float(holding.get("avg_cost", 0.0))
    add_shares = max(0.0, float(add_shares))
    add_price = max(0.0, float(add_price))
    if add_shares <= 0:
        return {"shares": old_shares, "avg_cost": old_cost}
    new_shares = old_shares + add_shares
    if new_shares <= 0:
        return {"shares": 0.0, "avg_cost": add_price}
    new_avg = (old_shares * old_cost + add_shares * add_price) / new_shares
    return {"shares": new_shares, "avg_cost": new_avg}


def _default_balances() -> dict[str, float]:
    return {"510300.SS": 0.0, "510500.SS": 0.0}


def _load_balances() -> dict[str, float]:
    if not _BALANCE_FILE.exists():
        return _default_balances()
    try:
        data = json.loads(_BALANCE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _default_balances()
    out = _default_balances()
    for sym in out:
        try:
            out[sym] = max(0.0, float(data.get(sym, 0.0)))
        except (TypeError, ValueError):
            pass
    return out


def _save_balances(balances: dict[str, float]) -> None:
    payload = {sym: float(max(0.0, v)) for sym, v in _default_balances().items()}
    for sym in payload:
        payload[sym] = float(max(0.0, balances.get(sym, 0.0)))
    _BALANCE_FILE.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _db_conf() -> dict[str, str] | None:
    try:
        url = str(st.secrets.get("SUPABASE_URL", "")).strip().rstrip("/")
        key = str(st.secrets.get("SUPABASE_KEY", "")).strip()
    except Exception:
        return None
    if not url or not key:
        return None
    return {"url": url, "key": key}


def _normalize_holdings(raw: Any) -> dict[str, dict[str, float]]:
    holdings = _default_holdings()
    if not isinstance(raw, dict):
        return holdings
    for sym in holdings:
        item = raw.get(sym, {})
        if not isinstance(item, dict):
            continue
        try:
            holdings[sym]["shares"] = max(0.0, float(item.get("shares", 0.0)))
            holdings[sym]["avg_cost"] = max(
                0.0, float(item.get("avg_cost", holdings[sym]["avg_cost"]))
            )
        except (TypeError, ValueError):
            continue
    return holdings


def _normalize_balances(raw: Any) -> dict[str, float]:
    balances = _default_balances()
    if not isinstance(raw, dict):
        return balances
    for sym in balances:
        try:
            balances[sym] = max(0.0, float(raw.get(sym, 0.0)))
        except (TypeError, ValueError):
            continue
    return balances


def _load_from_supabase(user_id: str) -> tuple[dict[str, dict[str, float]], dict[str, float]] | None:
    conf = _db_conf()
    if not conf or not user_id:
        return None
    url = (
        f"{conf['url']}/rest/v1/portfolio_state"
        f"?user_id=eq.{user_id}&select=holdings,balances&limit=1"
    )
    headers = {
        "apikey": conf["key"],
        "Authorization": f"Bearer {conf['key']}",
    }
    r = requests.get(url, headers=headers, timeout=_HTTP_TIMEOUT)
    if r.status_code >= 400:
        return None
    rows = r.json()
    if not rows:
        return None
    row = rows[0]
    return _normalize_holdings(row.get("holdings")), _normalize_balances(row.get("balances"))


def _save_to_supabase(user_id: str, holdings: dict[str, dict[str, float]], balances: dict[str, float]) -> bool:
    conf = _db_conf()
    if not conf or not user_id:
        return False
    url = f"{conf['url']}/rest/v1/portfolio_state?on_conflict=user_id"
    headers = {
        "apikey": conf["key"],
        "Authorization": f"Bearer {conf['key']}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    payload = [
        {
            "user_id": user_id,
            "holdings": holdings,
            "balances": balances,
            "updated_at": datetime.utcnow().isoformat() + "Z",
        }
    ]
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=_HTTP_TIMEOUT)
    return r.status_code < 400


def _load_user_state(user_id: str) -> tuple[dict[str, dict[str, float]], dict[str, float], str]:
    if user_id:
        cloud = _load_from_supabase(user_id)
        if cloud is not None:
            h, b = cloud
            return h, b, "cloud"
    # Cloud 未配置或读失败时，回退本地
    return _load_holdings(), _load_balances(), "local"


def _save_user_state(
    user_id: str,
    holdings: dict[str, dict[str, float]],
    balances: dict[str, float],
) -> str:
    if user_id and _save_to_supabase(user_id, holdings, balances):
        return "cloud"
    _save_holdings(holdings)
    _save_balances(balances)
    return "local"


def _defaults_from_fetch() -> dict[str, float]:
    raw = _fetch_spot_prices()
    return {
        "voo": raw["VOO"],
        "tlt": raw["TLT"],
        "gld": raw["GLD"],
        "hs300": raw["510300.SS"],
        "zz500": raw["510500.SS"],
    }


def _ensure_price_session_defaults() -> None:
    if st.session_state.get("_prices_initialized"):
        return
    d = _defaults_from_fetch()
    st.session_state.setdefault("def_voo", d["voo"])
    st.session_state.setdefault("def_tlt", d["tlt"])
    st.session_state.setdefault("def_gld", d["gld"])
    st.session_state.setdefault("def_hs300", d["hs300"])
    st.session_state.setdefault("def_zz500", d["zz500"])
    st.session_state["_prices_initialized"] = True


def _ensure_fx_session_default() -> None:
    if st.session_state.get("_fx_initialized"):
        return
    st.session_state.setdefault("def_fx", _fetch_usdcny_rate())
    st.session_state["_fx_initialized"] = True


st.title("📊 定投计算器")
user_id = st.sidebar.text_input("用户ID（用于跨设备同步）", value="hty12").strip()
if _db_conf():
    st.sidebar.caption(f"存储后端：Supabase（user_id: {user_id or '未填写'}）")
else:
    st.sidebar.caption("存储后端：本地文件（未配置 Supabase Secrets）")

# 输入
rmb = st.number_input("每月投入（人民币）", value=5000.0)
_ensure_fx_session_default()
fx = st.number_input("汇率（USD/CNY）", value=float(st.session_state.def_fx), key="inp_fx")

st.subheader("输入价格")
spot_meta = _fetch_spot_prices_meta()
fx_meta = _fetch_usdcny_rate_meta()
spot_sources = spot_meta["source_by_symbol"]
st.caption(
    "数据来源标签："
    f" 汇率={fx_meta['source']}（更新时间 {fx_meta['fetched_at']}）"
    f" | VOO={spot_sources['VOO']}, TLT={spot_sources['TLT']}, GLD={spot_sources['GLD']}"
    f" | 沪深300={spot_sources['510300.SS']}, 中证500={spot_sources['510500.SS']}"
    f"（更新时间 {spot_meta['fetched_at']}）"
)
col_a, col_b = st.columns([1, 1])
with col_a:
    if st.button(
        "刷新市价",
        help="腾讯财经(美股)+东方财富(A股)拉取现价；失败时用新浪美股作备用。约 2 分钟内结果会缓存。",
    ):
        _fetch_spot_prices_meta.clear()
        _fetch_usdcny_rate_meta.clear()
        d = _defaults_from_fetch()
        st.session_state.def_fx = _fetch_usdcny_rate()
        st.session_state.def_voo = d["voo"]
        st.session_state.def_tlt = d["tlt"]
        st.session_state.def_gld = d["gld"]
        st.session_state.def_hs300 = d["hs300"]
        st.session_state.def_zz500 = d["zz500"]
        for k in ("inp_fx", "inp_voo", "inp_tlt", "inp_gld", "inp_hs300", "inp_zz500"):
            if k in st.session_state:
                del st.session_state[k]
        st.rerun()

_ensure_price_session_defaults()

voo_price = st.number_input("VOO价格", value=float(st.session_state.def_voo), key="inp_voo")
tlt_price = st.number_input("TLT价格", value=float(st.session_state.def_tlt), key="inp_tlt")
gld_price = st.number_input("黄金GLD价格", value=float(st.session_state.def_gld), key="inp_gld")

hs300_price = st.number_input("沪深300价格", value=float(st.session_state.def_hs300), key="inp_hs300")
zz500_price = st.number_input("中证500价格", value=float(st.session_state.def_zz500), key="inp_zz500")

prices_now = {
    "VOO": voo_price,
    "TLT": tlt_price,
    "GLD": gld_price,
    "510300.SS": hs300_price,
    "510500.SS": zz500_price,
}

if st.button("计算"):
    # 比例
    weights_us = {"VOO": 0.4, "TLT": 0.2, "GLD": 0.1}

    us_ratio = sum(weights_us.values())

    usd_total_raw = (rmb * us_ratio) / fx
    usd_total = round(usd_total_raw)

    st.subheader("📈 投资结果")

    # 美股
    st.write("### 美股")
    voo_usd = usd_total * (0.4 / us_ratio)
    tlt_usd = usd_total * (0.2 / us_ratio)
    gld_usd = usd_total * (0.1 / us_ratio)

    st.write(f"VOO：{voo_usd:.2f} USD → {voo_usd/voo_price:.3f} 股")
    st.write(f"TLT：{tlt_usd:.2f} USD → {tlt_usd/tlt_price:.3f} 股")
    st.write(f"GLD：{gld_usd:.2f} USD → {gld_usd/gld_price:.3f} 股")

    us_allocated_usd = voo_usd + tlt_usd + gld_usd
    st.write(
        f"**美股美元合计：{us_allocated_usd:.2f} USD**（本月按整数美元换汇，原始应换约 {usd_total_raw:.2f} USD）"
    )

    # A股
    st.write("### A股")

    _, balances, _ = _load_user_state(user_id)
    hs300_amount = rmb * 0.2
    zz500_amount = rmb * 0.1

    hs300_budget = hs300_amount + balances["510300.SS"]
    zz500_budget = zz500_amount + balances["510500.SS"]
    hs300_lot_cost = hs300_price * 100
    zz500_lot_cost = zz500_price * 100
    hs300_lots = int(hs300_budget // hs300_lot_cost) if hs300_lot_cost > 0 else 0
    zz500_lots = int(zz500_budget // zz500_lot_cost) if zz500_lot_cost > 0 else 0
    hs300_balance_next = hs300_budget - hs300_lots * hs300_lot_cost
    zz500_balance_next = zz500_budget - zz500_lots * zz500_lot_cost

    st.write(f"沪深300：{hs300_lots*100} 股（{hs300_lots} 手）")
    st.write(f"中证500：{zz500_lots*100} 股（{zz500_lots} 手）")
    st.caption(
        f"A股余额结转：沪深300 结转 {hs300_balance_next:.2f} CNY；"
        f"中证500 结转 {zz500_balance_next:.2f} CNY"
    )

    calc_buys = {
        "VOO": {"shares": voo_usd / voo_price, "price": voo_price},
        "TLT": {"shares": tlt_usd / tlt_price, "price": tlt_price},
        "GLD": {"shares": gld_usd / gld_price, "price": gld_price},
        "510300.SS": {"shares": hs300_lots * 100.0, "price": hs300_price},
        "510500.SS": {"shares": zz500_lots * 100.0, "price": zz500_price},
    }
    if st.button("将本月定投更新到我的持仓"):
        holdings, balances_loaded, _ = _load_user_state(user_id)
        for sym, buy in calc_buys.items():
            holdings[sym] = _merge_buy(holdings[sym], buy["shares"], buy["price"])
        balances_loaded["510300.SS"] = hs300_balance_next
        balances_loaded["510500.SS"] = zz500_balance_next
        save_mode = _save_user_state(user_id, holdings, balances_loaded)
        st.success(f"已更新到持仓（{'云端数据库' if save_mode == 'cloud' else '本地文件'}）")

st.subheader("📦 我的持仓")
holdings, balances_for_view, storage_mode = _load_user_state(user_id)
st.caption(f"当前持仓读取来源：{'云端数据库' if storage_mode == 'cloud' else '本地文件'}")

with st.expander("编辑持仓（会保存）", expanded=False):
    with st.form("holdings_edit_form"):
        for sym, meta in _ASSET_META.items():
            c1, c2 = st.columns([1, 1])
            with c1:
                shares = st.number_input(
                    f"{meta['label']} 持有数量",
                    min_value=0.0,
                    value=float(holdings[sym]["shares"]),
                    step=1.0 if sym.endswith(".SS") else 0.01,
                    key=f"edit_shares_{sym}",
                )
            with c2:
                avg_cost = st.number_input(
                    f"{meta['label']} 持仓成本({meta['currency']})",
                    min_value=0.0,
                    value=float(holdings[sym]["avg_cost"]),
                    step=0.01,
                    key=f"edit_cost_{sym}",
                )
            holdings[sym]["shares"] = shares
            holdings[sym]["avg_cost"] = avg_cost
        if st.form_submit_button("保存持仓"):
            save_mode = _save_user_state(user_id, holdings, balances_for_view)
            st.success(f"持仓已保存（{'云端数据库' if save_mode == 'cloud' else '本地文件'}）")

rows = []
total_cost_cny = 0.0
total_value_cny = 0.0
value_cny_by_symbol: dict[str, float] = {}
for sym, meta in _ASSET_META.items():
    shares = float(holdings[sym]["shares"])
    avg_cost = float(holdings[sym]["avg_cost"])
    current = float(prices_now[sym])
    cost = shares * avg_cost
    value = shares * current
    pnl = value - cost
    pnl_pct = (pnl / cost * 100) if cost > 0 else 0.0

    if meta["currency"] == "USD":
        cost_cny = cost * fx
        value_cny = value * fx
    else:
        cost_cny = cost
        value_cny = value

    total_cost_cny += cost_cny
    total_value_cny += value_cny
    value_cny_by_symbol[sym] = value_cny
    rows.append(
        {
            "标的": meta["label"],
            "币种": meta["currency"],
            "持有数量": round(shares, 3),
            "持仓成本": round(avg_cost, 3),
            "当前价": round(current, 3),
            "持仓市值": round(value, 2),
            "浮动盈亏": round(pnl, 2),
            "涨跌幅%": round(pnl_pct, 2),
        }
    )

st.dataframe(rows, width="stretch", hide_index=True)
total_pnl_cny = total_value_cny - total_cost_cny
total_pnl_pct = (total_pnl_cny / total_cost_cny * 100) if total_cost_cny > 0 else 0.0
st.write(f"总成本(折合CNY)：{total_cost_cny:.2f}")
st.write(f"总市值(折合CNY)：{total_value_cny:.2f}")
st.write(f"总浮盈亏(折合CNY)：{total_pnl_cny:.2f}（{total_pnl_pct:.2f}%）")

st.subheader("🎯 持仓比例对比")
ratio_rows = []
for sym, meta in _ASSET_META.items():
    target = _TARGET_WEIGHTS.get(sym, 0.0)
    current = (value_cny_by_symbol.get(sym, 0.0) / total_value_cny) if total_value_cny > 0 else 0.0
    ratio_rows.append(
        {
            "标的": meta["label"],
            "目标比例%": round(target * 100, 2),
            "当前比例%": round(current * 100, 2),
            "偏离(当前-目标)%": round((current - target) * 100, 2),
        }
    )

st.dataframe(ratio_rows, width="stretch", hide_index=True)
