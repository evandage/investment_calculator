import React, { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { hierarchy, treemap, treemapSquarify } from "d3-hierarchy";
import { BaselineSeries, CandlestickSeries, createChart, HistogramSeries, LineSeries } from "lightweight-charts";
import { Activity, BookOpen, Check, CircleAlert, Gauge, Home, Minus, Plus, RefreshCcw, Save, Trash2, TrendingUp, Undo2, X } from "lucide-react";

const API_BASE =
  import.meta.env.VITE_API_BASE ||
  `${window.location.protocol}//${window.location.hostname}:8010`;
const WS_BASE = API_BASE.replace(/^http/i, "ws");
const HEATMAP_LAYOUT_WIDTH = 100;
const HEATMAP_LAYOUT_HEIGHT = 72;
const HEATMAP_MAX_CANVAS_HEIGHT = 612;
const SATELLITE_HOVER_LAYOUT_WIDTH = 100;
const SATELLITE_HOVER_LAYOUT_HEIGHT = 42;
const TERMINAL_CHART = {
  yellow: "#facc15",
  cyan: "#22d3ee",
  deepBlue: "#2563eb",
  violet: "#a78bfa",
  green: "#34d399",
  coral: "#fb7185",
  textSoft: "#dbeafe",
  textMuted: "#cbd5e1",
  axis: "rgba(203, 213, 225, 0.34)",
  grid: "rgba(148, 163, 184, 0.16)",
  zero: "rgba(226, 232, 240, 0.42)",
  surface: "#0b1b2e",
  legend: "#10233a",
};
const PLOT_FONT = "-apple-system, BlinkMacSystemFont, SF Pro Display, SF Pro Text, Inter, Microsoft YaHei, system-ui, sans-serif";
const USD_PERFORMANCE_SYMBOLS = ["VOO", "QQQ", "ISRG", "TEM", "PLTR", "GOOGL", "MSFT", "AVGO", "NVDA", "SGOV"];

const SHANGHAI_TIME_ZONE = "Asia/Shanghai";
const shanghaiDateFormatter = new Intl.DateTimeFormat("en-CA", {
  timeZone: SHANGHAI_TIME_ZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});
const shanghaiKlineTimeFormatter = new Intl.DateTimeFormat("zh-CN", {
  timeZone: SHANGHAI_TIME_ZONE,
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});
function formatPartsDate(formatter, date) {
  const parts = formatter.formatToParts(date);
  const year = parts.find((part) => part.type === "year")?.value || "1970";
  const month = parts.find((part) => part.type === "month")?.value || "01";
  const day = parts.find((part) => part.type === "day")?.value || "01";
  return `${year}-${month}-${day}`;
}

function previousTradingDateFromShanghai(date = new Date()) {
  const shanghaiToday = formatPartsDate(shanghaiDateFormatter, date);
  const cursor = new Date(`${shanghaiToday}T12:00:00Z`);
  do {
    cursor.setUTCDate(cursor.getUTCDate() - 1);
  } while (cursor.getUTCDay() === 0 || cursor.getUTCDay() === 6);
  return cursor.toISOString().slice(0, 10);
}

function formatLightweightChartTime(time) {
  if (typeof time === "string") return time;
  if (typeof time === "number" && Number.isFinite(time)) {
    return shanghaiKlineTimeFormatter.format(new Date(time * 1000));
  }
  if (time && typeof time === "object" && "year" in time) {
    const month = String(time.month).padStart(2, "0");
    const day = String(time.day).padStart(2, "0");
    return `${time.year}-${month}-${day}`;
  }
  return "";
}

function fmtMoney(value, currency = "USD", digits = 2) {
  const num = Number(value || 0);
  return `${currency} ${num.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })}`;
}

function fmtSignedMoney(value, currency = "USD", digits = 2) {
  const num = Number(value || 0);
  const sign = num >= 0 ? "+" : "-";
  return `${sign}${fmtMoney(Math.abs(num), currency, digits)}`;
}

function fmtPct(value) {
  const num = Number(value || 0);
  return `${num >= 0 ? "+" : ""}${num.toFixed(2)}%`;
}

function displayAssetLabel(label, symbol = "") {
  const normalizedSymbol = String(symbol || "").trim().toUpperCase();
  const normalizedLabel = String(label || "").trim();
  if (normalizedSymbol === "SGOV" || ["短债", "短债(SGOV)", "短债（SGOV）"].includes(normalizedLabel)) {
    return "SGOV";
  }
  return label;
}

const BALANCE_FIELD_LABELS = {
  cash_usd: "USD 现金",
  cash_cny: "CNY 现金",
  cash_cost_basis_usd: "USD 现金成本基准",
  cash_cost_basis_cny: "CNY 现金成本基准",
  realized_usd: "USD 已变现",
  realized_cny: "CNY 已变现",
  voo_dividend_usd: "VOO 累计分红",
  sgov_dividend_usd: "SGOV 股息",
};

function parseAmountInput(value, label = "金额") {
  const raw = String(value ?? "").trim();
  if (!raw) return 0;
  const normalized = raw.replace(/[,\s，]/g, "");
  const amount = Number(normalized);
  if (!Number.isFinite(amount)) {
    throw new Error(`${label} 不是有效数字：${raw}`);
  }
  return amount;
}

async function readApiError(response, fallback) {
  const text = await response.text().catch(() => "");
  if (!text) return fallback;
  try {
    const body = JSON.parse(text);
    if (typeof body?.detail === "string") return body.detail;
    if (Array.isArray(body?.detail)) {
      return body.detail
        .map((item) => `${(item.loc || []).join(".")}: ${item.msg || item.type || "请求无效"}`)
        .join("；");
    }
  } catch {
    return text;
  }
  return fallback;
}

function buildBalancesPayload(balanceInputs) {
  return Object.fromEntries(Object.entries(balanceInputs).map(([key, value]) => [
    key,
    parseAmountInput(value, BALANCE_FIELD_LABELS[key] || key),
  ]));
}

function chartPriceDigits(symbol) {
  const normalized = String(symbol || "").toUpperCase();
  return normalized === "510330.SS" || /^\d{6}\.(SS|SZ)$/.test(normalized) ? 3 : 2;
}

function fmtChartPrice(value, symbol) {
  const num = Number(value || 0);
  return num.toFixed(chartPriceDigits(symbol));
}

function dailyAmount(value, pct) {
  const currentValue = Number(value || 0);
  const dailyPct = Number(pct || 0);
  const ratio = dailyPct / 100;
  if (!Number.isFinite(currentValue) || !Number.isFinite(ratio) || ratio <= -0.9999) return 0;
  return currentValue - currentValue / (1 + ratio);
}

function fmtCardPriceLine(value) {
  return String(value || "");
}

function fmtCostChange(trade, currency = "USD") {
  const prev = Number(trade?.prev_avg_cost);
  const next = Number(trade?.new_avg_cost);
  if (Number.isFinite(prev) && Number.isFinite(next) && !(prev === 0 && next === 0)) {
    const digits = currency === "USD" ? 2 : 4;
    return `${fmtMoney(prev, currency, digits)} -> ${fmtMoney(next, currency, digits)}`;
  }
  return "-";
}

function tradeCostDelta(trade) {
  const prev = Number(trade?.prev_avg_cost);
  const next = Number(trade?.new_avg_cost);
  if (!Number.isFinite(prev) || !Number.isFinite(next)) return 0;
  return next - prev;
}

function fmtCurrentPrice(value, currency = "USD") {
  const price = Number(value);
  if (!Number.isFinite(price) || price <= 0) return "-";
  return fmtMoney(price, currency, currency === "USD" ? 2 : 4);
}

function tradeCostTone(trade) {
  const delta = tradeCostDelta(trade);
  if (delta < 0) return "up";
  if (delta > 0) return "down";
  return "flat";
}

function fmtAvgCostChangeTitle(trade, currency = "USD") {
  const prev = Number(trade?.prev_avg_cost || 0);
  const next = Number(trade?.new_avg_cost || 0);
  if (!Number.isFinite(prev) || !Number.isFinite(next) || (prev === 0 && next === 0)) return "";
  const digits = currency === "USD" ? 2 : 4;
  return `均价 ${fmtMoney(prev, currency, digits)} -> ${fmtMoney(next, currency, digits)}`;
}

function fmtTradeCloseEffect(trade, currency = "USD") {
  const value = Number(trade?.close_effect);
  if (!Number.isFinite(value)) return "-";
  return fmtMoney(value, currency);
}

function tone(value) {
  const num = Number(value || 0);
  if (num > 0) return "up";
  if (num < 0) return "down";
  return "flat";
}

function TreemapPriceLine({ priceLine = "", regularPrice, extendedPrice, currency = "USD", regularPct = 0, extendedPct = null }) {
  const legacyMatch = String(priceLine || "").trim().match(
    /^(USD|CNY)\s+([\d,]+(?:\.\d+)?)(?:（([\d,]+(?:\.\d+)?)）)?$/,
  );
  const resolvedCurrency = legacyMatch?.[1] || currency;
  const legacyRegular = legacyMatch?.[2] ? Number(legacyMatch[2].replaceAll(",", "")) : null;
  const legacyExtended = legacyMatch?.[3] ? Number(legacyMatch[3].replaceAll(",", "")) : null;
  const regular = Number.isFinite(Number(regularPrice)) && Number(regularPrice) > 0
    ? Number(regularPrice)
    : legacyRegular;
  if (!Number.isFinite(regular) || regular <= 0) return "-";
  const digits = resolvedCurrency === "USD" ? 2 : 4;
  const formatPrice = (value) => Number(value).toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
  const extended = Number.isFinite(Number(extendedPrice)) && Number(extendedPrice) > 0
    ? Number(extendedPrice)
    : legacyExtended;
  const hasExtended = Number.isFinite(extended) && extended > 0;
  return (
    <>
      <span className={tone(regularPct)}>{resolvedCurrency} {formatPrice(regular)}</span>
      {hasExtended ? <span className={tone(extendedPct)}>（{formatPrice(extended)}）</span> : null}
    </>
  );
}

function tierClass(intensity) {
  return {
    manual_review_only: "tierManual",
    normal: "tierNormal",
    small: "tierSmall",
    medium: "tierMedium",
    large: "tierLarge",
  }[String(intensity || "").toLowerCase()] || "tierNone";
}

function tierLabel(intensity) {
  return {
    none: "-",
    normal: "正常",
    small: "小加",
    medium: "中加",
    large: "大加",
    manual_review_only: "人工复核",
    sell: "卖出",
  }[String(intensity || "none").toLowerCase()] || String(intensity || "-");
}

function rebalanceTierLabel(row) {
  const label = tierLabel(row.intensity);
  if (String(row.intensity || "").toLowerCase() === "sell") return label;
  if (String(row.intensity || "").toLowerCase() === "manual_review_only") return label;
  const share = Number(row.signal_multiplier || 0);
  const formatted = share.toFixed(3).replace(/\.0+$/, "").replace(/(\.\d*[1-9])0+$/, "$1");
  return `${label} ${formatted}x`;
}

function globalKlineColumns(width = window.innerWidth) {
  if (width >= 1500) return 5;
  if (width >= 1100) return 4;
  if (width >= 760) return 2;
  return 1;
}

function peTone(row) {
  if (row.symbol === "VOO" || row.symbol === "QQQ") return "";
  const ps = Number(row.forward_ps ?? row.ps);
  const psMatch = String(row.ps_band || "").match(/^\s*([0-9]+(?:\.[0-9]+)?)\s*-\s*([0-9]+(?:\.[0-9]+)?)\s*$/);
  if (Number.isFinite(ps) && psMatch) {
    const low = Number(psMatch[1]);
    const high = Number(psMatch[2]);
    if (ps > high) return "down";
    if (ps < low) return "up";
  }
  if (row.pe_judgment === "偏贵") return "down";
  if (row.pe_judgment === "偏低") return "up";
  return "";
}

function valuationLabel(row) {
  if (row.symbol === "VOO" || row.symbol === "QQQ") {
    return row.recent_5d_pct == null ? "-" : `5日 ${fmtPct(row.recent_5d_pct)}`;
  }
  if (row.forward_ps || row.ps) return `PS ${(row.forward_ps ?? row.ps).toFixed(2)}`;
  return row.forward_pe ? row.forward_pe.toFixed(2) : "-";
}

function valuationTooltip(row) {
  if (row.symbol === "VOO" || row.symbol === "QQQ") {
    return `近5日涨跌：${row.recent_5d_pct == null ? "-" : fmtPct(row.recent_5d_pct)}`;
  }
  if (row.symbol === "TEM") {
    const ps = Number(row.forward_ps ?? row.ps);
    return `PS：${Number.isFinite(ps) ? ps.toFixed(2) : "-"}\nPS区间：${row.ps_band || "-"}`;
  }
  const pe = Number(row.forward_pe);
  return [
    `Forward PE：${Number.isFinite(pe) ? pe.toFixed(2) : "-"}`,
    `PE区间：${row.pe_band || "-"}`,
    `PEG：${pegLabel(row)}`,
    `PEG区间：${row.peg_band || "-"}`,
  ].join("\n");
}

function pegLabel(row) {
  const peg = Number(row.peg);
  return Number.isFinite(peg) && peg > 0 ? peg.toFixed(2) : "-";
}

function pegTone(row) {
  const peg = Number(row.peg);
  const match = String(row.peg_band || "").match(/^\s*([0-9]+(?:\.[0-9]+)?)\s*-\s*([0-9]+(?:\.[0-9]+)?)\s*$/);
  if (!Number.isFinite(peg) || !match) return "";
  const low = Number(match[1]);
  const high = Number(match[2]);
  if (peg > high) return "down";
  if (peg < low) return "up";
  return "";
}

function valuationBandTone(value, bandText) {
  const num = Number(value);
  const match = String(bandText || "").match(/^\s*([0-9]+(?:\.[0-9]+)?)\s*-\s*([0-9]+(?:\.[0-9]+)?)\s*$/);
  if (!Number.isFinite(num) || !match) return "flat";
  const low = Number(match[1]);
  const high = Number(match[2]);
  if (num > high) return "down";
  if (num < low) return "up";
  return "flat";
}

function useDashboard() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const loadingRef = useRef(false);

  async function load() {
    if (loadingRef.current) return;
    loadingRef.current = true;
    try {
      setError("");
      const response = await fetch(`${API_BASE}/api/dashboard?user_id=evan`);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      setData(await response.json());
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      loadingRef.current = false;
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
    const id = window.setInterval(load, 1000);
    return () => window.clearInterval(id);
  }, []);

  return { data, loading, error, load };
}

function useTableGestureScroll() {
  useEffect(() => {
    function onWheel(event) {
      const wrap = event.target?.closest?.(".tableWrap");
      if (!wrap) return;
      const maxScrollLeft = wrap.scrollWidth - wrap.clientWidth;
      if (maxScrollLeft <= 1) return;
      const targetTag = String(event.target?.tagName || "").toLowerCase();
      if (["input", "select", "textarea", "button"].includes(targetTag)) return;

      const horizontalDelta = Math.abs(event.deltaX) > Math.abs(event.deltaY) ? event.deltaX : 0;
      const shiftedDelta = event.shiftKey && Math.abs(event.deltaY) > 0 ? event.deltaY : 0;
      const delta = horizontalDelta || shiftedDelta;
      if (!delta) return;

      const next = Math.max(0, Math.min(maxScrollLeft, wrap.scrollLeft + delta));
      if (Math.abs(next - wrap.scrollLeft) < 0.5) return;
      event.preventDefault();
      wrap.scrollLeft = next;
    }

    document.addEventListener("wheel", onWheel, { passive: false });
    return () => document.removeEventListener("wheel", onWheel);
  }, []);
}

function Header({ data }) {
  const market = data?.market;
  return (
    <header className="topbar">
      <div>
        <h1>Investment Dashboard</h1>
        <div className="muted">
          {market ? `行情源 ${market.provider} · ${market.fetched_at}` : "正在连接后端"}
        </div>
      </div>
      <button className="iconButton" onClick={() => window.location.reload()} title="刷新网页">
        <RefreshCcw size={18} />
      </button>
    </header>
  );
}

function PageNav({ page, setPage }) {
  const items = [
    ["dashboard", "主页"],
    ["holdings", "持仓"],
    ["rebalance", "平仓"],
    ["kline", "看板"],
  ];
  const activeIndex = Math.max(0, items.findIndex(([key]) => key === page));
  return (
    <nav className="pageNav" style={{ "--active-index": activeIndex }}>
      {items.map(([key, label]) => (
        <button key={key} className={page === key ? "active" : ""} onClick={() => setPage(key)}>
          {label}
        </button>
      ))}
    </nav>
  );
}

function SummaryBreakdownTooltip({ title, rows, currency, total, showContribution = false }) {
  const sortedRows = [...(rows || [])]
    .filter((row) => Math.abs(Number(row.amount || 0)) > 0.000001)
    .sort((left, right) => Math.abs(Number(right.amount || 0)) - Math.abs(Number(left.amount || 0)));
  return (
    <div className="summaryBreakdownTooltip" role="tooltip">
      <div className="summaryBreakdownHeader">
        <b>{title}</b>
        <small>按绝对值排序</small>
      </div>
      <div className={`summaryBreakdownColumns ${showContribution ? "withContribution" : ""}`}>
        <span>标的</span><span>金额</span>{showContribution ? <span>贡献</span> : <span>盈亏比例</span>}
      </div>
      <div className="summaryBreakdownRows">
        {sortedRows.length ? sortedRows.map((row, index) => (
          <div className={`summaryBreakdownRow ${showContribution ? "withContribution" : ""}`} key={`${title}-${row.symbol}-${index}`}>
            <span>{row.symbol}</span>
            <strong className={tone(row.amount)}>{fmtMoney(row.amount, currency)}</strong>
            {showContribution
              ? <em className={tone(row.contributionPct)}>{fmtPct(row.contributionPct)}</em>
              : <em className={tone(row.pct)}>{row.pct == null ? "" : fmtPct(row.pct)}</em>}
          </div>
        )) : <div className="summaryBreakdownEmpty">暂无明细</div>}
      </div>
      <div className="summaryBreakdownTotal">
        <span>合计</span>
        <strong className={tone(total)}>{fmtMoney(total, currency)}</strong>
      </div>
    </div>
  );
}

function Summary({ data }) {
  const summary = data.summary;
  const dailyAsOfLabel = summary.daily_carried_forward && summary.daily_as_of
    ? ` · 截至 ${String(summary.daily_as_of).slice(5).replace("-", "/")}`
    : "";
  const fx = Number(summary.fx || 0);
  const usdRows = (data.holdings || []).filter((row) => row.currency === "USD");
  const usdHoldingValue = usdRows.reduce((sum, row) => sum + Number(row.value || 0), 0);
  const usdHoldingCost = usdRows.reduce((sum, row) => sum + Number(row.shares || 0) * Number(row.avg_cost || 0), 0);
  const usdCash = Number(data.balances?.cash_usd || 0);
  const usdTotalAssets = usdHoldingValue + usdCash;
  const archivedPnlRows = Object.values(data.archived_pnl || {});
  const archivedPnlUsd = archivedPnlRows.reduce((sum, row) => sum + Number(row.pnl_usd || 0), 0);
  const includedArchivedPnlUsd = archivedPnlRows
    .filter((row) => Boolean(row.included_in_realized))
    .reduce((sum, row) => sum + Number(row.pnl_usd || 0), 0);
  const realizedTradeUsd = Number(data.balances?.realized_usd || 0);
  const realizedTradeCny = Number(data.balances?.realized_cny || 0);
  const usdPnl = Number.isFinite(Number(summary.usd_pnl_usd))
    ? Number(summary.usd_pnl_usd)
    : usdRows.reduce((sum, row) => sum + Number(row.pnl || 0), 0) + archivedPnlUsd;
  const usdPnlPct = usdHoldingCost > 0 ? (usdPnl / usdHoldingCost) * 100 : 0;
  const usdDailyChange = usdRows.reduce(
    (total, row) => total + dailyAmount(row.value, row.effective_daily_pct),
    0,
  );
  const usdDailyPct = usdHoldingValue - usdDailyChange > 0
    ? (usdDailyChange / (usdHoldingValue - usdDailyChange)) * 100
    : 0;
  const weightedDailyChangeCny = Number.isFinite(Number(summary.weighted_daily_change_cny))
    ? Number(summary.weighted_daily_change_cny)
    : (data.holdings || []).reduce(
        (total, row) => total + dailyAmount(row.value_cny, row.effective_daily_pct),
        0,
      );
  const usdHoldingPnlDetails = usdRows.map((row) => ({
    symbol: row.symbol,
    amount: Number(row.pnl || 0),
    pct: Number(row.pnl_pct || 0),
    contributionPct: usdHoldingCost > 0
      ? Number(row.pnl || 0) / usdHoldingCost * 100
      : 0,
  }));
  archivedPnlRows.forEach((row) => {
    const amount = Number(row.pnl_usd || 0);
    usdHoldingPnlDetails.push({
      symbol: `${row.label || row.symbol}*`,
      amount,
      pct: null,
      contributionPct: usdHoldingCost > 0 ? amount / usdHoldingCost * 100 : 0,
    });
  });
  const otherRealizedTradeUsd = realizedTradeUsd - includedArchivedPnlUsd;
  if (Math.abs(otherRealizedTradeUsd) > 0.000001) {
    usdHoldingPnlDetails.push({
      symbol: "其他已变现交易",
      amount: otherRealizedTradeUsd,
      pct: null,
      contributionPct: usdHoldingCost > 0 ? otherRealizedTradeUsd / usdHoldingCost * 100 : 0,
    });
  }
  const totalReturnBasisCny = Number(summary.total_return_basis_cny || 0);
  const totalHoldingPnlDetails = (data.holdings || []).map((row) => ({
    symbol: row.symbol,
    amount: Number(row.pnl_cny || 0),
    pct: Number(row.pnl_pct || 0),
    contributionPct: totalReturnBasisCny > 0
      ? Number(row.pnl_cny || 0) / totalReturnBasisCny * 100
      : 0,
  }));
  archivedPnlRows.forEach((row) => {
    const amount = Number(row.pnl_usd || 0) * fx;
    totalHoldingPnlDetails.push({
      symbol: `${row.label || row.symbol}*`,
      amount,
      pct: null,
      contributionPct: totalReturnBasisCny > 0 ? amount / totalReturnBasisCny * 100 : 0,
    });
  });
  const realizedTradeTotalCny = realizedTradeCny + otherRealizedTradeUsd * fx;
  if (Math.abs(realizedTradeTotalCny) > 0.000001) {
    totalHoldingPnlDetails.push({
      symbol: "其他已变现交易",
      amount: realizedTradeTotalCny,
      pct: null,
      contributionPct: totalReturnBasisCny > 0
        ? realizedTradeTotalCny / totalReturnBasisCny * 100
        : 0,
    });
  }
  if (Math.abs(Number(summary.usd_cash_fx_pnl_cny || 0)) > 0.000001) {
    totalHoldingPnlDetails.push({
      symbol: "美元现金汇兑",
      amount: Number(summary.usd_cash_fx_pnl_cny),
      pct: null,
      contributionPct: totalReturnBasisCny > 0
        ? Number(summary.usd_cash_fx_pnl_cny) / totalReturnBasisCny * 100
        : 0,
    });
  }
  const usdDailyDetails = usdRows.map((row) => ({
    symbol: row.symbol,
    amount: dailyAmount(row.value, row.effective_daily_pct),
    pct: Number(row.effective_daily_pct || 0),
  }));
  const usdDailyBasis = usdHoldingValue - usdDailyChange;
  usdDailyDetails.forEach((row) => {
    row.contributionPct = usdDailyBasis > 0 ? Number(row.amount || 0) / usdDailyBasis * 100 : 0;
  });
  const totalDailyDetails = (data.holdings || []).map((row) => ({
    symbol: row.symbol,
    amount: dailyAmount(row.value_cny, row.effective_daily_pct),
    pct: Number(row.effective_daily_pct || 0),
  }));
  const securityDailyTotalCny = totalDailyDetails.reduce(
    (sum, row) => sum + Number(row.amount || 0),
    0,
  );
  const dailyFxPnlCny = Number(summary.daily_fx_pnl_cny || 0);
  if (Math.abs(dailyFxPnlCny) > 0.000001) {
    totalDailyDetails.push({
      symbol: "USD/CNY汇率",
      amount: dailyFxPnlCny,
      pct: null,
    });
  }
  const unexplainedDailyBridgeCny = weightedDailyChangeCny - securityDailyTotalCny - dailyFxPnlCny;
  if (Math.abs(unexplainedDailyBridgeCny) > 0.01) {
    totalDailyDetails.push({
      symbol: "其他桥接调整",
      amount: unexplainedDailyBridgeCny,
      pct: null,
    });
  }
  const totalDailyBasis = Number(summary.total_value_cny || 0) - weightedDailyChangeCny;
  totalDailyDetails.forEach((row) => {
    row.contributionPct = totalDailyBasis > 0 ? Number(row.amount || 0) / totalDailyBasis * 100 : 0;
  });
  return (
    <section className="summaryGrid">
      <div className="summaryAssetGroup">
        <div className="summaryRowLabel">美元资产</div>
        <div className="summaryAssetMetrics">
          <div className="summaryItem">
            <span>资产规模</span>
            <strong>{fmtMoney(usdTotalAssets, "USD")}</strong>
          </div>
          <div className="summaryItem hasSummaryBreakdown" tabIndex="0">
            <span>持仓盈亏</span>
            <strong className={tone(usdPnl)}>
              {fmtMoney(usdPnl, "USD")} · {fmtPct(usdPnlPct)}
            </strong>
            <SummaryBreakdownTooltip title="美元持仓盈亏明细" rows={usdHoldingPnlDetails} currency="USD" total={usdPnl} showContribution />
          </div>
          <div className="summaryItem hasSummaryBreakdown" tabIndex="0">
            <span>当日加权{dailyAsOfLabel}</span>
            <strong className={tone(usdDailyChange)}>
              {fmtMoney(usdDailyChange, "USD")} · {fmtPct(usdDailyPct)}
            </strong>
            <SummaryBreakdownTooltip title="美元当日盈亏明细" rows={usdDailyDetails} currency="USD" total={usdDailyChange} showContribution />
          </div>
        </div>
      </div>
      <div className="summaryAssetGroup">
        <div className="summaryRowLabel">总资产</div>
        <div className="summaryAssetMetrics">
          <div className="summaryItem">
            <span>资产规模</span>
            <strong>{fmtMoney(summary.total_assets_cny, "CNY")}</strong>
          </div>
          <div className="summaryItem hasSummaryBreakdown" tabIndex="0">
            <span>持仓盈亏</span>
            <strong className={tone(summary.total_pnl_cny)}>
              {fmtMoney(summary.total_pnl_cny, "CNY")} · {fmtPct(summary.total_pnl_pct)}
            </strong>
            <SummaryBreakdownTooltip title="总资产持仓盈亏明细" rows={totalHoldingPnlDetails} currency="CNY" total={summary.total_pnl_cny} showContribution />
          </div>
          <div className="summaryItem hasSummaryBreakdown" tabIndex="0">
            <span>当日加权{dailyAsOfLabel}</span>
            <strong className={tone(weightedDailyChangeCny)}>
              {fmtMoney(weightedDailyChangeCny, "CNY")} · {fmtPct(summary.weighted_daily_pct)}
            </strong>
            <SummaryBreakdownTooltip title="总资产当日盈亏明细" rows={totalDailyDetails} currency="CNY" total={weightedDailyChangeCny} showContribution />
          </div>
        </div>
      </div>
      <div className="summaryItem fxSummaryItem">
        <span>汇率</span>
        <strong>{fx.toFixed(4)}</strong>
        <em>成本 {Number(summary.avg_fx_rate || fx).toFixed(4)}</em>
      </div>
    </section>
  );
}

function DailyCards({ cards }) {
  return (
    <section className="cardGrid">
      {cards.map((card) => {
        const fundPending = card.symbol === "001015" && ["pending", "preopen"].includes(card.daily_status);
        const fundStatusText = card.daily_status === "pending" ? "待更新" : card.daily_status === "preopen" ? "未开盘" : "";
        const fundEstimateTag = card.symbol === "001015" && card.daily_status === "estimated" ? "（估值）" : "";
        const regularPct = Number(card.regular_pct ?? 0);
        const extendedPct = Number(card.extended_pct);
        const hasDistinctExtendedPct = card.symbol !== "001015" && card.extended_pct != null && Math.abs(extendedPct - regularPct) > 0.0001;
        const regularUsd = Number(card.regular_change_usd ?? card.change_usd ?? 0);
        const regularCny = Number(card.regular_change_cny ?? card.change_cny ?? 0);
        const extendedUsd = Number(card.extended_change_usd);
        const extendedCny = Number(card.extended_change_cny);
      const hasDistinctExtendedUsd = card.session !== "regular" && card.extended_change_usd != null && Math.abs(extendedUsd - regularUsd) > 0.005;
      const hasDistinctExtendedCny = card.session !== "regular" && card.extended_change_cny != null && Math.abs(extendedCny - regularCny) > 0.005;
        return (
          <article className={`dailyCard ${card.wide ? "wideCard" : ""}`} key={card.symbol}>
            <div className="cardTitle">{displayAssetLabel(card.label, card.symbol)}</div>
            {card.price_line ? <div className="priceLine">{fmtCardPriceLine(card.price_line)}</div> : null}
            <div className={fundPending ? "flat" : tone(regularPct)}>
              {fundPending ? fundStatusText : `${fmtPct(regularPct)}${fundEstimateTag}`}
              {hasDistinctExtendedPct ? <span className={tone(extendedPct)}>（{fmtPct(extendedPct)}）</span> : null}
            </div>
            <div className={fundPending ? "flat" : tone(regularUsd)}>
              {fundPending ? "--" : fmtMoney(regularUsd, "USD")}
              {hasDistinctExtendedUsd ? <span className={tone(extendedUsd)}>（{extendedUsd.toFixed(2)}）</span> : null}
            </div>
            <div className={fundPending ? "flat" : tone(regularCny)}>
              {fundPending ? "--" : fmtMoney(regularCny, "CNY")}
              {hasDistinctExtendedCny ? <span className={tone(extendedCny)}>（{extendedCny.toFixed(2)}）</span> : null}
            </div>
          </article>
        );
      })}
    </section>
  );
}

function DailyHeatmap({ cards, holdings, dailyAsOf, dailyCarriedForward }) {
  const [satelliteHovered, setSatelliteHovered] = useState(false);
  const [satelliteHoverSymbol, setSatelliteHoverSymbol] = useState(null);
  const [heatmapLayoutHeight, setHeatmapLayoutHeight] = useState(HEATMAP_LAYOUT_HEIGHT);
  const heatmapCanvasRef = useRef(null);
  useEffect(() => {
    const canvas = heatmapCanvasRef.current;
    if (!canvas || typeof ResizeObserver === "undefined") return undefined;
    const updateLayoutHeight = (width) => {
      if (!Number.isFinite(width) || width <= 0) return;
      const capHeight = window.innerWidth > 1240 ? HEATMAP_MAX_CANVAS_HEIGHT : Number.POSITIVE_INFINITY;
      const nextHeight = Number(Math.min(HEATMAP_LAYOUT_HEIGHT, capHeight / width * HEATMAP_LAYOUT_WIDTH).toFixed(3));
      setHeatmapLayoutHeight((current) => Math.abs(current - nextHeight) < 0.05 ? current : nextHeight);
    };
    const observer = new ResizeObserver(([entry]) => updateLayoutHeight(entry?.contentRect?.width));
    observer.observe(canvas);
    updateLayoutHeight(canvas.clientWidth);
    return () => observer.disconnect();
  }, []);
  const satelliteSymbols = useMemo(
    () => new Set(["ISRG", "TEM", "PLTR", "GOOGL", "MSFT", "AVGO", "NVDA"]),
    [],
  );
  const holdingsBySymbol = useMemo(
    () => Object.fromEntries((holdings || []).map((row) => [row.symbol, row])),
    [holdings],
  );
  const totalValue = (holdings || []).reduce((sum, row) => sum + Math.max(0, Number(row.value_cny || 0)), 0);
  const satelliteCards = useMemo(
    () => (cards || []).filter((card) => satelliteSymbols.has(card.symbol)),
    [cards, satelliteSymbols],
  );
  const rows = useMemo(() => {
    const sourceCards = (cards || []).filter((card) => card.symbol !== "SATELLITE");
    const displayCards = sourceCards.filter((card) => !satelliteSymbols.has(card.symbol));
    if (satelliteCards.length) {
      const satelliteValueCny = satelliteCards.reduce((sum, card) => sum + Math.max(0, Number(holdingsBySymbol[card.symbol]?.value_cny || 0)), 0);
      const regularUsd = satelliteCards.reduce((sum, card) => sum + Number(card.regular_change_usd ?? card.change_usd ?? 0), 0);
      const regularCny = satelliteCards.reduce((sum, card) => sum + Number(card.regular_change_cny ?? card.change_cny ?? 0), 0);
      const extendedUsdValues = satelliteCards.filter((card) => card.extended_change_usd != null);
      const extendedCnyValues = satelliteCards.filter((card) => card.extended_change_cny != null);
      const extendedUsd = extendedUsdValues.reduce((sum, card) => sum + Number(card.extended_change_usd || 0), 0);
      const extendedCny = extendedCnyValues.reduce((sum, card) => sum + Number(card.extended_change_cny || 0), 0);
      displayCards.push({
        symbol: "SATELLITE_GROUP",
        label: "卫星仓位",
        session: satelliteCards.every((card) => card.session === "regular") ? "regular" : "extended",
        value_cny: satelliteValueCny,
        regular_change_usd: regularUsd,
        regular_change_cny: regularCny,
        extended_change_usd: extendedUsdValues.length ? extendedUsd : null,
        extended_change_cny: extendedCnyValues.length ? extendedCny : null,
        regular_pct: satelliteValueCny > 0 ? regularCny / satelliteValueCny * 100 : 0,
        extended_pct: satelliteValueCny > 0 && extendedCnyValues.length ? extendedCny / satelliteValueCny * 100 : null,
        effective_pct: satelliteValueCny > 0 ? (regularCny + (extendedCnyValues.length ? extendedCny : 0)) / satelliteValueCny * 100 : 0,
      });
    }
    return displayCards.map((card) => {
    const fundPending = card.symbol === "001015" && ["pending", "preopen"].includes(card.daily_status);
    const fundStatusText = card.daily_status === "pending" ? "待更新" : card.daily_status === "preopen" ? "未开盘" : "";
    const holding = holdingsBySymbol[card.symbol] || {};
    const rawValueCny = Number(holding.value_cny ?? card.value_cny ?? 0);
    const valueCny = Number.isFinite(rawValueCny) ? Math.max(0, rawValueCny) : 0;
    const assetPct = totalValue > 0 ? (valueCny / totalValue) * 100 : 0;
    const rawRegularPct = Number(card.regular_pct ?? 0);
    const regularPct = Number.isFinite(rawRegularPct) ? rawRegularPct : 0;
    const rawExtendedPct = card.extended_pct == null ? null : Number(card.extended_pct);
    const extendedPct = rawExtendedPct != null && Number.isFinite(rawExtendedPct) ? rawExtendedPct : null;
    const hasDistinctExtendedPct = card.session !== "regular" && card.symbol !== "001015" && extendedPct != null && Math.abs(extendedPct - regularPct) > 0.0001;
    const regularUsd = Number(card.regular_change_usd ?? card.change_usd ?? 0);
    const regularCny = Number(card.regular_change_cny ?? card.change_cny ?? 0);
    const extendedUsd = Number(card.extended_change_usd);
    const extendedCny = Number(card.extended_change_cny);
      const hasDistinctExtendedUsd = card.session !== "regular" && card.extended_change_usd != null && Math.abs(extendedUsd - regularUsd) > 0.005;
      const hasDistinctExtendedCny = card.session !== "regular" && card.extended_change_cny != null && Math.abs(extendedCny - regularCny) > 0.005;
    const rawDailyPct = Number(card.effective_pct ?? card.extended_pct ?? card.regular_pct ?? 0);
    const dailyPct = Number.isFinite(rawDailyPct) ? rawDailyPct : 0;
    const magnitude = Math.min(1, Math.abs(dailyPct) / 4);
    const strength = 0.18 + magnitude * 0.72;
    const bg = dailyPct > 0
      ? `linear-gradient(145deg, rgba(22, 101, 52, ${strength}), rgba(15, 47, 46, ${0.82 + magnitude * 0.18}))`
      : dailyPct < 0
        ? `linear-gradient(145deg, rgba(127, 29, 29, ${strength}), rgba(42, 24, 37, ${0.82 + magnitude * 0.18}))`
        : "linear-gradient(145deg, #15263d, #10233a)";
      return {
        ...card,
        valueCny,
        assetPct,
        currentPrice: holding.price,
        currency: holding.currency || card.currency || "USD",
        regularPct,
        extendedPct,
        hasDistinctExtendedPct,
        regularUsd,
        regularCny,
        extendedUsd,
        extendedCny,
        hasDistinctExtendedUsd,
        hasDistinctExtendedCny,
        dailyPct,
        bg,
        magnitude,
        fundPending,
        fundStatusText,
      };
    });
  }, [cards, holdingsBySymbol, satelliteCards, satelliteSymbols, totalValue]);
  const satelliteHoverRects = useMemo(() => {
    if (!satelliteCards.length) return [];
    const root = hierarchy({
      children: satelliteCards.map((card) => ({
        ...card,
        currentPrice: holdingsBySymbol[card.symbol]?.price,
        currency: holdingsBySymbol[card.symbol]?.currency || "USD",
        layoutValue: Math.max(0, Number(holdingsBySymbol[card.symbol]?.value_cny || 0)),
      })),
    })
      .sum((item) => item.layoutValue || 0)
      .sort((a, b) => (b.value || 0) - (a.value || 0));
    treemap()
      .tile(treemapSquarify.ratio(1))
      .size([SATELLITE_HOVER_LAYOUT_WIDTH, SATELLITE_HOVER_LAYOUT_HEIGHT])
      .paddingInner(0.8)
      .round(false)(root);
    return root.leaves().map((leaf) => {
      const card = leaf.data;
      const regularPct = Number(card.regular_pct || 0);
      const extendedPct = card.extended_pct == null ? null : Number(card.extended_pct);
      const effectivePct = Number.isFinite(Number(card.effective_pct))
        ? Number(card.effective_pct)
        : (extendedPct != null && Number.isFinite(extendedPct) ? ((1 + regularPct / 100) * (1 + extendedPct / 100) - 1) * 100 : regularPct);
      const magnitude = Math.min(1, Math.abs(effectivePct) / 4);
      const strength = 0.18 + magnitude * 0.72;
      const bg = effectivePct > 0
        ? `linear-gradient(145deg, rgba(22, 101, 52, ${strength}), rgba(15, 47, 46, ${0.82 + magnitude * 0.18}))`
        : effectivePct < 0
          ? `linear-gradient(145deg, rgba(127, 29, 29, ${strength}), rgba(42, 24, 37, ${0.82 + magnitude * 0.18}))`
          : "linear-gradient(145deg, #15263d, #10233a)";
      return {
      ...card,
      effectivePct,
      magnitude,
      bg,
      x: leaf.x0,
      y: leaf.y0,
      width: leaf.x1 - leaf.x0,
      height: leaf.y1 - leaf.y0,
      };
    });
  }, [holdingsBySymbol, satelliteCards]);
  const minLayoutValue = totalValue > 0 ? totalValue * 0.0025 : 1;
  const rects = useMemo(() => {
    if (!rows.length) return [];
    const root = hierarchy({
      children: rows.map((row) => ({ ...row, layoutValue: Math.max(row.valueCny, minLayoutValue) })),
    })
      .sum((item) => item.layoutValue || 0)
      .sort((a, b) => (b.value || 0) - (a.value || 0));
    treemap()
      .tile(treemapSquarify.ratio(1))
      .size([HEATMAP_LAYOUT_WIDTH, heatmapLayoutHeight])
      .paddingInner(0.7)
      .round(false)(root);
    return root.leaves().map((leaf) => ({
      ...leaf.data,
      x: leaf.x0,
      y: leaf.y0,
      width: leaf.x1 - leaf.x0,
      height: leaf.y1 - leaf.y0,
    }));
  }, [rows, minLayoutValue, heatmapLayoutHeight]);

  return (
    <section className="chartPanel heatmapPanel" onMouseLeave={() => setSatelliteHovered(false)}>
      <div className="heatmapToolbar">
        <h2>当日收益热力图</h2>
        {dailyCarriedForward && dailyAsOf ? <span>涨跌截至 {String(dailyAsOf).slice(5).replace("-", "/")}</span> : null}
      </div>
      {satelliteHovered ? (
        <div className="satelliteHoverPanel" onMouseEnter={() => setSatelliteHovered(true)}>
          <strong>卫星仓位明细</strong>
          <div className="satelliteMiniCanvas">
            {satelliteHoverRects.map((card) => (
              <div
                className="satelliteMiniCell"
                key={card.symbol}
                style={{
                  left: `${card.x / SATELLITE_HOVER_LAYOUT_WIDTH * 100}%`,
                  top: `${card.y / SATELLITE_HOVER_LAYOUT_HEIGHT * 100}%`,
                  width: `${card.width / SATELLITE_HOVER_LAYOUT_WIDTH * 100}%`,
                  height: `${card.height / SATELLITE_HOVER_LAYOUT_HEIGHT * 100}%`,
                  "--mini-bg": card.bg,
                  "--mini-border": card.effectivePct > 0 ? `rgba(52, 211, 153, ${0.3 + card.magnitude * 0.45})` : card.effectivePct < 0 ? `rgba(248, 113, 113, ${0.3 + card.magnitude * 0.45})` : "rgba(148, 163, 184, 0.28)",
                }}
                onMouseEnter={() => setSatelliteHoverSymbol(card.symbol)}
                onMouseLeave={() => setSatelliteHoverSymbol(null)}
                title={`${displayAssetLabel(card.label, card.symbol)} · 当前价 ${fmtCurrentPrice(card.currentPrice, card.currency)} · 收盘 ${fmtPct(card.regular_pct)}${card.session !== "regular" && card.extended_pct != null ? ` · 拓展盘 ${fmtPct(card.extended_pct)}` : ""} · 综合 ${fmtPct(card.effectivePct)}`}
              >
                <b>{displayAssetLabel(card.label, card.symbol)}</b>
                {card.regular_price || card.price_line ? (
                  <div className="heatPrice">
                    <TreemapPriceLine
                      priceLine={card.price_line}
                      regularPrice={card.regular_price}
                      extendedPrice={card.extended_price}
                      currency={card.currency}
                      regularPct={card.regular_pct}
                      extendedPct={card.extended_pct}
                    />
                  </div>
                ) : null}
                <span className={tone(card.regular_pct)}>
                  {fmtPct(card.regular_pct)}
                  {card.session !== "regular" && card.extended_pct != null ? (
                    <span className={tone(card.extended_pct)}>（{fmtPct(card.extended_pct)}）</span>
                  ) : null}
                </span>
                <span className={tone(card.regular_change_usd ?? card.change_usd)}>
                  {fmtMoney(card.regular_change_usd ?? card.change_usd ?? 0, "USD")}
                  {card.session !== "regular" && card.extended_change_usd != null ? (
                    <span className={tone(card.extended_change_usd)}>（{fmtMoney(card.extended_change_usd, "USD")}）</span>
                  ) : null}
                </span>
                <span className={tone(card.regular_change_cny ?? card.change_cny)}>
                  {fmtMoney(card.regular_change_cny ?? card.change_cny ?? 0, "CNY")}
                  {card.session !== "regular" && card.extended_change_cny != null ? (
                    <span className={tone(card.extended_change_cny)}>（{fmtMoney(card.extended_change_cny, "CNY")}）</span>
                  ) : null}
                </span>
              </div>
            ))}
          </div>
          {satelliteHoverSymbol ? (() => {
            const card = satelliteHoverRects.find((item) => item.symbol === satelliteHoverSymbol);
            if (!card) return null;
            return (
              <div className="satelliteMiniTooltip">
                <b>{displayAssetLabel(card.label, card.symbol)}</b>
                <span className="satelliteTooltipPrice">
                  价格&nbsp;
                  <TreemapPriceLine
                    priceLine={card.price_line}
                    regularPrice={card.regular_price}
                    extendedPrice={card.extended_price}
                    currency={card.currency}
                    regularPct={card.regular_pct}
                    extendedPct={card.extended_pct}
                  />
                </span>
                <span>收盘 {fmtPct(card.regular_pct)}{card.session !== "regular" && card.extended_pct != null ? ` · 拓展盘 ${fmtPct(card.extended_pct)}` : ""} · 综合 {fmtPct(card.effectivePct)}</span>
                <span>{fmtMoney(card.regular_change_usd ?? card.change_usd ?? 0, "USD")} · {fmtMoney(card.regular_change_cny ?? card.change_cny ?? 0, "CNY")}</span>
              </div>
            );
          })() : null}
        </div>
      ) : null}
      <div
        className="heatmapCanvas"
        ref={heatmapCanvasRef}
        style={{ aspectRatio: `${HEATMAP_LAYOUT_WIDTH} / ${heatmapLayoutHeight}` }}
      >
        {rects.map((row) => {
          return (
            <article
              className={`heatCell ${row.width < 7 || row.height < 7 ? "compact" : ""} ${row.width < 4 || row.height < 4 ? "tiny" : ""}`}
              key={row.symbol}
              style={{
                "--heat-bg": row.bg,
                "--heat-border": row.dailyPct > 0 ? `rgba(52, 211, 153, ${0.24 + row.magnitude * 0.42})` : row.dailyPct < 0 ? `rgba(248, 113, 113, ${0.26 + row.magnitude * 0.44})` : "rgba(148, 163, 184, 0.24)",
                left: `${row.x / HEATMAP_LAYOUT_WIDTH * 100}%`,
                top: `${row.y / heatmapLayoutHeight * 100}%`,
                width: `${row.width / HEATMAP_LAYOUT_WIDTH * 100}%`,
                height: `${row.height / heatmapLayoutHeight * 100}%`,
              }}
              title={`${displayAssetLabel(row.label, row.symbol)} · 资产占比 ${row.assetPct.toFixed(2)}%${row.symbol !== "SATELLITE_GROUP" ? ` · 价格 ${row.price_line ? fmtCardPriceLine(row.price_line) : fmtCurrentPrice(row.currentPrice, row.currency)}` : ""} · 收盘 ${fmtPct(row.regularPct)}${row.hasDistinctExtendedPct ? ` · 拓展盘 ${fmtPct(row.extendedPct)}` : ""} · 当前综合 ${fmtPct(row.dailyPct)}`}
              onMouseEnter={row.symbol === "SATELLITE_GROUP" ? () => setSatelliteHovered(true) : undefined}
              role={row.symbol === "SATELLITE_GROUP" ? "button" : undefined}
            >
              <div className="heatSymbol">{displayAssetLabel(row.label, row.symbol)}</div>
              {row.symbol !== "SATELLITE_GROUP" && (row.regular_price || row.price_line) ? (
                <div className="heatPrice">
                  <TreemapPriceLine
                    priceLine={row.price_line}
                    regularPrice={row.regular_price}
                    extendedPrice={row.extended_price}
                    currency={row.currency}
                    regularPct={row.regularPct}
                    extendedPct={row.extendedPct}
                  />
                </div>
              ) : null}
              <strong className={row.fundPending ? "flat" : tone(row.regularPct)}>
                {row.fundPending ? row.fundStatusText : fmtPct(row.regularPct)}
                {row.hasDistinctExtendedPct ? <span className={tone(row.extendedPct)}>（{fmtPct(row.extendedPct)}）</span> : null}
              </strong>
              <div className={`heatPnl heatPnlUsd ${row.fundPending ? "flat" : tone(row.regularUsd)}`}>
                {row.fundPending ? "--" : fmtMoney(row.regularUsd, "USD")}
                {row.hasDistinctExtendedUsd ? <span className={tone(row.extendedUsd)}>（{fmtMoney(row.extendedUsd, "USD")}）</span> : null}
              </div>
              <div className={`heatPnl heatPnlCny ${row.fundPending ? "flat" : tone(row.regularCny)}`}>
                {row.fundPending ? "--" : fmtMoney(row.regularCny, "CNY")}
                {row.hasDistinctExtendedCny ? <span className={tone(row.extendedCny)}>（{fmtMoney(row.extendedCny, "CNY")}）</span> : null}
              </div>
              <span>{row.assetPct.toFixed(1)}%</span>
            </article>
          );
        })}
      </div>
    </section>
  );
}

function BarList({ title, rows, valueKey, formatValue }) {
  const max = Math.max(1, ...rows.map((row) => Math.abs(Number(row[valueKey] || 0))));
  return (
    <section className="chartPanel rankChartPanel">
      <h2>{title}</h2>
      <div className="barList">
        {rows.map((row) => {
          const value = Number(row[valueKey] || 0);
          return (
            <div className="barRow" key={row.symbol || row.label}>
              <div className="barLabel">{displayAssetLabel(row.label || row.symbol, row.symbol)}</div>
              <div className="barTrack">
                <div className={`barFill ${tone(value)}`} style={{ width: `${Math.max(3, Math.abs(value) / max * 100)}%` }} />
              </div>
              <div className={`barValue ${tone(value)}`}>{formatValue(value, row)}</div>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function CompareBars({ title, rows, amountKey = "current_usd", className = "", alertThresholdPct = null }) {
  const max = Math.max(1, ...rows.flatMap((row) => [Number(row.current_pct || 0), Number(row.target_pct || 0)]));
  const currentTotal = rows.reduce((sum, row) => sum + Number(row.current_pct || 0), 0);
  const targetTotal = rows.reduce((sum, row) => sum + Number(row.target_pct || 0), 0);
  return (
    <section className={`chartPanel verticalChartPanel ${className}`}>
      <h2>{title}</h2>
      <div className="verticalBars">
        {rows.map((row) => (
          <div
            className={`verticalGroup ${Number.isFinite(alertThresholdPct) && Math.abs(Number(row.current_pct || 0) - Number(row.target_pct || 0)) > alertThresholdPct ? "allocationDeviationAlert" : ""}`}
            key={row.key || row.symbol}
            title={`当前 ${Number(row.current_pct || 0).toFixed(2)}% · ${fmtMoney(row[amountKey], "USD")} / 目标 ${Number(row.target_pct || 0).toFixed(2)}%`}
          >
            <div className="verticalPlot">
              <div className={`verticalBar current ${Number.isFinite(alertThresholdPct) && Math.abs(Number(row.current_pct || 0) - Number(row.target_pct || 0)) > alertThresholdPct ? "deviationAlert" : ""}`} style={{ height: `${Number(row.current_pct || 0) / max * 100}%` }}>
                <span>{Number(row.current_pct || 0).toFixed(1)}%</span>
              </div>
              <div className="verticalBar target" style={{ height: `${Number(row.target_pct || 0) / max * 100}%` }}>
                <span>{Number(row.target_pct || 0).toFixed(1)}%</span>
              </div>
            </div>
            <div className="verticalLabel">{displayAssetLabel(row.label, row.symbol)}</div>
            <div className="verticalAmount">{fmtMoney(row[amountKey], "USD")}</div>
          </div>
        ))}
      </div>
      <div className="legendLine">
        <span><i className="legendSwatch current" />当前 {currentTotal.toFixed(1)}%</span>
        <span><i className="legendSwatch target" />目标 {targetTotal.toFixed(1)}%</span>
      </div>
    </section>
  );
}

function Visualizations({ data }) {
  const viz = data.visualizations || {};
  return (
    <section className="visualGrid">
      <BarList title="核心仓位浮盈亏排名" rows={viz.pnl_rank || []} valueKey="pnl_usd" formatValue={(value, row) => row.symbol === "001015" ? fmtMoney(row.pnl_cny, "CNY") : fmtMoney(value, "USD")} />
      <BarList title="卫星仓位浮盈亏排名" rows={viz.satellite_pnl_rank || []} valueKey="pnl" formatValue={(value) => fmtMoney(value, "USD")} />
      <CompareBars title="美元资产配置占比" rows={viz.allocation_compare || []} />
      <CompareBars title="卫星仓位内部占比" rows={viz.satellite_split || []} className="compactVerticalChart" alertThresholdPct={2.5} />
    </section>
  );
}

function withUsdPerformanceFallback(points) {
  let cumulativeUsd = 1;
  return (points || []).map((point, index) => {
    const explicitReturn = Number(point?.usd_return_pct);
    const explicitDaily = Number(point?.usd_daily_pct);
    if (Number.isFinite(explicitReturn)) {
      if (index > 0) cumulativeUsd = 1 + explicitReturn / 100;
      return point;
    }

    if (index === 0) {
      cumulativeUsd = 1;
      return { ...point, usd_return_pct: 0, usd_daily_pct: 0 };
    }

    const snapshot = point?.holdings_snapshot || {};
    const symbolDailyPct = point?.symbol_position_pct || point?.symbol_daily_pct || {};
    let basis = 0;
    let pnl = 0;
    USD_PERFORMANCE_SYMBOLS.forEach((symbol) => {
      const holding = snapshot[symbol];
      const dailyPct = Number(symbolDailyPct[symbol]);
      if (!holding || !Number.isFinite(dailyPct)) return;
      const cost = Number(holding.shares || 0) * Number(holding.avg_cost || 0);
      if (!Number.isFinite(cost) || cost <= 0) return;
      basis += cost;
      pnl += cost * dailyPct / 100;
    });

    if (basis <= 0) return point;
    const fallbackDaily = Number.isFinite(explicitDaily) ? explicitDaily : pnl / basis * 100;
    cumulativeUsd *= 1 + fallbackDaily / 100;
    return {
      ...point,
      usd_daily_pct: fallbackDaily,
      usd_return_pct: (cumulativeUsd - 1) * 100,
    };
  });
}

function PerformanceChart({ history }) {
  const points = useMemo(() => withUsdPerformanceFallback(history?.points || []), [history?.points]);
  const latest = points[points.length - 1];
  const series = useMemo(() => [
    ["portfolio_return_pct", "总资产", TERMINAL_CHART.yellow, 4],
    ["usd_return_pct", "美元资产", TERMINAL_CHART.deepBlue, 3],
    ["001015_return_pct", "沪深300", TERMINAL_CHART.coral, 2],
    ["VOO_return_pct", "VOO", TERMINAL_CHART.violet, 2],
    ["QQQ_return_pct", "QQQ", TERMINAL_CHART.cyan, 2],
  ], []);

  return (
    <section className="chartPanel performancePanel">
      <div className="sectionHeader compactHeader">
        <h2>累计日收益走势</h2>
        <span className="muted">
          单日收益复利累计 · 当日可含估值/夜盘预计 · 投资日 {history?.started_on || "-"} 至 {latest?.date || "-"}
        </span>
      </div>
      <div className="performanceStats">
        {series.map(([key, name, color]) => (
          <div className="performanceStat" key={key} style={{ "--series-color": color }}>
            <span>{name}</span>
            <strong className={tone(latest?.[key])}>{latest?.[key] == null ? "-" : fmtPct(latest[key])}</strong>
          </div>
        ))}
      </div>
      <div className="performancePlot">
        <PerformanceLightweightChart points={points} series={series} />
      </div>
    </section>
  );
}

function PerformanceLightweightChart({ points, series }) {
  const containerRef = useRef(null);
  const chartRef = useRef(null);
  const lineSeriesRef = useRef([]);
  const pointByTimeRef = useRef(new Map());
  const [axisMarkers, setAxisMarkers] = useState([]);
  const [tooltip, setTooltip] = useState({ visible: false, left: 0, top: 0, point: null });

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return undefined;
    const chart = createChart(container, {
      autoSize: true,
      layout: {
        background: { color: "#0b1b2e" },
        textColor: TERMINAL_CHART.textMuted,
        fontFamily: PLOT_FONT,
        fontSize: 12,
      },
      grid: {
        vertLines: { color: "rgba(148, 163, 184, 0.08)" },
        horzLines: { color: "rgba(148, 163, 184, 0.14)" },
      },
      rightPriceScale: {
        borderColor: "rgba(148, 163, 184, 0.22)",
        scaleMargins: { top: 0.12, bottom: 0.14 },
      },
      timeScale: {
        borderColor: "rgba(148, 163, 184, 0.22)",
        timeVisible: false,
        rightOffset: 0,
        fixRightEdge: true,
        tickMarkFormatter: formatLightweightChartTime,
      },
      localization: {
        priceFormatter: (value) => `${Number(value).toFixed(2)}%`,
        timeFormatter: formatLightweightChartTime,
      },
      crosshair: {
        vertLine: { color: "rgba(226, 232, 240, 0.34)", width: 1, style: 2 },
        horzLine: { color: "rgba(226, 232, 240, 0.22)", width: 1, style: 2 },
      },
    });
    chartRef.current = chart;
    lineSeriesRef.current = series.map(([, , color, width], index) => {
      if (index <= 1) {
        const portfolioSeries = chart.addSeries(BaselineSeries, {
          baseValue: { type: "price", price: 0 },
          topLineColor: color,
          topFillColor1: index === 0 ? "rgba(250, 204, 21, 0.52)" : "rgba(37, 99, 235, 0.42)",
          topFillColor2: index === 0 ? "rgba(250, 204, 21, 0.10)" : "rgba(37, 99, 235, 0.08)",
          bottomLineColor: color,
          bottomFillColor1: index === 0 ? "rgba(250, 204, 21, 0.24)" : "rgba(37, 99, 235, 0.22)",
          bottomFillColor2: index === 0 ? "rgba(250, 204, 21, 0.04)" : "rgba(37, 99, 235, 0.04)",
          lineWidth: width,
          priceLineVisible: false,
          lastValueVisible: true,
        });
        return portfolioSeries;
      }
      return chart.addSeries(LineSeries, {
        color,
        lineWidth: width,
        priceLineVisible: false,
        lastValueVisible: true,
      });
    });
    chart.subscribeCrosshairMove((param) => {
      if (!param?.point || !param.time) {
        setTooltip((current) => current.visible ? { ...current, visible: false } : current);
        return;
      }
      const point = pointByTimeRef.current.get(String(param.time));
      if (!point) {
        setTooltip((current) => current.visible ? { ...current, visible: false } : current);
        return;
      }
      const left = Math.min(Math.max(param.point.x + 14, 8), Math.max(8, container.clientWidth - 300));
      const top = Math.min(Math.max(param.point.y + 14, 8), Math.max(8, container.clientHeight - 220));
      setTooltip({ visible: true, left, top, point });
    });
    return () => {
      chartRef.current = null;
      lineSeriesRef.current = [];
      chart.remove();
    };
  }, [series]);

  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;
    pointByTimeRef.current = new Map((points || []).map((point) => [String(point.date), point]));
    lineSeriesRef.current.forEach((line, index) => {
      const [key] = series[index];
      const rows = [];
      for (const point of points || []) {
        const value = Number(point?.[key]);
        if (!point?.date || !Number.isFinite(value)) continue;
        rows.push({ time: point.date, value });
      }
      line.setData(rows);
    });
    function updateAxisMarkers() {
      const markers = (points || [])
        .filter((point) => point?.date && point.cash_flow_flag)
        .map((point) => {
          const coordinate = chart.timeScale().timeToCoordinate(point.date);
          return Number.isFinite(coordinate) ? { date: point.date, left: coordinate } : null;
        })
        .filter(Boolean);
      setAxisMarkers(markers);
    }
    if (points?.length) {
      chart.timeScale().fitContent();
      window.requestAnimationFrame(updateAxisMarkers);
    } else {
      setAxisMarkers([]);
    }
    chart.timeScale().subscribeVisibleTimeRangeChange(updateAxisMarkers);
    return () => chart.timeScale().unsubscribeVisibleTimeRangeChange(updateAxisMarkers);
  }, [points, series]);

  return (
    <div className="performanceLwCanvas" ref={containerRef}>
      <div className="performanceAxisMarkers" aria-hidden="true">
        {axisMarkers.map((marker) => (
          <span className="performanceAxisMarker" key={marker.date} style={{ left: marker.left }}>T</span>
        ))}
      </div>
      {tooltip.visible && tooltip.point ? (
        <div className="performanceTooltip" style={{ left: tooltip.left, top: tooltip.top }}>
          <strong>{tooltip.point.date}</strong>
          <span>
            USD/CNY&nbsp;
            {tooltip.point.fx_rate == null ? "-" : Number(tooltip.point.fx_rate).toFixed(4)}
            {tooltip.point.fx_source ? " · " + tooltip.point.fx_source : ""}
          </span>
          <span className={tone(tooltip.point.usd_pnl_usd)}>
            美元资产持仓盈亏&nbsp;
            {tooltip.point.usd_pnl_usd == null ? "-" : fmtMoney(tooltip.point.usd_pnl_usd, "USD")}
            &nbsp;·&nbsp;{tooltip.point.usd_return_pct == null ? "-" : fmtPct(tooltip.point.usd_return_pct)}
          </span>
          <span className={tone(tooltip.point.usd_daily_pnl_usd)}>
            美元资产当日加权&nbsp;
            {tooltip.point.usd_daily_pnl_usd == null ? "-" : fmtMoney(tooltip.point.usd_daily_pnl_usd, "USD")}
            &nbsp;·&nbsp;{tooltip.point.usd_daily_pct == null ? "-" : fmtPct(tooltip.point.usd_daily_pct)}
          </span>
          <span className={tone(tooltip.point.total_pnl_cny)}>
            总资产持仓盈亏&nbsp;
            {tooltip.point.total_pnl_cny == null ? "-" : fmtMoney(tooltip.point.total_pnl_cny, "CNY")}
            &nbsp;·&nbsp;{tooltip.point.portfolio_return_pct == null ? "-" : fmtPct(tooltip.point.portfolio_return_pct)}
          </span>
          <span className={tone(tooltip.point.total_daily_pnl_cny)}>
            总资产当日加权&nbsp;
            {tooltip.point.total_daily_pnl_cny == null ? "-" : fmtMoney(tooltip.point.total_daily_pnl_cny, "CNY")}
            &nbsp;·&nbsp;{tooltip.point.portfolio_daily_pct == null ? "-" : fmtPct(tooltip.point.portfolio_daily_pct)}
          </span>
          {tooltip.point.cash_flow_flag ? (
            <span>资金流/交易 {fmtMoney(tooltip.point.cash_flow_cny || 0, "CNY")}</span>
          ) : null}
          {series.slice(2).map(([key, name, color]) => (
            <span key={key} style={{ "--series-color": color }}>
              <i />{name} {tooltip.point[key] == null ? "-" : fmtPct(tooltip.point[key])}
            </span>
          ))}
        </div>
      ) : null}
    </div>
  );
}
function LightweightKlineCard({ item, displayRange, onOpenSymbol }) {
  const containerRef = useRef(null);
  const chartRef = useRef(null);
  const candleSeriesRef = useRef(null);
  const volumeSeriesRef = useRef(null);
  const didFitContentRef = useRef(false);
  const dataRangeRef = useRef({ first: null, last: null, length: 0 });
  const { candles, volumes } = useMemo(() => {
    const volumeByTime = new Map((item?.volumes || []).map((bar) => [bar.time, bar]));
    const nextCandles = [];
    const nextVolumes = [];
    let lastTime = null;
    for (const raw of item?.candles || []) {
      const time = normalizeLightweightTime(raw.time);
      const open = Number(raw.open);
      const high = Number(raw.high);
      const low = Number(raw.low);
      const close = Number(raw.close);
      if (time == null || !Number.isFinite(open) || !Number.isFinite(high) || !Number.isFinite(low) || !Number.isFinite(close)) continue;
      if (open <= 0 || high <= 0 || low <= 0 || close <= 0) continue;
      if (lastTime != null && typeof time === typeof lastTime && time <= lastTime) continue;
      const candle = { time, open, high, low, close };
      const volume = volumeByTime.get(raw.time) || volumeByTime.get(time) || {};
      nextCandles.push(candle);
      nextVolumes.push({
        time,
        value: Number.isFinite(Number(volume.value)) ? Number(volume.value) : 0,
        color: volume.color || (close >= open ? "rgba(34, 197, 94, 0.28)" : "rgba(239, 68, 68, 0.28)"),
      });
      lastTime = time;
    }
    const patchedCandles = patchLatestCandle(nextCandles, item?.latest_price, item?.interval);
    if (patchedCandles !== nextCandles && nextVolumes.length) {
      const lastIndex = nextVolumes.length - 1;
      const lastCandle = patchedCandles[patchedCandles.length - 1];
      nextVolumes[lastIndex] = {
        ...nextVolumes[lastIndex],
        color: lastCandle.close >= lastCandle.open ? "rgba(34, 197, 94, 0.28)" : "rgba(239, 68, 68, 0.28)",
      };
    }
    return { candles: patchedCandles, volumes: nextVolumes };
  }, [item, displayRange]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return undefined;
    const chart = createChart(container, {
      autoSize: true,
      layout: {
        background: { color: "#0b1b2e" },
        textColor: TERMINAL_CHART.textMuted,
        fontFamily: PLOT_FONT,
        fontSize: 11,
      },
      grid: {
        vertLines: { color: "rgba(148, 163, 184, 0.08)" },
        horzLines: { color: "rgba(148, 163, 184, 0.12)" },
      },
      rightPriceScale: {
        borderColor: "rgba(148, 163, 184, 0.22)",
        scaleMargins: { top: 0.08, bottom: 0.24 },
      },
      timeScale: {
        borderColor: "rgba(148, 163, 184, 0.22)",
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 2,
        barSpacing: 7,
        tickMarkFormatter: formatLightweightChartTime,
      },
      localization: {
        timeFormatter: formatLightweightChartTime,
      },
      crosshair: {
        vertLine: { color: "rgba(203, 213, 225, 0.35)", labelBackgroundColor: "#1d4ed8" },
        horzLine: { color: "rgba(203, 213, 225, 0.35)", labelBackgroundColor: "#1d4ed8" },
      },
    });
    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: TERMINAL_CHART.green,
      downColor: TERMINAL_CHART.coral,
      borderUpColor: TERMINAL_CHART.green,
      borderDownColor: TERMINAL_CHART.coral,
      wickUpColor: TERMINAL_CHART.green,
      wickDownColor: TERMINAL_CHART.coral,
      borderVisible: true,
      wickVisible: true,
      priceLineColor: TERMINAL_CHART.yellow,
      priceLineWidth: 1,
    });
    const volumeSeries = chart.addSeries(HistogramSeries, {
      priceFormat: { type: "volume" },
      priceScaleId: "",
      lastValueVisible: false,
      priceLineVisible: false,
    });
    volumeSeries.priceScale().applyOptions({ scaleMargins: { top: 0.78, bottom: 0 } });
    chartRef.current = chart;
    candleSeriesRef.current = candleSeries;
    volumeSeriesRef.current = volumeSeries;
    const handleVisibleRangeChange = () => requestPriceAutoscale(candleSeries);
    chart.timeScale().subscribeVisibleLogicalRangeChange(handleVisibleRangeChange);
    return () => {
      chart.timeScale().unsubscribeVisibleLogicalRangeChange(handleVisibleRangeChange);
      chartRef.current = null;
      candleSeriesRef.current = null;
      volumeSeriesRef.current = null;
      chart.remove();
    };
  }, []);

  useEffect(() => {
    const candleSeries = candleSeriesRef.current;
    const volumeSeries = volumeSeriesRef.current;
    const chart = chartRef.current;
    if (!candleSeries || !volumeSeries || !chart) return;
    const nextRange = candles.length
      ? { first: candles[0].time, last: candles[candles.length - 1].time, length: candles.length }
      : { first: null, last: null, length: 0 };
    const previousRange = dataRangeRef.current;
    const rangeChanged =
      nextRange.first !== previousRange.first ||
      nextRange.last !== previousRange.last ||
      Math.abs(nextRange.length - previousRange.length) > 8;
    if (rangeChanged && Math.abs(nextRange.length - previousRange.length) > 8) {
      didFitContentRef.current = false;
    }
    dataRangeRef.current = nextRange;
    candleSeries.setData(candles);
    volumeSeries.setData(volumes);
    // When a new trading session/bar arrives, keep the board on the latest
    // candle. Without this, data updates successfully but remain off-screen
    // to the right when the page was opened before the session started.
    if (rangeChanged && previousRange.last != null && nextRange.last !== previousRange.last) {
      chart.timeScale().scrollToRealTime();
    }
    if (!didFitContentRef.current && candles.length) {
      applyKlineDisplayRange(chart, candles, item?.interval === "1d" ? displayRange : "all");
      requestPriceAutoscale(candleSeries);
      didFitContentRef.current = true;
    }
  }, [candles, volumes]);

  function openSymbol() {
    onOpenSymbol?.(item.symbol);
  }

  return (
    <article
      className={`lwChartCard ${onOpenSymbol ? "isClickable" : ""}`}
      role={onOpenSymbol ? "button" : undefined}
      tabIndex={onOpenSymbol ? 0 : undefined}
      aria-label={onOpenSymbol ? `打开 ${item.symbol} 个股看板` : undefined}
      onClick={onOpenSymbol ? openSymbol : undefined}
      onKeyDown={onOpenSymbol ? (event) => {
        if (event.key === "Enter" || event.key === " ") { event.preventDefault(); openSymbol(); }
      } : undefined}
    >
      <div className="lwChartHeader">
        <div>
          <strong>{item.symbol}</strong>
          <span>{item.full_label || item.label}</span>
        </div>
        <div className={tone(item.latest_change_pct)}>
          <strong>{fmtChartPrice(item.latest_price, item.symbol)}</strong>
          <span>{item.latest_change_pct == null ? "-" : fmtPct(item.latest_change_pct)}</span>
        </div>
      </div>
      <div className="lwChartCanvas" ref={containerRef}>
        {!candles.length ? <div className="muted lwEmpty">暂无K线数据</div> : null}
      </div>
    </article>
  );
}

function GlobalLightweightBoard({ data, viewKey, displayRange, onOpenSymbol }) {
  const columns = Math.min(5, Math.max(1, Number(data?.columns || 1)));
  const charts = data?.charts || [];
  return (
    <div className="lwChartGrid" style={{ "--lw-cols": columns }}>
      {charts.map((item) => <LightweightKlineCard item={item} displayRange={displayRange} onOpenSymbol={onOpenSymbol} key={`${item.symbol}-${viewKey}-${displayRange}`} />)}
    </div>
  );
}

function normalizeLightweightTime(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    if (/^\d+$/.test(value)) return Number(value);
    return value;
  }
  return value;
}

function patchLatestCandle(candles = [], latestPrice, interval) {
  const price = Number(latestPrice);
  if (!candles.length || !Number.isFinite(price) || price <= 0 || interval === "1d") return candles;
  const last = candles[candles.length - 1];
  if (!last) return candles;
  const next = candles.slice();
  next[next.length - 1] = {
    ...last,
    close: price,
    high: Math.max(Number(last.high), price),
    low: Math.min(Number(last.low), price),
  };
  return next;
}

function candleDayKey(time) {
  if (typeof time === "number" && Number.isFinite(time)) {
    return new Date(time * 1000).toISOString().slice(0, 10);
  }
  return String(time || "").slice(0, 10);
}

function klineDisplayStartIndex(candles = [], mode = "250", anchorDate = "") {
  if (!candles.length) return 0;
  let startIndex = 0;
  const count = Number(mode);
  if (Number.isFinite(count) && count > 0) {
    startIndex = Math.max(0, candles.length - count);
  } else if (anchorDate) {
    const normalizedAnchor = String(anchorDate).slice(0, 10);
    const found = candles.findIndex((row) => candleDayKey(row.time) >= normalizedAnchor);
    startIndex = found >= 0 ? found : 0;
  } else if (mode === "selloff_60d" || mode === "rally_60d") {
    const recentStart = Math.max(0, candles.length - 60);
    let selectedIndex = recentStart;
    let selectedReturn = mode === "selloff_60d" ? Number.POSITIVE_INFINITY : Number.NEGATIVE_INFINITY;
    for (let index = recentStart + 1; index < candles.length; index += 1) {
      const previousClose = Number(candles[index - 1]?.close);
      const close = Number(candles[index]?.close);
      if (!(previousClose > 0) || !Number.isFinite(close)) continue;
      const dailyReturn = close / previousClose - 1;
      if ((mode === "selloff_60d" && dailyReturn < selectedReturn) || (mode === "rally_60d" && dailyReturn > selectedReturn)) {
        selectedReturn = dailyReturn;
        selectedIndex = index;
      }
    }
    startIndex = selectedIndex;
  }
  return startIndex;
}

const SINGLE_PROFILE_WIDTH_RATIO = 0.2;

function applyKlineDisplayRange(chart, candles = [], mode = "250", anchorDate = "", rightPaddingRatio = 0) {
  if (!chart || !candles.length) return;
  const startIndex = klineDisplayStartIndex(candles, mode, anchorDate);
  const clampedRatio = Math.max(0, Math.min(0.4, Number(rightPaddingRatio) || 0));
  if (clampedRatio <= 0 && startIndex <= 0) {
    chart.timeScale().fitContent();
    return;
  }
  const visibleDataBars = Math.max(1, candles.length - startIndex);
  const rightOffsetBars = Math.ceil(visibleDataBars * clampedRatio / Math.max(0.01, 1 - clampedRatio));
  chart.timeScale().applyOptions({ rightOffset: rightOffsetBars });
  chart.timeScale().setVisibleLogicalRange({ from: startIndex, to: candles.length - 1 + rightOffsetBars });
}

function formatThresholdSet(thresholds) {
  if (!thresholds) return "未配置";
  const value = (key) => {
    const number = Number(thresholds[key]);
    return Number.isFinite(number) ? `${number}%` : "-";
  };
  return `${value("small")} / ${value("medium")} / ${value("large")}`;
}

function formatStatisticPct(value, digits = 1) {
  const number = Number(value);
  return Number.isFinite(number) ? `${number.toFixed(digits)}%` : "-";
}

function formatStatisticRate(value) {
  const number = Number(value);
  return Number.isFinite(number) ? `${(number * 100).toFixed(0)}%` : "-";
}

function percentReferencePrice(candles = [], referencePrice) {
  if (!candles.length) return null;
  const suppliedReference = Number(referencePrice);
  if (Number.isFinite(suppliedReference) && suppliedReference > 0) return suppliedReference;
  const latestDay = candleDayKey(candles[candles.length - 1].time);
  return candles.find((row) => candleDayKey(row.time) === latestDay)?.open;
}

function openingPercentRows(candles = [], referencePrice) {
  const reference = percentReferencePrice(candles, referencePrice);
  if (!Number.isFinite(reference) || reference <= 0) return [];
  return candles.map((row) => ({ time: row.time, value: (row.close / reference - 1) * 100 }));
}

function openingPercentAxisTicks(candles = [], candleSeries, referencePrice) {
  if (!candles.length || !candleSeries?.priceToCoordinate) return [];
  const reference = percentReferencePrice(candles, referencePrice);
  if (!Number.isFinite(reference) || reference <= 0) return [];
  const values = candles.flatMap((row) => [Number(row.low), Number(row.high)]).filter(Number.isFinite);
  if (!values.length) return [];
  const lowPct = (Math.min(...values) / reference - 1) * 100;
  const highPct = (Math.max(...values) / reference - 1) * 100;
  const rawStep = Math.max((highPct - lowPct) / 4, 0.05);
  const magnitude = 10 ** Math.floor(Math.log10(rawStep));
  const normalized = rawStep / magnitude;
  const step = (normalized <= 1 ? 1 : normalized <= 2 ? 2 : normalized <= 5 ? 5 : 10) * magnitude;
  const start = Math.floor(lowPct / step) * step;
  const end = Math.ceil(highPct / step) * step;
  const ticks = [];
  for (let pct = start; pct <= end + step * 0.1; pct += step) {
    const y = candleSeries.priceToCoordinate(reference * (1 + pct / 100));
    if (y != null && Number.isFinite(y)) ticks.push({ pct, y });
  }
  return ticks;
}

function normalizeLineData(rows = []) {
  const out = [];
  let lastTime = null;
  for (const row of rows || []) {
    const time = normalizeLightweightTime(row.time);
    const value = Number(row.value);
    if (time == null || !Number.isFinite(value)) continue;
    if (lastTime != null && typeof time === typeof lastTime && time <= lastTime) continue;
    out.push({ time, value });
    lastTime = time;
  }
  return out;
}

function calculateVisibleVolumeProfile(candles = [], volumes = [], logicalRange = null, bins = 24) {
  if (!candles.length || bins <= 0) return [];
  const from = logicalRange && Number.isFinite(Number(logicalRange.from))
    ? Math.max(0, Math.floor(Number(logicalRange.from)))
    : 0;
  const to = logicalRange && Number.isFinite(Number(logicalRange.to))
    ? Math.min(candles.length - 1, Math.ceil(Number(logicalRange.to)))
    : candles.length - 1;
  if (to < from) return [];

  const volumeByTime = new Map(volumes.map((row) => [String(row.time), Math.max(0, Number(row.value || 0))]));
  const source = candles.slice(from, to + 1);
  const lows = source.map((row) => Number(row.low)).filter(Number.isFinite);
  const highs = source.map((row) => Number(row.high)).filter(Number.isFinite);
  if (!lows.length || !highs.length) return [];
  const rangeLow = Math.min(...lows);
  const rangeHigh = Math.max(...highs);

  if (!(rangeHigh > rangeLow)) {
    const volume = source.reduce((sum, row) => sum + (volumeByTime.get(String(row.time)) || 0), 0);
    if (volume <= 0) return [];
    const halfStep = Math.max(Math.abs(rangeLow) * 0.0005, 0.0001);
    return [{ low: rangeLow - halfStep, high: rangeHigh + halfStep, price: rangeLow, volume, pct: 1, isPoc: true }];
  }

  const step = (rangeHigh - rangeLow) / bins;
  const totals = Array.from({ length: bins }, () => 0);
  for (const row of source) {
    const amount = volumeByTime.get(String(row.time)) || 0;
    if (amount <= 0) continue;
    const rawLow = Number(row.low);
    const rawHigh = Number(row.high);
    if (!Number.isFinite(rawLow) || !Number.isFinite(rawHigh)) continue;
    const barLow = Math.min(rawLow, rawHigh);
    const barHigh = Math.max(rawLow, rawHigh);
    if (barHigh > barLow) {
      const firstBin = Math.max(0, Math.min(bins - 1, Math.floor((barLow - rangeLow) / step)));
      const lastBin = Math.max(0, Math.min(bins - 1, Math.floor((barHigh - rangeLow) / step)));
      const overlaps = [];
      let overlapSum = 0;
      for (let index = firstBin; index <= lastBin; index += 1) {
        const low = rangeLow + index * step;
        const high = low + step;
        const overlap = Math.max(0, Math.min(high, barHigh) - Math.max(low, barLow));
        overlaps.push([index, overlap]);
        overlapSum += overlap;
      }
      if (overlapSum > 0) {
        overlaps.forEach(([index, overlap]) => { totals[index] += amount * overlap / overlapSum; });
        continue;
      }
    }
    const close = Number(row.close);
    const reference = Number.isFinite(close) ? close : barLow;
    const index = Math.max(0, Math.min(bins - 1, Math.floor((reference - rangeLow) / step)));
    totals[index] += amount;
  }

  const maxVolume = Math.max(...totals);
  if (!(maxVolume > 0)) return [];
  const pocIndex = totals.indexOf(maxVolume);
  return totals.map((volume, index) => {
    const low = rangeLow + index * step;
    const high = low + step;
    return {
      low,
      high,
      price: (low + high) / 2,
      volume,
      pct: volume / maxVolume,
      isPoc: index === pocIndex,
    };
  }).filter((row) => row.volume > 0);
}

function latestLineValue(rows = []) {
  for (let index = rows.length - 1; index >= 0; index -= 1) {
    const value = Number(rows[index]?.value);
    if (Number.isFinite(value)) return value;
  }
  return null;
}

function lineIsRising(rows = [], lookback = 5) {
  const values = rows.map((row) => Number(row?.value)).filter(Number.isFinite);
  if (values.length < 2) return null;
  const current = values[values.length - 1];
  const previous = values[Math.max(0, values.length - 1 - lookback)];
  return current > previous;
}

function profileValueArea(profile = [], targetShare = 0.7) {
  if (!profile.length) return { vah: null, val: null };
  const total = profile.reduce((sum, row) => sum + Number(row.volume || 0), 0);
  const pocIndex = profile.findIndex((row) => row.isPoc);
  if (!(total > 0) || pocIndex < 0) return { vah: null, val: null };
  let lowIndex = pocIndex;
  let highIndex = pocIndex;
  let covered = Number(profile[pocIndex].volume || 0);
  while (covered / total < targetShare && (lowIndex > 0 || highIndex < profile.length - 1)) {
    const below = lowIndex > 0 ? Number(profile[lowIndex - 1].volume || 0) : -1;
    const above = highIndex < profile.length - 1 ? Number(profile[highIndex + 1].volume || 0) : -1;
    if (above >= below) {
      highIndex += 1;
      covered += Math.max(0, above);
    } else {
      lowIndex -= 1;
      covered += Math.max(0, below);
    }
  }
  return { val: profile[lowIndex]?.low ?? null, vah: profile[highIndex]?.high ?? null };
}

function mergeTechnicalLevels(levels = [], referencePrice = 0) {
  const tolerance = Math.max(referencePrice * 0.008, 0.01);
  const sorted = levels.filter((row) => Number.isFinite(row.price) && row.price > 0).sort((a, b) => a.price - b.price);
  const merged = [];
  sorted.forEach((level) => {
    const previous = merged[merged.length - 1];
    if (previous && Math.abs(previous.price - level.price) <= tolerance) {
      const weight = previous.weight + level.weight;
      previous.price = (previous.price * previous.weight + level.price * level.weight) / weight;
      previous.weight = weight;
      previous.labels.push(level.label);
    } else {
      merged.push({ ...level, labels: [level.label] });
    }
  });
  return merged;
}

function buildTechnicalSnapshot(data, displayRange, visibleProfile = null) {
  if (!data?.candles?.length || data.interval !== "1d") return null;
  const candles = data.candles;
  const startIndex = klineDisplayStartIndex(candles, displayRange, displayRange === "earnings" ? data.earnings_anchor : "");
  const profile = visibleProfile?.length
    ? visibleProfile
    : calculateVisibleVolumeProfile(candles, data.volumes || [], { from: startIndex, to: candles.length - 1 }, 24);
  const poc = profile.find((row) => row.isPoc)?.price ?? null;
  const { vah, val } = profileValueArea(profile);
  const price = Number(data.latest_price || candles[candles.length - 1]?.close);
  const ema20 = latestLineValue(data.overlays?.ema20);
  const ma50 = latestLineValue(data.overlays?.ma50);
  const ma200 = latestLineValue(data.overlays?.ma200);
  const avwap = Number.isFinite(Number(data.avwap_value)) ? Number(data.avwap_value) : null;
  const rsi = latestLineValue(data.indicators?.rsi);
  const slopes = {
    ema20: lineIsRising(data.overlays?.ema20),
    ma50: lineIsRising(data.overlays?.ma50, 10),
    ma200: lineIsRising(data.overlays?.ma200, 20),
  };

  const checks = [
    { label: "价格站上 EMA20", met: Number.isFinite(ema20) && price > ema20, points: 1 },
    { label: "EMA20 拐头向上", met: slopes.ema20 === true, points: 1 },
    { label: "价格站上 AVWAP", met: Number.isFinite(avwap) && price > avwap, points: 2 },
    { label: "回到 Value Area", met: Number.isFinite(val) && Number.isFinite(vah) && price >= val && price <= vah, points: 1 },
    { label: "价格站上 POC", met: Number.isFinite(poc) && price > poc, points: 2 },
    { label: "MA50 方向向上", met: slopes.ma50 === true, points: 1 },
    { label: "价格站上 MA200", met: Number.isFinite(ma200) && price > ma200, points: 2 },
  ].filter((item) => item.met !== null);
  const repairScore = checks.reduce((sum, item) => sum + (item.met ? item.points : 0), 0);
  const maxRepairScore = checks.reduce((sum, item) => sum + item.points, 0) || 10;
  const score = Math.round((repairScore / maxRepairScore) * 100) / 10;

  const hvnRows = profile.filter((row, index) => {
    const previous = Number(profile[index - 1]?.volume || 0);
    const next = Number(profile[index + 1]?.volume || 0);
    return row.pct >= 0.55 && row.volume >= previous && row.volume >= next && !row.isPoc;
  });
  const levels = mergeTechnicalLevels([
    { label: "EMA20", price: ema20, weight: 1 },
    { label: "MA50", price: ma50, weight: 1.25 },
    { label: "MA200", price: ma200, weight: 1.75 },
    { label: data.avwap_label || "AVWAP", price: avwap, weight: 1.8 },
    { label: "POC", price: poc, weight: 2.2 },
    { label: "VAH", price: vah, weight: 1.1 },
    { label: "VAL", price: val, weight: 1.1 },
    ...hvnRows.map((row) => ({ label: "HVN", price: row.price, weight: 1.6 })),
  ], price).map((level) => ({
    ...level,
    distancePct: price > 0 ? (level.price / price - 1) * 100 : 0,
    strength: Math.min(5, Math.max(1, Math.round(level.weight))),
  }));
  const supports = levels.filter((row) => row.price <= price).sort((a, b) => b.price - a.price).slice(0, 4);
  const resistances = levels.filter((row) => row.price > price).sort((a, b) => a.price - b.price).slice(0, 3);
  let state = "破位观察";
  let toneName = "weak";
  let summary = "趋势尚未修复，优先观察能否收复关键成本线。";
  if (score >= 7.5) {
    state = "多头趋势";
    toneName = "strong";
    summary = "多数趋势与成本条件占优，可按支撑层级管理仓位。";
  } else if (score >= 4) {
    state = "修复进行中";
    toneName = "neutral";
    summary = "部分条件已经修复，仍需关键价位确认，避免把反弹当反转。";
  }
  return { price, ema20, ma50, ma200, avwap, rsi, poc, vah, val, score, checks, supports, resistances, state, toneName, summary };
}

function TechnicalCheatSheetModal({ onClose }) {
  useEffect(() => {
    const handleKeyDown = (event) => { if (event.key === "Escape") onClose(); };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);
  return createPortal((
    <div className="modalBackdrop cheatSheetBackdrop" role="presentation" onClick={onClose}>
      <div className="modalPanel cheatSheetModal" role="dialog" aria-modal="true" aria-labelledby="cheat-sheet-title" onClick={(event) => event.stopPropagation()}>
        <div className="cheatSheetHeader">
          <div><span>30 秒看盘模板</span><h2 id="cheat-sheet-title">技术分析 Cheat Sheet</h2></div>
          <button className="iconButton" type="button" onClick={onClose} aria-label="关闭"><X size={18} /></button>
        </div>
        <div className="cheatSheetAssetGrid">
          <article className="assetPlaybook etfPlaybook">
            <header><span>ETF PLAYBOOK</span><h3>ETF · VOO / QQQ / 沪深300</h3><p>技术面权重更高，重点判断长期趋势与资金成本。</p></header>
            <section><b>01 · 长期趋势检查</b><p><strong>MA200</strong><span>价格在上方且均线向上</span></p><p><strong>VP250</strong><span>看一年 POC / HVN / Value Area</span></p><p><strong>结论</strong><span>Price &gt; MA200 + POC，长期结构健康</span></p></section>
            <section><b>02 · 中期趋势检查</b><p><strong>AVWAP 年初</strong><span>全年资金平均成本 · 默认</span></p><p><strong>Swing H/L</strong><span>本轮压力成本 / 反转成本</span></p><p><strong>MA50</strong><span>方向向上，价格最好在其上方</span></p></section>
            <section><b>03 · 节奏与买点</b><p><strong>EMA20</strong><span>趋势加仓与短期节奏</span></p><p><strong>RSI14</strong><span>只做情绪过滤，不单独抄底</span></p><p><strong>优先级</strong><span>EMA20 → MA50 → HVN/POC → MA200</span></p></section>
            <footer><b>30 秒流程</b><span>MA200 → VP250 → 年初 AVWAP → MA50 → EMA20</span></footer>
          </article>
          <article className="assetPlaybook stockPlaybook">
            <header><span>STOCK PLAYBOOK</span><h3>个股 · ISRG / TEM / PLTR</h3><p>基本面优先，技术面用于确认资金是否接受新估值。</p></header>
            <section><b>01 · 长期趋势检查</b><p><strong>基本面</strong><span>财报、Guidance、增长逻辑先过关</span></p><p><strong>MA200</strong><span>长期牛熊分界与风险边界</span></p><p><strong>VP250</strong><span>一年长期 POC / HVN 成本</span></p></section>
            <section><b>02 · 中期趋势检查</b><p><strong>财报 AVWAP</strong><span>财报后平均成本 · 默认</span></p><p><strong>Gap AVWAP</strong><span>重大消息后的重新定价成本</span></p><p><strong>Swing H/L</strong><span>套牢压力 / 反转资金成本</span></p><p><strong>MA50</strong><span>中期趋势是否重新向上</span></p></section>
            <section><b>03 · 节奏与买点</b><p><strong>财报 VP</strong><span>看近期 POC / HVN；同时对照 VP250</span></p><p><strong>EMA20</strong><span>短期节奏，不等于真正底部</span></p><p><strong>优先级</strong><span>EMA20 → MA50 → 财报VP → VP250 → MA200</span></p></section>
            <footer><b>30 秒流程</b><span>基本面 → 财报 AVWAP → 财报 VP → VP250 → EMA20</span></footer>
          </article>
        </div>
        <div className="cheatSheetCommonGrid">
          <article><h3>Volume Profile 口径</h3><p><b>POC</b><span>成交量最大的价格点</span></p><p><b>HVN</b><span>成交密集区域</span></p><p><b>VAH / VAL</b><span>70% 价值区上下边界</span></p><em>视窗切换后，VP 与智能面板同步重算。</em></article>
          <article><h3>趋势修复 Checklist</h3><p><b>+1</b><span>站回 EMA20；EMA20 向上</span></p><p><b>+2</b><span>站回 AVWAP；突破 POC</span></p><p><b>确认</b><span>回到 Value Area；MA50向上；放量突破</span></p><em>开始修复不等于反转完成，优先等待回踩确认。</em></article>
          <article><h3>共振与纪律</h3><p><b>强支撑</b><span>POC + HVN + AVWAP 重合</span></p><p><b>破位后</b><span>原支撑会转为压力，不急于猜底</span></p><p><b>仓位</b><span>ETF可按趋势分层；个股先看财报是否证伪</span></p><em>MA 看趋势，AVWAP 看成本，VP 看筹码。</em></article>
        </div>
      </div>
    </div>
  ), document.body);
}

function TechnicalInsightPanel({ data, displayRange, visibleProfile }) {
  const snapshot = useMemo(() => buildTechnicalSnapshot(data, displayRange, visibleProfile), [data, displayRange, visibleProfile]);
  if (!snapshot) return null;
  const rangeLabel = displayRange === "250" ? "250根 · 一年" : displayRange === "125" ? "125根 · 半年" : displayRange === "60" ? "60根 · 季度" : displayRange === "earnings" ? "财报反应日起" : "当前波段";
  const levelRows = snapshot.supports.length ? snapshot.supports : snapshot.resistances;
  const levelTitle = snapshot.supports.length ? "下一支撑" : "上方压力";
  return (
    <aside className="technicalInsightPanel" aria-label="智能技术分析">
      <div className="insightTitle"><div><span>SMART ANALYSIS</span><h3>{data.symbol} 技术状态</h3></div><Gauge size={20} /></div>
      <div className={`insightScore ${snapshot.toneName}`}>
        <div><strong>{snapshot.score.toFixed(1)}</strong><span>/ 10</span></div>
        <p><b>{snapshot.state}</b><small>{rangeLabel}</small></p>
      </div>
      <p className="insightSummary">{snapshot.summary}</p>
      <div className="insightSection">
        <div className="insightSectionTitle"><span>修复检查</span><small>{snapshot.checks.filter((item) => item.met).length}/{snapshot.checks.length}</small></div>
        <div className="repairChecks">
          {snapshot.checks.map((item) => <span className={item.met ? "met" : "unmet"} key={item.label}>{item.met ? <Check size={13} /> : <CircleAlert size={13} />}{item.label}<b>+{item.points}</b></span>)}
        </div>
      </div>
      <div className="insightSection">
        <div className="insightSectionTitle"><span>{levelTitle}</span><small>当前 {fmtChartPrice(snapshot.price, data.symbol)}</small></div>
        <div className="levelList">
          {levelRows.map((level, index) => <div key={`${level.price}-${index}`}><span>{index + 1}</span><p><b>{level.labels.join(" + ")}</b><small>{level.distancePct.toFixed(1)}% · {"★".repeat(level.strength)}</small></p><strong>{fmtChartPrice(level.price, data.symbol)}</strong></div>)}
          {!levelRows.length ? <p className="muted">当前视窗暂无可用支撑。</p> : null}
        </div>
      </div>
      <div className="insightMetrics">
        <span><small>POC</small><b>{fmtChartPrice(snapshot.poc, data.symbol)}</b></span>
        <span><small>AVWAP</small><b>{fmtChartPrice(snapshot.avwap, data.symbol)}</b></span>
        <span><small>RSI14</small><b>{Number.isFinite(snapshot.rsi) ? snapshot.rsi.toFixed(1) : "-"}</b></span>
      </div>
      <p className="insightDisclaimer">规则化技术读数，仅用于辅助判断；成长股仍需结合财报与指引。</p>
    </aside>
  );
}

function requestPriceAutoscale(series) {
  if (!series?.priceScale) return;
  window.requestAnimationFrame(() => {
    try {
      series.priceScale().setAutoScale(true);
    } catch {
      // Ignore chart lifecycle races during fast page switches.
    }
  });
}

const KLINE_DRAWINGS_STORAGE_KEY = "investment-dashboard:kline-drawings:v1";

function readKlineDrawings(viewKey) {
  if (!viewKey) return [];
  try {
    const stored = JSON.parse(window.localStorage.getItem(KLINE_DRAWINGS_STORAGE_KEY) || "{}");
    return Array.isArray(stored?.[viewKey]) ? stored[viewKey] : [];
  } catch {
    return [];
  }
}

function writeKlineDrawings(viewKey, drawings) {
  if (!viewKey) return;
  try {
    const stored = JSON.parse(window.localStorage.getItem(KLINE_DRAWINGS_STORAGE_KEY) || "{}");
    stored[viewKey] = drawings;
    window.localStorage.setItem(KLINE_DRAWINGS_STORAGE_KEY, JSON.stringify(stored));
  } catch {
    // Drawing persistence is a convenience; keep the chart usable if storage is unavailable.
  }
}

function drawingTimeValue(time) {
  if (typeof time === "number") return time;
  if (typeof time === "string") return Date.parse(time) || 0;
  if (time && typeof time === "object") return Date.UTC(time.year, Number(time.month || 1) - 1, Number(time.day || 1));
  return 0;
}

function SingleLightweightChart({ data, viewKey, displayRange, onVisibleProfileChange }) {
  const containerRef = useRef(null);
  const chartRef = useRef(null);
  const seriesRef = useRef({});
  const priceLinesRef = useRef({});
  const drawingSeriesRef = useRef([]);
  const drawingPriceLinesRef = useRef([]);
  const didFitContentRef = useRef(false);
  const dataRangeRef = useRef({ first: null, last: null, length: 0 });
  const candlesRef = useRef([]);
  const volumesRef = useRef([]);
  const profileUpdaterRef = useRef(null);
  const visibleProfileCallbackRef = useRef(onVisibleProfileChange);
  const [profileBars, setProfileBars] = useState([]);
  const [profilePoc, setProfilePoc] = useState(null);
  const [candleTooltip, setCandleTooltip] = useState({ visible: false });
  const [percentAxisTicks, setPercentAxisTicks] = useState([]);
  const drawingKey = `${data?.symbol || ""}|${data?.interval || ""}`;
  const drawingKeyRef = useRef(drawingKey);
  const [drawings, setDrawings] = useState(() => readKlineDrawings(drawingKey));
  const [drawingTool, setDrawingTool] = useState("");
  const drawingToolRef = useRef("");
  const [drawingDraft, setDrawingDraft] = useState(null);
  const drawingDraftRef = useRef(null);
  const [drawingHint, setDrawingHint] = useState("");
  const commitDrawingRef = useRef(null);
  drawingKeyRef.current = drawingKey;
  drawingToolRef.current = drawingTool;
  drawingDraftRef.current = drawingDraft;
  commitDrawingRef.current = (drawing) => {
    setDrawings((current) => {
      const next = [...current, drawing];
      writeKlineDrawings(drawingKeyRef.current, next);
      return next;
    });
  };

  useEffect(() => {
    setDrawings(readKlineDrawings(drawingKey));
    setDrawingTool("");
    setDrawingDraft(null);
    setDrawingHint("");
  }, [drawingKey]);

  useEffect(() => {
    const handleKeyDown = (event) => {
      if (event.key !== "Escape") return;
      setDrawingTool("");
      setDrawingDraft(null);
      setDrawingHint("");
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, []);

  useEffect(() => {
    visibleProfileCallbackRef.current = onVisibleProfileChange;
  }, [onVisibleProfileChange]);
  const rawCandles = useMemo(() => {
    const out = [];
    let lastTime = null;
    for (const raw of data?.candles || []) {
      const time = normalizeLightweightTime(raw.time);
      const open = Number(raw.open);
      const high = Number(raw.high);
      const low = Number(raw.low);
      const close = Number(raw.close);
      if (time == null || !Number.isFinite(open) || !Number.isFinite(high) || !Number.isFinite(low) || !Number.isFinite(close)) continue;
      if (open <= 0 || high <= 0 || low <= 0 || close <= 0) continue;
      if (lastTime != null && typeof time === typeof lastTime && time <= lastTime) continue;
      out.push({ time, open, high, low, close });
      lastTime = time;
    }
    return patchLatestCandle(out, data?.latest_price, data?.interval);
  }, [data]);
  const rawVolumes = useMemo(() => {
    const volumeRows = (data?.volumes || []).map((row) => ({
      time: normalizeLightweightTime(row.time),
      value: Number(row.value || 0),
      color: row.color || "rgba(148, 163, 184, 0.24)",
    })).filter((row) => row.time != null && Number.isFinite(row.value));
    if (!volumeRows.length || !rawCandles.length || data?.interval === "1d") return volumeRows;
    const lastVolumeIndex = volumeRows.length - 1;
    const lastCandle = rawCandles[rawCandles.length - 1];
    if (String(volumeRows[lastVolumeIndex].time) !== String(lastCandle.time)) return volumeRows;
    volumeRows[lastVolumeIndex] = {
      ...volumeRows[lastVolumeIndex],
      color: lastCandle.close >= lastCandle.open ? "rgba(34, 197, 94, 0.28)" : "rgba(239, 68, 68, 0.28)",
    };
    return volumeRows;
  }, [data, rawCandles]);
  const candles = rawCandles;
  const volumes = rawVolumes;
  const showOpeningPercentAxis = data?.interval === "5m" || data?.interval === "15m";
  const previousClose = Number(data?.previous_close);
  const percentReference = useMemo(() => percentReferencePrice(candles, previousClose), [candles, previousClose]);
  const percentRows = useMemo(
    () => (showOpeningPercentAxis ? openingPercentRows(candles, previousClose) : []),
    [candles, previousClose, showOpeningPercentAxis],
  );
  const overlays = data?.overlays || {};
  const indicators = data?.indicators || {};
  const isDaily = data?.interval === "1d";
  const rsiPeriod = Number(data?.rsi_period) || (data?.interval === "5m" ? 7 : 14);
  const avwapValue = Number(data?.avwap_value);
  const avwapSkewsIntradayScale = useMemo(() => {
    if (isDaily || !Number.isFinite(avwapValue) || !candles.length) return false;
    const lows = candles.map((row) => row.low).filter(Number.isFinite);
    const highs = candles.map((row) => row.high).filter(Number.isFinite);
    if (!lows.length || !highs.length) return false;
    const low = Math.min(...lows);
    const high = Math.max(...highs);
    const span = Math.max(high - low, high * 0.005, 1e-9);
    return avwapValue > high + span * 0.25 || avwapValue < low - span * 0.25;
  }, [avwapValue, candles, isDaily]);
  const avwapText = Number.isFinite(avwapValue)
    ? `${data?.avwap_label || "AVWAP"} ${fmtChartPrice(avwapValue, data?.symbol)}${avwapSkewsIntradayScale ? "（未绘制）" : ""}`
    : (data?.avwap_label || "AVWAP");
  useEffect(() => {
    const container = containerRef.current;
    if (!container) return undefined;
    didFitContentRef.current = false;
    const chart = createChart(container, {
      autoSize: true,
      layout: {
        background: { color: "#0b1b2e" },
        textColor: TERMINAL_CHART.textMuted,
        fontFamily: PLOT_FONT,
        fontSize: 11,
        panes: {
          separatorColor: "rgba(148, 163, 184, 0.22)",
          separatorHoverColor: "rgba(96, 165, 250, 0.45)",
        },
      },
      grid: {
        vertLines: { color: "rgba(148, 163, 184, 0.08)" },
        horzLines: { color: "rgba(148, 163, 184, 0.12)" },
      },
      rightPriceScale: {
        borderColor: "rgba(148, 163, 184, 0.22)",
        scaleMargins: { top: 0.08, bottom: 0.24 },
      },
      timeScale: {
        borderColor: "rgba(148, 163, 184, 0.22)",
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 0,
        barSpacing: 8,
        tickMarkFormatter: formatLightweightChartTime,
      },
      localization: { timeFormatter: formatLightweightChartTime },
      crosshair: {
        vertLine: { color: "rgba(203, 213, 225, 0.35)", labelBackgroundColor: "#1d4ed8" },
        horzLine: { color: "rgba(203, 213, 225, 0.35)", labelBackgroundColor: "#1d4ed8" },
      },
    });
    drawingSeriesRef.current = [];
    drawingPriceLinesRef.current = [];
    const candle = chart.addSeries(CandlestickSeries, {
      upColor: TERMINAL_CHART.green,
      downColor: TERMINAL_CHART.coral,
      borderUpColor: TERMINAL_CHART.green,
      borderDownColor: TERMINAL_CHART.coral,
      wickUpColor: TERMINAL_CHART.green,
      wickDownColor: TERMINAL_CHART.coral,
      borderVisible: true,
      wickVisible: true,
      priceLineVisible: false,
      lastValueVisible: false,
    }, 0);
    const volume = chart.addSeries(HistogramSeries, {
      priceFormat: { type: "volume" },
      priceScaleId: "",
      lastValueVisible: false,
      priceLineVisible: false,
    }, 0);
    volume.priceScale().applyOptions({ scaleMargins: { top: 0.78, bottom: 0 } });
    const percent = showOpeningPercentAxis ? chart.addSeries(LineSeries, {
      // Hidden calibration series: the axis is visible, the line is not.
      color: "rgba(0, 0, 0, 0)",
      lineWidth: 1,
      priceScaleId: "percent",
      priceFormat: { type: "custom", formatter: (value) => `${Number(value).toFixed(2)}%`, minMove: 0.01 },
      lastValueVisible: false,
      priceLineVisible: false,
    }, 0) : null;
    if (showOpeningPercentAxis) chart.priceScale("percent").applyOptions({
      visible: true,
      position: "right",
      autoScale: true,
      minimumWidth: 58,
      borderColor: "rgba(112, 215, 255, 0.34)",
      scaleMargins: { top: 0.08, bottom: 0.24 },
    });
    const avwapUpper = chart.addSeries(LineSeries, { color: "rgba(45, 212, 191, 0.52)", lineWidth: 1, lineStyle: 2, lastValueVisible: false, priceLineVisible: false }, 0);
    const avwapLower = chart.addSeries(LineSeries, { color: "rgba(45, 212, 191, 0.52)", lineWidth: 1, lineStyle: 2, lastValueVisible: false, priceLineVisible: false }, 0);
    const avwap = chart.addSeries(LineSeries, { color: TERMINAL_CHART.cyan, lineWidth: 2, lastValueVisible: false, priceLineVisible: false }, 0);
    const ema20 = chart.addSeries(LineSeries, { color: "rgba(245, 158, 11, 0.96)", lineWidth: 1, title: "EMA20", lastValueVisible: false, priceLineVisible: false }, 0);
    const ma50 = chart.addSeries(LineSeries, { color: "rgba(192, 132, 252, 0.96)", lineWidth: 1, title: "MA50", lastValueVisible: false, priceLineVisible: false }, 0);
    const ma200 = chart.addSeries(LineSeries, { color: "rgba(156, 163, 175, 0.95)", lineWidth: 1, title: "MA200", lastValueVisible: false, priceLineVisible: false }, 0);
    const rsi = chart.addSeries(LineSeries, { color: TERMINAL_CHART.violet, lineWidth: 1, title: `RSI(${rsiPeriod})` }, 1);
    const rsiMa = chart.addSeries(LineSeries, { color: TERMINAL_CHART.yellow, lineWidth: 1, title: "RSI EMA" }, 1);
    const macdHist = chart.addSeries(HistogramSeries, { color: "rgba(148, 163, 184, 0.35)", priceLineVisible: false, lastValueVisible: false }, 2);
    const macd = chart.addSeries(LineSeries, { color: TERMINAL_CHART.cyan, lineWidth: 1, title: "MACD" }, 2);
    const macdSignal = chart.addSeries(LineSeries, { color: TERMINAL_CHART.coral, lineWidth: 1, title: "Signal" }, 2);
    [percent, avwapUpper, avwapLower, avwap, ema20, ma50, ma200, rsi, rsiMa, macd, macdSignal]
      .filter(Boolean)
      .forEach((lineSeries) => lineSeries.applyOptions({ crosshairMarkerVisible: false }));
    rsi.createPriceLine({ price: 70, color: "rgba(248, 113, 113, 0.45)", lineWidth: 1, lineStyle: 2, axisLabelVisible: false });
    rsi.createPriceLine({ price: 30, color: "rgba(52, 211, 153, 0.45)", lineWidth: 1, lineStyle: 2, axisLabelVisible: false });
    seriesRef.current = { candle, volume, percent, avwapUpper, avwapLower, avwap, ema20, ma50, ma200, rsi, rsiMa, macdHist, macd, macdSignal };
    chartRef.current = chart;
    const handleCrosshairMove = (param) => {
      if (drawingToolRef.current) {
        setCandleTooltip((current) => current.visible ? { visible: false } : current);
        return;
      }
      const point = param?.point;
      const candleRow = param?.seriesData?.get(candle);
      if (!point || !candleRow || point.x < 0 || point.y < 0 || point.x > container.clientWidth || point.y > container.clientHeight) {
        setCandleTooltip((current) => current.visible ? { visible: false } : current);
        return;
      }
      const open = Number(candleRow.open);
      const high = Number(candleRow.high);
      const low = Number(candleRow.low);
      const close = Number(candleRow.close);
      const volumeRow = param.seriesData.get(volume);
      const tooltipWidth = 216;
      const tooltipHeight = 154;
      setCandleTooltip({
        visible: true,
        left: Math.max(8, Math.min(container.clientWidth - tooltipWidth - 8, point.x + 14)),
        top: Math.max(8, Math.min(container.clientHeight - tooltipHeight - 8, point.y + 14)),
        time: formatLightweightChartTime(param.time),
        open,
        high,
        low,
        close,
        change: close - open,
        changePct: open > 0 ? (close / open - 1) * 100 : 0,
        amplitudePct: low > 0 ? (high / low - 1) * 100 : 0,
        volume: Number(volumeRow?.value || 0),
      });
    };
    chart.subscribeCrosshairMove(handleCrosshairMove);
    const handleChartClick = (param) => {
      const activeTool = drawingToolRef.current;
      if (!activeTool || !param?.point) return;
      const clickedTime = param.time ?? chart.timeScale().coordinateToTime(param.point.x);
      const clickedPrice = candle.coordinateToPrice(param.point.y);
      const mainPaneHeight = chart.panes?.()?.[0]?.getHeight?.() || container.clientHeight * 0.6;
      if (param.point.y > mainPaneHeight || clickedTime == null || !Number.isFinite(Number(clickedPrice)) || Number(clickedPrice) <= 0) {
        setDrawingHint("请在主图 K 线区域内落点");
        return;
      }
      const point = { time: clickedTime, value: Number(clickedPrice) };
      if (activeTool === "horizontal") {
        commitDrawingRef.current?.({
          id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
          type: "horizontal",
          price: point.value,
        });
        setDrawingTool("");
        setDrawingHint("");
        return;
      }
      const start = drawingDraftRef.current;
      if (!start) {
        setDrawingDraft(point);
        setDrawingHint("起点已选，再点击一次确定终点");
        return;
      }
      if (drawingTimeValue(start.time) === drawingTimeValue(point.time)) {
        setDrawingHint("终点请选择另一根 K 线");
        return;
      }
      const points = [start, point].sort((left, right) => drawingTimeValue(left.time) - drawingTimeValue(right.time));
      commitDrawingRef.current?.({
        id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
        type: "trend",
        points,
      });
      setDrawingDraft(null);
      setDrawingTool("");
      setDrawingHint("");
    };
    chart.subscribeClick(handleChartClick);
    const updateProfile = (logicalRange = null) => {
      const candleSeries = seriesRef.current.candle;
      if (!candleSeries) return;
      const profile = calculateVisibleVolumeProfile(candlesRef.current, volumesRef.current, logicalRange, 24);
      const poc = profile.find((row) => row.isPoc);
      setProfilePoc(poc?.price ?? null);
      visibleProfileCallbackRef.current?.({ profile, viewKey });
      const bars = profile.map((row) => {
        const yLow = candleSeries.priceToCoordinate(row.low);
        const yHigh = candleSeries.priceToCoordinate(row.high);
        if (yLow == null || yHigh == null) return null;
        return {
          top: Math.min(yLow, yHigh),
          height: Math.max(2, Math.abs(yLow - yHigh)),
          width: `${Math.max(0.5, row.pct * 100)}%`,
          low: row.low,
          high: row.high,
          volume: row.volume,
          pct: row.pct,
          isPoc: row.isPoc,
        };
      }).filter(Boolean);
      setProfileBars(bars);
    };
    profileUpdaterRef.current = updateProfile;
    const handleVisibleRangeChange = (logicalRange) => {
      requestPriceAutoscale(candle);
      window.requestAnimationFrame(() => updateProfile(logicalRange));
    };
    chart.timeScale().subscribeVisibleLogicalRangeChange(handleVisibleRangeChange);
    return () => {
      chart.timeScale().unsubscribeVisibleLogicalRangeChange(handleVisibleRangeChange);
      chartRef.current = null;
      seriesRef.current = {};
      priceLinesRef.current = {};
      drawingSeriesRef.current = [];
      drawingPriceLinesRef.current = [];
      profileUpdaterRef.current = null;
      chart.unsubscribeCrosshairMove(handleCrosshairMove);
      chart.unsubscribeClick(handleChartClick);
      chart.remove();
    };
  }, [viewKey, rsiPeriod, showOpeningPercentAxis]);

  useEffect(() => {
    const chart = chartRef.current;
    const candle = seriesRef.current.candle;
    if (!chart || !candle) return;
    for (const series of drawingSeriesRef.current) {
      try {
        chart.removeSeries(series);
      } catch {
        // Ignore chart lifecycle races while switching symbols or intervals.
      }
    }
    for (const priceLine of drawingPriceLinesRef.current) {
      try {
        candle.removePriceLine(priceLine);
      } catch {
        // Ignore chart lifecycle races while switching symbols or intervals.
      }
    }
    drawingSeriesRef.current = [];
    drawingPriceLinesRef.current = [];
    for (const drawing of drawings) {
      if (drawing.type === "horizontal" && Number(drawing.price) > 0) {
        drawingPriceLinesRef.current.push(candle.createPriceLine({
          price: Number(drawing.price),
          color: "#fbbf24",
          lineWidth: 2,
          lineStyle: 2,
          axisLabelVisible: true,
          title: "Line",
        }));
      }
      if (drawing.type === "trend" && Array.isArray(drawing.points) && drawing.points.length === 2) {
        const anchorIndexes = drawing.points.map((point) => candles.reduce(
          (best, row, index) => {
            const distance = Math.abs(drawingTimeValue(row.time) - drawingTimeValue(point.time));
            return distance < best.distance ? { index, distance } : best;
          },
          { index: 0, distance: Number.POSITIVE_INFINITY },
        ).index);
        const [startIndex, endIndex] = anchorIndexes;
        const canExtend = candles.length > 1 && startIndex !== endIndex;
        const slope = canExtend
          ? (Number(drawing.points[1].value) - Number(drawing.points[0].value)) / (endIndex - startIndex)
          : 0;
        const extendedPoints = canExtend
          ? [
            {
              time: candles[0].time,
              value: Number(drawing.points[0].value) - slope * startIndex,
            },
            {
              time: candles[candles.length - 1].time,
              value: Number(drawing.points[0].value) + slope * (candles.length - 1 - startIndex),
            },
          ]
          : drawing.points;
        const line = chart.addSeries(LineSeries, {
          color: slope < 0 ? TERMINAL_CHART.coral : "#fbbf24",
          lineWidth: 2,
          crosshairMarkerVisible: false,
          lastValueVisible: false,
          priceLineVisible: false,
          autoscaleInfoProvider: () => null,
        }, 0);
        line.setData(extendedPoints);
        drawingSeriesRef.current.push(line);
      }
    }
  }, [candles, drawings, viewKey]);

  useEffect(() => {
    const chart = chartRef.current;
    const series = seriesRef.current;
    if (!chart || !series.candle) return;
    const nextRange = candles.length
      ? { first: candles[0].time, last: candles[candles.length - 1].time, length: candles.length }
      : { first: null, last: null, length: 0 };
    const previousRange = dataRangeRef.current;
    const rangeChanged =
      nextRange.first !== previousRange.first ||
      nextRange.last !== previousRange.last ||
      Math.abs(nextRange.length - previousRange.length) > 8;
    dataRangeRef.current = nextRange;
    candlesRef.current = candles;
    volumesRef.current = volumes;
    series.candle.applyOptions({
      borderVisible: true,
      wickVisible: true,
      wickUpColor: TERMINAL_CHART.green,
      wickDownColor: TERMINAL_CHART.coral,
    });
    series.candle.setData(candles);
    series.volume.setData(volumes);
    if (rangeChanged && previousRange.last != null && nextRange.last !== previousRange.last) {
      chart.timeScale().scrollToRealTime();
    }
    if (series.percent) series.percent.setData(percentRows);
    const shouldDrawAvwap = isDaily || !avwapSkewsIntradayScale;
    const shouldDrawAvwapBands = shouldDrawAvwap && data?.avwap_mode === "today_open";
    const avwapUpperRows = shouldDrawAvwapBands ? normalizeLineData(overlays.avwap_upper) : [];
    const avwapLowerRows = shouldDrawAvwapBands ? normalizeLineData(overlays.avwap_lower) : [];
    series.avwapUpper.setData(avwapUpperRows);
    series.avwapLower.setData(avwapLowerRows);
    series.avwap.setData(shouldDrawAvwap ? normalizeLineData(overlays.avwap) : []);
    series.ema20.setData(isDaily ? normalizeLineData(overlays.ema20) : []);
    series.ma50.setData(isDaily ? normalizeLineData(overlays.ma50) : []);
    series.ma200.setData(isDaily ? normalizeLineData(overlays.ma200) : []);
    series.rsi.setData(normalizeLineData(indicators.rsi));
    series.rsiMa.setData(normalizeLineData(indicators.rsi_ma));
    series.macd.setData(normalizeLineData(indicators.macd));
    series.macdSignal.setData(normalizeLineData(indicators.macd_signal));
    series.macdHist.setData(normalizeLineData(indicators.macd_hist).map((row) => ({
      ...row,
      color: row.value >= 0 ? "rgba(52, 211, 153, 0.42)" : "rgba(248, 113, 113, 0.42)",
    })));
    if (priceLinesRef.current.latest) series.candle.removePriceLine(priceLinesRef.current.latest);
    if (priceLinesRef.current.cost) series.candle.removePriceLine(priceLinesRef.current.cost);
    if (priceLinesRef.current.avwap) series.avwap.removePriceLine(priceLinesRef.current.avwap);
    priceLinesRef.current = {};
    if (Number(data?.latest_price) > 0) {
      priceLinesRef.current.latest = series.candle.createPriceLine({
        price: Number(data.latest_price),
        color: TERMINAL_CHART.yellow,
        lineWidth: 1,
        lineStyle: 2,
        axisLabelVisible: true,
        title: "Latest",
      });
    }
    if (Number(data?.user_avg_cost) > 0) {
      priceLinesRef.current.cost = series.candle.createPriceLine({
        price: Number(data.user_avg_cost),
        color: "rgba(180, 83, 9, 0.96)",
        lineWidth: 1,
        lineStyle: 2,
        axisLabelVisible: true,
        title: "Cost",
      });
    }
    if (shouldDrawAvwap && Number.isFinite(avwapValue) && avwapValue > 0) {
      priceLinesRef.current.avwap = series.avwap.createPriceLine({
        price: avwapValue,
        color: TERMINAL_CHART.cyan,
        lineWidth: 1,
        lineStyle: 2,
        axisLabelVisible: true,
        title: "AVWAP",
      });
    }
    if (!didFitContentRef.current && candles.length) {
      applyKlineDisplayRange(
        chart,
        candles,
        data?.interval === "1d" ? displayRange : "all",
        data?.interval === "1d" && displayRange === "earnings" ? data?.earnings_anchor : "",
        SINGLE_PROFILE_WIDTH_RATIO,
      );
      requestPriceAutoscale(series.candle);
      didFitContentRef.current = true;
    }
    window.requestAnimationFrame(() => {
      window.requestAnimationFrame(() => {
        setPercentAxisTicks(openingPercentAxisTicks(candles, series.candle, previousClose));
        const logicalRange = chart.timeScale().getVisibleLogicalRange();
        profileUpdaterRef.current?.(logicalRange);
      });
    });
  }, [candles, volumes, percentRows, overlays, indicators, data, isDaily, showOpeningPercentAxis, avwapSkewsIntradayScale, previousClose, displayRange, viewKey]);

  return (
    <div className="singleLwWrap">
      <div className="singleLwHeader">
        <div>
          <strong>{data?.symbol}</strong>
          <span>{data?.label} · {data?.interval} · {avwapText}{data?.avwap_anchor ? ` · 锚点 ${data.avwap_anchor}` : ""}{Number.isFinite(profilePoc) ? ` · 可见区间 POC ${fmtChartPrice(profilePoc, data?.symbol)}` : ""}</span>
        </div>
        <div className="klineDrawingTools" role="toolbar" aria-label="画线工具">
          <button type="button" className={drawingTool === "trend" ? "active" : ""} aria-pressed={drawingTool === "trend"} title="趋势线：依次点击起点和终点" onClick={() => {
            const next = drawingTool === "trend" ? "" : "trend";
            setDrawingTool(next);
            setDrawingDraft(null);
            setDrawingHint(next ? "点击图表选择趋势线起点" : "");
          }}><TrendingUp size={15} /><span>趋势线</span></button>
          <button type="button" className={drawingTool === "horizontal" ? "active" : ""} aria-pressed={drawingTool === "horizontal"} title="水平线：点击图表选择价位" onClick={() => {
            const next = drawingTool === "horizontal" ? "" : "horizontal";
            setDrawingTool(next);
            setDrawingDraft(null);
            setDrawingHint(next ? "点击图表选择水平价位" : "");
          }}><Minus size={15} /><span>水平线</span></button>
          <button type="button" disabled={!drawings.length} title="撤销上一条画线" onClick={() => setDrawings((current) => {
            const next = current.slice(0, -1);
            writeKlineDrawings(drawingKey, next);
            return next;
          })}><Undo2 size={15} /><span>撤销</span></button>
          <button type="button" disabled={!drawings.length} title="清空当前标的和周期的画线" onClick={() => {
            setDrawings([]);
            writeKlineDrawings(drawingKey, []);
            setDrawingDraft(null);
            setDrawingHint("");
          }}><Trash2 size={15} /><span>清空</span></button>
          {drawingHint ? <em>{drawingHint}</em> : null}
        </div>
        <div className={tone(data?.latest_change_pct)}>
          <strong>{fmtChartPrice(data?.latest_price, data?.symbol)}</strong>
          <span>{data?.latest_change_pct == null ? "-" : fmtPct(data.latest_change_pct)}</span>
        </div>
      </div>
      <div className={`singleLwCanvas ${drawingTool ? "isDrawing" : ""}`} ref={containerRef}>
        {candleTooltip.visible ? (
          <div className="klineCandleTooltip" style={{ left: `${candleTooltip.left}px`, top: `${candleTooltip.top}px` }}>
            <strong>{candleTooltip.time || "-"}</strong>
            <div><span>开</span><b>{fmtChartPrice(candleTooltip.open, data?.symbol)}</b><span>高</span><b>{fmtChartPrice(candleTooltip.high, data?.symbol)}</b></div>
            <div><span>低</span><b>{fmtChartPrice(candleTooltip.low, data?.symbol)}</b><span>收</span><b>{fmtChartPrice(candleTooltip.close, data?.symbol)}</b></div>
            <div><span>涨跌</span><b className={tone(candleTooltip.change)}>{candleTooltip.change >= 0 ? "+" : ""}{fmtChartPrice(candleTooltip.change, data?.symbol)} · {fmtPct(candleTooltip.changePct)}</b></div>
            <div><span>振幅</span><b>{Number(candleTooltip.amplitudePct || 0).toFixed(2)}%</b><span>量</span><b>{Number(candleTooltip.volume || 0).toLocaleString()}</b></div>
          </div>
        ) : null}
        {showOpeningPercentAxis ? <div className="klinePercentAxis singleKlinePercentAxis" aria-label="以当日开盘价为零的涨跌幅坐标轴">
          {Number.isFinite(percentReference) && percentReference > 0 ? <span className="klinePercentAxisBase">昨收 {fmtChartPrice(percentReference, data?.symbol)}</span> : null}
          {percentAxisTicks.map((tick) => <span key={tick.pct} style={{ top: `${tick.y}px` }}>{fmtPct(tick.pct)}</span>)}
        </div> : null}
        <div className="singleLwProfile" aria-label="可见区间成交量价格分布">
          {profileBars.map((bar, index) => (
            <span
              className={`singleLwProfileBar ${bar.isPoc ? "poc" : ""}`}
              key={`${index}-${bar.top}`}
              style={{ top: `${bar.top}px`, height: `${bar.height}px`, width: bar.width }}
              data-poc-label={bar.isPoc ? `价格箱 ${fmtChartPrice(bar.low, data?.symbol)} – ${fmtChartPrice(bar.high, data?.symbol)}\n成交量 ${Number(bar.volume || 0).toLocaleString()} · 峰值占比 ${(Number(bar.pct || 0) * 100).toFixed(1)}% · POC ${fmtChartPrice(profilePoc, data?.symbol)}` : undefined}
              title={bar.isPoc ? undefined : `价格箱 ${fmtChartPrice(bar.low, data?.symbol)} – ${fmtChartPrice(bar.high, data?.symbol)}\n成交量 ${Number(bar.volume || 0).toLocaleString()} · 峰值占比 ${(Number(bar.pct || 0) * 100).toFixed(1)}%`}
            />
          ))}
        </div>
        {!candles.length ? <div className="muted lwEmpty">暂无K线数据</div> : null}
      </div>
    </div>
  );
}

let klinePageMemory = {};

function defaultKlineAvwapMode(interval, symbol) {
  if (interval === "5m" || interval === "15m") return "today_open";
  return ["VOO", "QQQ", "SGOV", "510330.SS"].includes(symbol) ? "year_start" : "earnings";
}

function KlinePage({ dashboardData }) {
  const restoredState = useMemo(() => klinePageMemory, []);
  const [scope, setScope] = useState("global");
  const [symbol, setSymbol] = useState(() => String(restoredState.symbol || "VOO"));
  const [interval, setInterval] = useState(() => String(restoredState.interval || "1d"));
  const [avwapMode, setAvwapMode] = useState(() => String(
    restoredState.avwapMode
      || defaultKlineAvwapMode(String(restoredState.interval || "1d"), String(restoredState.symbol || "VOO"))
  ));
  const [displayRange, setDisplayRange] = useState(() => String(restoredState.displayRange || "60"));
  const [showExtended, setShowExtended] = useState(() => Boolean(restoredState.showExtended));
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [realtimeError, setRealtimeError] = useState("");
  const [realtimeConnected, setRealtimeConnected] = useState(false);
  const [globalColumns, setGlobalColumns] = useState(() => Number(restoredState.globalColumns) || globalKlineColumns());
  const [showCheatSheet, setShowCheatSheet] = useState(false);
  const [visibleProfileState, setVisibleProfileState] = useState(null);
  const isEtf = ["VOO", "QQQ", "SGOV", "510330.SS"].includes(symbol);
  const loadRequestRef = useRef(0);

  useEffect(() => {
    klinePageMemory = {
      symbol,
      interval,
      avwapMode,
      displayRange,
      showExtended,
      globalColumns,
    };
  }, [symbol, interval, avwapMode, displayRange, showExtended, globalColumns]);
  const requestSignature = `${scope}|${symbol}|${interval}|${avwapMode}|${showExtended}|${globalColumns}`;
  const requestSignatureRef = useRef(requestSignature);
  requestSignatureRef.current = requestSignature;

  async function load(options = {}) {
    const silent = Boolean(options.silent);
    const requestId = ++loadRequestRef.current;
    const signature = requestSignature;
    if (!silent) setLoading(true);
    setError("");
    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => controller.abort(), scope === "global" ? 30000 : 20000);
    try {
      const isEtfSymbol = ["VOO", "QQQ", "SGOV", "510330.SS"].includes(symbol);
      let effectiveAvwapMode = avwapMode;
      if (isEtfSymbol && effectiveAvwapMode === "earnings") effectiveAvwapMode = "high_60d";
      if (interval === "1d" && effectiveAvwapMode === "today_open") {
        effectiveAvwapMode = isEtfSymbol ? "high_60d" : "earnings";
      }
      const qs = new URLSearchParams(
        scope === "global"
          ? { interval, show_extended: String(showExtended), columns: String(globalColumns) }
          : { symbol, interval, avwap_mode: effectiveAvwapMode, show_extended: String(showExtended) }
      );
      const endpoint = scope === "global" ? "chart-board-global-light" : "chart-board-light";
      const response = await fetch(`${API_BASE}/api/${endpoint}?${qs.toString()}`, { signal: controller.signal });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      if (requestId !== loadRequestRef.current || signature !== requestSignatureRef.current) return;
      if (silent && scope === "global" && payload?.charts && payload.charts.length === 0) return;
      setData(payload);
    } catch (err) {
      if (requestId !== loadRequestRef.current || signature !== requestSignatureRef.current) return;
      setError(err instanceof DOMException && err.name === "AbortError" ? "K线请求超时" : (err instanceof Error ? err.message : String(err)));
    } finally {
      window.clearTimeout(timeoutId);
      if (!silent && requestId === loadRequestRef.current && signature === requestSignatureRef.current) setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, [scope, symbol, interval, avwapMode, showExtended, globalColumns]);

  useEffect(() => {
    if (interval === "1d") {
      setRealtimeConnected(false);
      setRealtimeError("");
      return undefined;
    }
    setRealtimeError("");
    const isEtfSymbol = ["VOO", "QQQ", "SGOV", "510330.SS"].includes(symbol);
    let effectiveAvwapMode = avwapMode;
    if (isEtfSymbol && effectiveAvwapMode === "earnings") effectiveAvwapMode = "high_60d";
    const qs = new URLSearchParams(
      scope === "global"
        ? { interval, show_extended: String(showExtended), columns: String(globalColumns) }
        : { symbol, interval, avwap_mode: effectiveAvwapMode, show_extended: String(showExtended) }
    );
    const endpoint = scope === "global" ? "chart-board-global-light" : "chart-board-light";
    const signature = requestSignature;
    const socket = new WebSocket(`${WS_BASE}/ws/${endpoint}?${qs.toString()}`);
    socket.onopen = () => {
      if (requestSignatureRef.current === signature) {
        setRealtimeConnected(true);
        setRealtimeError("");
      }
    };
    socket.onmessage = (event) => {
      if (requestSignatureRef.current !== signature) return;
      try {
        setData(JSON.parse(event.data));
      } catch {
        setRealtimeError("K线推送数据解析失败");
      }
    };
    socket.onerror = () => {
      if (requestSignatureRef.current === signature) setRealtimeError("实时订阅连接失败，当前显示已加载的 K 线");
    };
    socket.onclose = () => {
      if (requestSignatureRef.current === signature) setRealtimeConnected(false);
    };
    return () => {
      socket.close();
    };
  }, [scope, symbol, interval, avwapMode, showExtended, globalColumns]);

  useEffect(() => {
    function onResize() {
      setGlobalColumns((current) => {
        const next = globalKlineColumns();
        return next === current ? current : next;
      });
    }
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  useEffect(() => {
    if (scope === "single" && interval !== "1d") return undefined;
    setRealtimeConnected(false);
    return undefined;
  }, [scope, symbol, interval, avwapMode, showExtended]);

  useEffect(() => {
    if (displayRange === "earnings" && (scope !== "single" || isEtf)) setDisplayRange("60");
  }, [displayRange, scope, isEtf]);

  const avwapSelectValue = interval === "1d" && avwapMode === "today_open"
    ? (isEtf ? "high_60d" : "earnings")
    : (isEtf && avwapMode === "earnings" ? "high_60d" : avwapMode);

  function changeDisplayRange(nextRange) {
    setDisplayRange(nextRange);
  }

  function changeKlineSymbol(nextSymbol) {
    setSymbol(nextSymbol);
    setAvwapMode(defaultKlineAvwapMode(interval, nextSymbol));
  }

  function changeKlineInterval(nextInterval) {
    setInterval(nextInterval);
    setAvwapMode(defaultKlineAvwapMode(nextInterval, symbol));
  }

  function openSingleSymbolFromGlobal(nextSymbol) {
    setScope("single");
    setDisplayRange("60");
    changeKlineSymbol(nextSymbol);
  }

  const singleViewKey = `${data?.symbol || symbol}-${data?.interval || interval}-${data?.show_extended}-${data?.avwap_mode || avwapMode}-${displayRange}`;
  const activeVisibleProfile = visibleProfileState?.viewKey === singleViewKey ? visibleProfileState.profile : null;

  return (
    <section className="chartPanel technicalPanel">
      <div className="toolbarRow klineToolbar">
        <div className="klineControlGroup">
          {scope === "single" ? (
            <button className="klineHomeButton" type="button" onClick={() => {
              setScope("global");
              setDisplayRange("60");
            }} aria-label="返回全局 K 线看板" title="返回全局 K 线看板"><Home size={18} /><span>全局</span></button>
          ) : null}
          <label className="klineControl">
            <span>周期</span>
            <select value={interval} onChange={(event) => changeKlineInterval(event.target.value)} aria-label="K线周期">
              <option value="1d">日线</option>
              <option value="15m">15 min</option>
              <option value="5m">5 min</option>
            </select>
          </label>
          {scope === "single" ? <label className="klineControl klineSymbolControl">
            <span>标的</span>
            <select value={symbol} onChange={(event) => changeKlineSymbol(event.target.value)} aria-label="标的">
              {[...(dashboardData?.holdings || [])
                .filter((row) => row.currency === "USD" && row.symbol !== "SGOV")
                .map((row) => row.symbol)
                , "510330.SS"]
                .map((item) => <option key={item} value={item}>{item === "510330.SS" ? "510330 沪深300ETF" : item}</option>)}
            </select>
          </label> : null}
        </div>
        {scope === "single" ? <div className="klineControlGroup klineAnalysisControls">
          {interval === "1d" ? (
            <label className="klineControl klineRangeControl">
              <span>日 K 视窗</span>
              <select value={displayRange} onChange={(event) => changeDisplayRange(event.target.value)} aria-label="日K视窗">
                <option value="60">最近 60 根</option>
                <option value="250">最近 250 根 · 一年</option>
                <option value="125">最近 125 根 · 半年</option>
                <option value="selloff_60d">最近一次大跌起</option>
                <option value="rally_60d">最近一次大涨起</option>
                {!isEtf ? <option value="earnings">最近财报反应日起</option> : null}
              </select>
            </label>
          ) : null}
          <label className={`klineControl klineAvwapControl ${avwapSelectValue !== "none" ? "isActive" : ""}`}>
            <span>AVWAP 锚点</span>
            <select value={avwapSelectValue} onChange={(event) => setAvwapMode(event.target.value)} aria-label="AVWAP锚点">
              {!isEtf ? <option value="earnings">最近财报反应日</option> : null}
              <option value="year_start">年初</option>
              {!isEtf ? <option value="gap_60d">最近 Gap 日</option> : null}
              <option value="high_60d">最近 Swing High</option>
              <option value="low_60d">最近 Swing Low</option>
              {interval !== "1d" ? <option value="today_open">今日开盘</option> : null}
            </select>
          </label>
          {interval !== "1d" ? (
            <label className="klineControl">
              <span>交易时段</span>
              <select value={showExtended ? "extended" : "regular"} onChange={(event) => setShowExtended(event.target.value === "extended")} aria-label="交易时段">
                <option value="regular">仅常规盘</option>
                <option value="extended">含扩展盘</option>
              </select>
            </label>
          ) : null}
        </div> : null}
        {scope === "global" && interval !== "1d" ? (
          <div className="klineControlGroup klineAnalysisControls">
            <label className="klineControl">
              <span>交易时段</span>
              <select value={showExtended ? "extended" : "regular"} onChange={(event) => setShowExtended(event.target.value === "extended")} aria-label="交易时段">
                <option value="regular">仅常规盘</option>
                <option value="extended">含扩展盘</option>
              </select>
            </label>
          </div>
        ) : null}
        <button className="klineGuideButton" type="button" onClick={() => setShowCheatSheet(true)}><BookOpen size={16} />指标模板</button>
      </div>
      {data && scope === "global" ? <div className="muted">全局看板：{data.symbols?.join(" / ")} · {data.interval} · 手动刷新</div> : null}
      {data && scope === "single" ? <div className="muted">行情源 {data.market_provider || "-"} · {data.interval} · {realtimeConnected ? "实时订阅中" : "实时连接中"}{data.avwap_mode !== "none" && data.avwap_label ? ` · AVWAP：${data.avwap_label}${data.avwap_anchor ? `（锚点 ${data.avwap_anchor}）` : ""}` : ""}{data.user_avg_cost ? ` · 成本线 ${Number(data.user_avg_cost).toFixed(2)}` : ""}</div> : null}
      {loading ? <div className="muted">K线加载中</div> : null}
      {error || data?.error ? <div className="errorInline">K线加载失败：{error || data.error}</div> : null}
      {realtimeError && !(error || data?.error) ? <div className="muted">{realtimeError}</div> : null}
      {scope === "global" && data?.charts ? <GlobalLightweightBoard data={data} displayRange="all" viewKey={`${data.interval}-${data.show_extended}-${data.columns}`} onOpenSymbol={openSingleSymbolFromGlobal} /> : null}
      {scope === "single" && data?.candles ? (
        <div className={`klineAnalysisLayout ${interval !== "1d" ? "withoutInsight" : ""}`}>
          <SingleLightweightChart data={data} displayRange={displayRange} viewKey={singleViewKey} onVisibleProfileChange={setVisibleProfileState} />
          {interval === "1d" ? <TechnicalInsightPanel data={data} displayRange={displayRange} visibleProfile={activeVisibleProfile} /> : null}
        </div>
      ) : null}
      {showCheatSheet ? <TechnicalCheatSheetModal onClose={() => setShowCheatSheet(false)} /> : null}
    </section>
  );
}

function PnlBreakdownPanel({ data }) {
  const summary = data.summary || {};
  const fx = Number(summary.fx || 0);
  const avgFx = Number(summary.avg_fx_rate || fx);
  const usdRows = (data.holdings || []).filter((row) => row.currency === "USD");
  const usdCost = usdRows.reduce(
    (sum, row) => sum + Number(row.shares || 0) * Number(row.avg_cost || 0),
    0,
  );
  const usdCash = Number(data.balances?.cash_usd || 0);
  const fallbackFxPnlCny = (usdCost + usdCash) * (fx - avgFx);
  const fxPnlCny = Number.isFinite(Number(summary.usd_fx_pnl_cny))
    ? Number(summary.usd_fx_pnl_cny)
    : fallbackFxPnlCny;
  const realizedPnlCny = Number.isFinite(Number(summary.total_realized_pnl_cny))
    ? Number(summary.total_realized_pnl_cny)
    : Number(data.balances?.realized_cny || 0) + Number(data.balances?.realized_usd || 0) * fx;
  const totalPnlCny = Number(summary.total_pnl_cny || 0);
  const totalUnrealizedPnlCny = Number.isFinite(Number(summary.total_unrealized_pnl_cny))
    ? Number(summary.total_unrealized_pnl_cny)
    : totalPnlCny - realizedPnlCny;
  const investmentUnrealizedPnlCny = totalUnrealizedPnlCny - fxPnlCny;
  const investmentPnlCny = realizedPnlCny + investmentUnrealizedPnlCny;
  const splitTotal = Math.max(
    1,
    Math.abs(realizedPnlCny) + Math.abs(investmentUnrealizedPnlCny) + Math.abs(fxPnlCny),
  );
  const parts = [
    {
      key: "realized",
      label: "已变现盈亏",
      amount: realizedPnlCny,
      detail: `已平仓交易累计 = ${fmtMoney(realizedPnlCny, "CNY")}`,
    },
    {
      key: "unrealized",
      label: "未变现浮盈亏",
      amount: investmentUnrealizedPnlCny,
      detail: `总未变现 ${fmtMoney(totalUnrealizedPnlCny, "CNY")} - 汇率 ${fmtMoney(fxPnlCny, "CNY")} = ${fmtMoney(investmentUnrealizedPnlCny, "CNY")}`,
    },
    {
      key: "fx",
      label: "汇率浮盈亏",
      amount: fxPnlCny,
      detail: `(${fmtMoney(usdCost, "USD")} 投资成本 + ${fmtMoney(usdCash, "USD")} 现金) × (${fx.toFixed(4)} - ${avgFx.toFixed(4)}) = ${fmtMoney(fxPnlCny, "CNY")}`,
    },
  ];

  return (
    <section className="pnlBreakdownPanel homePnlBreakdown" aria-label="总资产累计盈亏构成">
      <div className="pnlBreakdownHeader">
        <strong>投资组合盈亏 vs 汇率盈亏</strong>
        <span>投资组合细分为已变现与未变现</span>
      </div>
      <div className="pnlBreakdownGrid">
        <div className="pnlBreakdownItem">
          <span>投资组合盈亏</span>
          <strong className={tone(investmentPnlCny)}>{fmtMoney(investmentPnlCny, "CNY")}</strong>
          <small>
            已变现 {fmtMoney(realizedPnlCny, "CNY")} + 未变现 {fmtMoney(investmentUnrealizedPnlCny, "CNY")} = {fmtMoney(investmentPnlCny, "CNY")}
          </small>
        </div>
        <div className="pnlBreakdownItem">
          <span>汇率盈亏</span>
          <strong className={tone(fxPnlCny)}>{fmtMoney(fxPnlCny, "CNY")}</strong>
          <small>{parts.find((part) => part.key === "fx")?.detail}</small>
        </div>
      </div>
      <div className="pnlSplitViz">
        <div
          className="pnlSplitTrack"
          tabIndex="0"
          aria-label={parts.map((part) => `${part.label} ${fmtMoney(part.amount, "CNY")}`).join("，")}
        >
          {parts.map((part) => (
            <div
              className={`pnlSplitSegment ${part.key} ${tone(part.amount)}`}
              key={`segment-${part.key}`}
              style={{ width: `${Math.abs(part.amount) / splitTotal * 100}%` }}
              title={`${part.label} ${fmtMoney(part.amount, "CNY")}`}
            />
          ))}
        </div>
        <div className="pnlSplitLegend">
          {parts.map((part) => (
            <span key={`legend-${part.key}`}><i className={`${part.key} ${tone(part.amount)}`} />{part.label} {fmtMoney(part.amount, "CNY")}</span>
          ))}
        </div>
      </div>
    </section>
  );
}

function AssetMetricCards({ data, holdings, balances, totalActions = null }) {
  const fx = Number(data.summary?.fx || 7.1);
  const avgFx = Number(data.summary?.avg_fx_rate || fx);
  const rows = data.holdings.map((row) => {
    const draft = holdings[row.symbol] || {};
    const shares = Number(draft.shares ?? row.shares ?? 0);
    const avgCost = Number(draft.avg_cost ?? row.avg_cost ?? 0);
    const price = Number(row.price || 0);
    const value = shares * price;
    const cost = shares * avgCost;
    const isUsd = row.currency === "USD";
    return { ...row, value, cost, valueCny: isUsd ? value * fx : value, costCny: isUsd ? cost * avgFx : cost };
  });

  const usdRows = rows.filter((row) => row.currency === "USD");
  const usdCost = usdRows.reduce((sum, row) => sum + row.cost, 0);
  const usdValue = usdRows.reduce((sum, row) => sum + row.value, 0);
  const attributedDividendUsd = Number(balances.voo_dividend_usd || 0) + Number(balances.sgov_dividend_usd || 0);
  const archivedPnlUsd = Object.values(data.archived_pnl || {})
    .filter((row) => !Boolean(row.included_in_realized))
    .reduce((sum, row) => sum + Number(row.pnl_usd || 0), 0);
  const usdUnrealized = Number.isFinite(Number(data.summary?.usd_unrealized_pnl_usd))
    ? Number(data.summary.usd_unrealized_pnl_usd)
    : usdValue - usdCost;
  const usdCash = Number(balances.cash_usd || 0);
  const usdCashCostBasis = Number(balances.cash_cost_basis_usd || 0);
  const cnyCashCostBasis = Number(balances.cash_cost_basis_cny || 0);
  const usdRealized = Number.isFinite(Number(data.summary?.usd_realized_pnl_usd))
    ? Number(data.summary.usd_realized_pnl_usd)
    : Number(balances.realized_usd || 0);
  const usdTotalPnl = usdRealized + usdUnrealized;
  const usdTotal = usdValue + usdCash;
  const usdReturn = usdCost ? (usdTotalPnl / usdCost) * 100 : 0;
  const cnyInvestmentUnrealized = rows
    .filter((row) => row.currency !== "USD")
    .reduce((sum, row) => sum + (row.value - row.cost), 0);

  const totalCostCny = rows.reduce((sum, row) => sum + row.costCny, 0);
  const totalValueCny = rows.reduce((sum, row) => sum + row.valueCny, 0);
  const investmentUnrealizedCny = usdUnrealized * fx + cnyInvestmentUnrealized;
  const fxUnrealizedCny = (usdCost + usdCashCostBasis) * (fx - avgFx);
  const calculatedTotalUnrealizedCny = investmentUnrealizedCny + fxUnrealizedCny;
  const totalReturnBasisCny = totalCostCny + usdCashCostBasis * avgFx + cnyCashCostBasis;
  const cashCny = Number(balances.cash_cny || 0) + usdCash * fx;
  const totalRealizedCny = Number.isFinite(Number(data.summary?.total_realized_pnl_cny))
    ? Number(data.summary.total_realized_pnl_cny)
    : Number(balances.realized_cny || 0) + usdRealized * fx;
  const totalUnrealizedCny = Number.isFinite(Number(data.summary?.total_unrealized_pnl_cny))
    ? Number(data.summary.total_unrealized_pnl_cny)
    : calculatedTotalUnrealizedCny;
  const totalPnlCny = totalRealizedCny + totalUnrealizedCny;
  const totalAssetsCny = totalValueCny + cashCny;
  const totalReturn = totalReturnBasisCny ? (totalPnlCny / totalReturnBasisCny) * 100 : 0;
  const usdDetailItems = [
    ["成本", fmtMoney(usdCost, "USD")],
    ["持仓市值", fmtMoney(usdValue, "USD")],
    ["已变现盈亏", fmtMoney(usdRealized, "USD")],
    ["合计盈亏", fmtMoney(usdTotalPnl, "USD")],
    ["现金", fmtMoney(usdCash, "USD")],
    ["现金成本基准", fmtMoney(usdCashCostBasis, "USD")],
    ["总资产", fmtMoney(usdTotal, "USD")],
    ["收益率", fmtPct(usdReturn)],
  ];
  const totalDetailItems = [
    ["成本基准", fmtMoney(totalReturnBasisCny, "CNY")],
    ["持仓市值", fmtMoney(totalValueCny, "CNY")],
    ["已变现盈亏", fmtMoney(totalRealizedCny, "CNY")],
    ["合计盈亏", fmtMoney(totalPnlCny, "CNY")],
    ["现金", fmtMoney(cashCny, "CNY")],
    ["现金成本基准", fmtMoney(usdCashCostBasis * avgFx + cnyCashCostBasis, "CNY")],
    ["总资产", fmtMoney(totalAssetsCny, "CNY")],
    ["收益率", fmtPct(totalReturn)],
  ];

  return (
    <div className="assetSections">
      <div className="assetMetricBlock">
        <h2>美元资产</h2>
        <div className="assetMetricGrid">
          <div className="assetMetricCard"><span>已变现盈亏</span><strong>{fmtMoney(usdRealized, "USD")}</strong></div>
          <div className="assetMetricCard"><span>未实现浮盈亏</span><strong className={tone(usdUnrealized)}>{fmtMoney(usdUnrealized, "USD")}</strong><em className={tone(usdReturn)}>{fmtPct(usdReturn)}</em></div>
          <div className="assetMetricCard"><span>合计盈亏</span><strong className={tone(usdTotalPnl)}>{fmtMoney(usdTotalPnl, "USD")}</strong></div>
        </div>
        <div className="assetDetailPanel">
          <div className="assetDetailGrid">
            {usdDetailItems.map(([label, value]) => (
              <div className="assetDetailItem" key={`usd-${label}`}>
                <span>{label}</span>
                <strong>{value}</strong>
              </div>
            ))}
          </div>
          <p>合计盈亏 = 已变现盈亏 + 未实现浮盈亏；收益率 = 合计盈亏 / 美元持仓成本。</p>
        </div>
      </div>
      <div className="assetMetricBlock">
        <div className="assetMetricTitleRow">
          <h2>总资产（折合CNY）</h2>
          {totalActions}
        </div>
        <div className="assetMetricGrid">
          <div className="assetMetricCard"><span>已变现盈亏</span><strong>{fmtMoney(totalRealizedCny, "CNY")}</strong></div>
          <div className="assetMetricCard"><span>未实现浮盈亏</span><strong className={tone(totalUnrealizedCny)}>{fmtMoney(totalUnrealizedCny, "CNY")}</strong><em className={tone(totalReturn)}>{fmtPct(totalReturn)}</em></div>
          <div className="assetMetricCard"><span>合计盈亏</span><strong className={tone(totalPnlCny)}>{fmtMoney(totalPnlCny, "CNY")}</strong></div>
        </div>
        <div className="assetDetailPanel">
          <div className="assetDetailGrid">
            {totalDetailItems.map(([label, value]) => (
              <div className="assetDetailItem" key={`total-${label}`}>
                <span>{label}</span>
                <strong>{value}</strong>
              </div>
            ))}
          </div>
          <p>合计盈亏 = 已变现盈亏 + 未实现浮盈亏；收益率 = 合计盈亏 / 成本基准。</p>
        </div>
      </div>
    </div>
  );
}

function buildSatelliteUniverseDraft(data) {
  const explicit = Array.isArray(data.satellite_universe) ? data.satellite_universe : [];
  const source = explicit.length
    ? explicit
    : Object.entries(data.satellite_targets || {}).map(([symbol, targetPct]) => {
        const holding = (data.holdings || []).find((row) => row.symbol === symbol) || {};
        return {
          symbol,
          target_pct: targetPct,
        };
      });
  return source.map((item) => ({
    symbol: item.symbol || "",
    target_pct: String(item.target_pct ?? 0),
  }));
}

function EditableHoldingsPage({ data, onSaved }) {
  const backdropPointerStartedOnSelf = useRef(false);
  const [holdings, setHoldings] = useState({});
  const [balances, setBalances] = useState({});
  const [budgetOpen, setBudgetOpen] = useState(false);
  const [editingHoldings, setEditingHoldings] = useState(false);
  const [editingBalances, setEditingBalances] = useState(false);
  const [universeOpen, setUniverseOpen] = useState(false);
  const [budgetInputs, setBudgetInputs] = useState({});
  const [balanceInputs, setBalanceInputs] = useState({});
  const [universeInputs, setUniverseInputs] = useState([]);
  const [savingBudget, setSavingBudget] = useState(false);
  const [savingHoldings, setSavingHoldings] = useState(false);
  const [savingBalances, setSavingBalances] = useState(false);
  const [savingUniverse, setSavingUniverse] = useState(false);
  const [balanceMessage, setBalanceMessage] = useState("");
  const [holdingMessage, setHoldingMessage] = useState("");
  const realtimeTotalValueCny = (data.holdings || []).reduce(
    (sum, row) => sum + Math.max(0, Number(row.value_cny || 0)),
    0,
  );

  function resetDraft() {
    setHoldings(Object.fromEntries(data.holdings.map((row) => [row.symbol, { shares: String(row.shares ?? 0), avg_cost: String(row.avg_cost ?? 0) }])));
    setBalances({
      cash_usd: String(data.balances?.cash_usd ?? 0),
      cash_cny: String(data.balances?.cash_cny ?? 0),
      cash_cost_basis_usd: String(data.balances?.cash_cost_basis_usd ?? 0),
      cash_cost_basis_cny: String(data.balances?.cash_cost_basis_cny ?? 0),
      realized_usd: String(data.balances?.realized_usd ?? 0),
      realized_cny: String(data.balances?.realized_cny ?? 0),
      voo_dividend_usd: String(data.balances?.voo_dividend_usd ?? 0),
      sgov_dividend_usd: String(data.balances?.sgov_dividend_usd ?? 0),
    });
  }

  useEffect(() => {
    if (!editingHoldings) resetDraft();
  }, [data, editingHoldings]);

  useEffect(() => {
    setBudgetInputs(Object.fromEntries(Object.entries(data.rebalance?.future_cash_by_month || {}).map(([month, amount]) => [month, Number(amount || 0).toFixed(2)])));
    if (!universeOpen) setUniverseInputs(buildSatelliteUniverseDraft(data));
  }, [data.rebalance?.future_cash_by_month, data.satellite_universe, data.satellite_targets, universeOpen]);

  function resetBalanceDraft() {
    setBalanceInputs({
      cash_usd: String(data.balances?.cash_usd ?? 0),
      cash_cny: String(data.balances?.cash_cny ?? 0),
      cash_cost_basis_usd: String(data.balances?.cash_cost_basis_usd ?? 0),
      cash_cost_basis_cny: String(data.balances?.cash_cost_basis_cny ?? 0),
      realized_usd: String(data.balances?.realized_usd ?? 0),
      realized_cny: String(data.balances?.realized_cny ?? 0),
      voo_dividend_usd: String(data.balances?.voo_dividend_usd ?? 0),
      sgov_dividend_usd: String(data.balances?.sgov_dividend_usd ?? 0),
    });
  }

  useEffect(() => {
    if (!editingBalances) resetBalanceDraft();
  }, [data.balances, editingBalances]);

  function updateBudget(month, value) {
    setBudgetInputs((prev) => ({ ...prev, [month]: value }));
  }

  function updateBalance(key, value) {
    setBalanceInputs((prev) => ({ ...prev, [key]: value }));
  }

  function updateHolding(symbol, key, value) {
    setHoldings((prev) => ({
      ...prev,
      [symbol]: { ...(prev[symbol] || {}), [key]: value },
    }));
  }

  function trackBackdropPointerDown(event) {
    backdropPointerStartedOnSelf.current = event.target === event.currentTarget;
  }

  function shouldCloseFromBackdropClick(event) {
    const shouldClose = backdropPointerStartedOnSelf.current && event.target === event.currentTarget;
    backdropPointerStartedOnSelf.current = false;
    return shouldClose;
  }

  function updateUniverse(index, key, value) {
    setUniverseInputs((prev) => prev.map((item, idx) => (idx === index ? { ...item, [key]: value } : item)));
  }

  function addUniverseRow() {
    setUniverseInputs((prev) => [...prev, { symbol: "", target_pct: "0" }]);
  }

  function removeUniverseRow(index) {
    setUniverseInputs((prev) => prev.filter((_, idx) => idx !== index));
  }

  async function saveBudget() {
    setSavingBudget(true);
    try {
      const planned_cash_by_month = Object.fromEntries(Object.entries(budgetInputs).map(([month, value]) => [month, Number(value || 0)]));
      const response = await fetch(`${API_BASE}/api/rebalance/budget`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: data.user_id, planned_cash_by_month }),
      });
      if (!response.ok) throw new Error(`budget HTTP ${response.status}`);
      setBudgetOpen(false);
      await onSaved();
    } finally {
      setSavingBudget(false);
    }
  }

  async function saveBalances() {
    setSavingBalances(true);
    setBalanceMessage("");
    try {
      const nextBalances = buildBalancesPayload(balanceInputs);
      const response = await fetch(`${API_BASE}/api/balances`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ balances: nextBalances }),
      });
      if (!response.ok) throw new Error(await readApiError(response, `balances HTTP ${response.status}`));
      setBalanceMessage("现金与已变现已保存");
      window.alert("现金与已变现已保存");
      setEditingBalances(false);
      await onSaved();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setBalanceMessage(message);
      window.alert(`现金保存失败：${message}`);
    } finally {
      setSavingBalances(false);
    }
  }

  async function saveHoldingsAnchor() {
    setSavingHoldings(true);
    setHoldingMessage("");
    try {
      const nextHoldings = Object.fromEntries(data.holdings.map((row) => {
        const draft = holdings[row.symbol] || {};
        const shares = Number(draft.shares);
        const avgCost = Number(draft.avg_cost);
        if (!Number.isFinite(shares) || shares < 0 || !Number.isFinite(avgCost) || avgCost < 0) {
          throw new Error(`${row.symbol} 的数量和成本价必须是非负数字`);
        }
        return [row.symbol, { shares, avg_cost: avgCost }];
      }));
      const response = await fetch(`${API_BASE}/api/holdings`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ holdings: nextHoldings }),
      });
      if (!response.ok) throw new Error(await readApiError(response, `holdings HTTP ${response.status}`));
      const result = await response.json();
      const anchorDate = result?.adjustment?.effective_date || "今天";
      const successMessage = `持仓已保存，${anchorDate} 已设为新的准确锚点；浮盈亏和收益快照已重新计算`;
      setHoldingMessage(successMessage);
      setEditingHoldings(false);
      await onSaved();
      window.alert(successMessage);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setHoldingMessage(`持仓保存失败：${message}`);
      window.alert(`持仓保存失败：${message}`);
    } finally {
      setSavingHoldings(false);
    }
  }

  async function saveUniverse() {
    setSavingUniverse(true);
    try {
      const items = universeInputs
        .map((item) => ({
          symbol: String(item.symbol || "").trim().toUpperCase(),
          label: String(item.symbol || "").trim().toUpperCase(),
          target_pct: Number(item.target_pct || 0),
        }))
        .filter((item) => item.symbol);
      const response = await fetch(`${API_BASE}/api/satellite-universe`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ items }),
      });
      if (!response.ok) throw new Error(`satellite universe HTTP ${response.status}`);
      setUniverseOpen(false);
      await onSaved();
    } finally {
      setSavingUniverse(false);
    }
  }

  const futureBudgetTotal = useMemo(() => Object.values(budgetInputs).reduce((sum, value) => sum + Number(value || 0), 0), [budgetInputs]);
  const holdingActions = (
    <div className="assetTitleActions">
      <button className="toolButton compactTool" onClick={() => {
        resetDraft();
        setHoldingMessage("");
        setEditingHoldings((value) => !value);
      }}>{editingHoldings ? "取消编辑" : "编辑持仓"}</button>
      <button className="toolButton compactTool" onClick={() => setBudgetOpen(true)}>预算</button>
      <button className="toolButton compactTool" onClick={() => setEditingBalances(true)}>现金</button>
      <button className="toolButton compactTool" onClick={() => setUniverseOpen(true)}>标的</button>
    </div>
  );

  return (
    <section>
      <AssetMetricCards data={data} holdings={holdings} balances={balances} totalActions={holdingActions} />
      {editingHoldings ? (
        <div className="holdingAnchorBar">
          <span>修改数量或成本价后，保存日将成为新的准确持仓锚点；无需补齐锚点前的历史交易。</span>
          <button className="primary" onClick={saveHoldingsAnchor} disabled={savingHoldings}>
            <Save size={16} /> {savingHoldings ? "保存中…" : "保存并设为新锚点"}
          </button>
        </div>
      ) : null}
      {balanceMessage ? <div className={balanceMessage === "现金与已变现已保存" ? "saveMessage up" : "saveMessage down"}>{balanceMessage}</div> : null}
      <div className="tableWrap">
        <table className="editableHoldingsTable">
          <thead>
            <tr>
              <th>标的</th><th>实时占比</th><th>数量</th><th>当前价</th><th>当日涨跌</th><th>60日回撤</th><th>60日涨幅</th><th>成本</th><th>市值</th><th>盈亏</th>
            </tr>
          </thead>
          <tbody>
            {data.holdings.map((row) => (
              <tr key={row.symbol}>
                <th>{row.label}</th>
                <td>{realtimeTotalValueCny > 0 ? `${(Number(row.value_cny || 0) / realtimeTotalValueCny * 100).toFixed(2)}%` : "-"}</td>
                <td>{editingHoldings ? (
                  <input
                    className="holdingCellInput"
                    value={holdings[row.symbol]?.shares ?? ""}
                    onChange={(event) => updateHolding(row.symbol, "shares", event.target.value)}
                    inputMode="decimal"
                    aria-label={`${row.symbol} 数量`}
                  />
                ) : Number(row.shares || 0).toLocaleString(undefined, { maximumFractionDigits: 4 })}</td>
                <td>{fmtMoney(row.price, row.currency, row.currency === "USD" ? 2 : 4)}</td>
                <td className={tone(row.effective_daily_pct)}>{fmtPct(row.effective_daily_pct)}</td>
                <td className={tone(row.drawdown_pct)}>{row.drawdown_pct == null ? "-" : fmtPct(row.drawdown_pct)}</td>
                <td className={tone(row.rebound_pct)}>{row.rebound_pct == null ? "-" : fmtPct(row.rebound_pct)}</td>
                <td>{editingHoldings ? (
                  <input
                    className="holdingCellInput"
                    value={holdings[row.symbol]?.avg_cost ?? ""}
                    onChange={(event) => updateHolding(row.symbol, "avg_cost", event.target.value)}
                    inputMode="decimal"
                    aria-label={`${row.symbol} 成本价`}
                  />
                ) : fmtMoney(row.avg_cost, row.currency, row.currency === "USD" ? 2 : 4)}</td>
                <td>{fmtMoney(row.value, row.currency)}</td>
                <td className={tone(row.pnl)}>{fmtMoney(row.pnl, row.currency)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {budgetOpen ? (
        <div className="modalBackdrop" role="presentation" onClick={(event) => {
          if (event.target === event.currentTarget) setBudgetOpen(false);
        }}>
          <div className="modalPanel" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
            <div className="sectionHeader">
              <h2>预算</h2>
              <button className="toolButton compactTool" onClick={() => setBudgetOpen(false)} disabled={savingBudget}>关闭</button>
            </div>
            <div className="muted">未来预算 {fmtMoney(futureBudgetTotal, "USD")} · 计划分母 {fmtMoney(data.rebalance?.planned_total_usd || 0, "USD")}</div>
            <div className="budgetEditGrid">
              {Object.entries(budgetInputs).map(([month, value]) => (
                <label key={month}>{month} 可投入(USD)<input value={value} onChange={(event) => updateBudget(month, event.target.value)} inputMode="decimal" /></label>
              ))}
            </div>
            <div className="actions">
              <button onClick={() => setBudgetOpen(false)} disabled={savingBudget}>取消</button>
              <button className="primary" onClick={saveBudget} disabled={savingBudget}><Save size={16} /> 保存</button>
            </div>
          </div>
        </div>
      ) : null}
      {editingBalances ? (
        <div className="modalBackdrop" role="presentation" onPointerDown={trackBackdropPointerDown} onClick={(event) => {
          if (shouldCloseFromBackdropClick(event)) {
            resetBalanceDraft();
            setEditingBalances(false);
          }
        }}>
          <div className="modalPanel balanceModal" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
            <div className="sectionHeader">
              <div>
                <h2>现金与已变现</h2>
                <span className="muted">维护现金、已变现盈亏和 SGOV 股息，保存后会重算看板。</span>
              </div>
              <button className="toolButton compactTool" onClick={() => { resetBalanceDraft(); setEditingBalances(false); }} disabled={savingBalances}>关闭</button>
            </div>
            <div className="balanceEditGrid">
              <label><span>USD 现金</span><input value={balanceInputs.cash_usd ?? ""} onChange={(event) => updateBalance("cash_usd", event.target.value)} inputMode="decimal" /></label>
              <label><span>CNY 现金</span><input value={balanceInputs.cash_cny ?? ""} onChange={(event) => updateBalance("cash_cny", event.target.value)} inputMode="decimal" /></label>
              <label><span>USD 现金成本基准</span><input value={balanceInputs.cash_cost_basis_usd ?? ""} onChange={(event) => updateBalance("cash_cost_basis_usd", event.target.value)} inputMode="decimal" /></label>
              <label><span>CNY 现金成本基准</span><input value={balanceInputs.cash_cost_basis_cny ?? ""} onChange={(event) => updateBalance("cash_cost_basis_cny", event.target.value)} inputMode="decimal" /></label>
              <label><span>USD 已变现</span><input value={balanceInputs.realized_usd ?? ""} onChange={(event) => updateBalance("realized_usd", event.target.value)} inputMode="decimal" /></label>
              <label><span>CNY 已变现</span><input value={balanceInputs.realized_cny ?? ""} onChange={(event) => updateBalance("realized_cny", event.target.value)} inputMode="decimal" /></label>
              <label><span>VOO 累计分红</span><input value={balanceInputs.voo_dividend_usd ?? ""} onChange={(event) => updateBalance("voo_dividend_usd", event.target.value)} inputMode="decimal" /></label>
              <label><span>SGOV 股息</span><input value={balanceInputs.sgov_dividend_usd ?? ""} onChange={(event) => updateBalance("sgov_dividend_usd", event.target.value)} inputMode="decimal" /></label>
            </div>
            <p className="muted">现金成本基准用于区分本金和已变现收益；普通入金、出金会自动同步，盈利再投资后允许显示为负数。</p>
            <div className="actions">
              <button onClick={() => { resetBalanceDraft(); setEditingBalances(false); }} disabled={savingBalances}>取消</button>
              <button className="primary" onClick={saveBalances} disabled={savingBalances}><Save size={16} /> 保存</button>
            </div>
          </div>
        </div>
      ) : null}
      {universeOpen ? (
        <div className="modalBackdrop" role="presentation" onClick={(event) => {
          if (event.target === event.currentTarget) setUniverseOpen(false);
        }}>
          <div className="modalPanel satelliteUniverseModal" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
            <div className="sectionHeader">
              <h2>卫星标的</h2>
              <button className="toolButton compactTool" onClick={() => setUniverseOpen(false)} disabled={savingUniverse}>关闭</button>
            </div>
            <div className="satelliteUniverseRows">
              {universeInputs.map((item, index) => {
                const targetValue = Number(item.target_pct || 0);
                return (
                  <div className="satelliteUniverseRow" key={`holdings-universe-${index}`}>
                    <label>标的<input value={item.symbol} onChange={(event) => updateUniverse(index, "symbol", event.target.value)} /></label>
                    <div className="universeTargetCell" style={{ "--target-fill": `${Math.min(100, Math.max(0, targetValue))}%` }}>
                      <div className="targetSliderLabel"><strong>目标比例</strong><span>{targetValue.toFixed(2)}%</span></div>
                      <input className="targetRange" type="range" min="0" max="100" step="0.1" value={targetValue} onChange={(event) => updateUniverse(index, "target_pct", event.target.value)} />
                      <div className="targetNumberWrap">
                        <input className="targetNumber" value={item.target_pct} onChange={(event) => updateUniverse(index, "target_pct", event.target.value)} inputMode="decimal" />
                        <span>%</span>
                      </div>
                    </div>
                    <button className="iconDanger" aria-label={`删除 ${item.symbol || "空行"}`} onClick={() => removeUniverseRow(index)}><Trash2 size={16} /></button>
                  </div>
                );
              })}
            </div>
            <div className="actions">
              <button onClick={addUniverseRow}><Plus size={16} /> 新增</button>
              <button className="primary" onClick={saveUniverse} disabled={savingUniverse}><Save size={16} /> 保存</button>
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
}

function Rebalance({ data, onSaved }) {
  const rows = data.rebalance.rows;
  const suggestionRows = useMemo(() => rows.filter((row) => row.symbol !== "001015"), [rows]);
  const realtimeHoldingBySymbol = useMemo(
    () => Object.fromEntries((data.holdings || []).map((row) => [row.symbol, row])),
    [data.holdings],
  );
  const tradeRows = useMemo(() => {
    const bySymbol = new Map((data.holdings || []).map((row) => [row.symbol, { ...row, intensity: "normal", suggested_buy_usd: 0 }]));
    rows.forEach((row) => {
      bySymbol.set(row.symbol, { ...(bySymbol.get(row.symbol) || {}), ...row });
    });
    return Array.from(bySymbol.values()).filter((row) => row.symbol);
  }, [data.holdings, rows]);
  const currencyBySymbol = useMemo(
    () => Object.fromEntries((data.holdings || []).map((row) => [row.symbol, row.currency || "USD"])),
    [data.holdings],
  );
  const backdropPointerStartedOnSelf = useRef(false);
  const defaultTradeDate = previousTradingDateFromShanghai();
  const [activeTradeSymbol, setActiveTradeSymbol] = useState("");
  const [budgetOpen, setBudgetOpen] = useState(false);
  const [rulesOpen, setRulesOpen] = useState(false);
  const [statisticsOpen, setStatisticsOpen] = useState(false);
  const [universeOpen, setUniverseOpen] = useState(false);
  const [fxConversionOpen, setFxConversionOpen] = useState(false);
  const [inputs, setInputs] = useState({});
  const [budgetInputs, setBudgetInputs] = useState({});
  const [balanceInputs, setBalanceInputs] = useState({});
  const [fxInputs, setFxInputs] = useState({});
  const [universeInputs, setUniverseInputs] = useState([]);
  const [editingBalances, setEditingBalances] = useState(false);
  const [saving, setSaving] = useState(false);
  const [savingBudget, setSavingBudget] = useState(false);
  const [savingBalances, setSavingBalances] = useState(false);
  const [savingFxConversion, setSavingFxConversion] = useState(false);
  const [savingUniverse, setSavingUniverse] = useState(false);
  const [deletingTradeId, setDeletingTradeId] = useState("");
  const [deletingFxConversionId, setDeletingFxConversionId] = useState("");
  const [tradeHistoryOpen, setTradeHistoryOpen] = useState(false);
  const [fxHistoryOpen, setFxHistoryOpen] = useState(false);
  const [tradeFilter, setTradeFilter] = useState("");
  const [tradeActionFilter, setTradeActionFilter] = useState("all");
  const [tradeSort, setTradeSort] = useState({ key: "trade_date", direction: "desc" });
  const [fxFilter, setFxFilter] = useState("");
  const [fxSort, setFxSort] = useState({ key: "converted_date", direction: "desc" });

  const [balanceMessage, setBalanceMessage] = useState("");
  const [tradeMessage, setTradeMessage] = useState("");
  const [fxConversionMessage, setFxConversionMessage] = useState("");
  const [tradeToast, setTradeToast] = useState(null);

  useEffect(() => {
    setBudgetInputs(Object.fromEntries(Object.entries(data.rebalance.future_cash_by_month || {}).map(([month, amount]) => [month, Number(amount || 0).toFixed(2)])));
    if (!universeOpen) {
      setUniverseInputs(buildSatelliteUniverseDraft(data));
    }
    if (activeTradeSymbol) return;
    const next = {};
    tradeRows.forEach((row) => {
      const buyAmount = Number(row.suggested_buy_usd || 0);
      const sellAmount = Number(row.suggested_sell_usd || 0);
      next[row.symbol] = {
        action: sellAmount > buyAmount ? "sell" : "buy",
        trade_date: defaultTradeDate,
        amount_usd: Math.max(buyAmount, sellAmount).toFixed(2),
        shares: "",
        intensity: row.intensity || "normal",
      };
    });
    setInputs(next);
  }, [data.rebalance.month_key, tradeRows, activeTradeSymbol, data.rebalance.future_cash_by_month, defaultTradeDate, data.satellite_universe, data.satellite_targets, data.holdings, universeOpen]);

  useEffect(() => {
    if (!tradeToast) return undefined;
    const id = window.setTimeout(() => setTradeToast(null), 3200);
    return () => window.clearTimeout(id);
  }, [tradeToast]);

  function showTradeToast(message, status = "up") {
    setTradeToast({ id: Date.now(), message, status });
  }

  function resetBalanceDraft() {
    setBalanceInputs({
      cash_usd: String(data.balances?.cash_usd ?? 0),
      cash_cny: String(data.balances?.cash_cny ?? 0),
      cash_cost_basis_usd: String(data.balances?.cash_cost_basis_usd ?? 0),
      cash_cost_basis_cny: String(data.balances?.cash_cost_basis_cny ?? 0),
      realized_usd: String(data.balances?.realized_usd ?? 0),
      realized_cny: String(data.balances?.realized_cny ?? 0),
      voo_dividend_usd: String(data.balances?.voo_dividend_usd ?? 0),
      sgov_dividend_usd: String(data.balances?.sgov_dividend_usd ?? 0),
    });
  }

  useEffect(() => {
    if (editingBalances) return;
    resetBalanceDraft();
  }, [data.balances, editingBalances]);

  useEffect(() => {
    setFxInputs((prev) => ({
      converted_date: prev.converted_date || defaultTradeDate,
      cny_amount: prev.cny_amount ?? "",
      usd_amount: prev.usd_amount ?? "",
      note: prev.note ?? "",
    }));
  }, [defaultTradeDate]);

  const tradeTotals = useMemo(() => Object.entries(inputs).reduce(
    (totals, item) => {
      const [symbol, value] = item;
      const side = value.action === "sell" ? "sell" : "buy";
      const currency = currencyBySymbol[symbol] === "CNY" ? "CNY" : "USD";
      totals[`${side}_${currency}`] += Number(value.amount_usd || 0);
      return totals;
    },
    { buy_USD: 0, sell_USD: 0, buy_CNY: 0, sell_CNY: 0 },
  ), [inputs, currencyBySymbol]);
  const futureBudgetTotal = useMemo(() => Object.values(budgetInputs).reduce((sum, value) => sum + Number(value || 0), 0), [budgetInputs]);
  const sortedTrades = useMemo(() => {
    const query = tradeFilter.trim().toLowerCase();
    const rows = (data.trades || []).filter((trade) => {
      if (tradeActionFilter !== "all" && String(trade.action || "buy") !== tradeActionFilter) return false;
      if (!query) return true;
      return [trade.trade_date, trade.date, trade.symbol, trade.action, trade.intensity]
        .some((value) => String(value || "").toLowerCase().includes(query));
    });
    const valueFor = (trade) => {
      if (tradeSort.key === "trade_date") return `${trade.trade_date || trade.date || ""} ${trade.created_at || ""}`;
      return trade[tradeSort.key] ?? "";
    };
    return rows.slice().sort((left, right) => {
      const leftValue = valueFor(left);
      const rightValue = valueFor(right);
      const leftNumber = Number(leftValue);
      const rightNumber = Number(rightValue);
      const comparison = Number.isFinite(leftNumber) && Number.isFinite(rightNumber)
        ? leftNumber - rightNumber
        : String(leftValue).localeCompare(String(rightValue), "zh-CN", { numeric: true });
      return tradeSort.direction === "asc" ? comparison : -comparison;
    });
  }, [data.trades, tradeFilter, tradeActionFilter, tradeSort]);
  const visibleTrades = tradeHistoryOpen ? sortedTrades : sortedTrades.slice(0, 3);
  const sortedFxConversions = useMemo(() => {
    const query = fxFilter.trim().toLowerCase();
    const rows = (data.fx_conversions || []).filter((record) => (
      !query || [record.converted_date, record.cny_amount, record.usd_amount, record.rate, record.note]
        .some((value) => String(value || "").toLowerCase().includes(query))
    ));
    return rows.slice().sort((left, right) => {
      const leftValue = left[fxSort.key] ?? "";
      const rightValue = right[fxSort.key] ?? "";
      const leftNumber = Number(leftValue);
      const rightNumber = Number(rightValue);
      const comparison = Number.isFinite(leftNumber) && Number.isFinite(rightNumber)
        ? leftNumber - rightNumber
        : String(leftValue).localeCompare(String(rightValue), "zh-CN", { numeric: true });
      return fxSort.direction === "asc" ? comparison : -comparison;
    });
  }, [data.fx_conversions, fxFilter, fxSort]);
  const visibleFxConversions = fxHistoryOpen ? sortedFxConversions : sortedFxConversions.slice(0, 3);

  function toggleRecordSort(setter, current, key) {
    setter(current.key === key
      ? { key, direction: current.direction === "asc" ? "desc" : "asc" }
      : { key, direction: "asc" });
  }

  function recordSortMark(current, key) {
    if (current.key !== key) return "";
    return current.direction === "asc" ? " \u25b2" : " \u25bc";
  }

  function update(symbol, key, value) {
    setInputs((prev) => ({ ...prev, [symbol]: { ...prev[symbol], [key]: value } }));
  }

  function updateBudget(month, value) {
    setBudgetInputs((prev) => ({ ...prev, [month]: value }));
  }

  function updateBalance(key, value) {
    setBalanceInputs((prev) => ({ ...prev, [key]: value }));
  }

  function updateFxInput(key, value) {
    setFxInputs((prev) => ({ ...prev, [key]: value }));
  }

  function updateUniverse(index, key, value) {
    setUniverseInputs((prev) => prev.map((item, idx) => (idx === index ? { ...item, [key]: value } : item)));
  }

  function trackBackdropPointerDown(event) {
    backdropPointerStartedOnSelf.current = event.target === event.currentTarget;
  }

  function shouldCloseFromBackdropClick(event) {
    const shouldClose = backdropPointerStartedOnSelf.current && event.target === event.currentTarget;
    backdropPointerStartedOnSelf.current = false;
    return shouldClose;
  }

  function addUniverseRow() {
    setUniverseInputs((prev) => [...prev, { symbol: "", target_pct: "0" }]);
  }

  function removeUniverseRow(index) {
    setUniverseInputs((prev) => prev.filter((_, idx) => idx !== index));
  }

  function openUniverseEditor() {
    setUniverseInputs(buildSatelliteUniverseDraft(data));
    setUniverseOpen(true);
  }

  function clearPending(symbolToClear = "") {
    setInputs((prev) => Object.fromEntries(Object.keys(prev).map((symbol) => [
      symbol,
      symbolToClear && symbol !== symbolToClear
        ? prev[symbol]
        : { ...prev[symbol], trade_date: prev[symbol]?.trade_date || defaultTradeDate, amount_usd: "0.00", shares: "0" },
    ])));
  }

  function openTradeEditor(symbol) {
    setActiveTradeSymbol((prev) => (prev === symbol ? "" : symbol));
  }

  function openFxConversionEditor() {
    setFxInputs((prev) => ({
      converted_date: prev.converted_date || defaultTradeDate,
      cny_amount: prev.cny_amount ?? "",
      usd_amount: prev.usd_amount ?? "",
      note: prev.note ?? "",
    }));
    setFxConversionOpen(true);
  }

  async function save(symbolToSave = "") {
    setSaving(true);
    setTradeMessage("");
    setTradeToast(null);
    try {
      const executions = Object.entries(inputs)
        .filter(([symbol]) => !symbolToSave || symbol === symbolToSave)
        .map(([symbol, item]) => ({
          symbol,
          action: item.action || "buy",
          trade_date: item.trade_date || defaultTradeDate,
          amount_usd: Number(item.amount_usd || 0),
          shares: Number(item.shares || 0),
          intensity: item.intensity,
        }))
        .filter((item) => item.amount_usd > 0 && item.shares > 0);
      if (!executions.length) {
        throw new Error("请填写成交金额和成交股数");
      }
      const response = await fetch(`${API_BASE}/api/rebalance/confirm`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ user_id: data.user_id, executions }) });
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.detail || `HTTP ${response.status}`);
      }
      setActiveTradeSymbol("");
      await onSaved();
      showTradeToast("交易已保存", "up");
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setTradeMessage(message);
      showTradeToast(`交易保存失败：${message}`, "down");
    } finally {
      setSaving(false);
    }
  }

  async function deleteTrade(trade) {
    const tradeId = String(trade.id || "");
    if (!tradeId) return;
    const confirmed = window.confirm(`确认撤销 ${trade.trade_date || trade.date || ""} ${trade.symbol} ${trade.action === "sell" ? "卖出" : "买入"} 记录？`);
    if (!confirmed) return;
    setDeletingTradeId(tradeId);
    setTradeMessage("");
    try {
      const response = await fetch(`${API_BASE}/api/trades/${encodeURIComponent(tradeId)}`, {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: data.user_id }),
      });
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.detail || `HTTP ${response.status}`);
      }
      await onSaved();
      showTradeToast("交易已撤销", "up");
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setTradeMessage(message);
      showTradeToast(`交易撤销失败：${message}`, "down");
    } finally {
      setDeletingTradeId("");
    }
  }

  async function saveFxConversion() {
    setSavingFxConversion(true);
    setFxConversionMessage("");
    try {
      const response = await fetch(`${API_BASE}/api/fx-conversions`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          user_id: data.user_id,
          converted_date: fxInputs.converted_date || defaultTradeDate,
          cny_amount: Number(fxInputs.cny_amount || 0),
          usd_amount: Number(fxInputs.usd_amount || 0),
          note: fxInputs.note || "",
        }),
      });
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.detail || `HTTP ${response.status}`);
      }
      await onSaved();
      setFxInputs((prev) => ({ ...prev, cny_amount: "", usd_amount: "", note: "" }));
      setFxConversionOpen(false);
      showTradeToast("购汇记录已保存", "up");
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setFxConversionMessage(message);
      showTradeToast(`购汇保存失败：${message}`, "down");
    } finally {
      setSavingFxConversion(false);
    }
  }

  async function deleteFxConversion(record) {
    const recordId = String(record.id || "");
    if (!recordId) return;
    const confirmed = window.confirm(`确认撤销 ${record.converted_date || ""} 购汇记录？`);
    if (!confirmed) return;
    setDeletingFxConversionId(recordId);
    setFxConversionMessage("");
    try {
      const response = await fetch(`${API_BASE}/api/fx-conversions/${encodeURIComponent(recordId)}`, {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: data.user_id }),
      });
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.detail || `HTTP ${response.status}`);
      }
      await onSaved();
      showTradeToast("购汇记录已撤销", "up");
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setFxConversionMessage(message);
      showTradeToast(`购汇撤销失败：${message}`, "down");
    } finally {
      setDeletingFxConversionId("");
    }
  }

  async function saveBudget() {
    setSavingBudget(true);
    try {
      const planned_cash_by_month = Object.fromEntries(Object.entries(budgetInputs).map(([month, value]) => [month, Number(value || 0)]));
      const response = await fetch(`${API_BASE}/api/rebalance/budget`, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ user_id: data.user_id, planned_cash_by_month }) });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      await onSaved();
      setBudgetOpen(false);
    } finally {
      setSavingBudget(false);
    }
  }

  async function saveBalances() {
    setSavingBalances(true);
    setBalanceMessage("");
    try {
      const balances = buildBalancesPayload(balanceInputs);
      const response = await fetch(`${API_BASE}/api/balances`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ balances }),
      });
      if (!response.ok) throw new Error(await readApiError(response, `balances HTTP ${response.status}`));
      setBalanceMessage("现金与已变现已保存");
      window.alert("现金与已变现已保存");
      setEditingBalances(false);
      onSaved().catch(() => {});
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setBalanceMessage(message);
      window.alert(`现金保存失败：${message}`);
    } finally {
      setSavingBalances(false);
    }
  }

  async function saveUniverse() {
    setSavingUniverse(true);
    try {
      const items = universeInputs
        .map((item) => ({
          symbol: String(item.symbol || "").trim().toUpperCase(),
          label: String(item.symbol || "").trim().toUpperCase(),
          target_pct: Number(item.target_pct || 0),
        }))
        .filter((item) => item.symbol);
      const response = await fetch(`${API_BASE}/api/satellite-universe`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ items }),
      });
      if (!response.ok) throw new Error(`satellite universe HTTP ${response.status}`);
      setUniverseOpen(false);
      await onSaved();
      window.alert("卫星标的已保存，相关数据已刷新");
    } catch (err) {
      window.alert(`卫星标的保存失败：${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setSavingUniverse(false);
    }
  }

  return (
    <section>
      {tradeToast ? (
        <div className={`toastNotice ${tradeToast.status}`} role="status" aria-live="polite" key={tradeToast.id}>
          <strong>{tradeToast.status === "down" ? "操作失败" : "操作完成"}</strong>
          <span>{tradeToast.message}</span>
        </div>
      ) : null}
      {saving ? (
        <div className="modalBackdrop savingBackdrop" role="status" aria-live="assertive">
          <div className="savingDialog">
            <div className="savingSpinner" />
            <strong>正在保存交易</strong>
            <span>请稍后，系统正在同步持仓并重算收益曲线。</span>
          </div>
        </div>
      ) : null}
      <div className="sectionHeader">
        <h2>再平衡建议</h2>
        <span className="muted">
          {data.rebalance.month_key} · 可动用 {fmtMoney(data.rebalance.remaining_deployable_usd, "USD")} ·
          待买 {fmtMoney(tradeTotals.buy_USD, "USD")}{tradeTotals.buy_CNY ? ` / ${fmtMoney(tradeTotals.buy_CNY, "CNY")}` : ""} ·
          待卖 {fmtMoney(tradeTotals.sell_USD, "USD")}{tradeTotals.sell_CNY ? ` / ${fmtMoney(tradeTotals.sell_CNY, "CNY")}` : ""}
        </span>
      </div>
      <div className="rulesToolbar">
        <span className="muted">
          建仓到 {data.rebalance.build_target} · 未来入金 {data.rebalance.future_cash_months} 个月 · 缩放 {Number(data.rebalance.suggestion_scale || 1).toFixed(2)}
        </span>
        <button className="toolButton compactTool" onClick={() => setRulesOpen(true)}>规则</button>
      </div>
      <div className="muted rebalanceFormulaBanner">
        共同分母：{data.rebalance.planned_total_formula || `USD ${fmtMoney(data.rebalance.planned_total_usd, "USD")}`} · VOO/QQQ 按周买入 · 个股一手按目标金额 × 0.1 × 档位倍率
      </div>
      {data.rebalance.monthly_recalculation ? (
        <div className={`monthlyRecalculationStatus ${data.rebalance.monthly_recalculation.status || ""}`}>
          月度档位：{data.rebalance.monthly_recalculation.effective_month} · {data.rebalance.monthly_recalculation.status === "success" ? "已重算" : "等待重算"}
          {data.rebalance.monthly_recalculation.attention_symbol_count ? ` · 需关注 ${data.rebalance.monthly_recalculation.attention_symbol_count}个标的` : " · 无需关注"}
          {data.rebalance.monthly_recalculation.review_symbol_count ? ` · 复核 ${data.rebalance.monthly_recalculation.review_symbol_count}个` : ""}
          <small>固定分位数 65% / 85% / 95% · {Number(data.rebalance.monthly_recalculation.diagnostic_count || 0)}项统计诊断已折叠</small>
          <button className="statisticsDetailButton" onClick={() => setStatisticsOpen(true)}>统计详情</button>
        </div>
      ) : null}
      {statisticsOpen ? (
        <div className="modalBackdrop" role="presentation" onPointerDown={trackBackdropPointerDown} onClick={(event) => {
          if (shouldCloseFromBackdropClick(event)) setStatisticsOpen(false);
        }}>
          <div className="modalPanel statisticsModal" role="dialog" aria-modal="true" aria-labelledby="statistics-detail-title" onClick={(event) => event.stopPropagation()}>
            <div className="sectionHeader">
              <div>
                <h2 id="statistics-detail-title">回撤档位统计详情</h2>
                <div className="muted">月度快照 {data.rebalance.monthly_recalculation?.effective_month || "-"} · 验证结果只用于提示，不会自动修改 65% / 85% / 95% 分位数</div>
              </div>
              <button onClick={() => setStatisticsOpen(false)}>关闭</button>
            </div>
            <div className="statisticsTickerList">
              {suggestionRows.map((row) => {
                const warning = row.walk_forward_warning || {};
                const snapshot = row.monthly_threshold_snapshot || {};
                const walkForward = warning.statistics || {};
                const tierStatistics = walkForward.statistics || {};
                const frequencies = walkForward.annual_frequency || {};
                const diagnostics = warning.diagnostics || [];
                const status = warning.status || "ok";
                const statusLabel = status === "review" ? "复核" : status === "attention" ? "需关注" : "正常";
                return (
                  <section className="statisticsTickerCard" key={`statistics-${row.symbol}`}>
                    <div className="statisticsTickerHeader">
                      <strong>{row.symbol}</strong>
                      <span className={`statisticsStatus ${status}`}>{statusLabel}</span>
                      <span className="muted">执行档位 {formatThresholdSet(snapshot.thresholds_pct)}</span>
                      <span className="muted">样本 {Number(snapshot.history_days || 0)} 日</span>
                    </div>
                    {warning.review_message ? <div className="statisticsReviewMessage">{warning.review_message}</div> : null}
                    {(warning.messages || []).length ? (
                      <ul className="statisticsAlerts">{warning.messages.map((message) => <li key={message}>{message}</li>)}</ul>
                    ) : null}
                    <div className="statisticsTableWrap">
                      <table className="statisticsTierTable">
                        <thead><tr><th>档位</th><th>年均触发</th><th>样本</th><th>60日中位收益</th><th>120日中位收益</th><th>120日胜率</th><th>后续MAE</th></tr></thead>
                        <tbody>
                          {[['small', '小加'], ['medium', '中加'], ['large', '大加']].map(([key, label]) => {
                            const stat = tierStatistics[key] || {};
                            return (
                              <tr key={key}>
                                <th>{label}</th>
                                <td>{Number.isFinite(Number(frequencies[key])) ? `${Number(frequencies[key]).toFixed(2)}次/年` : "-"}</td>
                                <td>{Number(stat.sample_count || 0)}</td>
                                <td>{formatStatisticPct(stat.forward_return_median_pct?.["60"])}</td>
                                <td>{formatStatisticPct(stat.forward_return_median_pct?.["120"])}</td>
                                <td>{formatStatisticRate(stat.forward_return_win_rate?.["120"])}</td>
                                <td>{formatStatisticPct(stat.mae_120d_median_pct)}</td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                    <details className="statisticsDiagnostics">
                      <summary>查看统计诊断（{Number(warning.diagnostic_count || diagnostics.length)}项）</summary>
                      {diagnostics.length ? <ul>{diagnostics.map((message, index) => <li key={`${row.symbol}-diagnostic-${index}`}>{message}</li>)}</ul> : <p className="muted">无折叠诊断</p>}
                    </details>
                  </section>
                );
              })}
            </div>
          </div>
        </div>
      ) : null}
      {budgetOpen ? (
        <div className="modalBackdrop" role="presentation" onPointerDown={trackBackdropPointerDown} onClick={(event) => {
          if (shouldCloseFromBackdropClick(event)) setBudgetOpen(false);
        }}>
          <div className="modalPanel" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
            <div className="sectionHeader">
              <h2>预算设置</h2>
              <button onClick={() => setBudgetOpen(false)} disabled={savingBudget}>关闭</button>
            </div>
            <div className="muted">
              未来预算 {fmtMoney(futureBudgetTotal, "USD")} · 计划分母 {fmtMoney(data.rebalance.planned_total_usd, "USD")} · 月初口径已扣除本月确认买入 · SGOV可动用 {fmtMoney(data.rebalance.sgov_available_usd || 0, "USD")}
              {data.rebalance.sgov_large_trigger_enabled ? " · 大档位已启用SGOV资金" : " · 普通情况SGOV保留20%"}
            </div>
            <div className="budgetEditGrid">
              {Object.entries(budgetInputs).map(([month, value]) => (
                <label key={month}>{month} 可投入(USD)<input value={value} onChange={(event) => updateBudget(month, event.target.value)} inputMode="decimal" /></label>
              ))}
            </div>
            <div className="actions">
              <button onClick={() => setBudgetOpen(false)} disabled={savingBudget}>取消</button>
              <button className="primary" onClick={saveBudget} disabled={savingBudget}><Save size={16} /> 保存预算并刷新建议</button>
            </div>
          </div>
        </div>
      ) : null}
      {rulesOpen ? (
        <div className="modalBackdrop" role="presentation" onPointerDown={trackBackdropPointerDown} onClick={(event) => {
          if (shouldCloseFromBackdropClick(event)) setRulesOpen(false);
        }}>
          <div className="modalPanel" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
            <div className="sectionHeader"><h2>{data.rebalance.rules?.title || "算法规则"}</h2><button onClick={() => setRulesOpen(false)}>关闭</button></div>
            {(data.rebalance.rules?.sections || []).map((section) => (
              <section className="ruleSection" key={section.heading}>
                <h3>{section.heading}</h3>
                <ul>{(section.items || []).map((item) => <li key={item}>{item}</li>)}</ul>
              </section>
            ))}
          </div>
        </div>
      ) : null}
      {universeOpen ? (
        <div className="modalBackdrop" role="presentation" onPointerDown={trackBackdropPointerDown} onClick={(event) => {
          if (shouldCloseFromBackdropClick(event)) setUniverseOpen(false);
        }}>
          <div className="modalPanel satelliteUniverseModal" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
            <div className="sectionHeader">
              <h2>编辑卫星标的</h2>
              <button onClick={() => setUniverseOpen(false)} disabled={savingUniverse}>关闭</button>
            </div>
            <div className="satelliteUniverseRows">
              {universeInputs.map((item, index) => {
                const targetValue = Number(item.target_pct || 0);
                return (
                  <div className="satelliteUniverseRow" key={`satellite-universe-${index}`}>
                    <label>标的<input value={item.symbol} onChange={(event) => updateUniverse(index, "symbol", event.target.value)} /></label>
                    <div className="universeTargetCell" style={{ "--target-fill": `${Math.min(100, Math.max(0, targetValue))}%` }}>
                      <div className="targetSliderLabel">
                        <strong>目标比例</strong>
                        <span>{targetValue.toFixed(2)}%</span>
                      </div>
                      <input
                        className="targetRange"
                        type="range"
                        min="0"
                        max="100"
                        step="0.1"
                        value={targetValue}
                        onChange={(event) => updateUniverse(index, "target_pct", event.target.value)}
                      />
                      <div className="targetNumberWrap">
                        <input
                          className="targetNumber"
                          value={item.target_pct}
                          onChange={(event) => updateUniverse(index, "target_pct", event.target.value)}
                          inputMode="decimal"
                        />
                        <span>%</span>
                      </div>
                    </div>
                    <button className="iconDanger" aria-label={`删除 ${item.symbol || "空行"}`} onClick={() => removeUniverseRow(index)}><Trash2 size={16} /></button>
                  </div>
                );
              })}
            </div>
            <div className="actions">
              <button onClick={addUniverseRow}><Plus size={16} /> 新增标的</button>
              <button className="primary" onClick={saveUniverse} disabled={savingUniverse}><Save size={16} /> 保存卫星标的</button>
            </div>
          </div>
        </div>
      ) : null}
      <div className="tableWrap">
        <table className="rebalanceTable">
          <thead>
            <tr>
              <th>标的</th><th>档位</th><th>实际差值</th><th>计划应买</th><th>该月净买入</th><th>60日回撤</th><th>估值系数</th>
            </tr>
          </thead>
          <tbody>
            {suggestionRows.map((row) => {
              const diagnostics = row.tier_diagnostics || null;
              const realtimeHolding = realtimeHoldingBySymbol[row.symbol] || {};
              const currency = row.currency || realtimeHolding.currency || "USD";
              const realtimeValue = Number(realtimeHolding.value || 0);
              const realtimeMarketGap = Number(row.target_usd || 0) - realtimeValue;
              const actualGapTooltip = [
                `\u6210\u672c\u53e3\u5f84\u5b9e\u9645\u5dee\u503c\uff1a${fmtMoney(row.buy_difference_usd, currency)}`,
                `\u5b9e\u65f6\u5e02\u503c\u5dee\u503c\uff1a${fmtMoney(realtimeMarketGap, currency)}`,
                `\u76ee\u6807\u91d1\u989d\uff1a${fmtMoney(row.target_usd, currency)}`,
                `\u5b9e\u65f6\u5e02\u503c\uff1a${fmtMoney(realtimeValue, currency)}`,
                `\u5f53\u524d\u5e02\u503c\u5360\u6bd4\uff1a${Number(row.current_pct || 0).toFixed(2)}%`,
                `\u76ee\u6807\u5360\u6bd4\uff1a${Number(row.target_pct || 0).toFixed(2)}%`,
              ].join("\n");
              const tierReviewTooltip = row.symbol === "TEM" && row.review_mode === "manual_review_only" && diagnostics
                ? [
                    "\u590d\u6838",
                    `\u81ea\u8eab\u6837\u672c\u6863\u4f4d\uff1a${formatThresholdSet(diagnostics.self_thresholds_pct)}`,
                    `\u540c\u884c\u6863\u4f4d\uff1a${formatThresholdSet(diagnostics.peer_thresholds_pct)}`,
                    `\u6536\u7f29\u540e\u6700\u7ec8\u6863\u4f4d\uff1a${formatThresholdSet(diagnostics.shrunk_thresholds_pct)}`,
                    `\u6709\u6548\u5386\u53f2\uff1a${Number(diagnostics.history_days || 0)}\u65e5\uff08\u53ef\u8ba1\u7b97\u56de\u64a4 ${Number(diagnostics.effective_drawdown_days || 0)}\u65e5\uff09`,
                    `\u72ec\u7acb\u56de\u64a4\u5468\u671f\uff1a${Number(diagnostics.independent_drawdown_cycles || 0)}\u4e2a`,
                    diagnostics.reason,
                  ].join("\n")
                : "";
              return (
                <React.Fragment key={row.symbol}>
                  <tr className={Number(row.buy_difference_usd || 0) <= 0 ? "inactiveRebalanceRow" : ""}>
                    <th>{row.symbol}</th>
                    <td className={tierReviewTooltip ? "hasDetailTooltip" : undefined} title={tierReviewTooltip || undefined}>
                      <div className="tierSignalStack">
                        <span className={`tierBadge ${tierClass(row.intensity)}`}>{rebalanceTierLabel(row)}</span>
                        {row.intraday_warning?.active ? (
                          <span className="intradayTierWarning">盘中预警：{tierLabel(row.intraday_warning.tier)}</span>
                        ) : null}
                        {row.walk_forward_warning?.active ? (
                          <span
                            className="walkForwardWarning"
                            title={(row.walk_forward_warning.messages || []).join("\n")}
                          >
                            统计提醒{Number(row.walk_forward_warning.count || 0) > 1 ? ` ${Number(row.walk_forward_warning.count)}` : ""}
                          </span>
                        ) : null}
                      </div>
                    </td>
                    <td className="planCell hasDetailTooltip" title={actualGapTooltip}>
                      <div className={tone(row.buy_difference_usd)}>{fmtMoney(row.buy_difference_usd, currency)}</div>
                    </td>
                    <td className="planCell hasDetailTooltip" title={row.planned_buy_formula || "-"}>
                      <div>{fmtMoney(row.planned_buy_usd, row.currency || "USD")}</div>
                    </td>
                    <td>{fmtMoney(row.net_bought_usd, row.currency || "USD")}</td>
                    <td className={`${tone(row.drawdown_pct)} hasDetailTooltip`} title={`正式确认：${row.confirmed_close_date || "-"}\n盘中回撤：${row.intraday_drawdown_pct == null ? "-" : fmtPct(row.intraday_drawdown_pct)}`}>
                      <div>{row.drawdown_pct == null ? "-" : fmtPct(row.drawdown_pct)}</div>
                      {row.intraday_warning?.active ? <small className="intradayDrawdown">盘中 {fmtPct(row.intraday_drawdown_pct)}</small> : null}
                    </td>
                    <td className={`${Number(row.valuation_split_factor || 1) < 1 ? "down" : "flat"} hasDetailTooltip`} title={valuationTooltip(row)}>{Number(row.valuation_split_factor || 1).toFixed(2)}</td>
                  </tr>
                  {row.symbol !== "TEM" && row.review_mode === "manual_review_only" && diagnostics ? (
                    <tr className="manualReviewDetailRow">
                      <td colSpan="7">
                        <div className="manualReviewDetail">
                          <strong>复核</strong>
                          <span>自身样本档位：{formatThresholdSet(diagnostics.self_thresholds_pct)}</span>
                          <span>同行档位：{formatThresholdSet(diagnostics.peer_thresholds_pct)}</span>
                          <span>收缩后最终档位：{formatThresholdSet(diagnostics.shrunk_thresholds_pct)}</span>
                          <span>有效历史：{Number(diagnostics.history_days || 0)}日（可计算回撤 {Number(diagnostics.effective_drawdown_days || 0)}日）</span>
                          <span>独立回撤周期：{Number(diagnostics.independent_drawdown_cycles || 0)}个</span>
                          <span className="manualReviewReason">{diagnostics.reason}</span>
                        </div>
                      </td>
                    </tr>
                  ) : null}
                </React.Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
      <div className="tradeHistoryBlock">
        <div className="sectionHeader subHeader">
          <div>
            <h2>交易记录</h2>
            <span className="muted">买入或卖出后会按交易日期重算该日之后的收益曲线</span>
          </div>
          <div className="headerActions">
            <button className="toolButton primaryTool" onClick={() => openTradeEditor(tradeRows[0]?.symbol || "")}>交易</button>
            {false && sortedTrades.length > 3 ? (
              <button className="toolButton compactTool" onClick={() => setTradeHistoryOpen((value) => !value)}>
                {tradeHistoryOpen ? "收起" : "更多"}
              </button>
            ) : null}
          </div>
        </div>
        {tradeMessage ? <div className="saveMessage down">{tradeMessage}</div> : null}
        <div className="recordTableToolbar">
          <input
            value={tradeFilter}
            onChange={(event) => setTradeFilter(event.target.value)}
            placeholder={"\u641c\u7d22\u65e5\u671f\u3001\u6807\u7684\u6216\u6863\u4f4d"}
            aria-label={"\u7b5b\u9009\u4ea4\u6613\u8bb0\u5f55"}
          />
          <select value={tradeActionFilter} onChange={(event) => setTradeActionFilter(event.target.value)} aria-label={"\u6309\u4ea4\u6613\u65b9\u5411\u7b5b\u9009"}>
            <option value="all">{"\u5168\u90e8\u65b9\u5411"}</option><option value="buy">{"\u4e70\u5165"}</option><option value="sell">{"\u5356\u51fa"}</option>
          </select>
          <span>{`\u7b5b\u9009\u540e ${sortedTrades.length} / ${(data.trades || []).length} \u6761`}</span>
        </div>
        <div className={`tableWrap recordTableWrap ${tradeHistoryOpen ? "expanded" : ""}`}>
          <table>
            <thead>
              <tr>
                <th><button type="button" className="recordSortButton" onClick={() => toggleRecordSort(setTradeSort, tradeSort, "trade_date")}>{"\u65e5\u671f"}{recordSortMark(tradeSort, "trade_date")}</button></th>
                <th><button type="button" className="recordSortButton" onClick={() => toggleRecordSort(setTradeSort, tradeSort, "symbol")}>{"\u6807\u7684"}{recordSortMark(tradeSort, "symbol")}</button></th>
                <th><button type="button" className="recordSortButton" onClick={() => toggleRecordSort(setTradeSort, tradeSort, "action")}>{"\u65b9\u5411"}{recordSortMark(tradeSort, "action")}</button></th>
                <th><button type="button" className="recordSortButton" onClick={() => toggleRecordSort(setTradeSort, tradeSort, "shares")}>{"\u80a1\u6570"}{recordSortMark(tradeSort, "shares")}</button></th>
                <th><button type="button" className="recordSortButton" onClick={() => toggleRecordSort(setTradeSort, tradeSort, "amount_usd")}>{"\u6210\u4ea4\u91d1\u989d"}{recordSortMark(tradeSort, "amount_usd")}</button></th>
                <th><button type="button" className="recordSortButton" onClick={() => toggleRecordSort(setTradeSort, tradeSort, "price")}>{"\u6210\u4ea4\u6210\u672c"}{recordSortMark(tradeSort, "price")}</button></th>
                <th><button type="button" className="recordSortButton" onClick={() => toggleRecordSort(setTradeSort, tradeSort, "close_effect")}>{"\u6536\u76d8\u5dee\u989d"}{recordSortMark(tradeSort, "close_effect")}</button></th>
                <th><button type="button" className="recordSortButton" onClick={() => toggleRecordSort(setTradeSort, tradeSort, "new_avg_cost")}>{"\u6301\u4ed3\u6210\u672c\u53d8\u5316"}{recordSortMark(tradeSort, "new_avg_cost")}</button></th>
                <th><button type="button" className="recordSortButton" onClick={() => toggleRecordSort(setTradeSort, tradeSort, "intensity")}>{"\u6863\u4f4d"}{recordSortMark(tradeSort, "intensity")}</button></th>
                <th>{"\u64cd\u4f5c"}</th>
              </tr>
            </thead>
            <tbody>
              {visibleTrades.map((trade, index) => (
                <tr key={`${trade.trade_date || trade.date}-${trade.symbol}-${index}`}>
                  <td>{trade.trade_date || trade.date || "-"}</td>
                  <td>{trade.symbol}</td>
                  <td>{trade.action === "sell" ? "卖出" : "买入"}</td>
                  <td>{Number(trade.shares || 0).toLocaleString(undefined, { maximumFractionDigits: 4 })}</td>
                  <td>{fmtMoney(trade.amount_usd, currencyBySymbol[trade.symbol] || "USD")}</td>
                  <td>{fmtMoney(trade.price, currencyBySymbol[trade.symbol] || "USD", (currencyBySymbol[trade.symbol] || "USD") === "USD" ? 2 : 4)}</td>
                  <td className={tone(trade.close_effect)} title={trade.close_price ? `当日收盘 ${fmtMoney(trade.close_price, currencyBySymbol[trade.symbol] || "USD", (currencyBySymbol[trade.symbol] || "USD") === "USD" ? 2 : 4)}` : ""}>
                    {fmtTradeCloseEffect(trade, currencyBySymbol[trade.symbol] || "USD")}
                  </td>
                  <td className={tradeCostTone(trade)} title={fmtAvgCostChangeTitle(trade, currencyBySymbol[trade.symbol] || "USD")}>
                    {fmtCostChange(trade, currencyBySymbol[trade.symbol] || "USD")}
                  </td>
                  <td><span className={`tierBadge ${tierClass(trade.intensity)}`}>{tierLabel(trade.intensity)}</span></td>
                  <td>
                    <button onClick={() => deleteTrade(trade)} disabled={deletingTradeId === trade.id}>
                      {deletingTradeId === trade.id ? "撤销中" : "撤销"}
                    </button>
                  </td>
                </tr>
              ))}
              {!sortedTrades.length ? (
                <tr><td colSpan={10} className="muted">暂无交易记录</td></tr>
              ) : null}
            </tbody>
          </table>
        </div>
        {sortedTrades.length > 3 ? (
          <button type="button" className={`recordExpandToggle ${tradeHistoryOpen ? "open" : ""}`} onClick={() => setTradeHistoryOpen((value) => !value)} aria-expanded={tradeHistoryOpen} title={tradeHistoryOpen ? "\u6536\u8d77\u4ea4\u6613\u8bb0\u5f55" : "\u5c55\u5f00\u5168\u90e8\u4ea4\u6613\u8bb0\u5f55"}>
            <span aria-hidden="true">{tradeHistoryOpen ? "\u25b2" : "\u25bc"}</span>
          </button>
        ) : null}

      </div>
      <div className="fxConversionBlock">
        <div className="sectionHeader subHeader">
          <div>
            <h2>购汇记录</h2>
            <span className="muted">
              平均汇率 {Number(data.summary?.avg_fx_rate || data.summary?.fx || 0).toFixed(4)} ·
              已购汇 {fmtMoney(data.summary?.fx_conversion_total_usd || 0, "USD")} / {fmtMoney(data.summary?.fx_conversion_total_cny || 0, "CNY")} ·
              美元资产汇兑影响 <span className={tone(data.summary?.usd_fx_pnl_cny)}>{fmtMoney(data.summary?.usd_fx_pnl_cny || 0, "CNY")}</span>
            </span>
          </div>
          <div className="headerActions">
            <button className="toolButton primaryTool" onClick={openFxConversionEditor}>换汇</button>
            {false && sortedFxConversions.length > 3 ? (
              <button className="toolButton compactTool" onClick={() => setFxHistoryOpen((value) => !value)}>
                {fxHistoryOpen ? "收起" : "更多"}
              </button>
            ) : null}
          </div>
        </div>
        {fxConversionMessage ? <div className="saveMessage down">{fxConversionMessage}</div> : null}
        <div className="recordTableToolbar">
          <input
            value={fxFilter}
            onChange={(event) => setFxFilter(event.target.value)}
            placeholder={"\u641c\u7d22\u65e5\u671f\u3001\u91d1\u989d\u6216\u5907\u6ce8"}
            aria-label={"\u7b5b\u9009\u8d2d\u6c47\u8bb0\u5f55"}
          />
          <span>{`\u7b5b\u9009\u540e ${sortedFxConversions.length} / ${(data.fx_conversions || []).length} \u6761`}</span>
        </div>

        <div className={`tableWrap recordTableWrap ${fxHistoryOpen ? "expanded" : ""}`}>
          <table>
            <thead>
              <tr>
                <th><button type="button" className="recordSortButton" onClick={() => toggleRecordSort(setFxSort, fxSort, "converted_date")}>{"\u65e5\u671f"}{recordSortMark(fxSort, "converted_date")}</button></th>
                <th><button type="button" className="recordSortButton" onClick={() => toggleRecordSort(setFxSort, fxSort, "cny_amount")}>{"\u4eba\u6c11\u5e01\u91d1\u989d"}{recordSortMark(fxSort, "cny_amount")}</button></th>
                <th><button type="button" className="recordSortButton" onClick={() => toggleRecordSort(setFxSort, fxSort, "usd_amount")}>{"\u7f8e\u5143\u91d1\u989d"}{recordSortMark(fxSort, "usd_amount")}</button></th>
                <th><button type="button" className="recordSortButton" onClick={() => toggleRecordSort(setFxSort, fxSort, "rate")}>{"\u6c47\u7387"}{recordSortMark(fxSort, "rate")}</button></th>
                <th><button type="button" className="recordSortButton" onClick={() => toggleRecordSort(setFxSort, fxSort, "note")}>{"\u5907\u6ce8"}{recordSortMark(fxSort, "note")}</button></th>
                <th>{"\u64cd\u4f5c"}</th>
              </tr>
            </thead>
            <tbody>
              {visibleFxConversions.map((record) => (
                <tr key={record.id}>
                  <td>{record.converted_date || "-"}</td>
                  <td>{fmtMoney(record.cny_amount, "CNY")}</td>
                  <td>{fmtMoney(record.usd_amount, "USD")}</td>
                  <td>{Number(record.rate || 0).toFixed(4)}</td>
                  <td>{record.note || "-"}</td>
                  <td>
                    <button onClick={() => deleteFxConversion(record)} disabled={deletingFxConversionId === record.id}>
                      {deletingFxConversionId === record.id ? "撤销中" : "撤销"}
                    </button>
                  </td>
                </tr>
              ))}
              {!sortedFxConversions.length ? (
                <tr><td colSpan={6} className="muted">暂无购汇记录</td></tr>
              ) : null}
            </tbody>
          </table>
        </div>
        {sortedFxConversions.length > 3 ? (
          <button type="button" className={`recordExpandToggle ${fxHistoryOpen ? "open" : ""}`} onClick={() => setFxHistoryOpen((value) => !value)} aria-expanded={fxHistoryOpen} title={fxHistoryOpen ? "\u6536\u8d77\u8d2d\u6c47\u8bb0\u5f55" : "\u5c55\u5f00\u5168\u90e8\u8d2d\u6c47\u8bb0\u5f55"}>
            <span aria-hidden="true">{fxHistoryOpen ? "\u25b2" : "\u25bc"}</span>
          </button>
        ) : null}

      </div>
      {fxConversionOpen ? (
        <div className="modalBackdrop" role="presentation" onPointerDown={trackBackdropPointerDown} onClick={(event) => {
          if (shouldCloseFromBackdropClick(event)) setFxConversionOpen(false);
        }}>
          <div className="modalPanel fxConversionModal" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
            <div className="sectionHeader">
              <h2>记录换汇</h2>
              <button onClick={() => setFxConversionOpen(false)} disabled={savingFxConversion}>关闭</button>
            </div>
            <div className="fxConversionEditor">
              <label>日期<input type="date" value={fxInputs.converted_date || defaultTradeDate} onChange={(event) => updateFxInput("converted_date", event.target.value)} /></label>
              <label>人民币金额<input value={fxInputs.cny_amount ?? ""} onChange={(event) => updateFxInput("cny_amount", event.target.value)} inputMode="decimal" /></label>
              <label>美元金额<input value={fxInputs.usd_amount ?? ""} onChange={(event) => updateFxInput("usd_amount", event.target.value)} inputMode="decimal" /></label>
              <label>备注<input value={fxInputs.note ?? ""} onChange={(event) => updateFxInput("note", event.target.value)} /></label>
            </div>
            <div className="actions">
              <button onClick={() => setFxConversionOpen(false)} disabled={savingFxConversion}>取消</button>
              <button className="primary" onClick={saveFxConversion} disabled={savingFxConversion}><Save size={16} /> 保存换汇</button>
            </div>
          </div>
        </div>
      ) : null}
      {activeTradeSymbol ? (() => {
        const row = tradeRows.find((item) => item.symbol === activeTradeSymbol);
        const currentInput = inputs[activeTradeSymbol] || {};
        if (!row) return null;
        return (
          <div className="modalBackdrop" role="presentation" onPointerDown={trackBackdropPointerDown} onClick={(event) => {
            if (shouldCloseFromBackdropClick(event)) setActiveTradeSymbol("");
          }}>
            <div className="modalPanel singleTradeModal" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
              <div className="sectionHeader">
                <h2>记录买卖 · {row.symbol}</h2>
                <button onClick={() => setActiveTradeSymbol("")} disabled={saving}>关闭</button>
              </div>
              <div className="singleTradeGrid">
                <label>标的
                  <select value={activeTradeSymbol} onChange={(event) => setActiveTradeSymbol(event.target.value)}>
                    {tradeRows.map((item) => (
                      <option key={item.symbol} value={item.symbol}>{item.symbol}{item.label && item.label !== item.symbol ? ` · ${item.label}` : ""}</option>
                    ))}
                  </select>
                </label>
                <label>方向
                  <select value={currentInput.action || "buy"} onChange={(event) => update(row.symbol, "action", event.target.value)}>
                    <option value="buy">买入</option>
                    <option value="sell">卖出</option>
                  </select>
                </label>
                <label>日期
                  <input type="date" value={currentInput.trade_date || defaultTradeDate} onChange={(event) => update(row.symbol, "trade_date", event.target.value)} />
                </label>
                <label>档位
                  <select className={tierClass(currentInput.intensity || row.intensity)} value={currentInput.intensity || row.intensity} onChange={(event) => update(row.symbol, "intensity", event.target.value)}>
                    <option value="normal">普通</option>
                    <option value="small">小加</option>
                    <option value="medium">中加</option>
                    <option value="large">大加</option>
                    {row.review_mode === "manual_review_only" ? <option value="manual_review_only">复核</option> : null}
                  </select>
                </label>
                <label>成交金额
                  <input value={currentInput.amount_usd ?? ""} onChange={(event) => update(row.symbol, "amount_usd", event.target.value)} inputMode="decimal" />
                </label>
                <label>成交股数
                  <input value={currentInput.shares ?? ""} onChange={(event) => update(row.symbol, "shares", event.target.value)} inputMode="decimal" />
                </label>
                <div className="singleTradeHint">
                  <span>建议买/卖</span>
                  <strong>{fmtMoney(Number(row.suggested_sell_usd || 0) > Number(row.suggested_buy_usd || 0) ? -Number(row.suggested_sell_usd || 0) : Number(row.suggested_buy_usd || 0), row.currency || "USD")}</strong>
                </div>
              </div>
              <div className="actions">
                <button onClick={() => clearPending(row.symbol)} disabled={saving}>清零</button>
                <button className="primary" onClick={() => save(row.symbol)} disabled={saving}><Save size={16} /> 保存这一条</button>
              </div>
            </div>
          </div>
        );
      })() : null}
      {balanceMessage ? <div className={balanceMessage === "现金与已变现已保存" ? "saveMessage up" : "saveMessage down"}>{balanceMessage}</div> : null}
      {editingBalances ? (
        <div className="modalBackdrop" role="presentation" onPointerDown={trackBackdropPointerDown} onClick={(event) => {
          if (shouldCloseFromBackdropClick(event)) {
            resetBalanceDraft();
            setEditingBalances(false);
          }
        }}>
          <div className="modalPanel balanceModal" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
            <div className="sectionHeader">
              <div>
                <h2>现金与已变现</h2>
                <span className="muted">维护现金、已变现盈亏和 SGOV 股息，保存后会重算看板。</span>
              </div>
              <button className="toolButton compactTool" onClick={() => { resetBalanceDraft(); setEditingBalances(false); }} disabled={savingBalances}>关闭</button>
            </div>
            <div className="balanceEditGrid">
              <label><span>USD 现金</span><input value={balanceInputs.cash_usd ?? ""} onChange={(event) => updateBalance("cash_usd", event.target.value)} inputMode="decimal" /></label>
              <label><span>CNY 现金</span><input value={balanceInputs.cash_cny ?? ""} onChange={(event) => updateBalance("cash_cny", event.target.value)} inputMode="decimal" /></label>
              <label><span>USD 现金成本基准</span><input value={balanceInputs.cash_cost_basis_usd ?? ""} onChange={(event) => updateBalance("cash_cost_basis_usd", event.target.value)} inputMode="decimal" /></label>
              <label><span>CNY 现金成本基准</span><input value={balanceInputs.cash_cost_basis_cny ?? ""} onChange={(event) => updateBalance("cash_cost_basis_cny", event.target.value)} inputMode="decimal" /></label>
              <label><span>USD 已变现</span><input value={balanceInputs.realized_usd ?? ""} onChange={(event) => updateBalance("realized_usd", event.target.value)} inputMode="decimal" /></label>
              <label><span>CNY 已变现</span><input value={balanceInputs.realized_cny ?? ""} onChange={(event) => updateBalance("realized_cny", event.target.value)} inputMode="decimal" /></label>
              <label><span>VOO 累计分红</span><input value={balanceInputs.voo_dividend_usd ?? ""} onChange={(event) => updateBalance("voo_dividend_usd", event.target.value)} inputMode="decimal" /></label>
              <label><span>SGOV 股息</span><input value={balanceInputs.sgov_dividend_usd ?? ""} onChange={(event) => updateBalance("sgov_dividend_usd", event.target.value)} inputMode="decimal" /></label>
            </div>
            <p className="muted">现金成本基准用于区分本金和已变现收益；普通入金、出金会自动同步，盈利再投资后允许显示为负数。</p>
            <div className="actions">
              <button onClick={() => { resetBalanceDraft(); setEditingBalances(false); }} disabled={savingBalances}>取消</button>
              <button className="primary" onClick={saveBalances} disabled={savingBalances}><Save size={16} /> 保存</button>
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
}

function DashboardPage({ data }) {
  return (
    <div className="dashboardPage">
      <div className="dashboardTopGrid">
        <Summary data={data} />
        <PnlBreakdownPanel data={data} />
      </div>
      <div className="dashboardWorkspace">
        <DailyHeatmap
          cards={data.daily_cards}
          holdings={data.holdings}
          dailyAsOf={data.summary?.daily_as_of}
          dailyCarriedForward={data.summary?.daily_carried_forward}
        />
        <div className="dashboardInsights">
          <PerformanceChart history={data.performance_history} />
          <Visualizations data={data} />
        </div>
      </div>
    </div>
  );
}

function HoldingsPage({ data, onSaved }) {
  return <EditableHoldingsPage data={data} onSaved={onSaved} />;
}

function RebalancePage({ data, onSaved }) {
  return <Rebalance data={data} onSaved={onSaved} />;
}

export default function App() {
  const { data, loading, error, load } = useDashboard();
  const [page, setPage] = useState("dashboard");
  useTableGestureScroll();

  if (loading) {
    return <main className="appShell"><div className="loading"><Activity /> 加载中</div></main>;
  }
  if (error || !data) {
    return (
      <main className="appShell">
        <div className="error">后端连接失败：{error}</div>
        <button onClick={load}>重试</button>
      </main>
    );
  }
  return (
    <main className={`appShell ${page === "dashboard" ? "dashboardShell" : ""}`}>
      <Header data={data} />
      <PageNav page={page} setPage={setPage} />
      {page === "dashboard" ? <DashboardPage data={data} /> : null}
      {page === "holdings" ? <HoldingsPage data={data} onSaved={load} /> : null}
      {page === "rebalance" ? <RebalancePage data={data} onSaved={load} /> : null}
      {page === "kline" ? <KlinePage dashboardData={data} /> : null}
    </main>
  );
}
