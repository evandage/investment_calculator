import React, { useEffect, useMemo, useRef, useState } from "react";
import { hierarchy, treemap, treemapSquarify } from "d3-hierarchy";
import { BaselineSeries, CandlestickSeries, createChart, HistogramSeries, LineSeries } from "lightweight-charts";
import { Activity, Plus, RefreshCcw, Save, Trash2 } from "lucide-react";

const API_BASE =
  import.meta.env.VITE_API_BASE ||
  `${window.location.protocol}//${window.location.hostname}:8010`;
const WS_BASE = API_BASE.replace(/^http/i, "ws");
const HEATMAP_LAYOUT_WIDTH = 100;
const HEATMAP_LAYOUT_HEIGHT = 78;
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
  const delta = tradeCostDelta(trade);
  if (Number.isFinite(delta) && Math.abs(delta) > 1e-9) return fmtSignedMoney(delta, currency);
  return "-";
}

function tradeCostDelta(trade) {
  const amount = Number(trade?.amount_usd || 0);
  const costBasis = Number(trade?.cost_basis || 0);
  return trade?.action === "sell" ? -(costBasis > 0 ? costBasis : amount) : amount;
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

function tierClass(intensity) {
  return {
    normal: "tierNormal",
    small: "tierSmall",
    medium: "tierMedium",
    large: "tierLarge",
    probe: "tierSmall",
    month_end: "tierMedium",
  }[String(intensity || "").toLowerCase()] || "tierNone";
}

function tierLabel(intensity) {
  return {
    none: "-",
    normal: "普通",
    probe: "QQQ -2%分批",
    month_end: "QQQ月底补齐",
    small: "小加",
    medium: "中加",
    large: "大加",
    sell: "卖出",
  }[String(intensity || "none").toLowerCase()] || String(intensity || "-");
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

function Header({ data, onRefresh }) {
  const market = data?.market;
  return (
    <header className="topbar">
      <div>
        <h1>Investment Dashboard</h1>
        <div className="muted">
          {market ? `行情源 ${market.provider} · ${market.fetched_at}` : "正在连接后端"}
        </div>
      </div>
      <button className="iconButton" onClick={onRefresh} title="刷新">
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

function Summary({ data }) {
  const summary = data.summary;
  const fx = Number(summary.fx || 0);
  const usdRows = (data.holdings || []).filter((row) => row.currency === "USD");
  const usdHoldingValue = usdRows.reduce((sum, row) => sum + Number(row.value || 0), 0);
  const usdHoldingCost = usdRows.reduce((sum, row) => sum + Number(row.shares || 0) * Number(row.avg_cost || 0), 0);
  const usdCash = Number(data.balances?.cash_usd || 0);
  const usdTotalAssets = usdHoldingValue + usdCash;
  const usdPnl = usdRows.reduce((sum, row) => sum + Number(row.pnl || 0), 0);
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
  return (
    <section className="summaryGrid">
      <div className="summaryRowLabel">美元资产</div>
      <div className="summaryItem">
        <span>资产规模</span>
        <strong>{fmtMoney(usdTotalAssets, "USD")}</strong>
      </div>
      <div className="summaryItem">
        <span>持仓盈亏</span>
        <strong className={tone(usdPnl)}>
          {fmtMoney(usdPnl, "USD")} · {fmtPct(usdPnlPct)}
        </strong>
      </div>
      <div className="summaryItem">
        <span>当日加权</span>
        <strong className={tone(usdDailyChange)}>
          {fmtMoney(usdDailyChange, "USD")} · {fmtPct(usdDailyPct)}
        </strong>
      </div>
      <div className="summaryItem fxSummaryItem">
        <span>汇率</span>
        <strong>{fx.toFixed(4)}</strong>
        <em>成本 {Number(summary.avg_fx_rate || fx).toFixed(4)}</em>
      </div>
      <div className="summaryRowLabel">总资产</div>
      <div className="summaryItem">
        <span>资产规模</span>
        <strong>{fmtMoney(summary.total_assets_cny, "CNY")}</strong>
      </div>
      <div className="summaryItem">
        <span>持仓盈亏</span>
        <strong className={tone(summary.total_pnl_cny)}>
          {fmtMoney(summary.total_pnl_cny, "CNY")} · {fmtPct(summary.total_pnl_pct)}
        </strong>
      </div>
      <div className="summaryItem">
        <span>当日加权</span>
        <strong className={tone(weightedDailyChangeCny)}>
          {fmtMoney(weightedDailyChangeCny, "CNY")} · {fmtPct(summary.weighted_daily_pct)}
        </strong>
      </div>
    </section>
  );
}

function DailyCards({ cards }) {
  return (
    <section className="cardGrid">
      {cards.map((card) => {
        const regularPct = Number(card.regular_pct ?? 0);
        const extendedPct = Number(card.extended_pct);
        const hasDistinctExtendedPct = card.extended_pct != null && Math.abs(extendedPct - regularPct) > 0.0001;
        const regularUsd = Number(card.regular_change_usd ?? card.change_usd ?? 0);
        const regularCny = Number(card.regular_change_cny ?? card.change_cny ?? 0);
        const extendedUsd = Number(card.extended_change_usd);
        const extendedCny = Number(card.extended_change_cny);
        const hasDistinctExtendedUsd = card.extended_change_usd != null && Math.abs(extendedUsd - regularUsd) > 0.005;
        const hasDistinctExtendedCny = card.extended_change_cny != null && Math.abs(extendedCny - regularCny) > 0.005;
        return (
          <article className={`dailyCard ${card.wide ? "wideCard" : ""}`} key={card.symbol}>
            <div className="cardTitle">{card.label}</div>
            {card.price_line ? <div className="priceLine">{fmtCardPriceLine(card.price_line)}</div> : null}
            <div className={tone(regularPct)}>
              {fmtPct(regularPct)}
              {hasDistinctExtendedPct ? <span className={tone(extendedPct)}>（{fmtPct(extendedPct)}）</span> : null}
            </div>
            <div className={tone(regularUsd)}>
              {fmtMoney(regularUsd, "USD")}
              {hasDistinctExtendedUsd ? <span className={tone(extendedUsd)}>（{extendedUsd.toFixed(2)}）</span> : null}
            </div>
            <div className={tone(regularCny)}>
              {fmtMoney(regularCny, "CNY")}
              {hasDistinctExtendedCny ? <span className={tone(extendedCny)}>（{extendedCny.toFixed(2)}）</span> : null}
            </div>
          </article>
        );
      })}
    </section>
  );
}

function DailyHeatmap({ cards, holdings }) {
  const holdingsBySymbol = useMemo(
    () => Object.fromEntries((holdings || []).map((row) => [row.symbol, row])),
    [holdings],
  );
  const totalValue = (holdings || []).reduce((sum, row) => sum + Math.max(0, Number(row.value_cny || 0)), 0);
  const rows = useMemo(() => (cards || []).filter((card) => card.symbol !== "SATELLITE").map((card) => {
    const holding = holdingsBySymbol[card.symbol] || {};
    const rawValueCny = Number(holding.value_cny || 0);
    const valueCny = Number.isFinite(rawValueCny) ? Math.max(0, rawValueCny) : 0;
    const assetPct = totalValue > 0 ? (valueCny / totalValue) * 100 : 0;
    const rawDailyPct = Number(card.effective_pct ?? card.extended_pct ?? card.regular_pct ?? 0);
    const dailyPct = Number.isFinite(rawDailyPct) ? rawDailyPct : 0;
    const magnitude = Math.min(1, Math.abs(dailyPct) / 4);
    const strength = 0.18 + magnitude * 0.72;
    const bg = dailyPct > 0
      ? `linear-gradient(145deg, rgba(22, 101, 52, ${strength}), rgba(15, 47, 46, ${0.82 + magnitude * 0.18}))`
      : dailyPct < 0
        ? `linear-gradient(145deg, rgba(127, 29, 29, ${strength}), rgba(42, 24, 37, ${0.82 + magnitude * 0.18}))`
        : "linear-gradient(145deg, #15263d, #10233a)";
    return { ...card, valueCny, assetPct, dailyPct, bg, magnitude };
  }), [cards, holdingsBySymbol, totalValue]);
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
      .size([HEATMAP_LAYOUT_WIDTH, HEATMAP_LAYOUT_HEIGHT])
      .paddingInner(0.7)
      .round(false)(root);
    return root.leaves().map((leaf) => ({
      ...leaf.data,
      x: leaf.x0,
      y: leaf.y0,
      width: leaf.x1 - leaf.x0,
      height: leaf.y1 - leaf.y0,
    }));
  }, [rows, minLayoutValue]);

  return (
    <section className="chartPanel heatmapPanel">
      <div className="heatmapCanvas">
        {rects.map((row) => {
          return (
            <article
              className={`heatCell ${row.width < 7 || row.height < 7 ? "compact" : ""} ${row.width < 4 || row.height < 4 ? "tiny" : ""}`}
              key={row.symbol}
              style={{
                "--heat-bg": row.bg,
                "--heat-border": row.dailyPct > 0 ? `rgba(52, 211, 153, ${0.24 + row.magnitude * 0.42})` : row.dailyPct < 0 ? `rgba(248, 113, 113, ${0.26 + row.magnitude * 0.44})` : "rgba(148, 163, 184, 0.24)",
                left: `${row.x / HEATMAP_LAYOUT_WIDTH * 100}%`,
                top: `${row.y / HEATMAP_LAYOUT_HEIGHT * 100}%`,
                width: `${row.width / HEATMAP_LAYOUT_WIDTH * 100}%`,
                height: `${row.height / HEATMAP_LAYOUT_HEIGHT * 100}%`,
              }}
              title={`${row.label} · 资产占比 ${row.assetPct.toFixed(2)}% · 当日 ${fmtPct(row.dailyPct)}`}
            >
              <div className="heatSymbol">{row.label}</div>
              <strong className={tone(row.dailyPct)}>{fmtPct(row.dailyPct)}</strong>
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
    <section className="chartPanel">
      <h2>{title}</h2>
      <div className="barList">
        {rows.map((row) => {
          const value = Number(row[valueKey] || 0);
          return (
            <div className="barRow" key={row.symbol || row.label}>
              <div className="barLabel">{row.label || row.symbol}</div>
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

function CompareBars({ title, rows, amountKey = "current_usd", className = "" }) {
  const max = Math.max(1, ...rows.flatMap((row) => [Number(row.current_pct || 0), Number(row.target_pct || 0)]));
  const currentTotal = rows.reduce((sum, row) => sum + Number(row.current_pct || 0), 0);
  const targetTotal = rows.reduce((sum, row) => sum + Number(row.target_pct || 0), 0);
  return (
    <section className={`chartPanel verticalChartPanel ${className}`}>
      <h2>{title}</h2>
      <div className="verticalBars">
        {rows.map((row) => (
          <div
            className="verticalGroup"
            key={row.key || row.symbol}
            title={`当前 ${Number(row.current_pct || 0).toFixed(2)}% · ${fmtMoney(row[amountKey], "USD")} / 目标 ${Number(row.target_pct || 0).toFixed(2)}%`}
          >
            <div className="verticalPlot">
              <div className="verticalBar current" style={{ height: `${Number(row.current_pct || 0) / max * 100}%` }}>
                <span>{Number(row.current_pct || 0).toFixed(1)}%</span>
              </div>
              <div className="verticalBar target" style={{ height: `${Number(row.target_pct || 0) / max * 100}%` }}>
                <span>{Number(row.target_pct || 0).toFixed(1)}%</span>
              </div>
            </div>
            <div className="verticalLabel">{row.label}</div>
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
      <BarList title="核心仓位浮盈亏排名" rows={viz.pnl_rank || []} valueKey="pnl_cny" formatValue={(value) => fmtMoney(value, "CNY")} />
      <BarList title="卫星仓位浮盈亏排名" rows={viz.satellite_pnl_rank || []} valueKey="pnl" formatValue={(value) => fmtMoney(value, "USD")} />
      <CompareBars title="VOO / QQQ / 卫星仓位 / 短债(SGOV) / 现金 当前与目标对比" rows={viz.allocation_compare || []} />
      <CompareBars title="卫星仓位内部占比" rows={viz.satellite_split || []} className="compactVerticalChart" />
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
    const symbolDailyPct = point?.symbol_daily_pct || {};
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
          bottomFillColor1: "rgba(250, 204, 21, 0)",
          bottomFillColor2: "rgba(250, 204, 21, 0)",
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
      const left = Math.min(Math.max(param.point.x + 14, 8), Math.max(8, container.clientWidth - 230));
      const top = Math.min(Math.max(param.point.y + 14, 8), Math.max(8, container.clientHeight - 154));
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
          <span className={tone(tooltip.point.portfolio_daily_pct)}>
            总资产当日 {tooltip.point.portfolio_daily_pct == null ? "-" : fmtPct(tooltip.point.portfolio_daily_pct)}
          </span>
          <span className={tone(tooltip.point.usd_daily_pct)}>
            美元资产当日 {tooltip.point.usd_daily_pct == null ? "-" : fmtPct(tooltip.point.usd_daily_pct)}
          </span>
          <span className={tone(tooltip.point.holding_pnl_cny)}>
            总资产盈亏 {tooltip.point.holding_pnl_cny == null ? "-" : fmtMoney(tooltip.point.holding_pnl_cny, "CNY")}
          </span>
          <span className={tone(tooltip.point.usd_pnl_usd)}>
            美元资产盈亏 {tooltip.point.usd_pnl_usd == null ? "-" : fmtMoney(tooltip.point.usd_pnl_usd, "USD")}
          </span>
          {tooltip.point.cash_flow_flag ? (
            <span>资金流/交易 {fmtMoney(tooltip.point.cash_flow_cny || 0, "CNY")}</span>
          ) : null}
          {series.map(([key, name, color]) => (
            <span key={key} style={{ "--series-color": color }}>
              <i />{name} {tooltip.point[key] == null ? "-" : fmtPct(tooltip.point[key])}
            </span>
          ))}
        </div>
      ) : null}
    </div>
  );
}
function LightweightKlineCard({ item }) {
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
    return { candles: nextCandles, volumes: nextVolumes };
  }, [item]);

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
    if (!didFitContentRef.current && candles.length) {
      chart.timeScale().fitContent();
      requestPriceAutoscale(candleSeries);
      didFitContentRef.current = true;
    }
  }, [candles, volumes]);

  return (
    <article className="lwChartCard">
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

function GlobalLightweightBoard({ data, viewKey }) {
  const columns = Math.min(5, Math.max(1, Number(data?.columns || 1)));
  const charts = data?.charts || [];
  return (
    <div className="lwChartGrid" style={{ "--lw-cols": columns }}>
      {charts.map((item) => <LightweightKlineCard item={item} key={`${item.symbol}-${viewKey}`} />)}
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

function SingleLightweightChart({ data, viewKey }) {
  const containerRef = useRef(null);
  const chartRef = useRef(null);
  const seriesRef = useRef({});
  const priceLinesRef = useRef({});
  const didFitContentRef = useRef(false);
  const volumeProfileRef = useRef([]);
  const [profileBars, setProfileBars] = useState([]);
  const candles = useMemo(() => {
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
    return out;
  }, [data]);
  const volumes = useMemo(() => (data?.volumes || []).map((row) => ({
    time: normalizeLightweightTime(row.time),
    value: Number(row.value || 0),
    color: row.color || "rgba(148, 163, 184, 0.24)",
  })).filter((row) => row.time != null && Number.isFinite(row.value)), [data]);
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
  const volumeProfile = useMemo(() => (data?.volume_profile || [])
    .map((row) => ({
      low: Number(row.low),
      high: Number(row.high),
      pct: Math.max(0, Math.min(1, Number(row.pct || 0))),
    }))
    .filter((row) => Number.isFinite(row.low) && Number.isFinite(row.high) && row.pct > 0), [data]);

  useEffect(() => {
    volumeProfileRef.current = volumeProfile;
  }, [volumeProfile]);

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
        rightOffset: 4,
        barSpacing: 8,
        tickMarkFormatter: formatLightweightChartTime,
      },
      localization: { timeFormatter: formatLightweightChartTime },
      crosshair: {
        vertLine: { color: "rgba(203, 213, 225, 0.35)", labelBackgroundColor: "#1d4ed8" },
        horzLine: { color: "rgba(203, 213, 225, 0.35)", labelBackgroundColor: "#1d4ed8" },
      },
    });
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
    rsi.createPriceLine({ price: 70, color: "rgba(248, 113, 113, 0.45)", lineWidth: 1, lineStyle: 2, axisLabelVisible: false });
    rsi.createPriceLine({ price: 30, color: "rgba(52, 211, 153, 0.45)", lineWidth: 1, lineStyle: 2, axisLabelVisible: false });
    seriesRef.current = { candle, volume, avwapUpper, avwapLower, avwap, ema20, ma50, ma200, rsi, rsiMa, macdHist, macd, macdSignal };
    chartRef.current = chart;
    const updateProfile = () => {
      const candleSeries = seriesRef.current.candle;
      if (!candleSeries) return;
      const bars = volumeProfileRef.current.map((row) => {
        const yLow = candleSeries.priceToCoordinate(row.low);
        const yHigh = candleSeries.priceToCoordinate(row.high);
        if (yLow == null || yHigh == null) return null;
        return {
          top: Math.min(yLow, yHigh),
          height: Math.max(2, Math.abs(yLow - yHigh)),
          width: `${Math.max(5, row.pct * 100)}%`,
        };
      }).filter(Boolean);
      setProfileBars(bars);
    };
    chart.timeScale().subscribeVisibleTimeRangeChange(updateProfile);
    const handleVisibleRangeChange = () => requestPriceAutoscale(candle);
    chart.timeScale().subscribeVisibleLogicalRangeChange(handleVisibleRangeChange);
    return () => {
      chart.timeScale().unsubscribeVisibleTimeRangeChange(updateProfile);
      chart.timeScale().unsubscribeVisibleLogicalRangeChange(handleVisibleRangeChange);
      chartRef.current = null;
      seriesRef.current = {};
      priceLinesRef.current = {};
      chart.remove();
    };
  }, [viewKey, rsiPeriod]);

  useEffect(() => {
    const chart = chartRef.current;
    const series = seriesRef.current;
    if (!chart || !series.candle) return;
    series.candle.applyOptions({
      borderVisible: true,
      wickVisible: true,
      wickUpColor: TERMINAL_CHART.green,
      wickDownColor: TERMINAL_CHART.coral,
    });
    series.candle.setData(candles);
    series.volume.setData(volumes);
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
      if (isDaily && candles.length > 100) {
        chart.timeScale().setVisibleLogicalRange({ from: candles.length - 100, to: candles.length + 14 });
      } else {
        chart.timeScale().fitContent();
      }
      requestPriceAutoscale(series.candle);
      didFitContentRef.current = true;
    }
    window.setTimeout(() => {
      const candleSeries = seriesRef.current.candle;
      if (!candleSeries) return;
      const bars = volumeProfileRef.current.map((row) => {
        const yLow = candleSeries.priceToCoordinate(row.low);
        const yHigh = candleSeries.priceToCoordinate(row.high);
        if (yLow == null || yHigh == null) return null;
        return {
          top: Math.min(yLow, yHigh),
          height: Math.max(2, Math.abs(yLow - yHigh)),
          width: `${Math.max(5, row.pct * 100)}%`,
        };
      }).filter(Boolean);
      setProfileBars(bars);
    }, 0);
  }, [candles, volumes, overlays, indicators, data, volumeProfile, isDaily, avwapSkewsIntradayScale]);

  return (
    <div className="singleLwWrap">
      <div className="singleLwHeader">
        <div>
          <strong>{data?.symbol}</strong>
          <span>{data?.label} · {data?.interval} · {avwapText}{data?.avwap_anchor ? ` · 锚点 ${data.avwap_anchor}` : ""}</span>
        </div>
        <div className={tone(data?.latest_change_pct)}>
          <strong>{fmtChartPrice(data?.latest_price, data?.symbol)}</strong>
          <span>{data?.latest_change_pct == null ? "-" : fmtPct(data.latest_change_pct)}</span>
        </div>
      </div>
      <div className="singleLwCanvas" ref={containerRef}>
        <div className="singleLwProfile" aria-hidden="true">
          {profileBars.map((bar, index) => (
            <span
              className="singleLwProfileBar"
              key={`${index}-${bar.top}`}
              style={{ top: `${bar.top}px`, height: `${bar.height}px`, width: bar.width }}
            />
          ))}
        </div>
        {!candles.length ? <div className="muted lwEmpty">暂无K线数据</div> : null}
      </div>
    </div>
  );
}

function KlinePage({ dashboardData }) {
  const [scope, setScope] = useState("global");
  const [symbol, setSymbol] = useState("VOO");
  const [interval, setInterval] = useState("1d");
  const [avwapMode, setAvwapMode] = useState("today_open");
  const [showExtended, setShowExtended] = useState(false);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [realtimeConnected, setRealtimeConnected] = useState(false);
  const [globalColumns, setGlobalColumns] = useState(globalKlineColumns);
  const loadRequestRef = useRef(0);
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
      return undefined;
    }
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
      if (requestSignatureRef.current === signature) setRealtimeConnected(true);
    };
    socket.onmessage = (event) => {
      if (requestSignatureRef.current !== signature) return;
      try {
        setData(JSON.parse(event.data));
      } catch {
        setError("K线推送数据解析失败");
      }
    };
    socket.onerror = () => {
      if (requestSignatureRef.current === signature) setError("K线实时订阅连接失败");
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

  const isEtf = ["VOO", "QQQ", "SGOV", "510330.SS"].includes(symbol);
  const avwapSelectValue = interval === "1d" && avwapMode === "today_open"
    ? (isEtf ? "high_60d" : "earnings")
    : (isEtf && avwapMode === "earnings" ? "high_60d" : avwapMode);

  return (
    <section className="chartPanel technicalPanel">
      <div className="toolbarRow">
        <div className="segmented toolbarBlock">
          <button className={scope === "global" ? "active" : ""} onClick={() => setScope("global")}>全局看板</button>
          <button className={scope === "single" ? "active" : ""} onClick={() => setScope("single")}>单标的</button>
        </div>
        <div className="segmented toolbarBlock">
          {[["1d", "日线"], ["15m", "15min"], ["5m", "5min"]].map(([value, label]) => (
            <button className={interval === value ? "active" : ""} key={value} onClick={() => setInterval(value)}>
              {label}
            </button>
          ))}
        </div>
        {scope === "single" ? <select value={symbol} onChange={(event) => setSymbol(event.target.value)}>
          {[...(dashboardData?.holdings || [])
            .filter((row) => row.currency === "USD" && row.symbol !== "SGOV")
            .map((row) => row.symbol)
            , "510330.SS"]
            .map((item) => <option key={item} value={item}>{item === "510330.SS" ? "510330 沪深300ETF" : item}</option>)}
        </select> : null}
        {scope === "single" ? (
          <select value={avwapSelectValue} onChange={(event) => setAvwapMode(event.target.value)} aria-label="AVWAP锚点">
            {!isEtf ? <option value="earnings">AVWAP：最近财报日</option> : null}
            <option value="high_60d">AVWAP：最近60日历史高点</option>
            <option value="selloff_60d">AVWAP：最近60日大跌低点</option>
            {interval !== "1d" ? <option value="today_open">AVWAP：今日开盘</option> : null}
          </select>
        ) : null}
        {interval !== "1d" ? (
          <button className={showExtended ? "active" : ""} onClick={() => setShowExtended((value) => !value)}>
            扩展盘：{showExtended ? "显示" : "隐藏"}
          </button>
        ) : null}
      </div>
      {data && scope === "global" ? <div className="muted">全局看板：{data.symbols?.join(" / ")} · {data.interval} · 手动刷新</div> : null}
      {data && scope === "single" ? <div className="muted">模板：我的旧版技术看板 · 行情源 {data.market_provider || "-"} · {data.interval} · {realtimeConnected ? "实时订阅中" : "实时连接中"}{data.avwap_label ? ` · AVWAP：${data.avwap_label}（锚点 ${data.avwap_anchor}）` : ""}{data.user_avg_cost ? ` · 成本线 ${Number(data.user_avg_cost).toFixed(2)}` : ""}</div> : null}
      {loading ? <div className="muted">K线加载中</div> : null}
      {error || data?.error ? <div className="errorInline">K线加载失败：{error || data.error}</div> : null}
      {scope === "global" && data?.charts ? <GlobalLightweightBoard data={data} viewKey={`${data.interval}-${data.show_extended}-${data.columns}`} /> : null}
      {scope === "single" && data?.candles ? (
        <SingleLightweightChart data={data} viewKey={`${data.symbol}-${data.interval}-${data.show_extended}-${data.avwap_mode}`} />
      ) : null}
    </section>
  );
}

function AssetMetricCards({ data, holdings, balances }) {
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
  const usdUnrealized = usdValue - usdCost;
  const usdCash = Number(balances.cash_usd || 0);
  const usdRealized = Number(balances.realized_usd || 0) + Number(balances.sgov_dividend_usd || 0);
  const usdTotal = usdValue + usdCash;
  const usdReturn = usdCost ? (usdUnrealized / usdCost) * 100 : 0;

  const totalCostCny = rows.reduce((sum, row) => sum + row.costCny, 0);
  const totalValueCny = rows.reduce((sum, row) => sum + row.valueCny, 0);
  const totalUnrealizedCny = totalValueCny - totalCostCny;
  const cashCny = Number(balances.cash_cny || 0) + usdCash * fx;
  const totalRealizedCny = Number(balances.realized_cny || 0) + usdRealized * fx;
  const totalAssetsCny = totalValueCny + cashCny;
  const totalReturn = totalCostCny ? (totalUnrealizedCny / totalCostCny) * 100 : 0;

  return (
    <div className="assetSections">
      <div className="assetMetricBlock">
        <h2>美元资产</h2>
        <div className="assetMetricGrid">
          <div className="assetMetricCard"><span>已变现盈亏</span><strong>{fmtMoney(usdRealized, "USD")}</strong></div>
          <div className="assetMetricCard"><span>未实现浮盈亏</span><strong className={tone(usdUnrealized)}>{fmtMoney(usdUnrealized, "USD")}</strong><em className={tone(usdReturn)}>{fmtPct(usdReturn)}</em></div>
        </div>
        <p className="assetCaption">
          成本 {fmtMoney(usdCost, "USD")} | 持仓市值 {fmtMoney(usdValue, "USD")} | 已变现盈亏 {fmtMoney(usdRealized, "USD")} | 现金 {fmtMoney(usdCash, "USD")} | 总资产 {fmtMoney(usdTotal, "USD")} | 收益率 = 未实现浮盈亏 / 美元持仓成本 = {fmtPct(usdReturn)}
        </p>
      </div>
      <div className="assetMetricBlock">
        <h2>总资产（折合CNY）</h2>
        <div className="assetMetricGrid">
          <div className="assetMetricCard"><span>已变现盈亏</span><strong>{fmtMoney(totalRealizedCny, "CNY")}</strong></div>
          <div className="assetMetricCard"><span>未实现浮盈亏</span><strong className={tone(totalUnrealizedCny)}>{fmtMoney(totalUnrealizedCny, "CNY")}</strong><em className={tone(totalReturn)}>{fmtPct(totalReturn)}</em></div>
        </div>
        <p className="assetCaption">
          成本 {fmtMoney(totalCostCny, "CNY")} | 持仓市值 {fmtMoney(totalValueCny, "CNY")} | 已变现盈亏 {fmtMoney(totalRealizedCny, "CNY")} | 现金 {fmtMoney(cashCny, "CNY")} | 总资产 {fmtMoney(totalAssetsCny, "CNY")} | 收益率 = 未实现浮盈亏 / 持仓成本 = {fmtPct(totalReturn)}
        </p>
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

function EditableHoldingsPage({ data }) {
  const [holdings, setHoldings] = useState({});
  const [balances, setBalances] = useState({});
  const realtimeTotalValueCny = (data.holdings || []).reduce(
    (sum, row) => sum + Math.max(0, Number(row.value_cny || 0)),
    0,
  );

  function resetDraft() {
    setHoldings(Object.fromEntries(data.holdings.map((row) => [row.symbol, { shares: String(row.shares ?? 0), avg_cost: String(row.avg_cost ?? 0) }])));
    setBalances({
      cash_usd: String(data.balances?.cash_usd ?? 0),
      cash_cny: String(data.balances?.cash_cny ?? 0),
      realized_usd: String(data.balances?.realized_usd ?? 0),
      realized_cny: String(data.balances?.realized_cny ?? 0),
      sgov_dividend_usd: String(data.balances?.sgov_dividend_usd ?? 0),
    });
  }

  useEffect(() => {
    resetDraft();
  }, [data]);

  return (
    <section>
      <AssetMetricCards data={data} holdings={holdings} balances={balances} />
      <div className="tableWrap">
        <table className="editableHoldingsTable">
          <thead>
            <tr>
              <th>标的</th><th>实时占比</th><th>数量</th><th>当前价</th><th>当日涨跌</th><th>60日回撤</th><th>60日涨幅</th><th>成本</th><th>市值</th><th>盈亏</th><th>Forward PE/近5日</th><th>PE区间</th><th>PEG</th><th>PEG区间</th>
            </tr>
          </thead>
          <tbody>
            {data.holdings.map((row) => (
              <tr key={row.symbol}>
                <th>{row.label}</th>
                <td>{realtimeTotalValueCny > 0 ? `${(Number(row.value_cny || 0) / realtimeTotalValueCny * 100).toFixed(2)}%` : "-"}</td>
                <td>{Number(row.shares || 0).toLocaleString(undefined, { maximumFractionDigits: 4 })}</td>
                <td>{fmtMoney(row.price, row.currency, row.currency === "USD" ? 2 : 4)}</td>
                <td className={tone(row.effective_daily_pct)}>{fmtPct(row.effective_daily_pct)}</td>
                <td className={tone(row.drawdown_pct)}>{row.drawdown_pct == null ? "-" : fmtPct(row.drawdown_pct)}</td>
                <td className={tone(row.rebound_pct)}>{row.rebound_pct == null ? "-" : fmtPct(row.rebound_pct)}</td>
                <td>{fmtMoney(row.avg_cost, row.currency, row.currency === "USD" ? 2 : 4)}</td>
                <td>{fmtMoney(row.value, row.currency)}</td>
                <td className={tone(row.pnl)}>{fmtMoney(row.pnl, row.currency)}</td>
                <td className={peTone(row)}>{valuationLabel(row)}</td>
                <td>{row.ps_band && row.ps_band !== "-" ? row.ps_band : (row.pe_band || "-")}</td>
                <td className={pegTone(row)}>{pegLabel(row)}</td>
                <td>{row.peg_band || "-"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function Rebalance({ data, onSaved }) {
  const rows = data.rebalance.rows;
  const suggestionRows = useMemo(() => rows.filter((row) => row.symbol !== "001015"), [rows]);
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
      realized_usd: String(data.balances?.realized_usd ?? 0),
      realized_cny: String(data.balances?.realized_cny ?? 0),
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
  const sortedTrades = useMemo(() => (data.trades || []).slice().reverse(), [data.trades]);
  const visibleTrades = tradeHistoryOpen ? sortedTrades.slice(0, 20) : sortedTrades.slice(0, 3);
  const sortedFxConversions = useMemo(() => (data.fx_conversions || []).slice().reverse(), [data.fx_conversions]);
  const visibleFxConversions = fxHistoryOpen ? sortedFxConversions.slice(0, 20) : sortedFxConversions.slice(0, 3);

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
      const balances = Object.fromEntries(Object.entries(balanceInputs).map(([key, value]) => [key, Number(value || 0)]));
      const response = await fetch(`${API_BASE}/api/balances`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ balances }),
      });
      if (!response.ok) throw new Error(`balances HTTP ${response.status}`);
      setBalanceMessage("现金与已变现已保存");
      setEditingBalances(false);
      onSaved().catch(() => {});
    } catch (err) {
      setBalanceMessage(err instanceof Error ? err.message : String(err));
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
      <div className="rebalanceActionRow">
        <button className="primary" onClick={() => setBudgetOpen(true)}>预算设置</button>
        <button className="primary" onClick={() => setEditingBalances(true)}>编辑现金</button>
        <button className="primary" onClick={openUniverseEditor}>编辑卫星标的</button>
        <button className="primary" onClick={() => openTradeEditor(tradeRows[0]?.symbol || "")}>记录一条买卖</button>
        <button className="primary" onClick={openFxConversionEditor}>记录换汇</button>
      </div>
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
        <button onClick={() => setRulesOpen(true)}>算法规则</button>
      </div>
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
        <table>
          <thead>
            <tr>
              <th>标的</th><th>月初占比</th><th>目标占比</th><th>60日回撤</th><th>计划应买</th><th>实际差值</th><th>净买入</th><th>档位</th><th>估值/追高系数</th><th>说明</th>
            </tr>
          </thead>
          <tbody>
            {suggestionRows.map((row) => {
              return (
                <React.Fragment key={row.symbol}>
                  <tr>
                    <th>{row.symbol}</th>
                    <td>{Number(row.month_start_pct ?? row.current_pct ?? 0).toFixed(2)}%</td>
                    <td>{Number(row.target_pct || 0).toFixed(2)}%</td>
                    <td className={tone(row.drawdown_pct)}>{row.drawdown_pct == null ? "-" : fmtPct(row.drawdown_pct)}</td>
                    <td className="planCell">
                      <div>{fmtMoney(row.planned_buy_usd, row.currency || "USD")}</div>
                      {row.planned_buy_formula ? <div className="cellSubtext">{row.planned_buy_formula}</div> : null}
                    </td>
                    <td className="planCell">
                      <div className={tone(row.buy_difference_usd)}>{fmtMoney(row.buy_difference_usd, row.currency || "USD")}</div>
                    </td>
                    <td>{fmtMoney(row.net_bought_usd, row.currency || "USD")}</td>
                    <td><span className={`tierBadge ${tierClass(row.intensity)}`}>{row.signal || row.intensity}</span></td>
                    <td className={Number(row.valuation_split_factor || 1) < 1 ? "down" : "flat"}>{Number(row.valuation_split_factor || 1).toFixed(2)}</td>
                    <td className="note">{row.note}</td>
                  </tr>
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
          {sortedTrades.length > 3 ? (
            <button onClick={() => setTradeHistoryOpen((value) => !value)}>
              {tradeHistoryOpen ? "收起" : "显示更多"}
            </button>
          ) : null}
        </div>
        {tradeMessage ? <div className="saveMessage down">{tradeMessage}</div> : null}
        <div className="tableWrap">
          <table>
            <thead><tr><th>日期</th><th>标的</th><th>方向</th><th>股数</th><th>成交金额</th><th>成交成本</th><th>收盘差额</th><th>持仓成本变化</th><th>档位</th><th>操作</th></tr></thead>
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
                  <td className={tone(tradeCostDelta(trade))} title={fmtAvgCostChangeTitle(trade, currencyBySymbol[trade.symbol] || "USD")}>
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
      </div>
      <div className="fxConversionBlock">
        <div className="sectionHeader subHeader">
          <div>
            <h2>购汇记录</h2>
            <span className="muted">
              平均汇率 {Number(data.summary?.avg_fx_rate || data.summary?.fx || 0).toFixed(4)} ·
              已购汇 {fmtMoney(data.summary?.fx_conversion_total_usd || 0, "USD")} / {fmtMoney(data.summary?.fx_conversion_total_cny || 0, "CNY")} ·
              美元持仓汇兑影响 <span className={tone(data.summary?.usd_fx_pnl_cny)}>{fmtMoney(data.summary?.usd_fx_pnl_cny || 0, "CNY")}</span>
            </span>
          </div>
          {sortedFxConversions.length > 3 ? (
            <button onClick={() => setFxHistoryOpen((value) => !value)}>
              {fxHistoryOpen ? "收起" : "显示更多"}
            </button>
          ) : null}
        </div>
        {fxConversionMessage ? <div className="saveMessage down">{fxConversionMessage}</div> : null}
        <div className="tableWrap">
          <table>
            <thead><tr><th>日期</th><th>人民币金额</th><th>美元金额</th><th>汇率</th><th>备注</th><th>操作</th></tr></thead>
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
                    <option value="probe">QQQ -2%分批</option>
                    <option value="month_end">QQQ月底补齐</option>
                    <option value="small">小加</option>
                    <option value="medium">中加</option>
                    <option value="large">大加</option>
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
          <div className="modalPanel" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
            <div className="sectionHeader">
              <h2>现金与已变现</h2>
              <button onClick={() => { resetBalanceDraft(); setEditingBalances(false); }} disabled={savingBalances}>关闭</button>
            </div>
            <div className="balanceEditGrid">
              <label>USD现金<input value={balanceInputs.cash_usd ?? ""} onChange={(event) => updateBalance("cash_usd", event.target.value)} inputMode="decimal" /></label>
              <label>CNY现金<input value={balanceInputs.cash_cny ?? ""} onChange={(event) => updateBalance("cash_cny", event.target.value)} inputMode="decimal" /></label>
              <label>USD已变现<input value={balanceInputs.realized_usd ?? ""} onChange={(event) => updateBalance("realized_usd", event.target.value)} inputMode="decimal" /></label>
              <label>CNY已变现<input value={balanceInputs.realized_cny ?? ""} onChange={(event) => updateBalance("realized_cny", event.target.value)} inputMode="decimal" /></label>
              <label>SGOV股息<input value={balanceInputs.sgov_dividend_usd ?? ""} onChange={(event) => updateBalance("sgov_dividend_usd", event.target.value)} inputMode="decimal" /></label>
            </div>
            <div className="actions">
              <button onClick={() => { resetBalanceDraft(); setEditingBalances(false); }} disabled={savingBalances}>取消</button>
              <button className="primary" onClick={saveBalances} disabled={savingBalances}><Save size={16} /> 保存现金</button>
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
}

function DashboardPage({ data }) {
  return (
    <>
      <Summary data={data} />
      <PerformanceChart history={data.performance_history} />
      <DailyHeatmap cards={data.daily_cards} holdings={data.holdings} />
      <DailyCards cards={data.daily_cards} />
      <Visualizations data={data} />
    </>
  );
}

function HoldingsPage({ data }) {
  return <EditableHoldingsPage data={data} />;
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
    <main className="appShell">
      <Header data={data} onRefresh={load} />
      <PageNav page={page} setPage={setPage} />
      {page === "dashboard" ? <DashboardPage data={data} /> : null}
      {page === "holdings" ? <HoldingsPage data={data} /> : null}
      {page === "rebalance" ? <RebalancePage data={data} onSaved={load} /> : null}
      {page === "kline" ? <KlinePage dashboardData={data} /> : null}
    </main>
  );
}
