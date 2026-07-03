import React, { Suspense, lazy, useEffect, useMemo, useRef, useState } from "react";
import { Activity, RefreshCcw, Save } from "lucide-react";
import { CandlestickSeries, createChart, HistogramSeries, TickMarkType } from "lightweight-charts";

const Plot = lazy(() => import("react-plotly.js"));

const API_BASE =
  import.meta.env.VITE_API_BASE ||
  `${window.location.protocol}//${window.location.hostname}:8010`;

function plotWrapStyle(figure) {
  const height = Number(figure?.layout?.height || 980);
  return { height: `${height}px` };
}

const SHANGHAI_TIME_ZONE = "Asia/Shanghai";
const shanghaiDateTimeFormatter = new Intl.DateTimeFormat("zh-CN", {
  timeZone: SHANGHAI_TIME_ZONE,
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  hourCycle: "h23",
});
const shanghaiDateFormatter = new Intl.DateTimeFormat("zh-CN", {
  timeZone: SHANGHAI_TIME_ZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});
const shanghaiTimeFormatter = new Intl.DateTimeFormat("zh-CN", {
  timeZone: SHANGHAI_TIME_ZONE,
  hour: "2-digit",
  minute: "2-digit",
  hourCycle: "h23",
});
const newYorkSessionFormatter = new Intl.DateTimeFormat("en-GB", {
  timeZone: "America/New_York",
  hour: "2-digit",
  minute: "2-digit",
  hourCycle: "h23",
});

function timeToDate(time) {
  if (typeof time === "number") return new Date(time * 1000);
  if (typeof time === "string") return new Date(`${time}T00:00:00+08:00`);
  return new Date(Date.UTC(time.year, time.month - 1, time.day));
}

function formatShanghaiChartTime(time) {
  return shanghaiDateTimeFormatter.format(timeToDate(time)).replace(/\//g, "-");
}

function formatShanghaiTick(time, tickMarkType) {
  const date = timeToDate(time);
  if (tickMarkType === TickMarkType.Time || tickMarkType === TickMarkType.TimeWithSeconds) {
    return shanghaiTimeFormatter.format(date);
  }
  return shanghaiDateFormatter.format(date).replace(/\//g, "-");
}

function formatShanghaiInputDate(date = new Date()) {
  const parts = shanghaiDateFormatter.formatToParts(date);
  const year = parts.find((part) => part.type === "year")?.value || "1970";
  const month = parts.find((part) => part.type === "month")?.value || "01";
  const day = parts.find((part) => part.type === "day")?.value || "01";
  return `${year}-${month}-${day}`;
}

function isRegularUsSession(time) {
  if (typeof time !== "number") return true;
  const parts = newYorkSessionFormatter.formatToParts(new Date(time * 1000));
  const hour = Number(parts.find((part) => part.type === "hour")?.value || 0);
  const minute = Number(parts.find((part) => part.type === "minute")?.value || 0);
  const totalMinutes = hour * 60 + minute;
  return totalMinutes >= 9 * 60 + 30 && totalMinutes < 16 * 60;
}

function fmtMoney(value, currency = "USD", digits = 2) {
  const num = Number(value || 0);
  return `${currency} ${num.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })}`;
}

function fmtPct(value) {
  const num = Number(value || 0);
  return `${num >= 0 ? "+" : ""}${num.toFixed(2)}%`;
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
  const prev = Number(trade?.prev_avg_cost || 0);
  const next = Number(trade?.new_avg_cost || 0);
  if (!Number.isFinite(prev) || !Number.isFinite(next) || (prev === 0 && next === 0)) return "-";
  const digits = currency === "USD" ? 2 : 4;
  return `${fmtMoney(prev, currency, digits)} -> ${fmtMoney(next, currency, digits)}`;
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

function globalKlineColumns(width = window.innerWidth) {
  if (width >= 1280) return 3;
  if (width >= 820) return 2;
  return 1;
}

function peTone(row) {
  if ((row.symbol === "VOO" || row.symbol === "QQQ") && Number(row.recent_5d_pct || 0) >= (row.symbol === "QQQ" ? 3 : 2)) return "down";
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
  const weightedDailyChangeCny = Number.isFinite(Number(summary.weighted_daily_change_cny))
    ? Number(summary.weighted_daily_change_cny)
    : (data.holdings || []).reduce(
        (total, row) => total + dailyAmount(row.value_cny, row.effective_daily_pct),
        0,
      );
  return (
    <section className="summaryGrid">
      <div className="summaryItem">
        <span>总资产</span>
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
        <strong className={tone(summary.weighted_daily_pct)}>
          {fmtMoney(weightedDailyChangeCny, "CNY")} · {fmtPct(summary.weighted_daily_pct)}
        </strong>
      </div>
      <div className="summaryItem">
        <span>汇率</span>
        <strong>{Number(summary.fx || 0).toFixed(4)}</strong>
      </div>
    </section>
  );
}

function DailyCards({ cards }) {
  return (
    <section className="cardGrid">
      {cards.map((card) => (
        <article className={`dailyCard ${card.wide ? "wideCard" : ""}`} key={card.symbol}>
          <div className="cardTitle">{card.label}</div>
          {card.price_line ? <div className="priceLine">{fmtCardPriceLine(card.price_line)}</div> : null}
          <div className={tone(card.regular_pct)}>
            {fmtPct(card.regular_pct)}
            {card.extended_pct != null ? <span className={tone(card.extended_pct)}>（{fmtPct(card.extended_pct)}）</span> : null}
          </div>
          <div className={tone(card.change_usd)}>
            {fmtMoney(card.change_usd, "USD")}
            {card.extended_change_usd != null ? <span className={tone(card.extended_change_usd)}>（{Number(card.extended_change_usd).toFixed(2)}）</span> : null}
          </div>
          <div className={tone(card.change_cny)}>
            {fmtMoney(card.change_cny, "CNY")}
            {card.extended_change_cny != null ? <span className={tone(card.extended_change_cny)}>（{Number(card.extended_change_cny).toFixed(2)}）</span> : null}
          </div>
        </article>
      ))}
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

function CompareBars({ title, rows, amountKey = "current_usd" }) {
  const max = Math.max(1, ...rows.flatMap((row) => [Number(row.current_pct || 0), Number(row.target_pct || 0)]));
  const currentTotal = rows.reduce((sum, row) => sum + Number(row.current_pct || 0), 0);
  const targetTotal = rows.reduce((sum, row) => sum + Number(row.target_pct || 0), 0);
  return (
    <section className="chartPanel verticalChartPanel">
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
      <CompareBars title="卫星仓位内部占比" rows={viz.satellite_split || []} />
    </section>
  );
}

function PerformanceChart({ history }) {
  const points = history?.points || [];
  const latest = points[points.length - 1];
  const series = [
    ["portfolio_return_pct", "我的组合", "#1d4ed8", "rgba(37, 99, 235, 0.20)", "solid", 4.5],
    ["001015_return_pct", "沪深300", "#a16207", "rgba(0,0,0,0)", "solid", 2.3],
    ["VOO_return_pct", "VOO", "#8b5cf6", "rgba(0,0,0,0)", "solid", 2.5],
    ["QQQ_return_pct", "QQQ", "#7dd3fc", "rgba(0,0,0,0)", "solid", 2.8],
  ];
  const figure = useMemo(() => {
    const dates = points.map((point) => point.date);
    return {
      data: series.map(([key, name, color, fillcolor, dash, width]) => {
        const isPortfolio = key === "portfolio_return_pct";
        return {
          type: "scatter",
          mode: points.length > 1 ? "lines+markers" : "markers",
          name,
          x: dates,
          y: points.map((point) => point[key]),
          customdata: points.map((point) => point[key.replace("_return_pct", "_daily_pct")]),
          connectgaps: false,
          fill: isPortfolio ? "tozeroy" : "none",
          fillcolor,
          line: {
            color,
            width,
            shape: "spline",
            smoothing: 0.65,
            dash,
          },
          marker: {
            color,
            size: isPortfolio ? 9 : 6,
            line: { color: "#0f172a", width: 1.2 },
          },
          hovertemplate: "%{x}<br>%{fullData.name}<br>累计: %{y:.2f}%<br>当日: %{customdata:.2f}%<extra></extra>",
        };
      }),
      layout: {
        autosize: true,
        height: 430,
        margin: { l: 58, r: 24, t: 18, b: 44 },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(2,6,23,0.18)",
        font: { color: "#cbd5e1", family: "Inter, Microsoft YaHei, system-ui, sans-serif" },
        xaxis: {
          type: "date",
          showgrid: false,
          linecolor: "rgba(148, 163, 184, 0.24)",
          tickfont: { color: "#94a3b8" },
        },
        yaxis: {
          title: { text: "收益率", font: { color: "#94a3b8" } },
          ticksuffix: "%",
          zeroline: true,
          zerolinecolor: "rgba(226, 232, 240, 0.42)",
          gridcolor: "rgba(148, 163, 184, 0.12)",
          linecolor: "rgba(148, 163, 184, 0.24)",
          tickfont: { color: "#94a3b8" },
        },
        legend: {
          orientation: "h",
          x: 0,
          xanchor: "left",
          y: 1.12,
          yanchor: "bottom",
          font: { color: "#cbd5e1" },
        },
        hovermode: "x unified",
        shapes: [
          {
            type: "rect",
            xref: "paper",
            yref: "paper",
            x0: 0,
            y0: 0,
            x1: 1,
            y1: 1,
            fillcolor: "rgba(15, 23, 42, 0.26)",
            line: { width: 0 },
            layer: "below",
          },
        ],
      },
      config: {
        displayModeBar: false,
        responsive: true,
      },
    };
  }, [points]);

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
        <Suspense fallback={<div className="plotLoading">加载图表中...</div>}>
          <Plot data={figure.data} layout={figure.layout} config={figure.config} useResizeHandler style={{ width: "100%", height: "100%" }} />
        </Suspense>
      </div>
    </section>
  );
}

function LightweightChart({ bars, showExtended }) {
  const hostRef = useRef(null);

  useEffect(() => {
    if (!hostRef.current || !bars?.length) return undefined;
    const chart = createChart(hostRef.current, {
      layout: { background: { color: "#0b0f14" }, textColor: "#cbd5e1", fontFamily: "Inter, Microsoft YaHei, system-ui, sans-serif" },
      grid: { vertLines: { color: "rgba(148, 163, 184, 0.08)" }, horzLines: { color: "rgba(148, 163, 184, 0.08)" } },
      rightPriceScale: { borderColor: "rgba(148, 163, 184, 0.18)" },
      timeScale: {
        borderColor: "rgba(148, 163, 184, 0.18)",
        timeVisible: true,
        tickMarkFormatter: formatShanghaiTick,
      },
      localization: {
        locale: "zh-CN",
        timeFormatter: formatShanghaiChartTime,
      },
      crosshair: { mode: 1 },
      autoSize: true,
    });
    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#22c55e",
      downColor: "#ef4444",
      borderUpColor: "#22c55e",
      borderDownColor: "#ef4444",
      wickUpColor: "#22c55e",
      wickDownColor: "#ef4444",
    });
    const visibleBars = showExtended ? bars : bars.filter((bar) => isRegularUsSession(bar.time));
    candleSeries.setData(visibleBars.map((bar) => {
      const extended = !isRegularUsSession(bar.time);
      const rising = Number(bar.close) >= Number(bar.open);
      const color = rising ? "#22c55e" : "#ef4444";
      return {
        time: bar.time,
        open: Number(bar.open),
        high: Number(bar.high),
        low: Number(bar.low),
        close: Number(bar.close),
        color: extended ? "rgba(0, 0, 0, 0)" : color,
        borderColor: color,
        wickColor: color,
      };
    }));
    const volumeSeries = chart.addSeries(HistogramSeries, { priceFormat: { type: "volume" }, priceScaleId: "" });
    volumeSeries.priceScale().applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });
    volumeSeries.setData(visibleBars.filter((bar) => Number.isFinite(Number(bar.volume)) && Number(bar.volume) > 0).map((bar) => {
      const extended = !isRegularUsSession(bar.time);
      const rising = Number(bar.close) >= Number(bar.open);
      return {
        time: bar.time,
        value: Number(bar.volume),
        color: rising
          ? `rgba(34, 197, 94, ${extended ? 0.2 : 0.38})`
          : `rgba(239, 68, 68, ${extended ? 0.2 : 0.38})`,
      };
    }));
    chart.timeScale().fitContent();
    return () => chart.remove();
  }, [bars, showExtended]);

  return <div className="lwChart" ref={hostRef} />;
}

function KlinePage() {
  const [mode, setMode] = useState("template");
  const [scope, setScope] = useState("global");
  const [symbol, setSymbol] = useState("VOO");
  const [interval, setInterval] = useState("1d");
  const [avwapMode, setAvwapMode] = useState("earnings");
  const [showExtended, setShowExtended] = useState(true);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [realtimeConnected, setRealtimeConnected] = useState(false);
  const [globalColumns, setGlobalColumns] = useState(globalKlineColumns);
  const [userXAxisRanges, setUserXAxisRanges] = useState({});
  const userXAxisRangesRef = useRef({});
  const loadRequestRef = useRef(0);
  const requestSignature = `${mode}|${scope}|${symbol}|${interval}|${avwapMode}|${showExtended}|${globalColumns}`;
  const requestSignatureRef = useRef(requestSignature);
  requestSignatureRef.current = requestSignature;

  async function load(options = {}) {
    const silent = Boolean(options.silent);
    const requestId = ++loadRequestRef.current;
    const signature = requestSignature;
    if (!silent) setLoading(true);
    setError("");
    try {
      const isEtfSymbol = ["VOO", "QQQ", "SGOV"].includes(symbol);
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
      const endpoint = scope === "global" ? "chart-board-global" : (mode === "template" ? "chart-board" : "ohlcv");
      const response = await fetch(`${API_BASE}/api/${endpoint}?${qs.toString()}`);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      if (requestId !== loadRequestRef.current || signature !== requestSignatureRef.current) return;
      if (silent && scope === "global" && payload?.figure && (!payload.figure.data || payload.figure.data.length === 0)) return;
      const payloadWithRanges = applySavedXAxisRangesToPayload(payload);
      if (scope === "single" && mode === "futu" && (!payload.bars || payload.bars.length === 0)) {
        const fallbackResponse = await fetch(`${API_BASE}/api/chart-board?${qs.toString()}`);
        if (requestId !== loadRequestRef.current || signature !== requestSignatureRef.current) return;
        if (fallbackResponse.ok) {
          const fallbackPayload = await fallbackResponse.json();
          setData(applySavedXAxisRangesToPayload({ ...payloadWithRanges, fallback_template: fallbackPayload }));
        } else {
          setData(payloadWithRanges);
        }
      } else {
        setData(payloadWithRanges);
      }
    } catch (err) {
      if (requestId !== loadRequestRef.current || signature !== requestSignatureRef.current) return;
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      if (!silent && requestId === loadRequestRef.current && signature === requestSignatureRef.current) setLoading(false);
    }
  }

  useEffect(() => {
    userXAxisRangesRef.current = {};
    setUserXAxisRanges({});
    load();
  }, [mode, scope, symbol, interval, avwapMode, showExtended, globalColumns]);

  function handlePlotRelayout(event) {
    if (!event) return;
    setUserXAxisRanges((current) => {
      const next = { ...userXAxisRangesRef.current, ...current };
      let changed = false;
      for (const [key, value] of Object.entries(event)) {
        const rangePart = key.match(/^(xaxis\d*)\.range\[(0|1)\]$/);
        if (rangePart) {
          const axis = rangePart[1];
          const index = Number(rangePart[2]);
          const range = Array.isArray(next[axis]) ? [...next[axis]] : [undefined, undefined];
          range[index] = value;
          if (range[0] !== undefined && range[1] !== undefined) {
            next[axis] = range;
            changed = true;
          }
          continue;
        }
        const range = key.match(/^(xaxis\d*)\.range$/);
        if (range && Array.isArray(value) && value.length >= 2) {
          next[range[1]] = [value[0], value[1]];
          changed = true;
          continue;
        }
        const autorange = key.match(/^(xaxis\d*)\.autorange$/);
        if (autorange && value) {
          delete next[autorange[1]];
          changed = true;
        }
      }
      if (changed) {
        userXAxisRangesRef.current = next;
        return next;
      }
      return current;
    });
  }

  function layoutWithUserXAxisRanges(layout) {
    const next = { ...(layout || {}) };
    const ranges = { ...userXAxisRanges, ...userXAxisRangesRef.current };
    for (const [axis, range] of Object.entries(ranges)) {
      if (Array.isArray(range) && range.length >= 2) {
        next[axis] = { ...(next[axis] || {}), range };
      }
    }
    return next;
  }

  function applySavedXAxisRangesToPayload(payload) {
    const ranges = userXAxisRangesRef.current;
    if (!payload || !ranges || Object.keys(ranges).length === 0) return payload;
    const applyToFigure = (figure) => {
      if (!figure?.layout) return figure;
      return { ...figure, layout: layoutWithUserXAxisRanges(figure.layout) };
    };
    return {
      ...payload,
      figure: applyToFigure(payload.figure),
      fallback_template: payload.fallback_template
        ? { ...payload.fallback_template, figure: applyToFigure(payload.fallback_template.figure) }
        : payload.fallback_template,
    };
  }

  useEffect(() => {
    if (scope !== "global") return undefined;
    setRealtimeConnected(false);
    const id = window.setInterval(() => load({ silent: true }), 2000);
    return () => window.clearInterval(id);
  }, [scope, interval, showExtended, globalColumns]);

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
    if (mode !== "template" || scope !== "single") {
      setRealtimeConnected(false);
      return undefined;
    }
    if (interval === "1d") {
      setRealtimeConnected(false);
      return undefined;
    }
    const isEtfSymbol = ["VOO", "QQQ", "SGOV"].includes(symbol);
    let effectiveAvwapMode = avwapMode;
    if (isEtfSymbol && effectiveAvwapMode === "earnings") effectiveAvwapMode = "high_60d";
    if (interval === "1d" && effectiveAvwapMode === "today_open") {
      effectiveAvwapMode = isEtfSymbol ? "high_60d" : "earnings";
    }
    const qs = new URLSearchParams({ symbol, interval, avwap_mode: effectiveAvwapMode, show_extended: String(showExtended) });
    const wsBase = API_BASE.replace(/^http/, "ws");
    let disposed = false;
    let socket;
    let reconnectTimer;

    function connect() {
      socket = new WebSocket(`${wsBase}/ws/chart-board?${qs.toString()}`);
      socket.onopen = () => setRealtimeConnected(false);
      socket.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data);
          setData(applySavedXAxisRangesToPayload(payload));
          setRealtimeConnected(Boolean(payload.kline_subscription));
          setError("");
        } catch {
          // Ignore malformed frames and wait for the next K-line update.
        }
      };
      socket.onerror = () => setRealtimeConnected(false);
      socket.onclose = () => {
        setRealtimeConnected(false);
        if (!disposed) reconnectTimer = window.setTimeout(connect, 2000);
      };
    }

    connect();
    return () => {
      disposed = true;
      window.clearTimeout(reconnectTimer);
      socket?.close();
    };
  }, [mode, scope, symbol, interval, avwapMode, showExtended]);

  const figure = data?.figure;
  const fallbackFigure = data?.fallback_template?.figure;
  const isEtf = ["VOO", "QQQ", "SGOV"].includes(symbol);

  return (
    <section className="chartPanel technicalPanel">
      <div className="sectionHeader">
        <h2>看板</h2>
        <button onClick={load} disabled={loading}>刷新看板</button>
      </div>
      <div className="toolbarRow">
        <div className="segmented">
          <button className={mode === "template" ? "active" : ""} onClick={() => setMode("template")}>我的模板</button>
          <button className={mode === "futu" ? "active" : ""} onClick={() => setMode("futu")}>Futu轻量</button>
        </div>
        <div className="segmented">
          <button className={scope === "single" ? "active" : ""} onClick={() => setScope("single")}>单标的</button>
          <button className={scope === "global" ? "active" : ""} onClick={() => { setScope("global"); setMode("template"); }}>全局看板</button>
        </div>
        {scope === "single" ? <select value={symbol} onChange={(event) => setSymbol(event.target.value)}>
          {["VOO", "QQQ", "ISRG", "GOOGL", "MSFT", "AVGO", "NVDA", "TEM"].map((item) => <option key={item} value={item}>{item}</option>)}
        </select> : null}
        {[["1d", "日线"], ["15m", "15m"], ["5m", "5m"]].map(([value, label]) => (
          <label className="checkItem" key={value}>
            <input type="radio" name="interval" checked={interval === value} onChange={() => setInterval(value)} />
            {label}
          </label>
        ))}
        {mode === "template" && scope === "single" ? (
          <select value={isEtf && avwapMode === "earnings" ? "high_60d" : (interval === "1d" && avwapMode === "today_open" ? (isEtf ? "high_60d" : "earnings") : avwapMode)} onChange={(event) => setAvwapMode(event.target.value)} aria-label="AVWAP锚点">
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
      {data && scope === "single" && mode === "futu" ? <div className="muted">K线源：{data.source}{data.fallback_reason ? ` · 兜底原因：${data.fallback_reason}` : ""} · {data.bars?.length || 0} 根 · 时间：北京时间</div> : null}
      {data?.fallback_template ? <div className="muted">轻量K线无数据，已自动切到我的模板 · {data.fallback_template.interval}</div> : null}
      {data && scope === "single" && mode === "template" ? <div className="muted">模板：我的旧版技术看板 · 行情源 {data.market_provider || "-"} · {data.interval} · {realtimeConnected ? "实时订阅中" : "实时连接中"}{data.avwap_label ? ` · AVWAP：${data.avwap_label}（锚点 ${data.avwap_anchor}）` : ""}{data.user_avg_cost ? ` · 成本线 ${Number(data.user_avg_cost).toFixed(2)}` : ""}</div> : null}
      {loading ? <div className="muted">K线加载中</div> : null}
      {error || data?.error ? <div className="errorInline">K线加载失败：{error || data.error}</div> : null}
      {scope === "single" && mode === "futu" && data?.bars?.length ? <LightweightChart bars={data.bars} showExtended={showExtended} /> : null}
      {scope === "single" && mode === "futu" && !data?.bars?.length && fallbackFigure ? (
        <div className="plotWrap" style={plotWrapStyle(fallbackFigure)}>
          <Suspense fallback={<div className="muted plotLoading">模板图加载中</div>}>
            <Plot
              data={fallbackFigure.data}
              revision={0}
              layout={{
                ...layoutWithUserXAxisRanges(fallbackFigure.layout),
                autosize: true,
                dragmode: "zoom",
                uirevision: `fallback-${symbol}-${interval}-${avwapMode}-${showExtended}`,
                editrevision: `fallback-${symbol}-${interval}-${avwapMode}-${showExtended}`,
                selectionrevision: `fallback-${symbol}-${interval}-${avwapMode}-${showExtended}`,
              }}
              onRelayout={handlePlotRelayout}
              config={{
                responsive: true,
                displaylogo: false,
                displayModeBar: true,
                scrollZoom: true,
                doubleClick: "reset+autosize",
                modeBarButtonsToRemove: ["select2d", "lasso2d"],
              }}
              useResizeHandler
              style={{ width: "100%", height: "100%" }}
            />
          </Suspense>
        </div>
      ) : null}
      {(scope === "global" || mode === "template") && figure ? (
        <div className="plotWrap" style={plotWrapStyle(figure)}>
          <Suspense fallback={<div className="muted plotLoading">模板图加载中</div>}>
            <Plot
              data={figure.data}
              revision={0}
              layout={{
                ...layoutWithUserXAxisRanges(figure.layout),
                autosize: true,
                dragmode: "zoom",
                uirevision: `${scope}-${symbol}-${interval}-${avwapMode}-${showExtended}`,
                editrevision: `${scope}-${symbol}-${interval}-${avwapMode}-${showExtended}`,
                selectionrevision: `${scope}-${symbol}-${interval}-${avwapMode}-${showExtended}`,
              }}
              onRelayout={handlePlotRelayout}
              config={{
                responsive: true,
                displaylogo: false,
                displayModeBar: true,
                scrollZoom: true,
                doubleClick: "reset+autosize",
                modeBarButtonsToRemove: ["select2d", "lasso2d"],
              }}
              useResizeHandler
              style={{ width: "100%", height: "100%" }}
            />
          </Suspense>
        </div>
      ) : null}
    </section>
  );
}

function AssetMetricCards({ data, holdings, balances }) {
  const fx = Number(data.summary?.fx || 7.1);
  const rows = data.holdings.map((row) => {
    const draft = holdings[row.symbol] || {};
    const shares = Number(draft.shares ?? row.shares ?? 0);
    const avgCost = Number(draft.avg_cost ?? row.avg_cost ?? 0);
    const price = Number(row.price || 0);
    const value = shares * price;
    const cost = shares * avgCost;
    const isUsd = row.currency === "USD";
    return { ...row, value, cost, valueCny: isUsd ? value * fx : value, costCny: isUsd ? cost * fx : cost };
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

function EditableHoldingsPage({ data, onSaved }) {
  const [holdings, setHoldings] = useState({});
  const [balances, setBalances] = useState({});
  const [editing, setEditing] = useState(false);
  const [saving, setSaving] = useState(false);
  const [savingTargets, setSavingTargets] = useState(false);
  const [targetInputs, setTargetInputs] = useState({});
  const [editingTargets, setEditingTargets] = useState(false);
  const [message, setMessage] = useState("");

  function resetDraft() {
    setHoldings(Object.fromEntries(data.holdings.map((row) => [row.symbol, { shares: String(row.shares ?? 0), avg_cost: String(row.avg_cost ?? 0) }])));
    setBalances({
      cash_usd: String(data.balances?.cash_usd ?? 0),
      cash_cny: String(data.balances?.cash_cny ?? 0),
      realized_usd: String(data.balances?.realized_usd ?? 0),
      realized_cny: String(data.balances?.realized_cny ?? 0),
      sgov_dividend_usd: String(data.balances?.sgov_dividend_usd ?? 0),
    });
    setTargetInputs(Object.fromEntries(Object.entries(data.satellite_targets || {}).map(([symbol, value]) => [symbol, Number(value || 0).toFixed(2)])));
  }

  useEffect(() => {
    if (editing) return;
    resetDraft();
  }, [data, editing]);

  function updateHolding(symbol, key, value) {
    setHoldings((prev) => ({ ...prev, [symbol]: { ...prev[symbol], [key]: value } }));
  }

  function updateBalance(key, value) {
    setBalances((prev) => ({ ...prev, [key]: value }));
  }

  function updateTarget(symbol, value) {
    setTargetInputs((prev) => ({ ...prev, [symbol]: value }));
  }

  async function save() {
    setSaving(true);
    setMessage("");
    try {
      const cleanHoldings = Object.fromEntries(Object.entries(holdings).map(([symbol, item]) => [symbol, { shares: Number(item.shares || 0), avg_cost: Number(item.avg_cost || 0) }]));
      const cleanBalances = Object.fromEntries(Object.entries(balances).map(([key, value]) => [key, Number(value || 0)]));
      const holdingsResp = await fetch(`${API_BASE}/api/holdings`, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ holdings: cleanHoldings }) });
      if (!holdingsResp.ok) throw new Error(`holdings HTTP ${holdingsResp.status}`);
      const balancesResp = await fetch(`${API_BASE}/api/balances`, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ balances: cleanBalances }) });
      if (!balancesResp.ok) throw new Error(`balances HTTP ${balancesResp.status}`);
      await onSaved();
      setMessage("已保存");
      setEditing(false);
    } catch (err) {
      setMessage(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  }

  async function saveTargets() {
    setSavingTargets(true);
    setMessage("");
    try {
      const targets = Object.fromEntries(Object.entries(targetInputs).map(([symbol, value]) => [symbol, Number(value || 0)]));
      const response = await fetch(`${API_BASE}/api/satellite-targets`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ targets }),
      });
      if (!response.ok) throw new Error(`satellite targets HTTP ${response.status}`);
      await onSaved();
      setMessage("卫星目标比例已保存");
      setEditingTargets(false);
    } catch (err) {
      setMessage(err instanceof Error ? err.message : String(err));
    } finally {
      setSavingTargets(false);
    }
  }

  const targetTotal = Object.values(targetInputs).reduce((sum, value) => sum + Number(value || 0), 0);

  return (
    <section>
      <div className="sectionHeader">
        <h2>持仓</h2>
        <div className="actions inlineActions" style={{ display: "none" }}>
          {editing ? <button onClick={() => { resetDraft(); setEditing(false); }} disabled={saving}>取消编辑</button> : null}
          {editing ? (
            <button className="primary" onClick={save} disabled={saving}><Save size={16} /> 保存持仓</button>
          ) : (
            <button className="primary" onClick={() => setEditing(true)}>编辑持仓</button>
          )}
        </div>
      </div>
      {message ? <div className={message === "已保存" ? "saveMessage up" : "saveMessage down"}>{message}</div> : null}
      <AssetMetricCards data={data} holdings={holdings} balances={balances} />
      {editing ? (
        <div className="balanceEditGrid">
          <label>USD现金<input value={balances.cash_usd ?? ""} onChange={(event) => updateBalance("cash_usd", event.target.value)} inputMode="decimal" /></label>
          <label>CNY现金<input value={balances.cash_cny ?? ""} onChange={(event) => updateBalance("cash_cny", event.target.value)} inputMode="decimal" /></label>
          <label>USD已变现<input value={balances.realized_usd ?? ""} onChange={(event) => updateBalance("realized_usd", event.target.value)} inputMode="decimal" /></label>
          <label>CNY已变现<input value={balances.realized_cny ?? ""} onChange={(event) => updateBalance("realized_cny", event.target.value)} inputMode="decimal" /></label>
          <label>SGOV股息<input value={balances.sgov_dividend_usd ?? ""} onChange={(event) => updateBalance("sgov_dividend_usd", event.target.value)} inputMode="decimal" /></label>
        </div>
      ) : null}
      <div className="targetEditor">
        <div className="sectionHeader compactHeader">
          <h3>卫星仓位目标比例</h3>
          <div className="actions inlineActions">
            <span className="muted">合计 {targetTotal.toFixed(2)}%</span>
            {editingTargets ? <button onClick={() => { resetDraft(); setEditingTargets(false); }} disabled={savingTargets}>取消</button> : null}
            {editingTargets ? (
              <button className="primary" onClick={saveTargets} disabled={savingTargets}><Save size={16} /> 保存目标</button>
            ) : (
              <button className="primary" onClick={() => setEditingTargets(true)}>编辑</button>
            )}
          </div>
        </div>
        {editingTargets ? (
          <div className="targetEditGrid">
            {Object.entries(targetInputs).map(([symbol, value]) => (
              <label key={symbol}>{symbol}<input value={value} onChange={(event) => updateTarget(symbol, event.target.value)} inputMode="decimal" /></label>
            ))}
          </div>
        ) : null}
      </div>
      <div className="tableWrap">
        <table className="editableHoldingsTable">
          <thead>
            <tr>
              <th>标的</th><th>当前价</th><th>当日涨跌</th><th>60日回撤</th><th>60日涨幅</th><th>数量</th><th>成本</th><th>市值</th><th>盈亏</th><th>Forward PE/近5日</th><th>PE区间</th><th>PEG</th><th>PEG区间</th>
            </tr>
          </thead>
          <tbody>
            {data.holdings.map((row) => (
              <tr key={row.symbol}>
                <th>{row.label}</th>
                <td>{fmtMoney(row.price, row.currency, row.currency === "USD" ? 2 : 4)}</td>
                <td className={tone(row.effective_daily_pct)}>{fmtPct(row.effective_daily_pct)}</td>
                <td className={tone(row.drawdown_pct)}>{row.drawdown_pct == null ? "-" : fmtPct(row.drawdown_pct)}</td>
                <td className={tone(row.rebound_pct)}>{row.rebound_pct == null ? "-" : fmtPct(row.rebound_pct)}</td>
                <td>{editing ? <input className="tableInput" value={holdings[row.symbol]?.shares ?? ""} onChange={(event) => updateHolding(row.symbol, "shares", event.target.value)} inputMode="decimal" /> : Number(row.shares || 0).toLocaleString(undefined, { maximumFractionDigits: 4 })}</td>
                <td>{editing ? <input className="tableInput" value={holdings[row.symbol]?.avg_cost ?? ""} onChange={(event) => updateHolding(row.symbol, "avg_cost", event.target.value)} inputMode="decimal" /> : fmtMoney(row.avg_cost, row.currency, row.currency === "USD" ? 2 : 4)}</td>
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
  const currencyBySymbol = useMemo(
    () => Object.fromEntries((data.holdings || []).map((row) => [row.symbol, row.currency || "USD"])),
    [data.holdings],
  );
  const defaultTradeDate = formatShanghaiInputDate();
  const [editing, setEditing] = useState(false);
  const [budgetOpen, setBudgetOpen] = useState(false);
  const [rulesOpen, setRulesOpen] = useState(false);
  const [inputs, setInputs] = useState({});
  const [budgetInputs, setBudgetInputs] = useState({});
  const [balanceInputs, setBalanceInputs] = useState({});
  const [editingBalances, setEditingBalances] = useState(false);
  const [saving, setSaving] = useState(false);
  const [savingBudget, setSavingBudget] = useState(false);
  const [savingBalances, setSavingBalances] = useState(false);
  const [deletingTradeId, setDeletingTradeId] = useState("");
  const [balanceMessage, setBalanceMessage] = useState("");
  const [tradeMessage, setTradeMessage] = useState("");

  useEffect(() => {
    setBudgetInputs(Object.fromEntries(Object.entries(data.rebalance.future_cash_by_month || {}).map(([month, amount]) => [month, Number(amount || 0).toFixed(2)])));
    if (editing) return;
    const next = {};
    rows.forEach((row) => {
      next[row.symbol] = {
        action: "buy",
        trade_date: defaultTradeDate,
        amount_usd: Number(row.suggested_buy_usd || 0).toFixed(2),
        shares: "",
        intensity: row.intensity || "normal",
      };
    });
    setInputs(next);
  }, [data.rebalance.month_key, rows, editing, data.rebalance.future_cash_by_month, defaultTradeDate]);

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

  function update(symbol, key, value) {
    setInputs((prev) => ({ ...prev, [symbol]: { ...prev[symbol], [key]: value } }));
  }

  function updateBudget(month, value) {
    setBudgetInputs((prev) => ({ ...prev, [month]: value }));
  }

  function updateBalance(key, value) {
    setBalanceInputs((prev) => ({ ...prev, [key]: value }));
  }

  function clearPending() {
    setInputs((prev) => Object.fromEntries(Object.keys(prev).map((symbol) => [
      symbol,
      { ...prev[symbol], trade_date: prev[symbol]?.trade_date || defaultTradeDate, amount_usd: "0.00", shares: "0" },
    ])));
  }

  async function save() {
    setSaving(true);
    setTradeMessage("");
    try {
      const executions = Object.entries(inputs)
        .map(([symbol, item]) => ({
          symbol,
          action: item.action || "buy",
          trade_date: item.trade_date || defaultTradeDate,
          amount_usd: Number(item.amount_usd || 0),
          shares: Number(item.shares || 0),
          intensity: item.intensity,
        }))
        .filter((item) => item.amount_usd > 0 && item.shares > 0);
      const response = await fetch(`${API_BASE}/api/rebalance/confirm`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ user_id: data.user_id, executions }) });
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.detail || `HTTP ${response.status}`);
      }
      setEditing(false);
      await onSaved();
      setTradeMessage("交易已保存");
    } catch (err) {
      setTradeMessage(err instanceof Error ? err.message : String(err));
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
      setTradeMessage("交易已撤销");
    } catch (err) {
      setTradeMessage(err instanceof Error ? err.message : String(err));
    } finally {
      setDeletingTradeId("");
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

  return (
    <section>
      <div className="rebalanceActionRow">
        <button className="primary" onClick={() => setBudgetOpen(true)}>预算设置</button>
        <button className="primary" onClick={() => setEditingBalances(true)}>编辑现金</button>
        <button className="primary" onClick={() => setEditing(true)}>记录买卖</button>
      </div>
      <div className="sectionHeader subHeader">
        <h2>交易记录</h2>
        <span className="muted">买入或卖出后会按交易日期重算该日之后的收益曲线</span>
      </div>
      {tradeMessage ? <div className={["交易已保存", "交易已撤销"].includes(tradeMessage) ? "saveMessage up" : "saveMessage down"}>{tradeMessage}</div> : null}
      <div className="tableWrap">
        <table>
          <thead><tr><th>日期</th><th>标的</th><th>方向</th><th>股数</th><th>成交金额</th><th>成交成本</th><th>持仓成本变化</th><th>档位</th><th>操作</th></tr></thead>
          <tbody>
            {(data.trades || []).slice().reverse().slice(0, 20).map((trade, index) => (
              <tr key={`${trade.trade_date || trade.date}-${trade.symbol}-${index}`}>
                <td>{trade.trade_date || trade.date || "-"}</td>
                <td>{trade.symbol}</td>
                <td>{trade.action === "sell" ? "卖出" : "买入"}</td>
                <td>{Number(trade.shares || 0).toLocaleString(undefined, { maximumFractionDigits: 4 })}</td>
                <td>{fmtMoney(trade.amount_usd, currencyBySymbol[trade.symbol] || "USD")}</td>
                <td>{fmtMoney(trade.price, currencyBySymbol[trade.symbol] || "USD", (currencyBySymbol[trade.symbol] || "USD") === "USD" ? 2 : 4)}</td>
                <td>{fmtCostChange(trade, currencyBySymbol[trade.symbol] || "USD")}</td>
                <td><span className={`tierBadge ${tierClass(trade.intensity)}`}>{trade.intensity || "-"}</span></td>
                <td>
                  <button onClick={() => deleteTrade(trade)} disabled={deletingTradeId === trade.id}>
                    {deletingTradeId === trade.id ? "撤销中" : "撤销"}
                  </button>
                </td>
              </tr>
            ))}
            {!(data.trades || []).length ? (
              <tr><td colSpan={9} className="muted">暂无交易记录</td></tr>
            ) : null}
          </tbody>
        </table>
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
        <div className="modalBackdrop" role="presentation" onClick={() => setBudgetOpen(false)}>
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
        <div className="modalBackdrop" role="presentation" onClick={() => setRulesOpen(false)}>
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
      <div className="tableWrap">
        <table>
          <thead>
            <tr>
              <th>标的</th><th>目前占比</th><th>目标占比</th><th>60日回撤</th><th>计划应买</th><th>建议买入</th><th>净买入</th><th>差值</th><th>档位</th><th>估值/追高系数</th><th>说明</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.symbol}>
                <th>{row.symbol}</th>
                <td>{Number(row.current_pct || 0).toFixed(2)}%</td>
                <td>{Number(row.target_pct || 0).toFixed(2)}%</td>
                <td className={tone(row.drawdown_pct)}>{row.drawdown_pct == null ? "-" : fmtPct(row.drawdown_pct)}</td>
                <td className="planCell">
                  <div>{fmtMoney(row.planned_buy_usd, row.currency || "USD")}</div>
                  {row.planned_buy_formula ? <div className="cellSubtext">{row.planned_buy_formula}</div> : null}
                </td>
                <td className={tone(row.suggested_buy_usd)}>{fmtMoney(row.suggested_buy_usd, row.currency || "USD")}</td>
                <td>{fmtMoney(row.net_bought_usd, row.currency || "USD")}</td>
                <td className={tone(row.buy_difference_usd)}>{fmtMoney(row.buy_difference_usd, row.currency || "USD")}</td>
                <td><span className={`tierBadge ${tierClass(row.intensity)}`}>{row.signal || row.intensity}</span></td>
                <td className={Number(row.valuation_split_factor || 1) < 1 ? "down" : "flat"}>{Number(row.valuation_split_factor || 1).toFixed(2)}</td>
                <td className="note">{row.note}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {editing ? (
        <div className="modalBackdrop" role="presentation" onClick={() => setEditing(false)}>
          <div className="modalPanel tradeModal" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
            <div className="sectionHeader">
              <h2>买卖确认</h2>
              <button onClick={() => setEditing(false)} disabled={saving}>关闭</button>
            </div>
            <div className="tableWrap">
              <table>
                <thead><tr><th>标的</th><th>方向</th><th>日期</th><th>当前档位</th><th>成交金额</th><th>成交股数</th><th>建议买入</th></tr></thead>
                <tbody>
                  {rows.map((row) => (
                    <tr key={row.symbol}>
                      <th>{row.symbol}</th>
                      <td>
                        <select value={inputs[row.symbol]?.action || "buy"} onChange={(event) => update(row.symbol, "action", event.target.value)}>
                          <option value="buy">买入</option>
                          <option value="sell">卖出</option>
                        </select>
                      </td>
                      <td><input type="date" value={inputs[row.symbol]?.trade_date || defaultTradeDate} onChange={(event) => update(row.symbol, "trade_date", event.target.value)} /></td>
                      <td>
                        <select className={tierClass(inputs[row.symbol]?.intensity || row.intensity)} value={inputs[row.symbol]?.intensity || row.intensity} onChange={(event) => update(row.symbol, "intensity", event.target.value)}>
                          <option value="normal">普通</option>
                          <option value="probe">QQQ -2%分批</option>
                          <option value="month_end">QQQ月底补齐</option>
                          <option value="small">小加</option>
                          <option value="medium">中加</option>
                          <option value="large">大加</option>
                        </select>
                      </td>
                      <td><input value={inputs[row.symbol]?.amount_usd ?? ""} onChange={(event) => update(row.symbol, "amount_usd", event.target.value)} inputMode="decimal" /></td>
                      <td><input value={inputs[row.symbol]?.shares ?? ""} onChange={(event) => update(row.symbol, "shares", event.target.value)} inputMode="decimal" /></td>
                      <td>{fmtMoney(row.suggested_buy_usd, row.currency || "USD")}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="actions">
              <button onClick={clearPending}>清零待确认交易</button>
              <button className="primary" onClick={save} disabled={saving}><Save size={16} /> 确认买卖并同步持仓</button>
            </div>
          </div>
        </div>
      ) : null}
      {balanceMessage ? <div className={balanceMessage === "现金与已变现已保存" ? "saveMessage up" : "saveMessage down"}>{balanceMessage}</div> : null}
      {editingBalances ? (
        <div className="modalBackdrop" role="presentation" onClick={() => { resetBalanceDraft(); setEditingBalances(false); }}>
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
      <DailyCards cards={data.daily_cards} />
      <Visualizations data={data} />
    </>
  );
}

function HoldingsPage({ data, onSaved }) {
  return <EditableHoldingsPage data={data} onSaved={onSaved} />;
}

function RebalancePage({ data, onSaved }) {
  return (
    <>
      <Summary data={data} />
      <Rebalance data={data} onSaved={onSaved} />
    </>
  );
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
      {page === "holdings" ? <HoldingsPage data={data} onSaved={load} /> : null}
      {page === "rebalance" ? <RebalancePage data={data} onSaved={load} /> : null}
      {page === "kline" ? <KlinePage /> : null}
    </main>
  );
}
