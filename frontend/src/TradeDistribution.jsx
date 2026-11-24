import React, { useMemo, useState } from "react";

const ticks = [0, 0.25, 0.5, 0.75, 1];
const axisNumber = new Intl.NumberFormat(undefined, { maximumFractionDigits: 4 });
const tickNumber = new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 });
const sideLabel = { buy: "买入", sell: "卖出" };
const dayMs = 86_400_000;
const minShares = 1e-8;
const chartLabels = {
  bars: { name: "买卖分离柱状图", xLabel: "单笔成交股数（股）", hint: "点击横条查看单笔成交" },
  steps: { name: "净股数与均价阶梯图", xLabel: "交易日期", hint: "点击蓝色节点查看当天成交及成本" },
};

function quantityAxis(values) {
  const extent = Math.max(...values.map(Math.abs)) * 1.1 || 1;
  const min = values.some((value) => value < 0) ? -extent : 0;
  const max = values.some((value) => value > 0) || min === 0 ? extent : 0;
  return {
    position: (value) => (value - min) / (max - min),
    valueAt: (fraction) => min + (max - min) * fraction,
    zero: -min / (max - min),
  };
}

function tradeBars(fills) {
  const bars = fills.filter((fill) => fill.price > 0).sort((a, b) => b.price - a.price);
  if (!bars.length) return null;
  const signedShares = (fill) => fill.action === "buy" ? fill.shares : -fill.shares;
  const axis = quantityAxis(bars.map(signedShares));
  return {
    axis,
    groups: bars.map((fill, index) => ({
      key: fill.id, side: fill.action, price: fill.price, shares: fill.shares, fills: [fill],
      y: 1 - (index + 0.5) / bars.length,
      x: axis.position(signedShares(fill)),
    })),
  };
}

function estimateImportedCosts(records, openingShares) {
  const firstPosted = records.findIndex((fill) => !fill.history_only);
  if (firstPosted <= 0) return { records, openingCost: null };
  const priorAverage = Number(records[firstPosted].prev_avg_cost);
  if (!Number.isFinite(priorAverage) || priorAverage <= 0) return { records, openingCost: null };

  const estimated = [...records];
  const imported = records.slice(0, firstPosted);
  let shares = imported.reduce((total, fill) => total + (fill.action === "buy" ? fill.shares : -fill.shares), openingShares);
  let basis = shares * priorAverage;
  // Walk backward from the first known pre-trade basis. A sale keeps the per-share basis.
  for (let index = firstPosted - 1; index >= 0; index--) {
    if (shares <= minShares || basis <= 0) return { records: estimated, openingCost: null };
    const fill = records[index];
    const average = basis / shares;
    estimated[index] = { ...fill, estimatedAvgCost: average };
    if (fill.action === "buy") {
      basis -= Number(fill.amount_usd);
      shares -= fill.shares;
    } else {
      basis += average * fill.shares;
      shares += fill.shares;
    }
  }
  return { records: estimated, openingCost: shares > minShares && basis > 0 ? basis / shares : null };
}

function tradeTimeline(fills) {
  const byDay = new Map();
  for (const fill of fills) {
    const date = fill.trade_date;
    const day = Date.parse(`${date}T00:00:00Z`) / dayMs;
    if (!Number.isFinite(day)) continue;
    if (!byDay.has(day)) byDay.set(day, { key: date, date, day, fills: [], net: 0 });
    const entry = byDay.get(day);
    entry.fills.push(fill);
    entry.net += fill.action === "buy" ? fill.shares : -fill.shares;
  }
  const opening = fills.find((fill) => fill.opening);
  if (!byDay.size && !opening) return null;
  if (opening) {
    const day = byDay.size ? Math.min(...byDay.keys()) - 1 : 0;
    byDay.set(day, { key: opening.id, date: "期初", day, fills: [opening], net: opening.shares });
  }

  let cumulative = 0;
  const days = [...byDay.values()].sort((a, b) => a.day - b.day).map((entry) => {
    cumulative += entry.net;
    const lastFill = entry.fills.at(-1);
    const cost = Number(lastFill.estimatedAvgCost ?? lastFill.new_avg_cost);
    return {
      ...entry, cumulative,
      cost: Number.isFinite(cost) && cost > 0 ? cost : null,
      costEstimated: lastFill.estimatedAvgCost != null || lastFill.opening === true,
    };
  });
  const shareAxis = quantityAxis(days.map((entry) => entry.cumulative));
  const costs = days.flatMap((entry) => entry.cost == null ? [] : [entry.cost]);
  const min = costs.length ? Math.min(...costs) : 0;
  const max = costs.length ? Math.max(...costs) : 0;
  const costPadding = Math.max((max - min) * 0.1, min * 0.002, 0.0001);
  const costMin = Math.max(0, min - costPadding);
  const costSpan = max + costPadding - costMin;
  const first = days[0].day;
  const last = days.at(-1).day;
  const dayPadding = Math.max(1, Math.ceil((last - first) * 0.04));
  const xAt = (day) => (day - first + dayPadding) / (last - first + dayPadding * 2);
  return {
    shareAxis,
    costAt: costs.length ? (tick) => costMin + costSpan * tick : null,
    dates: [...new Set(ticks.map((tick) => Math.round(first + (last - first) * tick)))].map((day) => ({
      label: opening && day === first ? "期初" : new Date(day * dayMs).toISOString().slice(0, 10), x: xAt(day),
    })),
    groups: days.map((entry) => ({
      ...entry,
      x: xAt(entry.day),
      y: shareAxis.position(entry.cumulative),
      costY: entry.cost == null ? null : (entry.cost - costMin) / costSpan,
    })),
  };
}

function StepLine({ groups, value, initialValue, color }) {
  const points = [...groups, { ...groups.at(-1), x: 1 }];
  return <div aria-hidden="true" className={`tradeDistStepLine ${color}`}>
    {points.map((point, index) => {
      const previous = points[index - 1] || { x: 0, [value]: initialValue };
      const start = previous[value];
      const end = point[value];
      const estimatedStart = value === "costY" && previous.costEstimated;
      const estimatedEnd = value === "costY" && point.costEstimated;
      return <React.Fragment key={point.x}>
        {start != null && <span className={estimatedStart ? "horizontal estimated" : "horizontal"} style={{ left: `${previous.x * 100}%`, bottom: `${start * 100}%`, width: `${(point.x - previous.x) * 100}%` }} />}
        {start != null && end != null && <span className={estimatedStart || estimatedEnd ? "vertical estimated" : "vertical"} style={{ left: `${point.x * 100}%`, bottom: `${Math.min(start, end) * 100}%`, height: `${Math.abs(end - start) * 100}%` }} />}
      </React.Fragment>;
    })}
  </div>;
}

function markLabel(group, mode, priceLabel) {
  if (mode === "bars") {
    if (group.fills[0].opening) {
      return `期初持仓 · ${priceLabel.format(group.price)} · ${axisNumber.format(group.shares)} 股（估算）`;
    }
    return `${sideLabel[group.side]} ${priceLabel.format(group.price)} · ${axisNumber.format(group.shares)} 股 · ${group.fills[0].trade_date}`;
  }
  const cost = group.cost == null ? "—" : `${priceLabel.format(group.cost)}${group.costEstimated ? "（估算）" : ""}`;
  return `${group.date} · 累计净股数 ${axisNumber.format(group.cumulative)} 股 · 均价 ${cost} · ${group.fills.length} 笔`;
}

function markPosition(group, mode, axis) {
  const bottom = `${group.y * 100}%`;
  if (mode === "bars") {
    return {
      bottom,
      ...(group.side === "buy" ? { left: `${axis.zero * 100}%` } : { right: `${(1 - axis.zero) * 100}%` }),
      width: `${Math.abs(group.x - axis.zero) * 100}%`,
    };
  }
  return { bottom, left: `${group.x * 100}%` };
}

function DistributionPlot({ mode, plot, priceLabel, selectedKey, onSelect }) {
  const chart = (
    <div className={`tradeDistPlot ${mode}`} role="group" aria-label={chartLabels[mode].name}
      style={mode === "bars" ? { "--trade-dist-bar-height": `${Math.max(100, plot.groups.length * 19)}px` } : undefined}>
      {ticks.map((tick) => mode === "bars"
        ? <span key={tick} className={`tradeDistVerticalGrid${tick === plot.axis.zero ? " zero" : ""}`} style={{ left: `${tick * 100}%` }} />
        : <span key={tick} className={`tradeDistGrid${tick === plot.shareAxis.zero ? " zero" : ""}`} style={{ bottom: `${tick * 100}%` }}>
          <span>{axisNumber.format(plot.shareAxis.valueAt(tick))}</span>
          {plot.costAt && <span className="costTick">{tickNumber.format(plot.costAt(tick))}</span>}
        </span>)}
      {mode === "steps" && <>
        <StepLine groups={plot.groups} value="y" initialValue={plot.shareAxis.zero} color="net" />
        {plot.costAt && <StepLine groups={plot.groups} value="costY" color="cost" />}
      </>}
      {plot.groups.map((group) => {
        const key = group.key;
        const label = markLabel(group, mode, priceLabel);
        return <React.Fragment key={key}>
          {mode === "bars" && <span className="tradeDistBarPrice" style={{ bottom: `${group.y * 100}%` }}>
            {axisNumber.format(group.price)}
          </span>}
          {mode === "steps" && group.costY != null && <span className="tradeDistCostPoint" aria-hidden="true"
            style={{ left: `${group.x * 100}%`, bottom: `${group.costY * 100}%` }} />}
          <button
            type="button"
            className={`tradeDistMark ${mode}${group.side ? ` ${group.side}` : ""}`}
            style={markPosition(group, mode, plot.axis)}
            title={label}
            aria-label={label}
            aria-pressed={key === selectedKey}
            onClick={() => onSelect(key)}
          />
        </React.Fragment>;
      })}
    </div>
  );
  return <>
    {chart}
    <div className={`tradeDistScale ${mode}`}>
      {mode === "bars"
        ? ticks.map((tick) => <span key={tick} className="tradeDistQuantityTick" style={{ left: `${tick * 100}%` }}>
          {axisNumber.format(plot.axis.valueAt(tick))}
        </span>)
        : plot.dates.map((day) => <span key={day.label} className="tradeDistDateTick" style={{ left: `${day.x * 100}%` }}>
          {day.label}
        </span>)}
    </div>
  </>;
}

function DistributionChart({ mode, plot, priceLabel, currency, hasOpening }) {
  const [selectedKey, setSelectedKey] = useState("");
  const selected = plot?.groups.find((group) => group.key === selectedKey);
  const labels = chartLabels[mode];
  return (
    <section className="tradeDistChart" aria-label={labels.name}>
      <h3>{labels.name}</h3>
      {plot ? <>
        <div className="tradeDistLegend">
          {mode === "steps" ? <>
            <span className="tradeDistNet">左轴 · 累计净股数（股）</span>
            {plot.costAt && <span className="tradeDistCost">
              右轴 · 持仓均价（{currency}/股）{plot.groups.some((entry) => entry.cost != null && entry.costEstimated) ? " · 虚线为估算" : ""}
            </span>}
          </> : <>
            <span>成交价从高到低（{currency}/股）</span>
            <span className="tradeDistBuy">买入</span><span className="tradeDistSell">卖出</span>
          </>}
        </div>
        <DistributionPlot mode={mode} plot={plot} priceLabel={priceLabel}
          selectedKey={selectedKey} onSelect={setSelectedKey} />
        <div className="tradeDistXAxis">{labels.xLabel}</div>
        <div className="tradeDistDetails">
          {selected
            ? <><p>{markLabel(selected, mode, priceLabel)}</p><ul aria-label="选中成交明细">{selected.fills.map((fill) => <li key={fill.id}>
              {fill.opening ? <span>期初持仓（估算）</span> : <>
                <time>{fill.trade_date}</time>
                <span className={fill.action === "buy" ? "tradeDistBuy" : "tradeDistSell"}>{sideLabel[fill.action]}</span>
              </>}
              <strong>{fill.price == null ? "成本未知" : priceLabel.format(fill.price)}</strong>
              <span>{axisNumber.format(fill.shares)} 股</span>
            </li>)}</ul></>
            : <span className="muted">{labels.hint}</span>}
        </div>
      </> : <p className="muted">{mode === "bars" && hasOpening ? "期初持仓缺少成本" : "暂无买卖成交记录"}</p>}
    </section>
  );
}

export default function TradeDistribution({ trades, holdings, symbol }) {
  const holding = holdings?.find((row) => row.symbol === symbol);
  const fills = useMemo(() => {
    const records = [];
    for (const trade of trades || []) {
      if (trade.symbol !== symbol || !["buy", "sell"].includes(trade.action)) continue;
      const price = Number(trade.price);
      const shares = Number(trade.shares);
      if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(shares) || shares <= 0) continue;
      records.push({ ...trade, price, shares });
    }
    const net = records.reduce((total, fill) => total + (fill.action === "buy" ? fill.shares : -fill.shares), 0);
    const shares = Math.max(0, Number(holding?.shares || 0) - net);
    const estimated = estimateImportedCosts(records, shares);
    if (shares <= minShares) return estimated.records;
    const earlyRecord = records.find((fill) => Number(fill.prev_avg_cost) > 0);
    const average = records[0]?.history_only
      ? estimated.openingCost
      : Number(records.length ? earlyRecord?.prev_avg_cost : holding?.avg_cost);
    const cost = Number.isFinite(average) && average > 0 ? average : null;
    return [{
      id: `opening-${symbol}`,
      action: "buy",
      opening: true,
      shares,
      price: cost,
      new_avg_cost: cost,
    }, ...estimated.records];
  }, [trades, holding, symbol]);
  const hasOpening = fills.some((fill) => fill.opening);
  const currency = holding?.currency || "USD";
  const priceLabel = new Intl.NumberFormat(undefined, { style: "currency", currency, maximumFractionDigits: 4 });
  const bars = useMemo(() => tradeBars(fills), [fills]);
  const timeline = useMemo(() => tradeTimeline(fills), [fills]);

  return (
    <section className="tradeDistribution" aria-label="个人成交分布">
      <header className="tradeDistHeader">
        <h2>个人成交分布 · {symbol}</h2>
        {fills.length > 0 && <span className="muted">{fills.length - Number(hasOpening)} 笔成交{hasOpening ? " · 含一笔期初持仓估算" : ""}</span>}
      </header>
      <div className="tradeDistCharts">
        <DistributionChart mode="bars" plot={bars} priceLabel={priceLabel} currency={currency} hasOpening={hasOpening} />
        <DistributionChart mode="steps" plot={timeline} priceLabel={priceLabel} currency={currency} />
      </div>
    </section>
  );
}
