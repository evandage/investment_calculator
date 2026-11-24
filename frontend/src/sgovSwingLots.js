// Dashboard trades arrive in ascending execution order.
export function estimateSgovSwingLots(trades, symbol, usedUsd, holdingShares) {
  const lots = [];
  let unallocatedUsd = Math.max(0, Number(usedUsd) || 0);
  let unallocatedShares = Math.max(0, Number(holdingShares) || 0);
  let soldShares = 0;
  let shares = 0;
  let cost = 0;

  for (let index = trades.length - 1; index >= 0 && unallocatedUsd > 1e-8 && unallocatedShares > 1e-8; index--) {
    const trade = trades[index];
    if (trade.symbol !== symbol || trade.history_only) continue;

    const tradeShares = Number(trade.shares);
    if (!Number.isFinite(tradeShares) || tradeShares <= 0) continue;
    if (trade.action === "sell") {
      soldShares += tradeShares;
      continue;
    }
    if (trade.action !== "buy") continue;

    // Infer the remaining buys by applying each sale to the latest preceding buys.
    const soldFromBuy = Math.min(soldShares, tradeShares);
    soldShares -= soldFromBuy;
    const openShares = tradeShares - soldFromBuy;
    const tradeCost = Number(trade.amount_usd);
    if (openShares <= 0 || !Number.isFinite(tradeCost) || tradeCost <= 0) continue;

    const fraction = Math.min(openShares / tradeShares, unallocatedShares / tradeShares, unallocatedUsd / tradeCost);
    const lotShares = tradeShares * fraction;
    const lotCost = tradeCost * fraction;
    lots.push({ trade, shares: lotShares, cost: lotCost, partial: fraction < 1 - 1e-8 });
    shares += lotShares;
    cost += lotCost;
    unallocatedShares -= lotShares;
    unallocatedUsd -= lotCost;
  }

  return { lots, shares, cost, unallocatedUsd };
}
