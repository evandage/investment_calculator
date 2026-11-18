from datetime import datetime

import pytest

from backend import portfolio


class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 10, 29, 12, 0, tzinfo=tz)


def test_valuation_uses_historical_metrics_and_ignores_peg(monkeypatch):
    monkeypatch.setattr(portfolio, "SATELLITE_SYMBOLS", ("APP", "TEM"))
    assert portfolio.valuation_split_for_row("APP", {"pe": 105, "forward_pe": 10, "peg": 0.1})[0] == 0.5
    assert portfolio.valuation_split_for_row("APP", {"pe": 60, "forward_pe": 200, "peg": 100})[0] == 1.0
    assert portfolio.valuation_split_for_row("APP", {"pe": None, "forward_pe": 200})[0] == 1.0
    assert portfolio.valuation_split_for_row("TEM", {"ps": 14, "forward_ps": 5})[0] == 0.5
    assert portfolio.valuation_split_for_row("TEM", {"ps": 10, "forward_ps": 50, "peg": 100})[0] == 1.0


def trade(symbol, trade_date, amount, action="buy"):
    return {"symbol": symbol, "trade_date": trade_date, "amount_usd": amount, "action": action, "cost_basis": amount}


def test_rebalance_reports_current_and_next_month_targets_without_buy_suggestions(monkeypatch):
    monkeypatch.setattr(portfolio, "datetime", FixedDatetime)
    budget = {"2026-11": 900.0, "2026-12": 400.0}
    monkeypatch.setattr(portfolio, "load_monthly_usage", lambda *_: {
        "planned_new_cash_usd": 700.0, "planned_cash_by_month": budget,
        "bought_amount_by_symbol": {"VOO": 5000.0, "QQQ": 5000.0},
        "bought_intensity_by_symbol": {"VOO": "large", "QQQ": "large"},
    })
    monkeypatch.setattr(portfolio, "load_trade_records", lambda *_: [
        trade("VOO", "2026-10-10", 50.0),
        trade("QQQ", "2026-10-11", 30.0),
        trade("ISRG", "2026-10-10", 100.0),
        trade("ISRG", "2026-10-20", 20.0),
        trade("ISRG", "2026-10-21", 5.0, "sell"),
    ])
    monkeypatch.setattr(portfolio, "evaluate_drawdown_episode_signals", lambda *_args, **_kwargs: {"QQQ": {"confirmed_signal": {"tier": "medium"}}})
    monkeypatch.setattr(portfolio, "load_drawdown_episode_store", lambda *_: {})
    holdings = [
        {"symbol": "VOO", "label": "VOO", "value_cny": 1100.0, "shares": 10.5, "avg_cost": 100.0, "recent_5d_pct": 4.2},
        {"symbol": "QQQ", "label": "QQQ", "value_cny": 1080.0, "shares": 10.3, "avg_cost": 100.0},
        {"symbol": "ISRG", "label": "ISRG", "value_cny": 1090.0, "shares": 10.15, "avg_cost": 100.0},
        {"symbol": "SGOV", "label": "SGOV", "value_cny": 600.0, "shares": 5.0, "avg_cost": 100.0},
    ]
    market = {"quotes": {symbol: {"price": 100.0} for symbol in ("VOO", "QQQ", "ISRG", "SGOV")}}
    holdings_value = {row["symbol"]: row["value_cny"] for row in holdings}
    holdings_value["CNY_FUND"] = 2000.0

    def rebalance(cash):
        return portfolio.build_rebalance_v2(
            "evan", holdings, {"cash_usd": cash, "cash_cost_basis_usd": 800.0, "cash_cny": 3000.0},
            market, holdings_value, 1.0,
        )

    result = rebalance(1000.0)
    assert result["current_assets_usd"] == pytest.approx(1100.0 + 1080.0 + 1090.0 + 600.0 + 1000.0)
    assert result["next_month_cash_usd"] == 900.0
    assert result["future_cash_by_month"] == {"2026-11": 900.0}
    rows = {row["symbol"]: row for row in result["rows"]}
    weights = portfolio.effective_target_weights()
    total = sum(weights[s] for s in portfolio.USD_SYMBOLS)
    for symbol in ("VOO", "QQQ", "ISRG"):
        row = rows[symbol]
        expected_denominator = result["current_assets_usd"]
        expected_target = expected_denominator * weights[symbol] / total
        expected_gap = expected_target - row["current_cost_usd"]
        if abs(expected_gap) <= max(5.0, expected_target * 0.01):
            expected_gap = 0.0
        assert row["target_usd"] == pytest.approx(expected_target)
        assert row["next_month_target_usd"] == pytest.approx((expected_denominator + 900.0) * weights[symbol] / total)
        assert row["target_denominator_usd"] == pytest.approx(expected_denominator)
        assert row["buy_difference_usd"] == pytest.approx(expected_gap)
        assert row["suggested_sell_usd"] == pytest.approx(max(0.0, -expected_gap))
        assert row["suggested_sell_shares"] == pytest.approx(row["suggested_sell_usd"] / 100.0)
        assert "suggested_buy_usd" not in row
        assert "suggested_buy_shares" not in row
        assert "planned_buy_usd" not in row
        assert "budget_month" not in row
        assert "period_net_bought_usd" not in row
    assert rows["QQQ"]["intensity"] == "medium"
    assert rows["VOO"]["valuation_split_factor"] == 0.5
    assert "strategy_budget_usd" not in result
    assert "suggestion_scale" not in result
    budget["2026-11"] = 0.0
    zero_budget = rebalance(1000.0)
    assert zero_budget["sgov_available_usd"] == result["sgov_available_usd"]
    for row in zero_budget["rows"]:
        assert row["next_month_target_usd"] == row["target_usd"]
        assert row["suggested_sell_usd"] == rows[row["symbol"]]["suggested_sell_usd"]

    limited = rebalance(100.0)
    assert limited["remaining_deployable_usd"] == pytest.approx(100.0)


def test_trade_quarters_follow_stock_cycles_and_fall_back_to_calendar():
    records = [trade("ISRG", "2026-07-01", 10), trade("ISRG", "2026-08-01", 10),
               trade("ISRG", "2026-10-20", 10), trade("VOO", "2026-10-20", 10)]
    quarters = {"ISRG": {"start_date": "2026-10-10", "quarter_label": "Y26Q3",
                         "history": [{"start_date": "2026-07-15", "end_date": "2026-10-10", "quarter_label": "Y26Q2"}]}}
    result = portfolio.annotate_trade_quarters(records, quarters)
    assert [(item["quarter_label"], item["quarter_source"]) for item in result] == [
        ("Y26Q3", "calendar"), ("Y26Q2", "cycle"), ("Y26Q3", "cycle"), ("Y26Q4", "calendar")]

def test_sgov_plan_uses_current_thresholds_gaps_and_recorded_sales():
    rows = [
        {"symbol": "VOO", "buy_difference_usd": 600},
        {"symbol": "QQQ", "buy_difference_usd": 300, "drawdown_anchor_price": 800},
    ]
    signal = {"threshold_snapshot": {"thresholds_pct": {"small": -10, "medium": -15, "large": -20}}}
    trades = [trade("SGOV", "2026-10-28", 200, "sell"), trade("SGOV", "2026-10-29", 50, "buy")]
    plan = portfolio.build_sgov_usage_plan(rows, 1200, {"QQQ": signal}, trades, "2026-10-29")
    assert plan["budget_usd"] == 1200
    assert plan["rows"][0]["signals"]["QQQ"]["trigger_price"] == pytest.approx(720)
    assert plan["rows"][0]["signals"]["VOO"]["trigger_price"] is None
    assert sum(row["voo_usd"] for row in plan["rows"]) == pytest.approx(1200 * 4 / 7)
    assert sum(row["qqq_usd"] for row in plan["rows"]) == pytest.approx(1200 * 3 / 7)
    assert plan["latest_sale"]["amount_usd"] == 200
    rows[0]["buy_difference_usd"] = 0
    rows[1]["buy_difference_usd"] = -5
    empty = portfolio.build_sgov_usage_plan(rows, 1200, {"QQQ": signal}, [], "2026-10-29")
    assert empty["budget_usd"] == 1200
    assert empty["latest_sale"] is None
    assert sum(row["total_usd"] for row in empty["rows"]) == pytest.approx(1200)

def test_sgov_trade_marker_survives_storage_and_updates_existing_trade(monkeypatch):
    from backend import main, storage
    record = {"id": "one", "symbol": "QQQ", "action": "buy", "trade_date": "2026-10-28", "amount_usd": 200, "shares": 1, "uses_sgov": True}
    normalized = storage.load_trade_records_from_rows([record])
    assert normalized[0]["uses_sgov"] is True
    assert storage.load_trade_records_from_rows([{**record, "action": "sell"}])[0]["uses_sgov"] is False
    saved = []
    monkeypatch.setattr(main, "active_user_id", lambda: "evan")
    monkeypatch.setattr(main, "load_trade_records", lambda _: normalized)
    monkeypatch.setattr(storage, "save_trade_records", lambda user, rows: saved.extend(rows))
    assert main.set_trade_sgov("one", main.TradeSgovPayload(uses_sgov=False))["saved"]
    assert saved[0]["uses_sgov"] is False


def test_sgov_plan_reports_marked_purchases_without_claiming_funding_amount():
    trades = [{**trade("QQQ", "2026-10-28", 200), "uses_sgov": True}, trade("VOO", "2026-10-28", 50)]
    result = portfolio.build_sgov_usage_plan([], 0, {}, trades, "2026-10-29")
    assert result["marked_count"] == 1
    assert result["marked_purchase_usd"] == 200


def test_sgov_progress_allocates_marked_buys_and_uses_confirmed_episode():
    rows = [{"symbol": "VOO", "drawdown_anchor_price": 200}, {"symbol": "QQQ", "drawdown_anchor_price": 100}]
    signals = {
        "VOO": {
            "threshold_snapshot": {"thresholds_pct": {"small": -4, "medium": -8, "large": -16}},
            "episode_state": {"episode_active": True, "small_triggered": True},
        },
        "QQQ": {
            "threshold_snapshot": {"thresholds_pct": {"small": -5, "medium": -10, "large": -20}},
            "episode_state": {"episode_active": True, "small_triggered": True, "medium_triggered": True},
        },
    }
    trades = [{**trade("VOO", "2026-10-28", 40), "uses_sgov": True, "sgov_tier": "small"}, {**trade("QQQ", "2026-10-28", 30), "uses_sgov": True}]
    result = portfolio.build_sgov_usage_plan(rows, 700, signals, trades, "2026-10-29")
    small, medium, large = [row["signals"] for row in result["rows"]]
    assert small["VOO"]["triggered"] and small["QQQ"]["triggered"]
    assert medium["VOO"] == {"drawdown_pct": -8, "trigger_price": pytest.approx(184), "triggered": False, "eligible_usd": 0}
    assert medium["QQQ"] == {"drawdown_pct": -10, "trigger_price": pytest.approx(90), "triggered": True, "eligible_usd": pytest.approx(result["rows"][1]["qqq_usd"])}
    assert not large["VOO"]["triggered"] and not large["QQQ"]["triggered"]
    assert result["rows"][0]["voo_used_usd"] == 40
    assert result["rows"][0]["qqq_used_usd"] == 30
    assert sum(row["voo_usd"] for row in result["rows"]) / sum(row["qqq_usd"] for row in result["rows"]) == pytest.approx(4 / 3)


def test_sgov_repurchase_reduces_outstanding_proportionally_in_trade_order():
    signal = {"threshold_snapshot": {"thresholds_pct": {"small": -5, "medium": -10}}}
    trades = [
        {**trade("VOO", "2026-10-01", 400), "uses_sgov": True, "sgov_tier": "small"},
        {**trade("QQQ", "2026-10-01", 300), "uses_sgov": True, "sgov_tier": "medium"},
        trade("SGOV", "2026-10-02", 350),
        {**trade("VOO", "2026-10-03", 100), "uses_sgov": True, "sgov_tier": "small"},
    ]
    plan = portfolio.build_sgov_usage_plan([], 500, {"QQQ": signal}, trades, "2026-10-04")
    assert plan["available_usd"] == 500
    assert sum(row["voo_used_usd"] for row in plan["rows"]) == pytest.approx(300)
    assert sum(row["qqq_used_usd"] for row in plan["rows"]) == pytest.approx(150)
    assert plan["budget_usd"] == pytest.approx(950)
    trades.append(trade("SGOV", "2026-10-04", 1000))
    assert all(row["voo_used_usd"] == row["qqq_used_usd"] == 0 for row in portfolio.build_sgov_usage_plan([], 500, {"QQQ": signal}, trades, "2026-10-04")["rows"])


def test_sgov_budget_uses_requested_tier_weights():
    signal = {"threshold_snapshot": {"thresholds_pct": {"small": -5.5, "medium": -13.5, "large": -27}}}
    plan = portfolio.build_sgov_usage_plan([], 1000, {"QQQ": signal}, [], "2026-11-06")
    assert [row["total_usd"] for row in plan["rows"]] == pytest.approx([200, 350, 450])
    for row in plan["rows"]:
        assert row["voo_usd"] / row["qqq_usd"] == pytest.approx(4 / 3)


def test_trade_tier_uses_execution_price_and_known_snapshot():
    store = {"episodes": {"QQQ": {"anchor_date": "2026-10-01", "anchor_price": 100, "threshold_snapshot_id": "snapshot"}}, "threshold_snapshots": {"snapshot": {"thresholds_pct": {"small": -5, "medium": -10, "large": -20}}}}
    assert portfolio.trade_intensity_from_price("QQQ", 89, "2026-10-10", store, "2026-10-29") == "medium"
    assert portfolio.trade_intensity_from_price("QQQ", 98, "2026-10-10", store, "2026-10-29") == "normal"
    assert portfolio.trade_intensity_from_price("QQQ", 89, "2026-09-10", store, "2026-10-29") == "none"
    assert portfolio.trade_intensity_from_price("QQQ", 89, "2026-11-10", store, "2026-10-29") == "none"


def test_sgov_overflow_moves_to_next_tier():
    signal = {"threshold_snapshot": {"thresholds_pct": {"small": -5, "medium": -10, "large": -20}}}
    trades = [{**trade("VOO", "2026-10-10", 400), "uses_sgov": True}]
    plan = portfolio.build_sgov_usage_plan([], 600, {"QQQ": signal}, trades, "2026-10-29")
    assert [row["voo_used_usd"] for row in plan["rows"]] == pytest.approx([1000 * .2 * 4 / 7, 1000 * .35 * 4 / 7, 400 - 1000 * .55 * 4 / 7])


def test_sgov_sale_reduces_largest_tier_first_and_never_below_zero():
    signal = {"threshold_snapshot": {"thresholds_pct": {"small": -5, "medium": -10, "large": -20}}}
    base = [{**trade("VOO", "2026-10-01", 1200), "uses_sgov": True}, {**trade("QQQ", "2026-10-01", 900), "uses_sgov": True}]
    before = portfolio.build_sgov_usage_plan([], 1000, {"QQQ": signal}, base, "2026-10-04")
    sale = trade("SGOV", "2026-10-02", 100, "sell")
    after = portfolio.build_sgov_usage_plan([], 1100, {"QQQ": signal}, base + [sale], "2026-10-04")
    assert after["budget_usd"] == pytest.approx(before["budget_usd"])
    for index in (0, 1):
        assert after["rows"][index]["voo_used_usd"] == pytest.approx(before["rows"][index]["voo_used_usd"])
        assert after["rows"][index]["qqq_used_usd"] == pytest.approx(before["rows"][index]["qqq_used_usd"])
    assert sum(before["rows"][2][key] - after["rows"][2][key] for key in ("voo_used_usd", "qqq_used_usd")) == pytest.approx(100)
    huge = trade("SGOV", "2026-10-03", 10000, "sell")
    zero = portfolio.build_sgov_usage_plan([], 1000, {"QQQ": signal}, base + [huge], "2026-10-04")
    assert all(row["voo_used_usd"] == row["qqq_used_usd"] == 0 for row in zero["rows"])
