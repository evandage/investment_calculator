from datetime import datetime

import pytest

from backend import portfolio


class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 10, 29, 12, 0, tzinfo=tz)


def test_stock_quarter_state_uses_current_satellite_symbols(monkeypatch):
    monkeypatch.setattr(portfolio, "SATELLITE_SYMBOLS", ("APP",))
    monkeypatch.setattr(portfolio, "USD_SYMBOLS", ("APP",))
    monkeypatch.setattr(portfolio, "load_stock_quarters", lambda *_: {})
    monkeypatch.setattr(portfolio, "load_monthly_usage", lambda *_: {"planned_new_cash_usd": 100.0})
    monkeypatch.setattr(portfolio, "effective_target_weights", lambda: {"APP": 1.0})
    monkeypatch.setattr(portfolio, "save_stock_quarters", lambda *_: None)
    assert portfolio.stock_quarter_state("evan", FixedDatetime.now())["APP"]["base_budget_usd"] == 300.0


def test_valuation_uses_historical_metrics_and_ignores_peg(monkeypatch):
    monkeypatch.setattr(portfolio, "SATELLITE_SYMBOLS", ("APP", "TEM"))
    assert portfolio.valuation_split_for_row("APP", {"pe": 105, "forward_pe": 10, "peg": 0.1})[0] == 0.5
    assert portfolio.valuation_split_for_row("APP", {"pe": 60, "forward_pe": 200, "peg": 100})[0] == 1.0
    assert portfolio.valuation_split_for_row("APP", {"pe": None, "forward_pe": 200})[0] == 1.0
    assert portfolio.valuation_split_for_row("TEM", {"ps": 14, "forward_ps": 5})[0] == 0.5
    assert portfolio.valuation_split_for_row("TEM", {"ps": 10, "forward_ps": 50, "peg": 100})[0] == 1.0


def trade(symbol, trade_date, amount, action="buy"):
    return {"symbol": symbol, "trade_date": trade_date, "amount_usd": amount, "action": action, "cost_basis": amount}


def test_long_term_target_drives_gaps_while_budget_and_bought_use_period(monkeypatch):
    monkeypatch.setattr(portfolio, "datetime", FixedDatetime)
    monkeypatch.setattr(portfolio, "load_etf_budget_months", lambda *_: {"VOO": "2026-11"})
    monkeypatch.setattr(portfolio, "load_monthly_usage", lambda *_: {
        "planned_new_cash_usd": 700.0, "planned_cash_by_month": {"2026-11": 900.0, "2026-12": 400.0},
        "bought_amount_by_symbol": {}, "sold_amount_by_symbol": {}, "bought_intensity_by_symbol": {},
    })
    monkeypatch.setattr(portfolio, "stock_quarter_state", lambda *_: {
        "ISRG": {"start_date": "2026-10-15", "carry_in_usd": -15.0},
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
        {"symbol": "VOO", "label": "VOO", "value_cny": 1100.0, "shares": 10.5, "avg_cost": 100.0},
        {"symbol": "QQQ", "label": "QQQ", "value_cny": 1080.0, "shares": 10.3, "avg_cost": 100.0},
        {"symbol": "ISRG", "label": "ISRG", "value_cny": 1090.0, "shares": 10.15, "avg_cost": 100.0},
        {"symbol": "SGOV", "label": "SGOV", "value_cny": 600.0, "shares": 5.0, "avg_cost": 100.0},
    ]
    market = {"quotes": {symbol: {"price": 100.0} for symbol in ("VOO", "QQQ", "ISRG", "SGOV")}}
    result = portfolio.build_rebalance_v2("evan", holdings, {"cash_usd": 1000.0, "cash_cost_basis_usd": 800.0}, market, {"SGOV": 600.0}, 1.0, build_months=3)
    assert result["current_assets_usd"] == pytest.approx(1050.0 + 1030.0 + 1015.0 + 500.0 + 800.0 )
    rows = {row["symbol"]: row for row in result["rows"]}
    weights = portfolio.effective_target_weights()
    total = sum(weights[s] for s in portfolio.USD_SYMBOLS)
    for symbol, bought, start_cost, market_value in (
        ("VOO", 0.0, 1050.0, 1100.0),
        ("QQQ", 30.0, 1000.0, 1080.0),
        ("ISRG", 15.0, 1000.0, 1090.0),
    ):
        row = rows[symbol]
        base = (900.0 if symbol == "VOO" else 700.0) * weights[symbol] / total
        expected_plan = max(0.0, base * 3.0 - 15.0) if symbol == "ISRG" else base
        assert row["planned_buy_usd"] == pytest.approx(expected_plan)
        assert row["period_net_bought_usd"] == pytest.approx(bought)
        expected_denominator = result["current_assets_usd"] + (1300.0 if symbol == "ISRG" else 0.0)
        expected_target = expected_denominator * weights[symbol] / total
        expected_gap = expected_target - row["current_cost_usd"]
        if abs(expected_gap) <= max(5.0, expected_target * 0.01):
            expected_gap = 0.0
        assert row["target_usd"] == pytest.approx(expected_target)
        assert row["target_denominator_usd"] == pytest.approx(expected_denominator)
        assert row["buy_difference_usd"] == pytest.approx(expected_gap)
        assert row["target_usd"] - market_value == pytest.approx(expected_target - market_value)
    assert rows["QQQ"]["intensity"] == "medium"
    assert rows["VOO"]["bought_period"] == rows["QQQ"]["bought_period"] == "month"
    assert rows["VOO"]["budget_month"] == "2026-11"
    assert rows["QQQ"]["budget_month"] == "2026-10"
    assert rows["VOO"]["period_start_cost_usd"] == pytest.approx(1050.0)
    assert rows["ISRG"]["bought_period"] == "quarter"
    short_horizon = portfolio.build_rebalance_v2("evan", holdings, {"cash_usd": 1000.0, "cash_cost_basis_usd": 800.0}, market, {"SGOV": 600.0}, 1.0, build_months=1)
    short_rows = {row["symbol"]: row for row in short_horizon["rows"]}
    assert short_rows["ISRG"]["target_denominator_usd"] == pytest.approx(result["current_assets_usd"] + 1300.0)
    assert short_rows["VOO"]["target_denominator_usd"] == pytest.approx(result["current_assets_usd"])


@pytest.mark.parametrize("previous_spend", [50.0, 500.0])
def test_manual_refresh_carries_surplus_or_overspend(monkeypatch, previous_spend):
    monkeypatch.setattr(portfolio, "datetime", FixedDatetime)
    monkeypatch.setattr(portfolio, "load_monthly_usage", lambda *_: {"planned_new_cash_usd": 700.0})
    monkeypatch.setattr(portfolio, "load_trade_records", lambda *_: [
        trade("ISRG", "2026-10-20", previous_spend),
        trade("ISRG", "2026-10-29", 30.0),
    ])
    state = {"ISRG": {"start_date": "2026-10-01", "carry_in_usd": 10.0, "base_budget_usd": 100.0, "quarter_label": "Y26Q3"}}
    monkeypatch.setattr(portfolio, "stock_quarter_state", lambda *_: state)
    saved = {}
    monkeypatch.setattr(portfolio, "save_stock_quarters", lambda _user, quarters: saved.update(quarters))
    result = portfolio.refresh_stock_quarter("evan", "ISRG")
    weights = portfolio.effective_target_weights()
    total = sum(weights[s] for s in portfolio.USD_SYMBOLS)
    expected_carry = 100.0 + 10.0 - previous_spend
    assert result["carry_in_usd"] == pytest.approx(expected_carry)
    assert result["previous_net_bought_usd"] == previous_spend
    assert saved["ISRG"]["start_date"] == "2026-10-29"
    assert saved["ISRG"]["carry_in_usd"] == pytest.approx(expected_carry)
    assert saved["ISRG"]["base_budget_usd"] == pytest.approx(700.0 * weights["ISRG"] / total * 3)
    assert saved["ISRG"]["history"][-1]["quarter_label"] == "Y26Q3"
    assert saved["ISRG"]["history"][-1]["carry_out_usd"] == pytest.approx(expected_carry)
    assert portfolio.period_trade_totals(
        [trade("ISRG", "2026-10-20", previous_spend), trade("ISRG", "2026-10-29", 30.0)],
        "ISRG", "2026-10-29", "2026-10-30",
    )["bought_usd"] == 30.0


def test_quarter_label_is_saved_without_refreshing_cycle(monkeypatch):
    state = {"ISRG": {"start_date": "2026-10-01", "carry_in_usd": 0.0, "base_budget_usd": 100.0}}
    monkeypatch.setattr(portfolio, "stock_quarter_state", lambda *_: state)
    saved = {}
    monkeypatch.setattr(portfolio, "save_stock_quarters", lambda _user, quarters: saved.update(quarters))
    result = portfolio.save_stock_quarter_label("evan", "ISRG", "y26q3")
    assert result == {"symbol": "ISRG", "quarter_label": "Y26Q3"}
    assert saved["ISRG"]["start_date"] == "2026-10-01"
    with pytest.raises(ValueError):
        portfolio.save_stock_quarter_label("evan", "ISRG", "Y26Q5")


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
    plan = portfolio.build_sgov_usage_plan(rows, 1200, signal, trades, "2026-10-29")
    assert plan["budget_usd"] == 1200
    assert plan["rows"][0]["trigger_price"] == pytest.approx(720)
    assert sum(row["voo_usd"] for row in plan["rows"]) == pytest.approx(1200 * 4 / 7)
    assert sum(row["qqq_usd"] for row in plan["rows"]) == pytest.approx(1200 * 3 / 7)
    assert plan["latest_sale"]["amount_usd"] == 200
    rows[0]["buy_difference_usd"] = 0
    rows[1]["buy_difference_usd"] = -5
    empty = portfolio.build_sgov_usage_plan(rows, 1200, signal, [], "2026-10-29")
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
    rows = [{"symbol": "VOO", "buy_difference_usd": 400}, {"symbol": "QQQ", "buy_difference_usd": 300, "drawdown_anchor_price": 100}]
    signal = {"threshold_snapshot": {"thresholds_pct": {"small": -5, "medium": -10, "large": -20}}, "episode_state": {"episode_active": True, "small_triggered": True}}
    trades = [{**trade("VOO", "2026-10-28", 40), "uses_sgov": True, "sgov_tier": "small"}, {**trade("QQQ", "2026-10-28", 30), "uses_sgov": True}]
    result = portfolio.build_sgov_usage_plan(rows, 700, signal, trades, "2026-10-29")
    assert result["rows"][0]["triggered"] is True
    assert result["rows"][1]["triggered"] is False
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
    plan = portfolio.build_sgov_usage_plan([], 500, signal, trades, "2026-10-04")
    assert plan["available_usd"] == 500
    assert sum(row["voo_used_usd"] for row in plan["rows"]) == pytest.approx(300)
    assert sum(row["qqq_used_usd"] for row in plan["rows"]) == pytest.approx(150)
    assert plan["budget_usd"] == pytest.approx(950)
    trades.append(trade("SGOV", "2026-10-04", 1000))
    assert all(row["voo_used_usd"] == row["qqq_used_usd"] == 0 for row in portfolio.build_sgov_usage_plan([], 500, signal, trades, "2026-10-04")["rows"])


def test_sgov_budget_uses_requested_tier_weights():
    signal = {"threshold_snapshot": {"thresholds_pct": {"small": -5.5, "medium": -13.5, "large": -27}}}
    plan = portfolio.build_sgov_usage_plan([], 1000, signal, [], "2026-11-06")
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
    plan = portfolio.build_sgov_usage_plan([], 600, signal, trades, "2026-10-29")
    assert [row["voo_used_usd"] for row in plan["rows"]] == pytest.approx([1000 * .2 * 4 / 7, 1000 * .35 * 4 / 7, 400 - 1000 * .55 * 4 / 7])


def test_sgov_sale_reduces_largest_tier_first_and_never_below_zero():
    signal = {"threshold_snapshot": {"thresholds_pct": {"small": -5, "medium": -10, "large": -20}}}
    base = [{**trade("VOO", "2026-10-01", 1200), "uses_sgov": True}, {**trade("QQQ", "2026-10-01", 900), "uses_sgov": True}]
    before = portfolio.build_sgov_usage_plan([], 1000, signal, base, "2026-10-04")
    sale = trade("SGOV", "2026-10-02", 100, "sell")
    after = portfolio.build_sgov_usage_plan([], 1100, signal, base + [sale], "2026-10-04")
    assert after["budget_usd"] == pytest.approx(before["budget_usd"])
    for index in (0, 1):
        assert after["rows"][index]["voo_used_usd"] == pytest.approx(before["rows"][index]["voo_used_usd"])
        assert after["rows"][index]["qqq_used_usd"] == pytest.approx(before["rows"][index]["qqq_used_usd"])
    assert sum(before["rows"][2][key] - after["rows"][2][key] for key in ("voo_used_usd", "qqq_used_usd")) == pytest.approx(100)
    huge = trade("SGOV", "2026-10-03", 10000, "sell")
    zero = portfolio.build_sgov_usage_plan([], 1000, signal, base + [huge], "2026-10-04")
    assert all(row["voo_used_usd"] == row["qqq_used_usd"] == 0 for row in zero["rows"])
