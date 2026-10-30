from datetime import datetime

import pytest

from backend import portfolio


class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 10, 29, 12, 0, tzinfo=tz)


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
    monkeypatch.setattr(portfolio, "evaluate_drawdown_episode_signals", lambda *_args, **_kwargs: {})
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
        expected_plan = max(0.0, base * 3.0 - 15.0) if symbol == "ISRG" else base * row["signal_multiplier"]
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
