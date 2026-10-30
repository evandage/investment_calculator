from contextlib import ExitStack
from copy import deepcopy
from unittest.mock import patch

from backend import portfolio, storage


def test_margin_buy_records_negative_cash_and_can_be_reversed():
    holdings = {"VOO": {"shares": 0.0, "avg_cost": 0.0}}
    balances = storage.normalize_balances({"cash_usd": 20.0})
    records = []

    with ExitStack() as stack:
        stack.enter_context(patch.object(portfolio, "load_user_state", side_effect=lambda _: (deepcopy(holdings), dict(balances), "local")))
        stack.enter_context(patch.object(portfolio, "save_user_state", side_effect=lambda _, h, b: (holdings.update(h), balances.update(b))))
        stack.enter_context(patch.object(portfolio, "load_trade_records", side_effect=lambda _: deepcopy(records)))
        stack.enter_context(patch.object(portfolio, "save_trade_records", side_effect=lambda _, r: records.__setitem__(slice(None), r)))
        stack.enter_context(patch.object(portfolio, "load_monthly_usage", return_value={"planned_new_cash_usd": 0}))
        for name in ("save_monthly_usage", "record_portfolio_adjustment", "invalidate_performance_history_from"):
            stack.enter_context(patch.object(portfolio, name))

        portfolio.confirm_trades("test", [{"symbol": "VOO", "action": "buy", "trade_date": "2026-10-28", "amount_usd": 100, "shares": 1}])
        assert balances["cash_usd"] == -80
        assert balances["cash_cost_basis_usd"] == -80
        assert holdings["VOO"]["shares"] == 1
        assert storage.normalize_balances(balances)["cash_usd"] == -80

        portfolio.delete_trade_record("test", records[0]["id"])
        assert balances["cash_usd"] == 20
        assert holdings["VOO"]["shares"] == 0


def test_historical_margin_cash_is_not_clamped():
    balances = {"cash_usd": -80.0, "cash_cny": -10.0, "cash_cost_basis_usd": -80.0, "cash_cost_basis_cny": -10.0}
    trades = [{"symbol": "VOO", "action": "buy", "trade_date": "2026-10-28", "amount_usd": 100.0}]

    before = portfolio.balances_for_history_day("2026-10-27", balances, trades)
    after = portfolio.balances_for_history_day("2026-10-28", balances, trades)
    assert before["cash_usd"] == 20
    assert after["cash_usd"] == -80
    assert after["cash_cny"] == -10


def test_negative_manual_balances_preserve_cash_basis():
    normalized = storage.normalize_balances({"cash_usd": -50, "cash_cny": -100, "realized_usd": 5})
    assert normalized["cash_usd"] == -50
    assert normalized["cash_cny"] == -100
    assert normalized["cash_cost_basis_usd"] == -55
    assert normalized["cash_cost_basis_cny"] == -100
