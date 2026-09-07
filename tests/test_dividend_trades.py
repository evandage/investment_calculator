from contextlib import ExitStack
from copy import deepcopy
from unittest.mock import patch

import pytest

from backend import portfolio, storage, main


@pytest.fixture
def book():
    holdings = {"SGOV": {"shares": 2.0, "avg_cost": 100.0}}
    balances = {"cash_usd": 20.0, "cash_cny": 0.0, "cash_cost_basis_usd": 4.8,
                "cash_cost_basis_cny": 0.0, "sgov_dividend_usd": 15.2, "voo_dividend_usd": 7.98}
    records = []
    with ExitStack() as stack:
        stack.enter_context(patch.object(portfolio, "load_user_state", side_effect=lambda _: (deepcopy(holdings), dict(balances), "local")))
        def save_state(_, h, b):
            holdings.clear()
            holdings.update(h)
            balances.update(b)
        stack.enter_context(patch.object(portfolio, "save_user_state", side_effect=save_state))
        stack.enter_context(patch.object(portfolio, "load_trade_records", side_effect=lambda _: deepcopy(records)))
        stack.enter_context(patch.object(portfolio, "save_trade_records", side_effect=lambda _, r: records.__setitem__(slice(None), storage.load_trade_records_from_rows(r))))
        stack.enter_context(patch.object(portfolio, "load_monthly_usage", return_value={"planned_new_cash_usd": 0}))
        for name in ("save_monthly_usage", "record_portfolio_adjustment", "invalidate_performance_history_from"):
            stack.enter_context(patch.object(portfolio, name))
        yield holdings, balances, records


def receipt(**changes):
    return {"symbol": "SGOV", "action": "dividend", "trade_date": "2026-09-04", "amount_usd": 3.5, **changes}


def test_receipt_roundtrip_and_reversal(book):
    holdings, balances, records = book
    original = deepcopy(holdings)
    portfolio.confirm_trades("test", [receipt()])
    assert holdings == original
    assert balances["cash_usd"] == 23.5
    assert balances["sgov_dividend_usd"] == 18.7
    assert balances["cash_cost_basis_usd"] == 4.8
    assert records[0]["shares"] == 0
    with patch.object(storage, "_read_json", return_value={"test": records}):
        assert storage.load_trade_records("test")[0]["action"] == "dividend"
    portfolio.delete_trade_record("test", records[0]["id"])
    assert holdings == original
    assert balances["cash_usd"] == 20
    assert balances["sgov_dividend_usd"] == 15.2
    assert balances["cash_cost_basis_usd"] == 4.8
    assert records == []


@pytest.mark.parametrize("changes", [{"symbol": "VOO"}, {"amount_usd": 0}, {"amount_usd": float("inf")}, {"trade_date": "invalid"}])
def test_invalid_receipt_does_not_save(book, changes):
    before = deepcopy(book)
    with pytest.raises(ValueError):
        portfolio.confirm_trades("test", [receipt(**changes)])
    assert book == before


def test_insufficient_cash_reversal_preserves_book(book):
    _, balances, records = book
    portfolio.confirm_trades("test", [receipt()])
    balances["cash_usd"] = 1
    before = deepcopy(book)
    with pytest.raises(ValueError):
        portfolio.delete_trade_record("test", records[0]["id"])
    assert book == before


def test_receipt_history_changes_cash_not_principal(book):
    holdings, balances, records = book
    portfolio.confirm_trades("test", [receipt()])
    prior = portfolio.balances_for_history_day("2026-09-03", balances, records)
    assert prior["cash_usd"] == 20
    assert prior["cash_cost_basis_usd"] == 4.8
    before = portfolio.historical_holding_pnl(holdings, {"SGOV": 100}, 1, balances, trades=records, history_day="2026-09-03")
    after = portfolio.historical_holding_pnl(holdings, {"SGOV": 100}, 1, balances, trades=records, history_day="2026-09-04")
    assert after["amount_cny"] - before["amount_cny"] == pytest.approx(3.5)


def test_cash_edit_preserves_hidden_income_and_calculates_basis():
    before = storage.normalize_balances({"cash_usd": 20, "voo_dividend_usd": 7.98, "sgov_dividend_usd": 15.2})
    saved = {}
    with (patch.object(main, "load_balances", side_effect=lambda: dict(saved or before)),
          patch.object(main, "save_balances", side_effect=saved.update),
          patch.object(main, "record_portfolio_adjustment", return_value=None)):
        main.update_balances(main.BalancesPayload(balances={"cash_usd": 30, "cash_cost_basis_usd": 999}))
    assert saved["voo_dividend_usd"] == 7.98
    assert saved["sgov_dividend_usd"] == 15.2
    assert saved["cash_cost_basis_usd"] == pytest.approx(before["cash_cost_basis_usd"] + 10)
