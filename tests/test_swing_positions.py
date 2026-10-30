from backend import storage


def test_swing_positions_cover_every_chart_symbol_and_preserve_qqq():
    positions = storage.normalize_swing_positions({
        "QQQ": {"shares": 2, "avg_cost": 400, "target_pct": 4},
    })

    assert set(positions) == {"VOO", "QQQ", *storage.SATELLITE_SYMBOLS}
    assert positions["QQQ"]["shares"] == 2
    assert positions["QQQ"]["target_pct"] == 4
    assert positions["VOO"]["target_pct"] == 0
    assert positions["VOO"]["stop_loss_min_pct"] == 2.0
    assert positions["VOO"]["take_profit_max_pct"] == 5.5
    assert "SGOV" not in positions


def test_swing_positions_follow_satellite_universe(monkeypatch):
    monkeypatch.setattr(storage, "SATELLITE_SYMBOLS", ("NEW",))
    assert set(storage.normalize_swing_positions({})) == {"VOO", "QQQ", "NEW"}
    assert storage.normalize_swing_positions({})["NEW"]["stop_loss_max_pct"] == 0
    assert "NEW" not in storage.swing_position_recommendations()


def test_recommendations_match_default_risk_levels():
    positions = storage.normalize_swing_positions({})
    recommendations = storage.swing_position_recommendations()
    for symbol, recommendation in recommendations.items():
        assert recommendation["as_of"] == "2026-10-28"
        for key in ("stop_loss_min_pct", "stop_loss_max_pct", "take_profit_min_pct", "take_profit_max_pct"):
            assert positions[symbol][key] == recommendation[key]


def test_new_rebalance_volatility_updates_recommendation(monkeypatch):
    monkeypatch.setattr(storage, "load_drawdown_episode_store", lambda: {"threshold_snapshots": {
        "new-month": {"symbol": "QQQ", "calculation_kind": "monthly_auto", "as_of_date": "2026-11-30", "rv20_pct": 32.0},
    }})
    recommendation = storage.swing_position_recommendations()["QQQ"]
    assert recommendation["as_of"] == "2026-11-30"
    assert recommendation["rv20_pct"] == 32.0
    assert storage.normalize_swing_positions({})["QQQ"]["stop_loss_min_pct"] == recommendation["stop_loss_min_pct"]
