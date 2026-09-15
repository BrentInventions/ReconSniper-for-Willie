"""Daily goal hunt + optional tip trail bank math for Willie Recon."""

from __future__ import annotations

import tempfile
from pathlib import Path

from mark2.config import Mark2Config
from mark2.engine import Mark2Engine
from mark2.exits import (
    PaperTrade,
    bank_dollars,
    bank_points,
    goal_hunt_bank_dollars,
    goal_hunting,
    lock_price,
    manage_paper,
)
from mark2.types import Side


def test_goal_hunt_bank_is_fifteen_total():
    cfg = Mark2Config()
    assert goal_hunt_bank_dollars(cfg, session_pnl=0.0) == 15.0
    assert goal_hunting(cfg) is True


def test_stock_bank_stays_per_contract_when_not_hunting_lock():
    cfg = Mark2Config()
    cfg.ENABLE_DAILY_GOAL = False
    cfg.ENABLE_RECON_TIP_TRAIL = False
    assert bank_dollars(2, cfg) == 50.0
    assert bank_points(cfg) == 12.5


def test_locked_hunt_bank_scales_with_contracts():
    cfg = Mark2Config()
    cfg.ENABLE_RECON_TIP_TRAIL = False
    trade = PaperTrade(
        side=Side.LONG,
        entry=20000.0,
        entry_ts=0.0,
        stop=19980.0,
        target=20002.5,
        peak=20000.0,
        trough=20000.0,
        qty=3,
        bank_dollars_locked=15.0,
    )
    assert abs(bank_points(cfg, trade=trade) - 2.5) < 1e-9
    assert bank_dollars(3, cfg, trade=trade) == 15.0


def test_tip_trail_option_never_red_after_arm():
    cfg = Mark2Config()
    cfg.ENABLE_RECON_TIP_TRAIL = True
    cfg.TRAIL_ARM_USD = 15.0
    cfg.RECON_TIP_TRAIL_POINTS = 5.5
    entry = 20000.0
    trade = PaperTrade(
        side=Side.LONG,
        entry=entry,
        entry_ts=0.0,
        stop=entry - 20.0,
        target=entry + 7.5,
        peak=entry,
        trough=entry,
        qty=1,
        bank_dollars_locked=15.0,
        hard_stop=entry - 20.0,
    )
    manage_paper(trade, price=entry + 7.5, atr=4.0, health=90.0, hold_sec=1.0, cfg=cfg)
    assert trade.target_touched is True
    tip = entry + 12.0
    manage_paper(trade, price=tip, atr=4.0, health=90.0, hold_sec=2.0, cfg=cfg)
    assert abs(trade.stop - (tip - 5.5)) < 1e-9
    assert trade.stop >= entry


def test_near_goal_uses_ten_dollar_bank():
    cfg = Mark2Config()
    assert goal_hunt_bank_dollars(cfg, session_pnl=325.0) == 10.0
    assert goal_hunt_bank_dollars(cfg, session_pnl=100.0) == 15.0


def test_engine_snapshot_shows_hunt_bank_when_goal_on():
    cfg = Mark2Config()
    cfg.CONTRACTS = 3
    with tempfile.TemporaryDirectory() as td:
        eng = Mark2Engine(cfg, log_path=Path(td) / "d.jsonl", persist_path=Path(td) / "s.json")
        snap = eng.hud_snapshot()
        assert snap["dailyGoal"]["enabled"] is True
        assert abs(float(snap["bankDollars"]) - 15.0) < 1e-6
        g = snap["dailyGoal"]
        assert g["hunting"] is True
        assert abs(float(g["huntBank"]) - 15.0) < 1e-6


def test_clear_session_unlocks_goal():
    cfg = Mark2Config()
    with tempfile.TemporaryDirectory() as td:
        eng = Mark2Engine(cfg, log_path=Path(td) / "d.jsonl", persist_path=Path(td) / "s.json")
        eng.goal_met = True
        eng.risk.goal_met = True
        eng.clear_session()
        assert eng.goal_met is False


def test_goal_window_clock_expires_hunt():
    cfg = Mark2Config()
    cfg.GOAL_WINDOW_HOURS = 1.0
    with tempfile.TemporaryDirectory() as td:
        eng = Mark2Engine(cfg, log_path=Path(td) / "d.jsonl", persist_path=Path(td) / "s.json")
        hunt0, _ = eng._goal_bank_for_entry(ts=100.0)
        assert hunt0 is True
        hunt, bank = eng._goal_bank_for_entry(ts=100.0 + 3601.0)
        assert hunt is False
        assert bank == 0.0


def test_set_goal_and_clock_from_engine():
    cfg = Mark2Config()
    with tempfile.TemporaryDirectory() as td:
        eng = Mark2Engine(cfg, log_path=Path(td) / "d.jsonl", persist_path=Path(td) / "s.json")
        g = eng.set_daily_goal(400)
        assert g["goal"] == 400.0
        g2 = eng.set_goal_window_hours(1.5)
        assert g2["windowHours"] == 1.5
