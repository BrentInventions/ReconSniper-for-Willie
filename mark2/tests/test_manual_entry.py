"""HUD manual entry — Willie watches the trade; bot must not stop him out early."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mark2.config import Mark2Config
from mark2.exits import (
    PaperTrade,
    broker_stop_for_nt,
    lock_price,
    manage_paper,
)
from mark2.types import MarketSnapshot, ScoreBundle, Side


def _snap(*, vel: float = 0.0, price: float = 20000.0) -> MarketSnapshot:
    return MarketSnapshot(
        ts=1.0,
        price=price,
        completed_bars=[],
        forming_bar={"open": price, "high": price + 1, "low": price - 1},
        atr=4.0,
        velocity=vel,
        acceleration=0.0,
        relative_volume=1.0,
        impulse_score=60.0,
        trend_bias="BULLISH",
        trend_regime="TRENDING",
        structure_state="HH_HL",
        longs_allowed=True,
        shorts_allowed=False,
    )


def _scores(*, long_cv: float = -0.25) -> ScoreBundle:
    return ScoreBundle(
        long_confidence=65.0,
        short_confidence=40.0,
        long_conf_velocity=long_cv,
        short_conf_velocity=-0.1,
    )


def test_manual_pre_target_no_stop_exit_on_pullback():
    cfg = Mark2Config()
    entry = 20000.0
    trade = PaperTrade(
        side=Side.LONG,
        entry=entry,
        entry_ts=0.0,
        stop=entry - 25.0,
        target=entry + 12.5,
        peak=entry,
        trough=entry,
        manual_entry=True,
    )
    manage_paper(trade, price=entry + 5.5, atr=4.0, health=80.0, hold_sec=1.0, cfg=cfg)
    assert trade.was_green is False
    assert trade.stop == entry - 25.0
    done, why, _ = manage_paper(
        trade, price=entry + 0.055, atr=4.0, health=45.0, hold_sec=2.0, cfg=cfg
    )
    assert done is False
    assert why == ""


def test_manual_pre_target_no_failed_event():
    cfg = Mark2Config()
    cfg.ENABLE_THESIS_ABORT = True
    cfg.ENABLE_MOMENTUM_EXIT = True
    entry = 20000.0
    trade = PaperTrade(
        side=Side.LONG,
        entry=entry,
        entry_ts=0.0,
        stop=entry - 25.0,
        target=entry + 12.5,
        peak=entry,
        trough=entry,
        manual_entry=True,
    )
    snap = _snap(vel=-0.2, price=entry + 2.0)
    scores = _scores(long_cv=-0.25)
    for _ in range(5):
        done, why, _ = manage_paper(
            trade,
            price=entry + 2.0,
            atr=4.0,
            health=20.0,
            hold_sec=3.0,
            cfg=cfg,
            scores=scores,
            snap=snap,
            event_alive=False,
        )
    assert done is False
    assert "FAILED_EVENT" not in why


def test_manual_post_target_bank_floor_at_target():
    cfg = Mark2Config()
    entry = 20000.0
    target = entry + 12.5
    trade = PaperTrade(
        side=Side.LONG,
        entry=entry,
        entry_ts=0.0,
        stop=entry - 25.0,
        target=target,
        peak=entry,
        trough=entry,
        manual_entry=True,
    )
    manage_paper(trade, price=target, atr=4.0, health=80.0, hold_sec=1.0, cfg=cfg)
    assert trade.target_touched is True
    assert trade.stop >= target


def test_manual_post_target_no_exit_while_climbing():
    cfg = Mark2Config()
    entry = 20000.0
    target = entry + 12.5
    trade = PaperTrade(
        side=Side.LONG,
        entry=entry,
        entry_ts=0.0,
        stop=target,
        target=target,
        peak=target + 2.0,
        trough=entry,
        target_touched=True,
        manual_entry=True,
    )
    snap = _snap(vel=0.15, price=target + 1.5)
    done, why, _ = manage_paper(
        trade,
        price=target + 1.5,
        atr=4.0,
        health=70.0,
        hold_sec=5.0,
        cfg=cfg,
        snap=snap,
    )
    assert done is False
    assert why == ""


def test_broker_stop_skipped_pre_bank_manual():
    cfg = Mark2Config()
    entry = 20000.0
    lock = lock_price(entry, Side.LONG, cfg)
    trade = PaperTrade(
        side=Side.LONG,
        entry=entry,
        entry_ts=0.0,
        stop=entry - 25.0,
        target=lock,
        peak=entry + 5.0,
        trough=entry,
        manual_entry=True,
    )
    stop, flat = broker_stop_for_nt(trade, trade.stop, entry + 5.0, lock, cfg)
    assert stop is None
    assert flat is False


def test_manual_long_target_touch_then_runner_stays_in():
    """Target hit = bank floor only — price above target must not flatten."""
    cfg = Mark2Config()
    entry = 20000.0
    target = entry + 12.5
    trade = PaperTrade(
        side=Side.LONG,
        entry=entry,
        entry_ts=0.0,
        stop=entry - 25.0,
        target=target,
        peak=entry,
        trough=entry,
        manual_entry=True,
    )
    done, why, _ = manage_paper(
        trade, price=target, atr=4.0, health=80.0, hold_sec=1.0, cfg=cfg
    )
    assert done is False
    assert why == ""
    assert trade.target_touched is True
    assert trade.stop >= target
    # Price continues above target — still in, no flatten at touch.
    for px in (target + 2.0, target + 5.0, target + 8.0):
        done, why, _ = manage_paper(
            trade, price=px, atr=4.0, health=85.0, hold_sec=2.0, cfg=cfg
        )
        assert done is False, why
        assert trade.stop >= target
    assert trade.peak_mfe_after_bank > 0


def test_manual_runner_exits_on_trail_break_not_target_touch():
    """MFE past target: no flatten at touch; exit only when trail breaks."""
    cfg = Mark2Config()
    entry = 20000.0
    target = entry + 12.5
    trade = PaperTrade(
        side=Side.LONG,
        entry=entry,
        entry_ts=0.0,
        stop=entry - 25.0,
        target=target,
        peak=entry,
        trough=entry,
        manual_entry=True,
    )
    done, why, _ = manage_paper(
        trade, price=target, atr=4.0, health=80.0, hold_sec=1.0, cfg=cfg
    )
    assert done is False
    assert why == ""
    assert trade.target_touched is True
    peak_px = target + 10.0
    done, why, _ = manage_paper(
        trade, price=peak_px, atr=4.0, health=90.0, hold_sec=2.0, cfg=cfg
    )
    assert done is False
    assert trade.runner is True
    room = float(cfg.RUNNER_TRAIL_POINTS)
    assert trade.stop >= target
    assert trade.stop == peak_px - room
    # Hold below peak but above trail — still in.
    hold_px = trade.stop + 1.0
    for i in range(4):
        done, why, _ = manage_paper(
            trade, price=hold_px, atr=4.0, health=85.0, hold_sec=3.0 + i, cfg=cfg
        )
        assert done is False, why
    # Trail break exits (not target touch).
    done, why, _ = manage_paper(
        trade, price=trade.stop - 0.25, atr=4.0, health=70.0, hold_sec=10.0, cfg=cfg
    )
    assert done is True
    assert why == "TRAIL"


def test_bot_trade_still_exits_on_stop_pre_target():
    cfg = Mark2Config()
    entry = 20000.0
    trade = PaperTrade(
        side=Side.LONG,
        entry=entry,
        entry_ts=0.0,
        stop=entry - 5.5,
        target=entry + 12.5,
        peak=entry + 6.0,
        trough=entry,
        was_green=True,
        manual_entry=False,
    )
    done, why, _ = manage_paper(
        trade, price=entry - 6.0, atr=4.0, health=50.0, hold_sec=3.0, cfg=cfg
    )
    assert done is True
    assert why == "STOP"
