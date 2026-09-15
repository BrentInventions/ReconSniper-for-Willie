"""AI momentum exit engine — off path must match the old hold exactly."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mark2.config import Mark2Config
from mark2.ema_strategy import manage_ema_hold
from mark2.exits import PaperTrade
from mark2.strategy_hud import apply_strategy, snapshot
from mark2.types import Side
from mark2.ai_exit import classify_spread, momentum_score, SPREAD_STABLE, SPREAD_COLLAPSE


def _trade(*, entry=100.0, stop=85.0, peak=100.0, mfe=0.0) -> PaperTrade:
    return PaperTrade(
        side=Side.LONG,
        entry=entry,
        entry_ts=1.0,
        stop=stop,
        target=0.0,
        peak=peak,
        trough=entry,
        hard_stop=stop,
        ema_strategy=True,
        atr_at_entry=10.0,
        mfe=mfe,
    )


def _cfg(*, ai: bool = True) -> Mark2Config:
    cfg = Mark2Config()
    cfg.ENABLE_AI_EXIT_ENGINE = ai
    cfg.ENABLE_MOMENTUM_SCORE = True
    cfg.ENABLE_MFE_RUNNER = True
    cfg.ENABLE_STRUCTURE_OVERRIDE = True
    cfg.ENABLE_MOMENTUM_REACCELERATION = True
    cfg.CATASTROPHIC_STOP_ATR = 1.50
    return cfg


def test_toggle_off_keeps_old_hold() -> None:
    cfg = _cfg(ai=False)
    trade = _trade(peak=101.0, mfe=1.0)
    done, why, _st = manage_ema_hold(
        trade, price=101.0, exit_armed=False, cfg=cfg, ema9=102.0, ema20=99.0, ema50=95.0, atr=10.0
    )
    assert done is False
    assert why == ""
    assert trade.ema_trade_state == "PROBATION"


def test_hard_stop_never_widens() -> None:
    cfg = _cfg(ai=True)
    trade = _trade(stop=85.0)
    hard = trade.hard_stop
    manage_ema_hold(
        trade,
        price=101.0,
        exit_armed=False,
        cfg=cfg,
        ema9=102.0,
        ema20=99.0,
        ema50=95.0,
        prev9=101.5,
        prev20=98.8,
        prev50=94.9,
        atr=10.0,
    )
    assert trade.hard_stop == hard
    assert trade.stop + 1e-9 >= hard


def test_hard_stop_hits() -> None:
    cfg = _cfg(ai=True)
    trade = _trade(stop=85.0)
    done, why, _ = manage_ema_hold(
        trade, price=84.0, exit_armed=False, cfg=cfg, ema9=90.0, ema20=92.0, ema50=95.0, atr=10.0
    )
    assert done is True
    assert why == "HARD_STOP"


def test_healthy_runner_holds() -> None:
    cfg = _cfg(ai=True)
    trade = _trade(entry=100.0, stop=85.0, peak=130.0, mfe=30.0)
    done, why, _ = manage_ema_hold(
        trade,
        price=128.0,
        exit_armed=False,
        cfg=cfg,
        ema9=126.0,
        ema20=118.0,
        ema50=110.0,
        prev9=124.0,
        prev20=117.0,
        prev50=109.5,
        atr=10.0,
    )
    assert done is False
    assert why == ""
    assert trade.ai_exit_state in ("RUNNER_HEALTHY", "RUNNER_REACCELERATING", "TRADE_PROTECTED")
    assert trade.giveback_floor_pts + 1e-9 >= 30.0 * 0.70


def test_mfe_floor_only_ratchets_up() -> None:
    cfg = _cfg(ai=True)
    trade = _trade(entry=100.0, stop=85.0, peak=140.0, mfe=40.0)
    manage_ema_hold(
        trade,
        price=138.0,
        exit_armed=False,
        cfg=cfg,
        ema9=136.0,
        ema20=128.0,
        ema50=118.0,
        prev9=134.0,
        prev20=127.0,
        prev50=117.5,
        atr=10.0,
    )
    floor1 = float(trade.giveback_floor_pts)
    trade.peak = 130.0
    trade.mfe = 30.0
    manage_ema_hold(
        trade,
        price=129.0,
        exit_armed=False,
        cfg=cfg,
        ema9=136.0,
        ema20=128.0,
        ema50=118.0,
        prev9=134.0,
        prev20=127.0,
        prev50=117.5,
        atr=10.0,
    )
    assert float(trade.giveback_floor_pts) + 1e-9 >= floor1


def test_healthy_pullback_does_not_exit() -> None:
    cfg = _cfg(ai=True)
    trade = _trade(entry=100.0, stop=85.0, peak=120.0, mfe=20.0)
    done, why, _ = manage_ema_hold(
        trade,
        price=114.0,
        exit_armed=False,
        cfg=cfg,
        ema9=116.0,
        ema20=112.0,
        ema50=108.0,
        prev9=116.2,
        prev20=111.6,
        prev50=107.7,
        atr=10.0,
        bars=[
            {"open": 118.0, "high": 120.0, "low": 117.0, "close": 119.0},
            {"open": 119.0, "high": 119.5, "low": 114.5, "close": 115.0},
            {"open": 115.0, "high": 116.0, "low": 113.5, "close": 114.0},
        ],
    )
    assert done is False
    assert why == ""


def test_structure_break_exits() -> None:
    cfg = _cfg(ai=True)
    trade = _trade(entry=100.0, stop=85.0, peak=120.0, mfe=20.0)
    done, why, _ = manage_ema_hold(
        trade,
        price=108.0,
        exit_armed=False,
        cfg=cfg,
        ema9=109.0,
        ema20=112.0,
        ema50=110.0,
        prev9=111.5,
        prev20=111.0,
        prev50=109.8,
        atr=10.0,
        bars=[{"open": 114.0, "high": 114.5, "low": 107.5, "close": 108.0}],
    )
    assert done is True
    assert why in ("MOMENTUM_STRUCTURE_BREAK", "MOMENTUM_DEATH", "EARLY_SETUP_FAILURE", "EMA_COMPRESSION_EXIT")


def test_tiny_spread_wiggle_is_stable() -> None:
    assert classify_spread(2.00, 1.99, 2.00, 10.0) == SPREAD_STABLE


def test_collapse_classifies() -> None:
    assert classify_spread(0.2, 2.0, 1.8, 10.0) == SPREAD_COLLAPSE


def test_score_healthy_is_low() -> None:
    n = momentum_score(
        side=Side.LONG,
        slope9=0.2,
        slope20=0.1,
        gap_now=8.0,
        gap_prev=7.0,
        close=120.0,
        ema9=118.0,
        ema20=110.0,
        compressed=False,
        bearish_cross=False,
        lower_high=False,
        lower_low=False,
        bearish_bar=False,
        spread_regime="EXPANDING",
        cfg=_cfg(),
    )
    assert n < 3


def test_hud_toggle_wires_ai_exit() -> None:
    cfg = _cfg(ai=False)
    assert snapshot(cfg)["ai_exit_engine"] is False
    out = apply_strategy(cfg, {"ai_exit_engine": True})
    assert out["ai_exit_engine"] is True
    assert cfg.ENABLE_AI_EXIT_ENGINE is True
    apply_strategy(cfg, {"ai_exit_engine": False, "mfe_runner": False})
    assert cfg.ENABLE_AI_EXIT_ENGINE is False
    assert cfg.ENABLE_MFE_RUNNER is False
