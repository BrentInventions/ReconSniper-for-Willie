"""Willie: FAILED_EVENT / thesis abort permanently off."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mark2.config import Mark2Config, load_config, thesis_abort_enabled
from mark2.exits import PaperTrade, manage_paper
from mark2.trade_abort import check_thesis_abort
from mark2.types import MarketSnapshot, ScoreBundle, Side


def _snap(*, vel: float = -0.2, acc: float = -0.12) -> MarketSnapshot:
    return MarketSnapshot(
        ts=1.0,
        price=20000.0,
        completed_bars=[],
        forming_bar={"open": 20000.0, "high": 20001.0, "low": 19999.0},
        atr=4.0,
        velocity=vel,
        acceleration=acc,
        relative_volume=1.0,
        impulse_score=60.0,
        trend_bias="BULLISH",
        trend_regime="TRENDING",
        structure_state="HH_HL",
        longs_allowed=True,
        shorts_allowed=False,
    )


def _scores(*, long_cv: float = -0.25, long_c: float = 65.0, short_c: float = 40.0) -> ScoreBundle:
    return ScoreBundle(
        long_confidence=long_c,
        short_confidence=short_c,
        long_conf_velocity=long_cv,
        short_conf_velocity=-0.1,
    )


def test_thesis_abort_disabled_by_default():
    cfg = Mark2Config()
    assert thesis_abort_enabled(cfg) is False


def test_momentum_exit_alone_does_not_arm_abort():
    cfg = Mark2Config()
    cfg.ENABLE_MOMENTUM_EXIT = True
    assert thesis_abort_enabled(cfg) is False


def test_stale_settings_migration_clears_all_abort_flags(tmp_path: Path):
    settings = tmp_path / "mark2_settings.json"
    settings.write_text(
        json.dumps(
            {
                "ENABLE_MOMENTUM_EXIT": True,
                "ENABLE_THESIS_ABORT": True,
                "ENABLE_EVENT_ABORT": True,
                "CONTRACTS": 2,
            }
        ),
        encoding="utf-8",
    )
    cfg = load_config(settings)
    assert cfg.ENABLE_THESIS_ABORT is False
    assert cfg.ENABLE_MOMENTUM_EXIT is False
    assert cfg.ENABLE_EVENT_ABORT is False
    assert cfg.CONTRACTS == 2
    assert thesis_abort_enabled(cfg) is False


def test_explicit_settings_cannot_arm_thesis_abort(tmp_path: Path):
    settings = tmp_path / "mark2_settings.json"
    settings.write_text(
        json.dumps({"ENABLE_THESIS_ABORT": True, "ENABLE_MOMENTUM_EXIT": True}),
        encoding="utf-8",
    )
    cfg = load_config(settings)
    assert thesis_abort_enabled(cfg) is False
    assert cfg.ENABLE_THESIS_ABORT is False
    assert cfg.ENABLE_MOMENTUM_EXIT is False


def _run_bot_manage(*, cfg: Mark2Config, snap: MarketSnapshot, scores: ScoreBundle, **kwargs):
    event_alive = kwargs.pop("event_alive", True)
    health = kwargs.pop("health", 55.0)
    trade = PaperTrade(
        side=Side.LONG,
        entry=20000.0,
        entry_ts=0.0,
        stop=19980.0,
        target=20012.5,
        peak=20000.0,
        trough=20000.0,
        **kwargs,
    )
    manage_paper(
        trade,
        price=20002.0,
        atr=4.0,
        health=60.0,
        hold_sec=0.0,
        cfg=cfg,
        scores=scores,
        snap=snap,
        event_alive=event_alive,
    )
    assert trade.was_green is True
    done = False
    why = ""
    for i in range(6):
        done, why, _ = manage_paper(
            trade,
            price=20002.0,
            atr=4.0,
            health=health,
            hold_sec=float(i + 1),
            cfg=cfg,
            scores=scores,
            snap=snap,
            event_alive=event_alive,
        )
        if done:
            break
    return done, why


def test_conf_vel_acc_does_not_abort_bot_trade_by_default():
    cfg = Mark2Config()
    done, why = _run_bot_manage(cfg=cfg, snap=_snap(), scores=_scores())
    assert done is False
    assert "FAILED_EVENT" not in why


def test_event_dead_does_not_abort_with_willie_defaults():
    cfg = Mark2Config()
    done, why = _run_bot_manage(
        cfg=cfg,
        snap=_snap(vel=0.05, acc=0.0),
        scores=_scores(long_cv=0.1),
        event_alive=False,
        health=60.0,
    )
    assert done is False
    assert "FAILED_EVENT" not in why
    assert "event_dead" not in why


def test_vel_hard_health_opp_flip_never_fire_with_willie_defaults():
    cfg = Mark2Config()
    snap = _snap(vel=-0.5, acc=-0.2)
    scores = _scores(long_cv=-0.5, long_c=30.0, short_c=55.0)
    abort, detail, _ = check_thesis_abort(
        cfg=cfg,
        snap=snap,
        scores=scores,
        side=Side.LONG,
        health=10.0,
        peak_health=80.0,
        hold_sec=5.0,
        bad_streak=5,
        event_alive=False,
    )
    assert abort is False
    assert detail == ""
    done, why = _run_bot_manage(
        cfg=cfg,
        snap=snap,
        scores=scores,
        event_alive=False,
        health=10.0,
    )
    assert done is False
    assert "FAILED_EVENT" not in why
