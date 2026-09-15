"""Opt-in Recon tip trail — default OFF; does not change stock $25/ct bank."""

from __future__ import annotations

from mark2.config import Mark2Config
from mark2.entry_tuning import apply_toggles, snapshot
from mark2.exits import PaperTrade, bank_dollars, bank_points, manage_paper
from mark2.types import Side


def _long(entry: float = 20000.0, *, qty: int = 1, stop: float = 19980.0) -> PaperTrade:
    return PaperTrade(
        side=Side.LONG,
        entry=entry,
        entry_ts=1.0,
        stop=stop,
        target=entry + 12.5,
        peak=entry,
        trough=entry,
        qty=qty,
        hard_stop=stop,
        bank_dollars_locked=15.0,
    )


def test_tip_trail_default_off_keeps_per_contract_bank():
    cfg = Mark2Config()
    assert cfg.ENABLE_RECON_TIP_TRAIL is False
    assert bank_dollars(2, cfg) == 50.0
    assert bank_points(cfg) == 12.5  # $25 / $2


def test_tip_trail_toggle_arms_on_total_usd():
    cfg = Mark2Config()
    apply_toggles(cfg, {"recon_tip_trail": True})
    assert cfg.ENABLE_RECON_TIP_TRAIL is True
    assert bank_dollars(3, cfg) == 15.0
    # 3 contracts → $15 / (2*3) = 2.5 pts
    trade = _long(qty=3)
    assert abs(bank_points(cfg, trade=trade) - 2.5) < 1e-9
    snap = snapshot(cfg)
    assert snap["toggles"]["recon_tip_trail"] is True


def test_tip_trail_never_red_after_arm():
    cfg = Mark2Config()
    cfg.ENABLE_RECON_TIP_TRAIL = True
    cfg.TRAIL_ARM_USD = 15.0
    cfg.RECON_TIP_TRAIL_POINTS = 5.5
    cfg.POINT_VALUE = 2.0
    trade = _long(qty=1, stop=19975.0)
    trade.target = trade.entry + 7.5  # $15 @ 1ct
    manage_paper(trade, price=trade.entry + 7.5, atr=4.0, health=80.0, hold_sec=1.0, cfg=cfg)
    assert trade.target_touched is True
    tip = trade.entry + 12.0
    manage_paper(trade, price=tip, atr=4.0, health=80.0, hold_sec=2.0, cfg=cfg)
    assert trade.stop == tip - 5.5
    assert trade.stop >= trade.entry
    # Giveback toward entry still floors at entry, not red.
    manage_paper(trade, price=trade.entry + 1.0, atr=4.0, health=70.0, hold_sec=3.0, cfg=cfg)
    assert trade.stop >= trade.entry
