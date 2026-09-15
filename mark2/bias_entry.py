"""Bias-aligned entry loosening — lower gates when tape agrees with direction."""

from __future__ import annotations

from dataclasses import dataclass

from .config import Mark2Config
from .types import MarketSnapshot, Side


@dataclass(frozen=True)
class BiasEntryGates:
    active: bool = False
    tag: str = ""
    conf_threshold: float | None = None
    opp_threshold: float | None = None
    min_conf_vel: float | None = None
    build_ticks: int | None = None
    relax_candle: bool = False
    min_tick_velocity: float | None = None
    direction_gap: float | None = None


def _tradeable_regime(regime: str) -> bool:
    return regime in ("TRENDING", "HIGH_VOL")


def bias_entry_gates(
    cfg: Mark2Config,
    snap: MarketSnapshot,
    side: Side,
    *,
    entry_profile: str = "default",
) -> BiasEntryGates:
    if not bool(getattr(cfg, "ENABLE_BIAS_ENTRY_ADJUST", True)):
        return BiasEntryGates()
    if entry_profile in ("chop_scalp", "chaotic_bank"):
        return BiasEntryGates()
    if side not in (Side.LONG, Side.SHORT):
        return BiasEntryGates()

    regime = (snap.trend_regime or "").upper()
    if not _tradeable_regime(regime):
        return BiasEntryGates()

    bias = (snap.trend_bias or "").upper()
    relax = bool(getattr(cfg, "BIAS_RELAX_CANDLES", True))
    build = max(1, int(getattr(cfg, "BIAS_BUILD_TICKS", 2)))
    min_tick = float(getattr(cfg, "BIAS_MIN_TICK_VEL", 0.05))
    gap = float(getattr(cfg, "BIAS_DIRECTION_GAP", 6.0))

    if side == Side.SHORT and bias in ("BEARISH", "BEAR") and snap.shorts_allowed:
        conf_delta = float(getattr(cfg, "BIAS_SHORT_CONF_DELTA", 4.0))
        opp_delta = float(getattr(cfg, "BIAS_SHORT_OPP_DELTA", 2.0))
        vel_delta = float(getattr(cfg, "BIAS_SHORT_CONF_VEL_DELTA", 0.12))
        return BiasEntryGates(
            active=True,
            tag="bias_short",
            conf_threshold=max(48.0, float(cfg.SHORT_CONFIDENCE_THRESHOLD) - conf_delta),
            opp_threshold=max(45.0, float(cfg.SHORT_OPPORTUNITY_THRESHOLD) - opp_delta),
            min_conf_vel=max(0.08, float(cfg.MIN_CONFIDENCE_VELOCITY) - vel_delta),
            build_ticks=build,
            relax_candle=relax,
            min_tick_velocity=min_tick,
            direction_gap=gap,
        )

    if side == Side.LONG and bias in ("BULLISH", "BULL") and snap.longs_allowed:
        conf_delta = float(getattr(cfg, "BIAS_LONG_CONF_DELTA", 4.0))
        opp_delta = float(getattr(cfg, "BIAS_LONG_OPP_DELTA", 2.0))
        vel_delta = float(getattr(cfg, "BIAS_LONG_CONF_VEL_DELTA", 0.12))
        return BiasEntryGates(
            active=True,
            tag="bias_long",
            conf_threshold=max(48.0, float(cfg.LONG_CONFIDENCE_THRESHOLD) - conf_delta),
            opp_threshold=max(45.0, float(cfg.LONG_OPPORTUNITY_THRESHOLD) - opp_delta),
            min_conf_vel=max(0.08, float(cfg.MIN_CONFIDENCE_VELOCITY) - vel_delta),
            build_ticks=build,
            relax_candle=relax,
            min_tick_velocity=min_tick,
            direction_gap=gap,
        )

    return BiasEntryGates()
