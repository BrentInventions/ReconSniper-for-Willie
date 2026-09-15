"""Optional Recon tip-trail experiment — OFF by default for Willie.

When ENABLE_RECON_TIP_TRAIL is true (HUD GATES → Recon tip trail):
  • Arm once total open profit hits TRAIL_ARM_USD (default $15, not × contracts)
  • Purple tip trail follows tip by RECON_TIP_TRAIL_POINTS (default 5.5)
  • After arm, stop floors at entry — never go red
  • Chart: gold entry · green arm · red hard stop · purple tip trail

When false, Recon keeps the stock $25/contract bank + floor behavior.
"""

from __future__ import annotations

from typing import Any

from .config import Mark2Config


def is_enabled(cfg: Mark2Config | None) -> bool:
    if cfg is None:
        return False
    return bool(getattr(cfg, "ENABLE_RECON_TIP_TRAIL", False))


def arm_usd(cfg: Mark2Config, *, locked: float = 0.0) -> float:
    if locked > 0:
        return max(1.0, float(locked))
    return max(1.0, float(getattr(cfg, "TRAIL_ARM_USD", 15.0) or 15.0))


def trail_points(cfg: Mark2Config) -> float:
    tick = max(float(getattr(cfg, "TICK_SIZE", 0.25) or 0.25), 0.25)
    raw = float(getattr(cfg, "RECON_TIP_TRAIL_POINTS", 5.5) or 5.5)
    return max(tick, raw)


def snapshot(cfg: Mark2Config) -> dict[str, Any]:
    return {
        "enabled": is_enabled(cfg),
        "armUsd": arm_usd(cfg),
        "trailPts": trail_points(cfg),
        "neverRed": True,
        "fourLines": True,
    }
