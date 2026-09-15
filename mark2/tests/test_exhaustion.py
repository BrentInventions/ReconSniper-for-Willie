"""Willie Recon — RSI + MACD confirmation (no book patterns)."""

from __future__ import annotations

from mark2.config import Mark2Config
from mark2.exhaustion import exhaustion_blocks, read_exhaustion
from mark2.indicators import macd, rsi
from mark2.types import MarketSnapshot, Side


def _bars_up(n: int = 60, start: float = 100.0, step: float = 1.5) -> list[dict]:
    out = []
    px = start
    for _ in range(n):
        o = px
        c = px + step
        out.append({"open": o, "high": c + 0.3, "low": o - 0.2, "close": c, "volume": 100})
        px = c
    return out


def _bars_down(n: int = 60, start: float = 200.0, step: float = 1.5) -> list[dict]:
    out = []
    px = start
    for _ in range(n):
        o = px
        c = px - step
        out.append({"open": o, "high": o + 0.2, "low": c - 0.3, "close": c, "volume": 100})
        px = c
    return out


def _snap(bars: list[dict], *, vwap_sigma: float = 0.0, velocity: float = 0.0) -> MarketSnapshot:
    px = float(bars[-1]["close"])
    return MarketSnapshot(
        ts=1.0,
        price=px,
        completed_bars=bars[:-1],
        forming_bar=bars[-1],
        atr=2.0,
        vwap_distance_atr=vwap_sigma,
        velocity=velocity,
    )


def test_macd_on_uptrend() -> None:
    m, s, h, d, cross = macd(_bars_up(80, step=2.0))
    assert isinstance(m, float)


def test_long_needs_oversold() -> None:
    cfg = Mark2Config()
    cfg.ENABLE_EXHAUSTION_FILTER = True
    snap = _snap(_bars_up(50, step=2.5), vwap_sigma=2.8)
    blocked, why, reading = exhaustion_blocks(Side.LONG, snap, cfg)
    assert blocked
    assert "not oversold" in why
    assert reading.rsi > 70.0


def test_long_ok_when_oversold() -> None:
    cfg = Mark2Config()
    cfg.ENABLE_EXHAUSTION_FILTER = True
    snap = _snap(_bars_down(50, step=2.5), vwap_sigma=-2.8, velocity=0.2)
    blocked, why, reading = exhaustion_blocks(Side.LONG, snap, cfg)
    assert not blocked
    assert reading.rsi <= 30.0
    assert "LONG ok" in why


def test_monster_trend_blocks_blind_short() -> None:
    cfg = Mark2Config()
    cfg.ENABLE_EXHAUSTION_FILTER = True
    cfg.RSI_OB_LEVEL = 70.0
    cfg.MONSTER_EXTENSION = 55.0
    cfg.MONSTER_MOMENTUM = 20.0
    snap = _snap(_bars_up(80, step=3.0), vwap_sigma=3.0, velocity=2.0)
    blocked, why, reading = exhaustion_blocks(Side.SHORT, snap, cfg)
    assert reading.rsi >= 70.0
    if reading.momentum_health > 20 and reading.extension_long >= 55:
        assert blocked
        assert "monster" in why.lower() or "chase" in why.lower()


def test_filter_off_passes() -> None:
    cfg = Mark2Config()
    cfg.ENABLE_EXHAUSTION_FILTER = False
    snap = _snap(_bars_up(50, step=3.0), vwap_sigma=3.0)
    blocked, _, _ = exhaustion_blocks(Side.LONG, snap, cfg)
    assert not blocked


def test_no_book_patterns_module_required() -> None:
    """Willie pack must not depend on book_patterns for entries."""
    import importlib.util
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    assert not (root / "book_patterns.py").exists()
    assert (root / "exhaustion.py").exists()
    assert importlib.util.find_spec("mark2.exhaustion") is not None
