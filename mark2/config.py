"""Centralized Recon Sniper configuration. Do not scatter experimental thresholds."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .types import RunMode, TargetMode, TrailMode

DEFAULTS_PATH = Path(__file__).resolve().parent / "mark2_defaults.json"
SETTINGS_PATH = Path(__file__).resolve().parent / "mark2_settings.json"


@dataclass
class ConfidenceWeights:
    trend_alignment: float = 0.18
    trend_strength: float = 0.08
    momentum: float = 0.14
    impulse: float = 0.12
    relative_volume: float = 0.08
    volume_acceleration: float = 0.08
    velocity: float = 0.10
    acceleration: float = 0.08
    breakout_quality: float = 0.06
    structure: float = 0.04
    volatility_state: float = 0.02
    vwap_relationship: float = 0.02

    def normalized(self) -> dict[str, float]:
        raw = asdict(self)
        total = sum(max(0.0, float(v)) for v in raw.values()) or 1.0
        return {k: max(0.0, float(v)) / total for k, v in raw.items()}


@dataclass
class EventToggles:
    momentum_expansion: bool = True
    volume_expansion: bool = True
    breakout: bool = True
    pullback_continuation: bool = True
    volatility_expansion: bool = True
    impulse: bool = True


@dataclass
class Mark2Config:
    MARK2_ENABLED: bool = True
    MODE: str = RunMode.LIVE.value

    LONG_CONFIDENCE_THRESHOLD: float = 58.0
    SHORT_CONFIDENCE_THRESHOLD: float = 58.0
    MIN_CONFIDENCE_VELOCITY: float = 0.35
    MOMENTUM_BUILD_TICKS: int = 3
    REQUIRE_CANDLE_ALIGNMENT: bool = True
    CANDLE_ALIGN_LOOKBACK: int = 3
    CANDLE_ALIGN_MIN_BARS: int = 1
    CANDLE_RELAX_IN_CHOPPY: bool = True
    CANDLE_MIN_FORMING_BODY_RATIO: float = 0.2
    REQUIRE_TICK_VELOCITY: bool = True
    MIN_TICK_VELOCITY: float = 0.08
    LONG_OPPORTUNITY_THRESHOLD: float = 52.0
    SHORT_OPPORTUNITY_THRESHOLD: float = 52.0
    MAX_EXTENSION_RISK: float = 72.0
    EVENT_RESET_THRESHOLD: float = 42.0
    EVENT_END_STREAK: int = 4
    REQUIRE_TRENDING: bool = True
    REQUIRE_STRUCTURE_ALIGNMENT: bool = True
    ENTRY_STRICTNESS: float = 45.0
    ENTRY_TUNING_CUSTOM: bool = False
    MIN_RELATIVE_VOLUME: float = 0.45
    MIN_RELATIVE_VOLUME_TREND: float = 0.35
    MIN_RELATIVE_VOLUME_HIGH_VOL: float = 0.32
    VOLUME_VEL_BYPASS: float = 0.25
    BIAS_STRONG_TICK_VEL: float = 0.25
    BIAS_ALLOW_ZERO_BARS: bool = True
    CHOP_CANDLE_MIN_BARS: int = 0

    VOLATILITY_TRAIL_MULTIPLIER: float = 1.6
    RUNNER_THRESHOLD: float = 70.0
    SCRATCH_THRESHOLD: float = 28.0
    SCRATCH_MAX_SECONDS: float = 0.0
    ENABLE_THESIS_ABORT: bool = False
    ENABLE_MOMENTUM_EXIT: bool = False
    ENABLE_EVENT_ABORT: bool = False
    MOMENTUM_EXIT_VEL: float = -0.12
    MOMENTUM_EXIT_CONF_VEL: float = -0.15
    MOMENTUM_EXIT_OPP_LEAD: float = 10.0
    MOMENTUM_EXIT_STREAK: int = 2
    MOMENTUM_EXIT_HEALTH_DROP: float = 22.0
    ENABLE_CHOP_SCALP: bool = False
    ENABLE_CHOPPY_BIAS: bool = False
    ENABLE_BOOK_PATTERNS: bool = False
    ENABLE_EXHAUSTION_FILTER: bool = False
    ENABLE_EMA_STRATEGY: bool = True
    # False = EMA is the only auto/manual entry path (product). Tests leave True.
    ALLOW_LEGACY_ENTRIES: bool = True
    EMA_FAST: int = 9
    EMA_MID: int = 20
    EMA_SLOW: int = 50
    EMA_INTERSECT_POINTS: float = 30.0
    EMA_INTERSECT_ATR: float = 2.0
    EMA_THROUGH_SLACK: float = 0.25
    EMA_INTERSECT_SHORT: bool = True
    # Intersection setup: 20/50 must already be spread before the 9/20 cross.
    # Score is |EMA20-EMA50| / ATR. 0.20 is the starting floor so replay can
    # compare runner rates; raise toward 0.60 if the tape shows it.
    EMA_REQUIRE_SEP: bool = True
    EMA_MIN_SEP_ATR: float = 0.20
    EMA_MIN_SEP_POINTS: float = 3.0
    EMA_SEP_LOOKBACK: int = 8
    EMA_SEP_MIN_BARS: int = 3
    EMA_SETUP_MAX_BARS: int = 16
    # EMA 9/20 playback refinements. Blue (EMA_SLOW) stays plot/log only.
    FAST_EMA: int = 9
    SLOW_EMA: int = 20
    REFERENCE_EMA: int = 50
    MAX_ENTRY_EXTENSION_ATR: float = 0.60
    EMA_ALLOW_LONG: bool = True
    EMA_ALLOW_SHORT: bool = True
    # Second eyes on 9/20/50. Can hold a missed fire and take shorts the sniper skips.
    ENABLE_AI_SCOUT: bool = True
    AI_SCOUT_LONG: bool = True
    AI_SCOUT_SHORT: bool = True
    AI_SCOUT_OVERRIDE_MISSED: bool = True
    AI_SCOUT_PAPER_FALLBACK: bool = True
    AI_SCOUT_MAX_EXT_ATR: float = 2.8
    # Wall-clock pause after a flatten so the next hop is not the same trade.
    EMA_REENTRY_COOLDOWN_SEC: float = 5.0
    # Intersection only. Leftover stacks, chop whites, and fade shorts stay off.
    EMA_BULL_FADE_SHORT: bool = False
    EMA_LONG_SNIPER: bool = True
    EMA_LONG_REQUIRES_BEARISH: bool = False
    EMA_SHORT_SNIPER: bool = True
    EMA_SHORT_REQUIRES_BULLISH: bool = False
    EMA_RSI_LONG: bool = False
    RSI_LONG_LEVEL: float = 65.0
    RSI_LONG_TRAIL_POINTS: float = 3.0
    BULL_LONG_TARGET_POINTS: float = 5.5
    BULL_TRAIL_ARM_POINTS: float = 3.0
    BULL_TRAIL_POINTS: float = 3.0
    BULL_USE_SNIPER_DOLLAR_LOCK: bool = False
    # Choppy: red/white cross only. $150 take-profit, else hold to white down-cross.
    EMA_CHOP_LONG: bool = False
    CHOP_TARGET_USD: float = 150.0
    CHOP_RSI_FADE: float = 5.0
    # Unused. Old 4.5 chop yank stays off.
    CHOPPY_TARGET_POINTS: float = 4.5
    CHOPPY_TRAIL_POINTS: float = 4.5
    # Kept for logs / HUD. Not an entry trigger.
    EMA_BLUE_APPROACH_ATR: float = 0.35
    ENABLE_PULLBACK_ENTRY: bool = True
    PULLBACK_ENTRY_ON_BAR_CLOSE: bool = True
    MAX_PULLBACK_WAIT_BARS: int = 10
    CATASTROPHIC_STOP_ATR: float = 1.50
    INITIAL_STOP_ATR: float = 1.50
    # 10-5 exit: 10-pt hard stop, $15 arm, 5.5-pt tip trail.
    EMA_CLASSIC_EXIT: bool = False
    EMA_HARD_STOP_POINTS: float = 10.0
    CONFIRMED_TREND_TRIGGER_ATR: float = 1.0
    CONFIRMED_STOP_ATR_FROM_ENTRY: float = 0.25
    # Runner: keep (1 - giveback) of MFE. Floor only ratchets up.
    MFE_GIVEBACK_FRAC: float = 0.30
    # Exit when EMA cluster loses this fraction of its peak spread.
    SPREAD_COMPRESS_FRAC: float = 0.35
    OPPOSITE_CROSS_REQUIRES_CONFIRM: bool = True
    # Off until we want red/white flip to flatten.
    EMA_OPPOSITE_CROSS_EXIT: bool = False
    ENABLE_RUNNER_TRAIL: bool = True
    RUNNER_TRIGGER_ATR: float = 2.0
    RUNNER_TRAIL_ATR: float = 1.25
    RUNNER_STRUCTURE_EXIT: bool = True
    DYNAMIC_EMA_TRAIL: bool = False
    RUNNER_HIGH_VOL_TRAIL: bool = False
    DOLLAR_LOCK_1_TRIGGER: float = 100.0
    DOLLAR_LOCK_1_PROFIT: float = 80.0
    DOLLAR_LOCK_2_TRIGGER: float = 150.0
    DOLLAR_LOCK_2_PROFIT: float = 97.5
    DOLLAR_LOCK_3_TRIGGER: float = 250.0
    DOLLAR_LOCK_3_PERCENT: float = 0.80
    TRAIL_PROFIT_KEEP: float = 0.80
    RUNNER_KEEP_HIGH_USD: float = 550.0
    RUNNER_KEEP_HIGH_PEAK_USD: float = 600.0
    TIP_TRAIL_ARM_USD: float = 100.0
    TIP_TRAIL_STEP_USD: float = 25.0
    TIP_TRAIL_START_POINTS: float = 7.5
    TIP_TRAIL_HIGH_POINTS: float = 25.0
    TIP_TRAIL_STALL_POINTS: float = 3.0
    TIP_TRAIL_STALL_BARS: int = 4
    # Brent-only $50 stall bank. Willie stays on the $100 trail / $80 floor.
    ENABLE_GROW_MODE: bool = False
    STARTING_EQUITY_USD: float = 250.0
    GROW_UNTIL_USD: float = 600.0
    GROW_GRAB_USD: float = 50.0
    GROW_KEEP_FRAC: float = 0.80
    GROW_STALL_BARS: int = 3
    GROW_STALL_KEEP_FRAC: float = 0.75
    # After a peak: 3 reds (long) / 3 greens (short) + real giveback → flatten.
    ADVERSE_STACK_BARS: int = 3
    ADVERSE_STACK_MIN_USD: float = 20.0
    ADVERSE_STACK_GIVE_ATR: float = 0.25
    TRAIL_PROFIT_KEEP_EARLY: float = 0.0
    TRAIL_KEEP_ARM_USD: float = 100.0
    TRAIL_KEEP_TIGHT_USD: float = 250.0
    TRAIL_KEEP_FLOOR_USD: float = 20.0
    RUNNER_TRAIL_MIN_ATR: float = 1.25
    RUNNER_TRAIL_WIDE_SPREAD_ATR: float = 0.35
    RUNNER_TRAIL_TIGHT_SPREAD_ATR: float = 0.08
    RUNNER_TRAIL_HIGH_VOL_ATR: float = 1.25
    MIN_BARS_SINCE_LAST_CROSS: int = 0
    EXHAUSTION_BLOCK_SCORE: float = 72.0
    RSI_PERIOD: int = 14
    RSI_OB_LEVEL: float = 75.0
    RSI_OS_LEVEL: float = 25.0
    BB_PERIOD: int = 20
    BB_STD: float = 2.0
    MACD_FAST: int = 12
    MACD_SLOW: int = 26
    MACD_SIGNAL: int = 9
    MONSTER_EXTENSION: float = 70.0
    MONSTER_MOMENTUM: float = 35.0
    # Deep hold: no real stop until $300 open profit (or market flip → tip trail).
    ENABLE_DEEP_HOLD: bool = False
    DEEP_HOLD_ARM_USD: float = 300.0
    DEEP_HOLD_STOP_POINTS: float = 180.0
    DEEP_HOLD_FLIP_TRAIL: float = 5.5
    # After +$15: tip trail on. Stall/chop → aggressively tighten.
    EARLY_TRAIL_ARM_USD: float = 15.0
    STALL_TRAIL_ROOM: float = 0.75
    STALL_SEC: float = 6.0
    STALL_BAR_ATR_FRAC: float = 0.32
    STALL_VEL_FRAC: float = 0.12
    CHOP_SCALP_CONFIDENCE: float = 52.0
    CHOP_SCALP_OPPORTUNITY: float = 55.0
    CHOP_SCALP_MIN_TICK_VEL: float = 0.20
    CHOP_SCALP_MIN_CONF_VEL: float = 0.18
    CHOP_SCALP_DIRECTION_GAP: float = 10.0
    CHOP_SCALP_BUILD_TICKS: int = 2
    CHOP_SCALP_MAX_EXTENSION: float = 55.0
    CHOP_SCALP_BANK_DOLLARS: float = 15.0
    CHOP_SCALP_STOP_POINTS: float = 10.0
    CHOP_SCALP_TRAIL_POINTS: float = 3.0
    APPROACH_TRAIL_POINTS: float = 5.5
    TRAIL_ARM_POINTS: float = 6.0
    TRAIL_TIGHTEN_AT_TARGET: float = 0.35
    TRAIL_TIGHTEN_BUFFER: float = 1.0
    CHOP_SCALP_COOLDOWN_SEC: float = 1.5
    CHOP_SCALP_EXIT_HEALTH: float = 35.0
    CHOP_SCALP_EXIT_VEL: float = -0.08
    CHOP_SCALP_EXIT_CONF_VEL: float = -0.1
    CHOP_SCALP_EXIT_OPP_LEAD: float = 8.0
    CHOP_SCALP_EXIT_HEALTH_DROP: float = 15.0
    CHOP_SCALP_EXIT_STREAK: int = 1
    ENABLE_CHAOTIC_BANK: bool = True
    CHAOTIC_BANK_CONFIDENCE: float = 55.0
    CHAOTIC_BANK_OPPORTUNITY: float = 54.0
    CHAOTIC_BANK_MIN_TICK_VEL: float = 0.28
    CHAOTIC_BANK_MIN_CONF_VEL: float = 0.22
    CHAOTIC_BANK_DIRECTION_GAP: float = 8.0
    CHAOTIC_BANK_BUILD_TICKS: int = 2
    CHAOTIC_BANK_MAX_EXTENSION: float = 65.0
    CHAOTIC_BANK_BANK_DOLLARS: float = 25.0
    CHAOTIC_BANK_STOP_POINTS: float = 14.0
    CHAOTIC_BANK_APPROACH_TRAIL: float = 3.0
    CHAOTIC_BANK_RUNNER_TRAIL: float = 3.0
    CHAOTIC_BANK_TRAIL_ARM: float = 1.0
    CHAOTIC_BANK_GREEN_LOCK: float = 1.0
    CHAOTIC_BANK_COOLDOWN_SEC: float = 2.0
    CHAOTIC_CANDLE_MIN_BARS: int = 0
    ENABLE_EXPERIMENTAL_PROFILE: bool = False
    EXPERIMENTAL_DIRECTION_GAP: float = 8.0
    EXPERIMENTAL_REQUIRE_QUALITY: bool = True
    EXPERIMENTAL_REQUIRE_SIDE_AGREEMENT: bool = True
    ENABLE_BIAS_ENTRY_ADJUST: bool = True
    BIAS_SHORT_CONF_DELTA: float = 4.0
    BIAS_SHORT_OPP_DELTA: float = 2.0
    BIAS_SHORT_CONF_VEL_DELTA: float = 0.12
    BIAS_LONG_CONF_DELTA: float = 4.0
    BIAS_LONG_OPP_DELTA: float = 2.0
    BIAS_LONG_CONF_VEL_DELTA: float = 0.12
    BIAS_RELAX_CANDLES: bool = True
    BIAS_MIN_TICK_VEL: float = 0.05
    BIAS_BUILD_TICKS: int = 2
    BIAS_DIRECTION_GAP: float = 6.0
    BIAS_HIGH_VOL_CONF_DELTA: float = 2.0
    BIAS_HIGH_VOL_OPP_DELTA: float = 1.0
    BIAS_HIGH_VOL_TICK_VEL: float = 0.02
    BIAS_HIGH_VOL_MIN_RVOL: float = 0.18
    BIAS_HIGH_VOL_CONF_FLOOR: float = 46.0
    BIAS_HIGH_VOL_RELAX_FORMING: bool = True
    BIAS_HIGH_VOL_RELAX_STRUCTURE: bool = True
    ENTRY_GRACE_SECONDS: float = 0.0
    ABORT_REQUIRES_GREEN: bool = True
    ENABLE_GREEN_FLOOR: bool = True
    THESIS_ABORT_COOLDOWN_SEC: float = 10.0
    DEFAULT_EXIT_COOLDOWN_SEC: float = 3.0
    TARGET_MODE: str = TargetMode.CHECKPOINT.value
    TRAIL_MODE: str = TrailMode.ATR_ADAPTIVE.value
    INITIAL_TARGET_ATR_MULT: float = 1.1
    BANK_DOLLARS_PER_CONTRACT: float = 25.0
    # Total open-profit $ to arm tip trail (NOT per-contract). 3 MNQ @ $15 ≈ 2.5 pts.
    TRAIL_ARM_USD: float = 15.0
    # Legacy HUD toggle. Default OFF — live exit is the $100 AI tip trail.
    ENABLE_RECON_TIP_TRAIL: bool = False
    RECON_TIP_TRAIL_POINTS: float = 5.5
    RUNNER_TRAIL_POINTS: float = 5.5
    CONTRACTS: int = 1
    INITIAL_STOP_POINTS: float = 20.0

    VELOCITY_WINDOW_SEC: float = 2.0
    ACCEL_WINDOW_SEC: float = 1.0
    CONFIDENCE_SMOOTH_ALPHA: float = 0.35
    CONFIDENCE_DERIV_WINDOW: int = 6
    OPPORTUNITY_BASELINE_BARS: int = 20
    TICK_BUFFER: int = 80

    CONTEXT_REFRESH_SEC: float = 5.0
    ATR_PERIOD: int = 14
    EMA_PERIOD: int = 20
    VWAP_LOOKBACK: int = 80
    VOLUME_LOOKBACK: int = 20

    MOMENTUM_Z: float = 1.4
    VOLUME_Z: float = 1.3
    VELOCITY_Z: float = 1.4
    IMPULSE_MIN: float = 55.0

    MAX_CONTRACTS: int = 10
    MAX_LOSS_DOLLARS: float = 50000.0
    ENABLE_DAILY_GOAL: bool = True
    DAILY_GOAL_DOLLARS: float = 350.0
    # Hunt green-line / trail arm — total open $ (same idea as TRAIL_ARM_USD).
    GOAL_HUNT_BANK_DOLLARS: float = 15.0
    GOAL_NEAR_DOLLARS: float = 30.0
    GOAL_NEAR_BANK_DOLLARS: float = 10.0
    GOAL_WINDOW_HOURS: float = 2.0
    GOAL_PRESSURE_ENABLED: bool = True
    GOAL_HUNT_OPEN_GATES: bool = True
    GOAL_PRESSURE_HUNT_FLOOR: float = 0.0
    GOAL_PRESSURE_MIN_BANK: float = 15.0
    GOAL_PRESSURE_CONF_CUT: float = 6.0
    GOAL_PRESSURE_OPP_CUT: float = 5.0
    GOAL_PRESSURE_VEL_CUT: float = 0.12
    GOAL_PRESSURE_BUILD_CUT: int = 1
    GOAL_PRESSURE_GAP_CUT: float = 2.0
    GOAL_PRESSURE_CHOP_AT: float = 0.65
    GOAL_PRESSURE_CANDLE_AT: float = 0.7
    GOAL_PRESSURE_COOLDOWN_SEC: float = 2.5
    GOAL_PRESSURE_CONF_FLOOR: float = 48.0
    GOAL_PRESSURE_OPP_FLOOR: float = 45.0
    GOAL_PRESSURE_VEL_FLOOR: float = 0.08
    POINT_VALUE: float = 2.0
    TICK_SIZE: float = 0.25
    BRIDGE_PORT: int = 5564
    PRODUCT_NAME: str = "RECON SNIPER"
    PRODUCT_ENGINE: str = "ReconSniper"
    KICKER_LONG: str = "RECON LONG"
    KICKER_SHORT: str = "RECON SHORT"
    BRIDGE_NAME: str = "ReconSniperBridge"

    weights: ConfidenceWeights = field(default_factory=ConfidenceWeights)
    events: EventToggles = field(default_factory=EventToggles)

    def contracts(self) -> int:
        cap = max(1, int(self.MAX_CONTRACTS))
        try:
            n = int(self.CONTRACTS)
        except (TypeError, ValueError):
            n = 1
        return max(1, min(cap, n))

    def mode(self) -> RunMode:
        try:
            return RunMode(str(self.MODE or "LIVE"))
        except ValueError:
            return RunMode.LIVE

    def target_mode(self) -> TargetMode:
        try:
            return TargetMode(str(self.TARGET_MODE or "CHECKPOINT"))
        except ValueError:
            return TargetMode.CHECKPOINT

    def trail_mode(self) -> TrailMode:
        try:
            return TrailMode(str(self.TRAIL_MODE or "ATR_ADAPTIVE"))
        except ValueError:
            return TrailMode.ATR_ADAPTIVE

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _apply_json_file(cfg: Mark2Config, path: Path) -> None:
    if not path.exists():
        return
    raw = json.loads(path.read_text(encoding="utf-8"))
    weights = raw.pop("weights", None) or {}
    events = raw.pop("events", None) or {}
    for k, v in raw.items():
        if hasattr(cfg, k):
            setattr(cfg, k, v)
    for k, v in weights.items():
        if hasattr(cfg.weights, k):
            setattr(cfg.weights, k, float(v))
    for k, v in events.items():
        if hasattr(cfg.events, k):
            setattr(cfg.events, k, bool(v))


def thesis_abort_enabled(cfg: Mark2Config) -> bool:
    """Willie: FAILED_EVENT / thesis abort permanently disabled."""
    return False


def _force_willie_abort_flags_off(cfg: Mark2Config) -> None:
    """Stale or explicit mark2_settings.json cannot re-arm event/thesis exits."""
    cfg.ENABLE_THESIS_ABORT = False
    cfg.ENABLE_MOMENTUM_EXIT = False
    cfg.ENABLE_EVENT_ABORT = False


def load_config(path: Path | None = None) -> Mark2Config:
    cfg = Mark2Config()
    _apply_json_file(cfg, DEFAULTS_PATH)
    settings = path if path is not None else SETTINGS_PATH
    user_raw: dict[str, Any] = {}
    if settings.exists():
        user_raw = json.loads(settings.read_text(encoding="utf-8"))
    _apply_json_file(cfg, settings)
    if bool(getattr(cfg, "ENABLE_EXPERIMENTAL_PROFILE", False)):
        from .experimental import EXPERIMENTAL_PROFILE, apply_experimental_profile

        apply_experimental_profile(cfg)
        # Saved settings for EXP gate keys win over baked Phase 1 defaults.
        for key in EXPERIMENTAL_PROFILE:
            if key in user_raw and hasattr(cfg, key):
                setattr(cfg, key, user_raw[key])
    _force_willie_abort_flags_off(cfg)
    return cfg


def save_config(cfg: Mark2Config, path: Path | None = None) -> None:
    p = path or SETTINGS_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(cfg.to_dict(), indent=2), encoding="utf-8")
