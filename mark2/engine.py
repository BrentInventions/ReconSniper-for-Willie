"""Recon Sniper event-driven engine. Standalone — no TradeChampion / ImpulseRuntime."""

from __future__ import annotations

import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

from .analytics import EngineStats
from .config import Mark2Config, load_config, save_config, SETTINGS_PATH
from .confidence import ConfidenceEngine
from .context import MarketContext
from .events import EventDetector
from .execution import ExecutionEngine, Intent
from .exits import (
    PaperTrade,
    bank_dollars,
    bank_points,
    broker_stop_for_nt,
    deep_hold_active,
    goal_hunt_bank_dollars,
    goal_hunting,
    initial_stop,
    initial_target,
    lock_price,
    manage_manual_hold,
    manage_paper,
    stop_points,
    trail_arm_usd,
)
from .goal_pressure import (
    GoalPressure,
    apply_pressure_to_chop_needs,
    compute_goal_pressure,
)
from .entry_gates import candle_min_aligned, volume_entry_ok
from .entry_tuning import (
    apply_custom_gate,
    apply_strictness,
    apply_toggles,
    enter_book_mode,
    enforce_book_mode_gates,
    enforce_ema_mode_gates,
    exit_book_mode,
    reset_to_defaults,
    snapshot as entry_tuning_snapshot,
)
from .experimental import (
    directional_agreement_ok,
    entry_quality_ok,
    is_experimental,
    snapshot_experimental,
    toggle_experimental,
)
from .candle_align import candle_alignment_ok
from .bias_entry import bias_entry_gates
from .chaotic_bank import chaotic_entry_ok
from .chop_scalp import chop_entry_ok
from .book_patterns import BookPatternDetector, book_pattern_entry_ok
from .ai_scout import ScoutView, scout_opportunity
from .ema_strategy import (
    EmaPullbackSetup,
    atr14,
    bars_are_sequential,
    chop_target_points,
    cross_side,
    crossover_note,
    ema_atr_stop,
    ema_dataset_row,
    ema_entry_signal,
    ema_line_lamps,
    ema_long_arm_ok,
    ema_short_arm_ok,
    long_sniper_reason,
    short_sniper_reason,
    short_stack_lock_should_clear,
    stack_lock_should_clear,
    ema_cluster_spread,
    ema_structure_valid,
    IntersectionStatus,
    intersection_status,
    entry_extension_ok,
    indicator_snapshot,
    manage_ema_hold,
    mfe_giveback_floor_pts,
    profit_keep_usd,
    grow_mode_active,
    runner_tip_trail_pts,
    trailing_adverse_bars,
    _one_r_points,
    pullback_zone_ok,
    read_ema_stack,
    red_above_white_and_blue,
    red_below_white_and_blue,
    red_clears_white_and_blue,
    red_rising,
)
from .exhaustion import exhaustion_blocks, read_exhaustion
from .extension import ExtensionFilter
from .features import FastFeatures
from .logger import AnalyticsLogger
from .opportunity import OpportunityEngine
from .risk import RiskGate
from .state_machine import Mark2StateMachine
from .trade_health import TradeHealthEngine
from .types import (
    EngineState,
    EventRecord,
    EventType,
    MarketSnapshot,
    RejectReason,
    RunMode,
    ScoreBundle,
    Side,
    Tick,
)

ROUND_TURN_FEES = 2.48
LEDGER_MAX = 120


class Mark2Engine:
    def __init__(
        self,
        cfg: Mark2Config | None = None,
        *,
        log_path: Path | None = None,
        sink=None,
        persist_path: Path | None = None,
    ) -> None:
        self.cfg = cfg or load_config()
        self._persist_path = persist_path or SETTINGS_PATH
        self.context = MarketContext(self.cfg)
        self.fast = FastFeatures(self.cfg)
        self.confidence = ConfidenceEngine(self.cfg)
        self.opportunity = OpportunityEngine(self.cfg)
        self.extension = ExtensionFilter(self.cfg)
        self.events = EventDetector(self.cfg)
        self.sm = Mark2StateMachine(self.cfg)
        self.health = TradeHealthEngine(self.cfg)
        self.risk = RiskGate(self.cfg)
        self.execution = ExecutionEngine(self.cfg, self.risk, sink=sink)
        logs = Path(__file__).resolve().parent / "logs"
        self.log = AnalyticsLogger(log_path or (logs / "mark2_decisions.jsonl"))
        self.stats = EngineStats(engine=str(self.cfg.PRODUCT_ENGINE or "ReconSniper"))
        self.closed_trades: deque[dict[str, Any]] = deque(maxlen=LEDGER_MAX)
        self.completed_bars: list[dict] = []
        self.forming_bar: Optional[dict] = None
        self.paper: Optional[PaperTrade] = None
        self.last_snap: Optional[MarketSnapshot] = None
        self.last_scores: Optional[ScoreBundle] = None
        self.last_reject: RejectReason = RejectReason.NONE
        self._entry_profile = "default"
        self.recent_decisions: deque[dict[str, Any]] = deque(maxlen=80)
        self._last_log_key: tuple | None = None
        self._last_manage_state: str = ""
        self._last_levels: tuple | None = None
        self._last_nt_stop: float | None = None
        self.account: dict[str, Any] = {}
        # NT realized+unrealized at last HUD session reset (clear_session).
        self._session_pnl_baseline: float | None = None
        self.goal_met = False
        self._goal_started_ts: float | None = None
        self._book_mode_restore: dict[str, bool] | None = None
        self._book_detector = BookPatternDetector(enabled=True)
        self._book_flash: dict[str, Any] | None = None
        self._book_flash_key: str = ""
        self._last_entry_tags: dict[str, Any] = {}
        self._ema_pending_side: Side = Side.NONE
        self._ema_pending_why: str = ""
        self._ema_pullback: EmaPullbackSetup | None = None
        self._ema_pullback_fill: dict | None = None
        self._ema_exit_armed: bool = False
        self._ema_armed: bool = False
        self._ema_entry_bar_i: int = 0
        self._ema_last_bar_time: str = ""
        self._ema_last_cross_bar_i: int = -10**9
        self._ema_log_bars_since: int = 0
        self._ema_reversal_side: Side = Side.NONE
        self._last_ema_overlay: tuple | None = None
        self._ema_watch: str = ""
        self._ema_watch_logged: str = ""
        self._ema_long_stack_taken: bool = False
        self._ema_short_stack_taken: bool = False
        self._ema_rsi_peak: float = 0.0
        self._ema_fade_armed: bool = False
        self._ema_fade_block_bar_i: int = -1
        self._scout_view: ScoutView = ScoutView()
        self._scout_missed_side: Side = Side.NONE
        self._scout_missed_why: str = ""
        self._scout_long_taken: bool = False
        self._scout_short_taken: bool = False
        self._last_exit_wall: float = 0.0
        if bool(getattr(self.cfg, "ENABLE_BOOK_PATTERNS", False)):
            # Settings already had book mode — lock the book-only gate set.
            enforce_book_mode_gates(self.cfg)
        if bool(getattr(self.cfg, "ENABLE_EMA_STRATEGY", False)):
            enforce_ema_mode_gates(self.cfg)
        if not bool(getattr(self.cfg, "ENTRY_TUNING_CUSTOM", False)):
            apply_strictness(self.cfg, float(getattr(self.cfg, "ENTRY_STRICTNESS", 45.0)), custom=False)

    def _bot_session_pnl(self) -> float:
        closed = float(self.stats.summary().get("net_pnl_after_fees") or 0)
        open_pnl = 0.0
        if self.paper is not None:
            px = self.last_snap.price if self.last_snap else self.paper.entry
            open_pnl = _open_pnl(self.paper, px, self.cfg)
        return round(closed + open_pnl, 2)

    def _choose_session_pnl(self, bot_session: float) -> tuple[float, bool]:
        """NT session when NT is marking the book; paper-only uses the tape."""
        nt = self._session_pnl_from_nt()
        paper_only = self.paper is not None and int(self.account.get("position") or 0) == 0
        if paper_only:
            return float(bot_session), False
        if nt is not None:
            return float(nt), True
        return float(bot_session), False

    def _live_session_pnl(self) -> float:
        bot = self._bot_session_pnl()
        chosen, _ = self._choose_session_pnl(bot)
        return float(chosen)

    def _goal_window_ok(self, *, ts: float | None = None) -> bool:
        try:
            window_h = float(getattr(self.cfg, "GOAL_WINDOW_HOURS", 0) or 0)
        except (TypeError, ValueError):
            window_h = 0.0
        if window_h <= 0:
            return True
        now = float(ts) if ts is not None else (
            float(self.last_snap.ts) if self.last_snap is not None else 0.0
        )
        if now <= 0:
            return True
        if self._goal_started_ts is None:
            self._goal_started_ts = now
            return True
        return (now - self._goal_started_ts) <= window_h * 3600.0

    def _goal_active(self, *, ts: float | None = None) -> bool:
        if not goal_hunting(self.cfg, goal_met=self.goal_met):
            return False
        return self._goal_window_ok(ts=ts)

    def _goal_bank_for_entry(self, *, ts: float | None = None) -> tuple[bool, float]:
        if not self._goal_active(ts=ts):
            return False, 0.0
        pressure = self._goal_pressure(ts=ts)
        if pressure.active and pressure.pressure > 0:
            return True, float(pressure.bank_dollars)
        bank = goal_hunt_bank_dollars(self.cfg, session_pnl=self._live_session_pnl())
        return True, bank

    def _goal_pressure(self, *, ts: float | None = None) -> GoalPressure:
        now = float(ts) if ts is not None else self._goal_now_ts()
        # Arms the clock on first hunt tick; expired / goal-met → no pressure.
        if not self._goal_active(ts=now if now > 0 else ts):
            return GoalPressure()
        return compute_goal_pressure(
            self.cfg,
            started_ts=self._goal_started_ts,
            now_ts=now if now > 0 else 0.0,
            session_pnl=self._live_session_pnl(),
            goal_met=self.goal_met,
        )

    def _maybe_complete_goal(self, *, tick: Tick | None = None) -> bool:
        """If session PnL hit the daily goal: disarm + block entries; leave open runner."""
        if self.goal_met:
            return True
        if not bool(getattr(self.cfg, "ENABLE_DAILY_GOAL", False)):
            return False
        goal = float(getattr(self.cfg, "DAILY_GOAL_DOLLARS", 0) or 0)
        if goal <= 0:
            return False
        pnl = self._live_session_pnl()
        if pnl + 1e-9 < goal:
            return False
        self.goal_met = True
        self.risk.goal_met = True
        # Never flatten an open runner for the goal — bank+trail owns the exit.
        self.set_enabled(False, persist=True)
        self.log.write(
            "MARK2_GOAL_HIT",
            goal=goal,
            sessionPnl=round(pnl, 2),
            openTrade=self.paper is not None,
        )
        self.recent_decisions.append(
            {
                "price": round(float(tick.price), 2) if tick else 0.0,
                "state": "GOAL",
                "direction": "—",
                "decision": "GOAL_HIT",
                "reject": "",
                "event": "DAILY_GOAL",
                "eventId": 0,
                "confidence": 0.0,
                "opportunity": 0.0,
                "extension": 0.0,
            }
        )
        return True

    def set_daily_goal(self, dollars: float, *, persist: bool = True) -> dict[str, Any]:
        try:
            goal = float(dollars)
        except (TypeError, ValueError):
            goal = 0.0
        goal = max(0.0, min(50_000.0, goal))
        self.cfg.DAILY_GOAL_DOLLARS = goal
        self.cfg.ENABLE_DAILY_GOAL = goal > 0
        if goal > 0 and self.goal_met and self._live_session_pnl() < goal:
            self.goal_met = False
            self.risk.goal_met = False
        if persist:
            self._persist()
        return self._goal_snapshot()

    def set_goal_enabled(self, on: bool, *, persist: bool = True) -> dict[str, Any]:
        self.cfg.ENABLE_DAILY_GOAL = bool(on)
        if not on:
            self.goal_met = False
            self.risk.goal_met = False
        elif self._live_session_pnl() >= float(getattr(self.cfg, "DAILY_GOAL_DOLLARS", 0) or 0) > 0:
            self.goal_met = True
            self.risk.goal_met = True
        if persist:
            self._persist()
        return self._goal_snapshot()

    def set_goal_window_hours(self, hours: float, *, persist: bool = True) -> dict[str, Any]:
        """Set how long the goal hunt clock runs. 0 = no time limit. Restarts the clock."""
        try:
            window = float(hours)
        except (TypeError, ValueError):
            window = 0.0
        window = max(0.0, min(24.0, window))
        # Snap to 15-minute steps so HUD ± controls stay clean.
        window = round(window * 4.0) / 4.0
        self.cfg.GOAL_WINDOW_HOURS = window
        if window <= 0:
            self._goal_started_ts = None
        else:
            now = self._goal_now_ts()
            # Arm from "now" so adjusting CLOCK starts a fresh time box.
            self._goal_started_ts = now if now > 0 else None
        if persist:
            self._persist()
        return self._goal_snapshot()

    def _goal_now_ts(self) -> float:
        if self.last_snap is not None and float(self.last_snap.ts) > 0:
            return float(self.last_snap.ts)
        return 0.0

    def _goal_snapshot(self) -> dict[str, Any]:
        goal = float(getattr(self.cfg, "DAILY_GOAL_DOLLARS", 0) or 0)
        enabled = bool(getattr(self.cfg, "ENABLE_DAILY_GOAL", False)) and goal > 0
        pnl = self._live_session_pnl()
        hunt, bank = self._goal_bank_for_entry()
        remaining = max(0.0, goal - pnl) if enabled else 0.0
        try:
            window_h = float(getattr(self.cfg, "GOAL_WINDOW_HOURS", 2.0))
        except (TypeError, ValueError):
            window_h = 2.0
        if window_h < 0:
            window_h = 0.0
        window_sec = window_h * 3600.0
        now = self._goal_now_ts()
        started = self._goal_started_ts
        if window_h <= 0:
            elapsed_sec = 0.0
            remain_sec = 0.0
            expired = False
            clock_on = False
        elif started is None or now <= 0:
            elapsed_sec = 0.0
            remain_sec = window_sec
            expired = False
            clock_on = enabled and not self.goal_met
        else:
            elapsed_sec = max(0.0, now - float(started))
            remain_sec = max(0.0, window_sec - elapsed_sec)
            expired = elapsed_sec > window_sec
            clock_on = enabled and not self.goal_met and not expired
        pressure = self._goal_pressure(ts=now if now > 0 else None)
        return {
            "enabled": enabled,
            "goal": goal,
            "sessionPnl": round(pnl, 2),
            "remaining": round(remaining, 2),
            "met": bool(self.goal_met),
            "hunting": bool(hunt) and not self.goal_met,
            "huntBank": round(bank if hunt else float(self.cfg.BANK_DOLLARS_PER_CONTRACT), 2),
            "windowHours": window_h,
            "windowSec": round(window_sec, 1),
            "elapsedSec": round(elapsed_sec, 1),
            "remainSec": round(remain_sec, 1),
            "clockOn": bool(clock_on),
            "expired": bool(expired),
            "pressure": pressure.pct if pressure.active else 0.0,
            "pressurePush": bool(pressure.active and pressure.pressure >= 0.5),
            "progressPct": round(min(100.0, 100.0 * pnl / goal), 1) if enabled and goal > 0 else 0.0,
        }

    @property
    def orders_submitted(self) -> int:
        return int(self.execution.orders_submitted)

    @property
    def mode(self) -> RunMode:
        return self.execution.mode

    def _persist(self) -> None:
        save_config(self.cfg, self._persist_path)

    def set_enabled(self, on: bool, *, persist: bool = True) -> bool:
        self.cfg.MARK2_ENABLED = bool(on)
        # Re-ARM after a goal hit only unlocks if session PnL is under the goal
        # (typically after clear_session). Otherwise the next tick would re-hit.
        if on and self.goal_met:
            goal = float(getattr(self.cfg, "DAILY_GOAL_DOLLARS", 0) or 0)
            if goal <= 0 or self._live_session_pnl() + 1e-9 < goal:
                self.goal_met = False
                self.risk.goal_met = False
        if persist:
            self._persist()
        return bool(self.cfg.MARK2_ENABLED)

    def set_mode(self, mode: str) -> str:
        try:
            self.cfg.MODE = RunMode(str(mode)).value
        except ValueError:
            self.cfg.MODE = RunMode.LIVE.value
        self._persist()
        return self.cfg.MODE

    def set_contracts(self, n: int, *, persist: bool = True) -> int:
        cap = max(1, int(self.cfg.MAX_CONTRACTS))
        try:
            qty = int(n)
        except (TypeError, ValueError):
            qty = 1
        self.cfg.CONTRACTS = max(1, min(cap, qty))
        if persist:
            self._persist()
        return self.cfg.contracts()

    def set_stop_points(self, pts: float, *, persist: bool = True) -> float:
        tick = max(float(self.cfg.TICK_SIZE), 0.25)
        try:
            raw = float(pts)
        except (TypeError, ValueError):
            raw = float(self.cfg.INITIAL_STOP_POINTS)
        val = max(tick * 4, min(80.0, round(raw * 4) / 4))
        self.cfg.INITIAL_STOP_POINTS = val
        if persist:
            self._persist()
        if self.paper is not None and not self.paper.target_touched:
            px = float(self.last_snap.price) if self.last_snap else self.paper.entry
            if self.paper.side == Side.LONG:
                ns = self.paper.entry - val
                if ns < px - tick:
                    self.paper.stop = ns
            else:
                ns = self.paper.entry + val
                if ns > px + tick:
                    self.paper.stop = ns
            self._last_levels = None
            self._push_levels()
        return val

    def set_entry_strictness(self, value: float, *, persist: bool = True) -> dict:
        apply_strictness(self.cfg, value, custom=False)
        if persist:
            self._persist()
        return entry_tuning_snapshot(self.cfg)

    def set_entry_gate(self, name: str, value: float, *, persist: bool = True) -> dict:
        apply_custom_gate(self.cfg, str(name), value)
        if persist:
            self._persist()
        return entry_tuning_snapshot(self.cfg)

    def set_entry_toggles(self, toggles: dict, *, persist: bool = True) -> dict:
        payload = dict(toggles or {})
        book_req = payload.pop("book_patterns", None)
        if book_req is not None:
            want_book = bool(book_req)
            was_book = bool(getattr(self.cfg, "ENABLE_BOOK_PATTERNS", False))
            if want_book and not was_book:
                self._book_mode_restore = enter_book_mode(self.cfg)
            elif not want_book and was_book:
                exit_book_mode(self.cfg, self._book_mode_restore)
                self._book_mode_restore = None
            elif want_book:
                enforce_book_mode_gates(self.cfg)
        if "experimental_profile" in payload:
            # Book mode refuses experimental — drop the request.
            if bool(getattr(self.cfg, "ENABLE_BOOK_PATTERNS", False)):
                payload.pop("experimental_profile", None)
            else:
                toggle_experimental(self.cfg, bool(payload.pop("experimental_profile")))
        ema_req = payload.pop("ema_strategy", None)
        if ema_req is not None:
            if bool(ema_req) and bool(getattr(self.cfg, "ENABLE_BOOK_PATTERNS", False)):
                exit_book_mode(self.cfg, self._book_mode_restore)
                self._book_mode_restore = None
            payload["ema_strategy"] = bool(ema_req)
        apply_toggles(self.cfg, payload)
        if bool(getattr(self.cfg, "ENABLE_BOOK_PATTERNS", False)):
            enforce_book_mode_gates(self.cfg)
        self._push_ema_overlay()
        if persist:
            self._persist()
        out = entry_tuning_snapshot(self.cfg)
        out["experimental"] = snapshot_experimental(self.cfg)
        return out

    def reset_entry_tuning(self, *, persist: bool = True) -> dict:
        self._book_mode_restore = None
        out = reset_to_defaults(self.cfg)
        toggle_experimental(self.cfg, False)
        if persist:
            self._persist()
        out["experimental"] = snapshot_experimental(self.cfg)
        return out

    def _note_decision(
        self,
        price: float,
        state: str,
        direction: Side,
        decision: str,
        reject: RejectReason,
        event: EventRecord | None,
        scores: ScoreBundle,
        *,
        exit_reason: str = "",
        tags: dict[str, Any] | None = None,
    ) -> None:
        side = direction if direction != Side.NONE else Side.LONG
        exh = {}
        if scores and isinstance(scores.contributors, dict):
            raw = scores.contributors.get("exhaustion")
            if isinstance(raw, dict):
                exh = raw
        row = {
            "price": round(price, 2),
            "state": state,
            "direction": direction.value,
            "decision": decision,
            "reject": reject.value if reject else "",
            "exitReason": str(exit_reason or ""),
            "event": event.event_type.value if event else "",
            "eventId": event.event_id if event else 0,
            "confidence": round(
                scores.long_confidence if side == Side.LONG else scores.short_confidence,
                1,
            ),
            "opportunity": round(
                scores.long_opportunity if side == Side.LONG else scores.short_opportunity,
                1,
            ),
            "extension": round(
                scores.extension_risk_long
                if side == Side.LONG
                else scores.extension_risk_short,
                1,
            ),
            "rsi": exh.get("rsi"),
            "book": (tags or {}).get("book") or self._last_entry_tags.get("book") or "",
            "rsiTag": (tags or {}).get("rsi") or self._last_entry_tags.get("rsi") or "",
            "trigger": (tags or {}).get("trigger")
            or self._last_entry_tags.get("trigger")
            or "",
        }
        if tags:
            row.update({k: v for k, v in tags.items() if k not in row})
        self.recent_decisions.append(row)

    def hud_snapshot(self) -> dict[str, Any]:
        try:
            return self._hud_snapshot()
        except Exception as exc:
            return {
                "product": str(getattr(self.cfg, "PRODUCT_NAME", None) or "RECON SNIPER"),
                "enabled": bool(getattr(self.cfg, "MARK2_ENABLED", False)),
                "mode": self.mode.value,
                "connected": bool(self.risk.connected),
                "state": "ERROR",
                "error": str(exc)[:160],
            }

    def _hud_snapshot(self) -> dict[str, Any]:
        snap = self.last_snap
        scores = self.last_scores
        ev = self.events.active
        paper = self.paper
        stats = self.stats.summary()
        pf = stats.get("profit_factor")
        if pf == float("inf"):
            stats["profit_factor"] = 99.99
            stats["primary_eval"]["profit_factor"] = 99.99
        closed_pnl = round(float(stats.get("net_pnl_after_fees") or 0), 2)
        open_pnl = 0.0
        if paper is not None:
            px = snap.price if snap else paper.entry
            open_pnl = round(_open_pnl(paper, px, self.cfg), 2)
        bot_session_pnl = round(closed_pnl + open_pnl, 2)
        session_pnl, from_nt = self._choose_session_pnl(bot_session_pnl)
        nt_session_pnl = session_pnl if from_nt else None
        last_exit: dict[str, Any] | None = None
        if self.closed_trades:
            row = self.closed_trades[0]
            last_exit = {
                "side": row.get("side", ""),
                "pnl": row.get("pnl", 0),
                "pts": row.get("pts", 0),
                "reason": row.get("reason", ""),
                "clock": row.get("clock", ""),
            }
        _hunt, _hunt_bank = self._goal_bank_for_entry()
        _ema_hud = self._ema_hud_stack()
        return {
            "product": str(self.cfg.PRODUCT_NAME or "RECON SNIPER"),
            "kickerLong": str(self.cfg.KICKER_LONG or "RECON LONG"),
            "kickerShort": str(self.cfg.KICKER_SHORT or "RECON SHORT"),
            "bridgeName": str(self.cfg.BRIDGE_NAME or "ReconSniperBridge"),
            "enabled": bool(self.cfg.MARK2_ENABLED),
            "mode": self.mode.value,
            "connected": bool(self.risk.connected),
            "kill": bool(self.risk.kill),
            "state": self.sm.state.value,
            "port": int(self.cfg.BRIDGE_PORT),
            "price": round(float(snap.price), 2) if snap else 0.0,
            "atr": round(float(snap.atr), 2) if snap else 0.0,
            "ema": round(float(snap.ema), 2) if snap else 0.0,
            "vwap": round(float(snap.vwap), 2) if snap else 0.0,
            "volume": round(float(snap.volume), 1) if snap else 0.0,
            "relativeVolume": round(float(snap.relative_volume), 2) if snap else 0.0,
            "velocity": round(float(snap.velocity), 3) if snap else 0.0,
            "acceleration": round(float(snap.acceleration), 3) if snap else 0.0,
            "impulse": round(float(snap.impulse_score), 1) if snap else 0.0,
            "trendBias": snap.trend_bias if snap else "NEUTRAL",
            "trendRegime": snap.trend_regime if snap else "QUIET",
            "chopScalpEnabled": bool(getattr(self.cfg, "ENABLE_CHOP_SCALP", False)),
            "chaoticBankEnabled": bool(getattr(self.cfg, "ENABLE_CHAOTIC_BANK", True)),
            "experimentalEnabled": is_experimental(self.cfg),
            "emaStrategyEnabled": bool(getattr(self.cfg, "ENABLE_EMA_STRATEGY", False)),
            "growMode": grow_mode_active(
                self.cfg,
                float(self.account.get("equity") or self.account.get("cash") or 0),
            ),
            "ema9": _ema_hud[0],
            "ema20": _ema_hud[1],
            "ema50": _ema_hud[2],
            "emaWatch": str(getattr(self, "_ema_watch", "") or ""),
            "intersection": self._intersection_hud(),
            "scout": (self._scout_view.hud() if getattr(self, "_scout_view", None) else ScoutView().hud()),
            "emaLights": ema_line_lamps(
                float(snap.price) if snap else 0.0,
                _ema_hud[0],
                _ema_hud[1],
                _ema_hud[2],
                float(self._ema_atr()) if self._ema_mode() else 0.0,
                float(getattr(self.cfg, "EMA_BLUE_APPROACH_ATR", 0.35) or 0.35),
            ),
            "experimentalProfile": snapshot_experimental(self.cfg),
            "biasAssistEnabled": bool(getattr(self.cfg, "ENABLE_BIAS_ENTRY_ADJUST", True)),
            "trendStrength": round(float(snap.trend_strength), 1) if snap else 0.0,
            "structure": snap.structure_state if snap else "MIXED",
            "volatility": snap.volatility_state if snap else "stable",
            "session": snap.session if snap else "",
            "contracts": self.cfg.contracts(),
            "maxContracts": int(self.cfg.MAX_CONTRACTS),
            "bankPerContract": float(getattr(self.cfg, "TRAIL_ARM_USD", 15.0)),
            "bankDollars": (
                round(float(_hunt_bank), 2)
                if _hunt
                else round(float(getattr(self.cfg, "TRAIL_ARM_USD", 15.0)), 2)
            ),
            "bankPoints": round(
                (
                    float(_hunt_bank)
                    if _hunt
                    else float(getattr(self.cfg, "TRAIL_ARM_USD", 15.0))
                )
                / max(float(self.cfg.POINT_VALUE) * max(1, self.cfg.contracts()), 1e-9),
                2,
            ),
            "dailyGoal": self._goal_snapshot(),
            "exitClassic": bool(getattr(self.cfg, "EMA_CLASSIC_EXIT", False)),
            "trailPoints": float(self.cfg.RUNNER_TRAIL_POINTS),
            "stopPoints": round(
                float(getattr(self.cfg, "EMA_HARD_STOP_POINTS", 10.0) or 10.0)
                if bool(getattr(self.cfg, "EMA_CLASSIC_EXIT", False))
                else stop_points(self.cfg),
                2,
            ),
            "stopDollars": round(
                (
                    float(getattr(self.cfg, "EMA_HARD_STOP_POINTS", 10.0) or 10.0)
                    if bool(getattr(self.cfg, "EMA_CLASSIC_EXIT", False))
                    else stop_points(self.cfg)
                )
                * float(self.cfg.POINT_VALUE)
                * self.cfg.contracts(),
                2,
            ),
            "long": {
                "confidence": round(scores.long_confidence, 1) if scores else 0.0,
                "velocity": round(scores.long_conf_velocity, 2) if scores else 0.0,
                "accel": round(scores.long_conf_accel, 2) if scores else 0.0,
                "opportunity": round(scores.long_opportunity, 1) if scores else 0.0,
                "extension": round(scores.extension_risk_long, 1) if scores else 0.0,
            },
            "short": {
                "confidence": round(scores.short_confidence, 1) if scores else 0.0,
                "velocity": round(scores.short_conf_velocity, 2) if scores else 0.0,
                "accel": round(scores.short_conf_accel, 2) if scores else 0.0,
                "opportunity": round(scores.short_opportunity, 1) if scores else 0.0,
                "extension": round(scores.extension_risk_short, 1) if scores else 0.0,
            },
            "health": round(scores.trade_health, 1) if scores else 0.0,
            "event": {
                "id": ev.event_id if ev else 0,
                "type": ev.event_type.value if ev else "",
                "direction": ev.direction.value if ev else "",
                "peakConfidence": round(ev.peak_confidence, 1) if ev else 0.0,
                "taken": bool(ev.entry_taken) if ev else False,
                "ended": bool(ev.ended) if ev else False,
            },
            "lastReject": self.last_reject.value if self.last_reject else "",
            "trade": None
            if paper is None
            else {
                "side": paper.side.value,
                "entry": round(paper.entry, 2),
                "stop": round(paper.stop, 2),
                "target": round(paper.target, 2),
                "mfe": round(paper.mfe, 2),
                "mae": round(paper.mae, 2),
                "runner": paper.runner,
                "health": round(scores.trade_health, 1) if scores else 0.0,
                "qty": int(paper.qty),
                "price": round(float(snap.price), 2) if snap else round(paper.entry, 2),
                "points": round(_open_points(paper, snap.price if snap else paper.entry), 2),
                "pnl": round(_open_pnl(paper, snap.price if snap else paper.entry, self.cfg), 2),
                "holdSec": round(max(0.0, (snap.ts if snap else paper.entry_ts) - paper.entry_ts), 1),
                "simulated": self.mode != RunMode.LIVE,
                "bankDollars": bank_dollars(paper.qty, self.cfg, trade=paper),
                "banked": bool(paper.target_touched),
                "chopScalp": bool(paper.chop_scalp),
                "chaoticBank": bool(paper.chaotic_bank),
                "manualEntry": bool(paper.manual_entry),
                "state": str(getattr(paper, "ema_trade_state", "") or ""),
                "tag": str(getattr(paper, "ema_entry_tag", "") or ""),
                "atrAtEntry": round(float(getattr(paper, "atr_at_entry", 0) or 0), 4),
                "peakPnl": round(
                    float(paper.mfe) * float(self.cfg.POINT_VALUE) * int(paper.qty),
                    2,
                ),
                "locks": self._hold_hud_locks_safe(paper, snap),
            },
            "stats": stats,
            "trades": self._pnl_log_rows(paper, snap),
            "lastExit": last_exit,
            "pnlLog": {
                "net": closed_pnl,
                "session": session_pnl,
                "botNet": bot_session_pnl,
                "open": open_pnl,
                "wins": int(stats.get("wins") or 0),
                "losses": int(stats.get("losses") or 0),
                "scratches": int(stats.get("scratches") or 0),
                "count": int(stats.get("trades") or 0),
                "inTrade": paper is not None,
                "fromNt": from_nt,
            },
            "log": list(self.recent_decisions)[-36:],
            "sessionPnl": session_pnl,
            "account": str(self.account.get("name") or ""),
            "accountSynced": bool(self.account.get("synced")),
            "cash": float(self.account.get("cash") or 0),
            "equity": float(self.account.get("equity") or 0),
            "net_liquidation": float(self.account.get("equity") or 0),
            "buying_power": float(self.account.get("buying_power") or 0),
            "unrealized": float(self.account.get("unrealized") or 0),
            "realized": float(self.account.get("realized") or 0),
            "instrument": str(self.account.get("instrument") or ""),
            "entryTuning": entry_tuning_snapshot(self.cfg),
            "exhaustion": self._exhaustion_hud(scores),
            "bookFlash": self._book_flash,
            "bookHits": (self._book_flash or {}).get("hits") if self._book_flash else [],
            "deepHold": bool(getattr(self.cfg, "ENABLE_DEEP_HOLD", False)),
            "deepHoldArm": float(getattr(self.cfg, "DEEP_HOLD_ARM_USD", 300.0) or 300.0),
        }

    def _scan_book_flash(self, bars: list[dict], *, ts: float) -> None:
        if not bool(getattr(self.cfg, "ENABLE_BOOK_PATTERNS", False)):
            self._book_flash = None
            return
        self._book_detector.enabled = True
        hits = self._book_detector.scan(bars)
        directional = [h for h in hits if h.name not in ("Doji", "Spinning Top")]
        if not directional:
            return
        primary = directional[0]
        key = f"{primary.name}|{primary.side.value}|{len(bars)}"
        if key == self._book_flash_key:
            # keep showing briefly
            if self._book_flash and (ts - float(self._book_flash.get("ts") or 0)) < 4.0:
                return
        self._book_flash_key = key
        self._book_flash = {
            "ts": ts,
            "name": primary.name,
            "side": primary.side.value,
            "detail": primary.detail or "",
            "hits": [
                {"name": h.name, "side": h.side.value, "detail": h.detail or ""}
                for h in directional[:4]
            ],
        }

    def _entry_trigger_tags(
        self,
        *,
        direction: Side,
        book_detail: str,
        exhaustion_detail: str,
        scores: ScoreBundle | None,
    ) -> dict[str, Any]:
        tags: dict[str, Any] = {"book": "", "rsi": "", "trigger": ""}
        parts: list[str] = []
        if book_detail and "book OK" in book_detail.lower():
            name = book_detail.split("—")[-1].strip() if "—" in book_detail else book_detail
            tags["book"] = name
            parts.append(f"BOOK:{name}")
        elif book_detail:
            tags["book"] = book_detail
        rsi_val = None
        if scores and isinstance(scores.contributors, dict):
            exh = scores.contributors.get("exhaustion")
            if isinstance(exh, dict):
                rsi_val = exh.get("rsi")
        if rsi_val is not None:
            r = float(rsi_val)
            ob = float(getattr(self.cfg, "RSI_OB_LEVEL", 75) or 75)
            os_lvl = float(getattr(self.cfg, "RSI_OS_LEVEL", 25) or 25)
            if direction == Side.LONG and r <= os_lvl:
                tags["rsi"] = f"OS {r:.0f}"
                parts.append(f"RSI:OS {r:.0f}")
            elif direction == Side.SHORT and r >= ob:
                tags["rsi"] = f"OB {r:.0f}"
                parts.append(f"RSI:OB {r:.0f}")
            else:
                tags["rsi"] = f"RSI {r:.0f}"
        if exhaustion_detail and "ok" in exhaustion_detail.lower():
            if not tags["rsi"]:
                tags["rsi"] = exhaustion_detail
        tags["trigger"] = " + ".join(parts) if parts else (
            f"{tags['book']} {tags['rsi']}".strip()
        )
        return tags

    def _exhaustion_hud(self, scores: ScoreBundle | None) -> dict[str, Any]:
        exh: dict[str, Any] = {}
        if scores and isinstance(scores.contributors, dict):
            raw = scores.contributors.get("exhaustion")
            if isinstance(raw, dict):
                exh = dict(raw)
        if not exh and self.last_snap is not None:
            try:
                r = read_exhaustion(self.last_snap, self.cfg)
                exh = {
                    "rsi": r.rsi,
                    "stochRsi": r.stoch_rsi,
                    "bbPctB": r.bb_pct_b,
                    "vwapSigma": r.vwap_sigma,
                    "williamsR": r.williams_r,
                    "cci": r.cci,
                    "extensionLong": round(r.extension_long, 1),
                    "extensionShort": round(r.extension_short, 1),
                    "momentumHealth": round(r.momentum_health, 1),
                    "macd": r.macd,
                    "macdHist": r.macd_hist,
                    "macdHistDelta": r.macd_hist_delta,
                    "macdCross": r.macd_cross,
                    "macdDiv": r.macd_div,
                    "longChase": round(r.long_chase, 1),
                    "shortChase": round(r.short_chase, 1),
                    "extremeOb": r.extreme_ob,
                    "extremeOs": r.extreme_os,
                }
            except Exception:
                exh = {}
        rsi = float(exh.get("rsi") or 50.0) if exh else None
        ob = float(getattr(self.cfg, "RSI_OB_LEVEL", 75.0) or 75.0)
        os_lvl = float(getattr(self.cfg, "RSI_OS_LEVEL", 25.0) or 25.0)
        zone = "mid"
        if rsi is not None:
            if rsi >= max(80.0, ob + 5.0):
                zone = "extreme_ob"
            elif rsi >= ob:
                zone = "ob"
            elif rsi <= min(20.0, os_lvl - 5.0):
                zone = "extreme_os"
            elif rsi <= os_lvl:
                zone = "os"
        return {
            "rsi": None if rsi is None else round(rsi, 1),
            "stochRsi": exh.get("stochRsi"),
            "bbPctB": exh.get("bbPctB"),
            "vwapSigma": exh.get("vwapSigma"),
            "extensionLong": exh.get("extensionLong"),
            "extensionShort": exh.get("extensionShort"),
            "momentumHealth": exh.get("momentumHealth"),
            "macdHist": exh.get("macdHist"),
            "macdHistDelta": exh.get("macdHistDelta"),
            "macdCross": exh.get("macdCross"),
            "macdDiv": exh.get("macdDiv"),
            "longChase": exh.get("longChase"),
            "shortChase": exh.get("shortChase"),
            "zone": zone,
            "obLevel": ob,
            "osLevel": os_lvl,
            "filterOn": bool(getattr(self.cfg, "ENABLE_EXHAUSTION_FILTER", False)),
        }

    def apply_account(self, msg: dict[str, Any]) -> None:
        prev = self.account
        # Heartbeats always include cash_value/net_liquidation (often 0 before NT
        # account cache fills). Treat ~0 as missing so we fall through to cash/sod/bp
        # and never clobber a previously synced positive balance.
        cash = _positive_num(msg, "cash_value", "cash")
        sod = _positive_num(msg, "sod_cash")
        if cash is None:
            cash = sod
        buying = _positive_num(msg, "buying_power")
        net = _positive_num(msg, "net_liquidation", "equity")
        unreal = _pick_num(msg, "unrealized_pnl", "unrealized")
        realized = _pick_num(msg, "realized_pnl", "realized")
        if net is not None:
            equity = net
        elif cash is not None:
            equity = cash
        elif buying is not None:
            equity = buying
        else:
            equity = None
        if cash is None:
            cash = _positive_num(prev, "cash")
        if equity is None:
            equity = _positive_num(prev, "equity") or cash
        if buying is None:
            buying = _positive_num(prev, "buying_power")
        synced = bool(prev.get("synced"))
        if any(
            k in msg and msg.get(k) not in (None, "")
            for k in (
                "cash_value",
                "cash",
                "net_liquidation",
                "equity",
                "sod_cash",
                "buying_power",
                "realized_pnl",
                "realized",
                "unrealized_pnl",
                "unrealized",
            )
        ):
            synced = True
        if (cash or 0) > 0 or (equity or 0) > 0:
            synced = True
        pnl_from_nt = bool(prev.get("pnl_from_nt"))
        if realized is not None or unreal is not None:
            pnl_from_nt = True
        pos = prev.get("position", 0)
        if "position" in msg:
            try:
                pos = int(float(msg.get("position") or 0))
            except (TypeError, ValueError):
                pos = 0
        self.account = {
            "name": str(msg.get("account") or prev.get("name") or ""),
            "instrument": str(msg.get("instrument") or msg.get("symbol") or prev.get("instrument") or ""),
            "cash": float(cash or 0),
            "equity": float(equity or 0),
            "buying_power": float(buying or 0),
            "unrealized": float(unreal if unreal is not None else prev.get("unrealized") or 0),
            "realized": float(realized if realized is not None else prev.get("realized") or 0),
            "pnl_from_nt": pnl_from_nt,
            "synced": synced,
            "position": int(pos or 0),
        }

    def _nt_total_pnl(self) -> float:
        acct = self.account
        return float(acct.get("realized") or 0) + float(acct.get("unrealized") or 0)

    def _session_pnl_from_nt(self) -> float | None:
        """Authoritative session net from NT heartbeat (realized + unrealized)."""
        if not self.account.get("synced") or not self.account.get("pnl_from_nt"):
            return None
        base = float(self._session_pnl_baseline or 0)
        return round(self._nt_total_pnl() - base, 2)

    def on_bar_close(self, bar: dict, *, seed: bool = False) -> None:
        """Completed candle updates slow context only. Not an entry event."""
        b = {
            "time": str(bar.get("time") or ""),
            "open": float(bar.get("open") or 0),
            "high": float(bar.get("high") or 0),
            "low": float(bar.get("low") or 0),
            "close": float(bar.get("close") or 0),
            "volume": float(bar.get("volume") or bar.get("vol") or 0),
        }
        self.completed_bars.append(b)
        if len(self.completed_bars) > 400:
            self.completed_bars = self.completed_bars[-400:]
        ts = _bar_ts(b)
        self.context.on_bar_close(self.completed_bars, ts)
        if self.forming_bar and str(self.forming_bar.get("time")) == b["time"]:
            self.forming_bar = None
        self._ema_on_completed_bar(allow_entry=not seed)

    def on_tick(self, tick: Tick) -> Optional[dict]:
        self._fold_forming(tick)
        if len(self.completed_bars) < 15:
            self.sm.state = EngineState.IDLE
            return None
        self.sm.on_ready()
        self.sm.maybe_rearm(tick.ts)

        avg_vol = _avg_volume(self.completed_bars, self.cfg.VOLUME_LOOKBACK)
        atr_hint = float(self.context.last_atr or self.cfg.TICK_SIZE)
        fast = self.fast.update(tick, atr_hint, avg_vol)
        snap = self.context.snapshot(tick, self.completed_bars, self.forming_bar, fast)
        scores = self.confidence.update(snap)
        scores = self.opportunity.update(snap, scores)
        bars_flash = list(self.completed_bars)
        if self.forming_bar:
            bars_flash = bars_flash + [self.forming_bar]
        self._scan_book_flash(bars_flash, ts=tick.ts)
        if self.paper is not None:
            scores = self.extension.update(snap, scores, event=self.events.active)
            self.last_snap, self.last_scores = snap, scores
            if self._ema_mode() and bool(getattr(self.paper, "ema_strategy", False)):
                out = self._manage_ema(tick, snap, scores)
            else:
                out = self._manage(tick, snap, scores)
            self._maybe_complete_goal(tick=tick)
            return out

        if self._ema_mode():
            scores = self.extension.update(snap, scores, event=None)
            self.last_snap, self.last_scores = snap, scores
            return self._ema_flat_tick(tick, snap, scores)

        if not bool(getattr(self.cfg, "ALLOW_LEGACY_ENTRIES", True)):
            scores = self.extension.update(snap, scores, event=None)
            self.last_snap, self.last_scores = snap, scores
            self.sm.state = EngineState.WATCHING
            return {"state": self.sm.state.value, "decision": "WAIT", "reject": "EMA_ONLY"}

        event, ev_status = self.events.detect(snap, scores)
        scores = self.extension.update(snap, scores, event=event)
        self.last_snap, self.last_scores = snap, scores
        if self._maybe_complete_goal(tick=tick):
            return {"state": self.sm.state.value, "decision": "GOAL_HIT"}
        if not self.cfg.MARK2_ENABLED:
            return {"state": self.sm.state.value, "decision": "DISABLED"}
        if self.goal_met:
            return {"state": self.sm.state.value, "decision": "GOAL_HIT"}
        volume_ok = True
        structure_ok = True
        ext_block = False
        trend_ok = True
        candle_ok = True
        candle_detail = ""
        entry_profile = "default"
        chop_detail = ""
        chaotic_detail = ""
        bias_gates = None
        alt_profile = False
        pressure = self._goal_pressure(ts=tick.ts)
        if event is not None:
            regime = (snap.trend_regime or "").upper()
            trend_ok = regime in ("TRENDING", "HIGH_VOL")
            if event.direction == Side.LONG:
                trend_ok = trend_ok and bool(snap.longs_allowed)
            elif event.direction == Side.SHORT:
                trend_ok = trend_ok and bool(snap.shorts_allowed)
            chop_on = bool(getattr(self.cfg, "ENABLE_CHOP_SCALP", False)) or pressure.force_chop
            if regime == "CHOPPY" and chop_on:
                c_need, o_need, v_need, cv_need, g_need = apply_pressure_to_chop_needs(
                    self.cfg, pressure
                )
                chop_ok, chop_detail = chop_entry_ok(
                    snap,
                    scores,
                    event.direction,
                    self.cfg,
                    force_enable=pressure.force_chop,
                    need_conf=c_need,
                    need_opp=o_need,
                    need_vel=v_need,
                    need_conf_v=cv_need,
                    gap=g_need,
                )
                if chop_ok:
                    trend_ok = True
                    entry_profile = "chop_scalp"
                    alt_profile = True
            elif regime == "CHAOTIC" and bool(getattr(self.cfg, "ENABLE_CHAOTIC_BANK", True)):
                chaotic_ok, chaotic_detail = chaotic_entry_ok(
                    snap, scores, event.direction, self.cfg
                )
                if chaotic_ok:
                    trend_ok = True
                    entry_profile = "chaotic_bank"
                    alt_profile = True
            if not alt_profile:
                bias_gates = bias_entry_gates(
                    self.cfg, snap, event.direction, entry_profile=entry_profile
                )
            if not alt_profile:
                if event.direction == Side.LONG and snap.structure_state == "LH_LL":
                    structure_ok = False
                if event.direction == Side.SHORT and snap.structure_state == "HH_HL":
                    structure_ok = False
            ext_max = None
            if entry_profile == "chop_scalp":
                ext_max = float(getattr(self.cfg, "CHOP_SCALP_MAX_EXTENSION", 55.0))
                if pressure.active and pressure.pressure > 0:
                    ext_max = min(75.0, ext_max + 12.0 * pressure.pressure)
            elif entry_profile == "chaotic_bank":
                ext_max = float(getattr(self.cfg, "CHAOTIC_BANK_MAX_EXTENSION", 65.0))
            ext_block = bool(
                self.extension.blocks(event.direction, scores, max_risk=ext_max)
            )
            signed_vel = (
                snap.velocity
                if event.direction == Side.LONG
                else -snap.velocity
            )
            volume_ok = volume_entry_ok(snap, self.cfg, signed_vel=signed_vel)
            if pressure.bypass_volume:
                volume_ok = True
            if pressure.bypass_structure:
                structure_ok = True
            if pressure.bypass_regime:
                trend_ok = True
            if self.cfg.REQUIRE_CANDLE_ALIGNMENT:
                regime = (snap.trend_regime or "").upper()
                relax = bool(self.cfg.CANDLE_RELAX_IN_CHOPPY) and regime in ("CHOPPY", "CHAOTIC")
                if bias_gates is not None and bias_gates.active and bias_gates.relax_candle:
                    relax = True
                if pressure.relax_candle:
                    relax = True
                min_tick_vel = float(self.cfg.MIN_TICK_VELOCITY)
                if bias_gates is not None and bias_gates.active and bias_gates.min_tick_velocity is not None:
                    min_tick_vel = float(bias_gates.min_tick_velocity)
                if pressure.active and pressure.pressure > 0:
                    min_tick_vel = max(0.0, min_tick_vel * (1.0 - 0.85 * pressure.pressure))
                min_aligned = candle_min_aligned(
                    self.cfg,
                    side_signed_vel=signed_vel,
                    bias_active=bool(bias_gates is not None and bias_gates.active),
                    entry_profile=entry_profile,
                )
                if pressure.relax_candle or pressure.bypass_candle:
                    min_aligned = 0
                candle_ok, candle_detail = candle_alignment_ok(
                    self.completed_bars,
                    self.forming_bar,
                    float(tick.price),
                    event.direction,
                    min_aligned=min_aligned,
                    lookback=int(self.cfg.CANDLE_ALIGN_LOOKBACK),
                    min_forming_body_ratio=float(self.cfg.CANDLE_MIN_FORMING_BODY_RATIO),
                    require_tick_velocity=bool(self.cfg.REQUIRE_TICK_VELOCITY)
                    and not pressure.bypass_candle,
                    velocity=signed_vel,
                    min_velocity=min_tick_vel,
                    relax_forming=relax or pressure.bypass_candle,
                )
                if pressure.bypass_candle:
                    candle_ok = True
                    if candle_detail:
                        candle_detail = f"hunt_bypass:{candle_detail}"
                    else:
                        candle_detail = "hunt_bypass"
        else:
            volume_ok = volume_entry_ok(snap, self.cfg)
            if pressure.bypass_volume:
                volume_ok = True

        self._entry_profile = entry_profile
        state, reject = self.sm.evaluate_entry(
            ts=tick.ts,
            event=event,
            scores=scores,
            extension_blocks=ext_block,
            volume_ok=volume_ok,
            structure_ok=structure_ok,
            trend_ok=trend_ok,
            candle_ok=candle_ok,
            entry_profile=entry_profile,
            bias_gates=bias_gates,
            goal_pressure=pressure if pressure.active else None,
        )
        quality_detail = ""
        side_detail = ""
        book_detail = ""
        exhaustion_detail = ""
        if (
            reject == RejectReason.NONE
            and event is not None
            and entry_profile == "default"
            and is_experimental(self.cfg)
            and not (pressure.active and pressure.bypass_quality)
        ):
            q_ok, quality_detail = entry_quality_ok(
                snap, scores, event.direction, self.cfg
            )
            if not q_ok:
                reject = RejectReason.REJECT_QUALITY
                state = EngineState.EVENT_DETECTED
            else:
                s_ok, side_detail = directional_agreement_ok(
                    snap, scores, event.direction, self.cfg
                )
                if not s_ok:
                    reject = RejectReason.REJECT_SIDE_AGREEMENT
                    state = EngineState.EVENT_DETECTED
        if reject == RejectReason.NONE and event is not None:
            bars_for_book = list(self.completed_bars)
            if self.forming_bar:
                bars_for_book = bars_for_book + [self.forming_bar]
            book_ok, book_detail = book_pattern_entry_ok(
                self.cfg, bars_for_book, event.direction
            )
            if not book_ok:
                reject = RejectReason.REJECT_BOOK_PATTERN
                state = EngineState.EVENT_DETECTED
        if reject == RejectReason.NONE and event is not None:
            exh_block, exhaustion_detail, _exh = exhaustion_blocks(
                event.direction, snap, self.cfg
            )
            if exh_block:
                reject = RejectReason.REJECT_EXHAUSTION
                state = EngineState.EVENT_DETECTED
        direction = event.direction if event else Side.NONE
        decision = "WAIT"
        extra = {"eventStatus": ev_status}
        if candle_detail:
            extra["candleAlign"] = candle_detail
        if chop_detail:
            extra["chopScalp"] = chop_detail
        if chaotic_detail:
            extra["chaoticBank"] = chaotic_detail
        if quality_detail:
            extra["entryQuality"] = quality_detail
        if side_detail:
            extra["sideAgreement"] = side_detail
        if book_detail:
            extra["bookPattern"] = book_detail
        if exhaustion_detail:
            extra["exhaustion"] = exhaustion_detail
        tags = self._entry_trigger_tags(
            direction=direction if event else Side.NONE,
            book_detail=book_detail,
            exhaustion_detail=exhaustion_detail,
            scores=scores,
        )
        if tags.get("trigger"):
            extra["trigger"] = tags["trigger"]
        if tags.get("book"):
            extra["bookTag"] = tags["book"]
        if tags.get("rsi"):
            extra["rsiTag"] = tags["rsi"]
        if self._book_flash:
            extra["bookFlash"] = self._book_flash.get("name")
        exh_c = (scores.contributors or {}).get("exhaustion") if scores else None
        if isinstance(exh_c, dict) and exh_c:
            extra["rsi"] = exh_c.get("rsi")
            extra["stochRsi"] = exh_c.get("stochRsi")
            extra["vwapSigma"] = exh_c.get("vwapSigma")
            extra["bbPctB"] = exh_c.get("bbPctB")
            extra["longChase"] = exh_c.get("longChase")
            extra["shortChase"] = exh_c.get("shortChase")
        if is_experimental(self.cfg):
            extra["experimentalProfile"] = "mark2_phase1"
        if entry_profile in ("chop_scalp", "chaotic_bank"):
            extra["entryProfile"] = entry_profile
        if bias_gates is not None and bias_gates.active:
            extra["biasAssist"] = bias_gates.tag
            extra["biasConfNeed"] = round(float(bias_gates.conf_threshold or 0), 1)
        if pressure.active and pressure.pressure > 0:
            extra["goalPressure"] = pressure.pct
            extra["goalHuntBypass"] = True
            if pressure.force_chop:
                extra["goalForceChop"] = True

        if reject == RejectReason.REJECT_DUPLICATE_EVENT:
            decision = "REJECT"
        elif state == EngineState.TRADE_ARMED and event is not None:
            self._last_entry_tags = tags
            decision = self._arm_and_maybe_execute(tick, snap, scores, event)
        elif reject and reject != RejectReason.NONE:
            decision = "REJECT"

        self.last_reject = reject
        if self._should_log_decision(decision, reject, state.value, event):
            self.log.decision(
                snap=snap,
                scores=scores,
                state=state.value,
                event=event,
                direction=direction,
                entry_decision=decision,
                rejection=reject,
                extra=extra,
            )
            self._note_decision(
                tick.price,
                state.value,
                direction,
                decision,
                reject,
                event,
                scores,
                tags=tags,
            )
        return {
            "state": state.value,
            "decision": decision,
            "reject": reject.value,
            "event": event.event_id if event else 0,
        }

    def _arm_and_maybe_execute(
        self,
        tick: Tick,
        snap: MarketSnapshot,
        scores: ScoreBundle,
        event: EventRecord,
    ) -> str:
        if self.paper is not None or self.risk.open_side != Side.NONE:
            return "IN_TRADE"
        if self.goal_met:
            return "GOAL_HIT"
        self.sm.execute()
        chop = self._entry_profile == "chop_scalp"
        chaotic = self._entry_profile == "chaotic_bank"
        hunt, hunt_bank = self._goal_bank_for_entry(ts=tick.ts)
        ema_x = event.event_type == EventType.EMA_CROSS
        deep = False if ema_x else deep_hold_active(self.cfg)
        # Deep hold locks tip-trail arm at $300 total open $. Hunt may shorten near goal.
        if deep:
            arm_usd = float(getattr(self.cfg, "DEEP_HOLD_ARM_USD", 300.0) or 300.0)
            if hunt:
                arm_usd = min(arm_usd, float(hunt_bank))
        else:
            arm_usd = (
                float(hunt_bank)
                if hunt
                else float(getattr(self.cfg, "TRAIL_ARM_USD", 15.0) or 15.0)
            )
        atr = max(snap.atr, self.cfg.TICK_SIZE)
        trig = str(self._last_entry_tags.get("trigger") or "")
        scalp = ema_x and trig in ("EMA_RSI_LONG", "EMA_INTERSECT_SHORT")
        tip_pts = 0.0
        target_pts = 0.0
        if ema_x:
            atr = self._ema_atr()
            stop = ema_atr_stop(tick.price, event.direction, self._ema_stop_atr(), self.cfg)
            if scalp:
                tip_pts = float(getattr(self.cfg, "BULL_TRAIL_POINTS", 3.0) or 3.0)
                target_pts = float(getattr(self.cfg, "BULL_LONG_TARGET_POINTS", 5.5) or 5.5)
            elif trig == "EMA_CHOP_LONG":
                target_pts = chop_target_points(self.cfg, int(self.cfg.contracts()))
            elif bool(getattr(self.cfg, "EMA_CLASSIC_EXIT", False)):
                tip_pts = float(getattr(self.cfg, "RUNNER_TRAIL_POINTS", 5.5) or 5.5)
        else:
            stop = initial_stop(
                tick.price,
                event.direction,
                atr,
                self.cfg,
                chop_scalp=chop and not hunt,
                chaotic_bank=chaotic and not hunt,
            )
        intent = Intent(
            side=event.direction,
            price=tick.price,
            stop=stop,
            quantity=int(self.cfg.contracts()),
            reason=f"RECON {event.event_type.value} {event.direction.value}",
            ts=tick.ts,
        )
        result = self.execution.enter(intent)
        if result not in ("OBSERVE_EXECUTE", "PAPER_EXECUTE", "LIVE_EXECUTE"):
            if (
                ema_x
                and bool(getattr(self.cfg, "AI_SCOUT_PAPER_FALLBACK", True))
                and not self.goal_met
                and not bool(self.risk.kill)
            ):
                print(f"EMA ENTRY FALLBACK PAPER  was:{result}", flush=True)
                self.risk.note_open(event.direction)
                result = "PAPER_EXECUTE"
        if result in ("OBSERVE_EXECUTE", "PAPER_EXECUTE", "LIVE_EXECUTE"):
            self.events.mark_consumed(taken=True)
            entry_conf = (
                scores.long_confidence
                if event.direction == Side.LONG
                else scores.short_confidence
            )
            health_at_entry = self.health.score(
                snap,
                scores,
                side=event.direction,
                entry_price=tick.price,
                mfe=0.0,
                mae=0.0,
                hold_sec=0.0,
            )
            entry_opp = (
                scores.long_opportunity
                if event.direction == Side.LONG
                else scores.short_opportunity
            )
            entry_ext = (
                scores.extension_risk_long
                if event.direction == Side.LONG
                else scores.extension_risk_short
            )
            signed_vel = (
                snap.velocity if event.direction == Side.LONG else -snap.velocity
            )
            self.paper = PaperTrade(
                side=event.direction,
                entry=tick.price,
                entry_ts=tick.ts,
                stop=stop,
                target=(
                    (tick.price + target_pts)
                    if target_pts > 0 and event.direction == Side.LONG
                    else (tick.price - target_pts)
                    if target_pts > 0
                    else initial_target(
                        tick.price,
                        event.direction,
                        atr,
                        self.cfg,
                        chop_scalp=chop and not hunt,
                        chaotic_bank=chaotic and not hunt,
                        goal_hunt=True,
                        bank_dollars_locked=arm_usd,
                    )
                ),
                peak=tick.price,
                trough=tick.price,
                qty=int(self.cfg.contracts()),
                event_id=event.event_id,
                event_type=event.event_type.value,
                fees=ROUND_TURN_FEES * int(self.cfg.contracts()),
                peak_health=health_at_entry,
                entry_confidence=entry_conf,
                entry_opportunity=entry_opp,
                entry_rvol=float(snap.relative_volume),
                entry_impulse=float(snap.impulse_score),
                entry_velocity=float(signed_vel),
                entry_regime=str(snap.trend_regime or ""),
                entry_bias=str(snap.trend_bias or ""),
                entry_extension=float(entry_ext),
                chop_scalp=chop and not hunt,
                chaotic_bank=chaotic and not hunt,
                goal_hunt=True,
                bank_dollars_locked=arm_usd,
                experimental=is_experimental(self.cfg),
                hard_stop=float(stop),
                deep_hold=deep,
                ema_strategy=self._ema_mode() and event.event_type == EventType.EMA_CROSS,
                ema_trade_state="PROBATION" if event.event_type == EventType.EMA_CROSS else "WAITING",
                atr_at_entry=float(atr) if event.event_type == EventType.EMA_CROSS else 0.0,
                tip_trail_pts=tip_pts,
                tip_target_pts=target_pts,
                ema_entry_tag=trig,
            )
            if self._ema_mode() and event.event_type == EventType.EMA_CROSS:
                if event.direction == Side.LONG:
                    self._ema_long_stack_taken = True
                elif event.direction == Side.SHORT and trig == "EMA_FADE_SHORT":
                    self._ema_short_stack_taken = True
                    self._ema_fade_armed = False
            self.sm.enter_management()
            self.last_reject = RejectReason.NONE
            self._push_levels()
            trig = self._last_entry_tags.get("trigger") or ""
            self.log.write(
                f"MARK2_{result}",
                price=tick.price,
                direction=event.direction.value,
                eventId=event.event_id,
                eventType=event.event_type.value,
                stop=self.paper.stop,
                target=self.paper.target,
                trigger=trig,
                book=self._last_entry_tags.get("book") or "",
                rsi=self._last_entry_tags.get("rsi") or "",
                deepHold=deep,
                armUsd=arm_usd,
            )
            if ema_x and self.paper is not None:
                self._ema_entry_bar_i = max(0, len(self.completed_bars) - 1)
                gap = abs(self.paper.entry - float(self.paper.hard_stop or self.paper.stop))
                print("TRADE STATE: PROBATION", flush=True)
                print(
                    f"CATASTROPHIC STOP SET  Side:{self.paper.side.value}  "
                    f"EntryPrice:{self.paper.entry}  ATRAtEntry:{atr:.4f}  "
                    f"StopDistance:{gap:.2f}  StopPrice:{self.paper.stop}",
                    flush=True,
                )
                self._ema_write(
                    "CATASTROPHIC_STOP_SET",
                    side=self.paper.side.value,
                    action="STOP_SET",
                    price=self.paper.entry,
                    timestamp=tick.ts,
                    trade=self.paper,
                    trade_state="PROBATION",
                    extra={
                        "message": "CATASTROPHIC STOP SET",
                        "entryPrice": self.paper.entry,
                        "atrAtEntry": round(atr, 4),
                        "stopDistance": round(gap, 4),
                        "stopPrice": self.paper.stop,
                    },
                )
                self._ema_write(
                    "EMA_TRADE_STATE",
                    side=self.paper.side.value,
                    action="STATE",
                    price=self.paper.entry,
                    timestamp=tick.ts,
                    trade=self.paper,
                    trade_state="PROBATION",
                    extra={"message": "TRADE STATE: PROBATION"},
                )
            self._note_decision(
                tick.price,
                self.sm.state.value,
                event.direction,
                result,
                RejectReason.NONE,
                event,
                scores,
                tags=self._last_entry_tags,
            )
            return result
        self.sm.observe_consume()
        if event.event_type == EventType.EMA_CROSS:
            print(f"EMA ENTRY FAILED: {result}", flush=True)
            self._scout_note_miss(event.direction, str(self._last_entry_tags.get("trigger") or result))
        return result

    def _event_alive_for_trade(
        self, snap: MarketSnapshot, scores: ScoreBundle
    ) -> bool:
        """True while the entry impulse still has life (mirrors event fade rules)."""
        assert self.paper is not None
        side = self.paper.side
        ev = self.events.active
        if ev is None or ev.ended or ev.direction != side:
            return False
        conf = scores.long_confidence if side == Side.LONG else scores.short_confidence
        if conf < float(self.cfg.EVENT_RESET_THRESHOLD):
            return False
        signed_vel = snap.velocity if side == Side.LONG else -snap.velocity
        if signed_vel <= 0 and abs(float(snap.velocity)) < 0.18:
            return False
        return True

    def _manage(self, tick: Tick, snap: MarketSnapshot, scores: ScoreBundle) -> dict:
        assert self.paper is not None
        hold = tick.ts - self.paper.entry_ts
        health = self.health.score(
            snap,
            scores,
            side=self.paper.side,
            entry_price=self.paper.entry,
            mfe=self.paper.mfe,
            mae=self.paper.mae,
            hold_sec=hold,
        )
        scores.trade_health = health
        if self.paper.manual_entry:
            done, why, st = manage_manual_hold(
                self.paper, price=tick.price, cfg=self.cfg
            )
        else:
            done, why, st = manage_paper(
                self.paper,
                price=tick.price,
                atr=snap.atr,
                health=health,
                hold_sec=hold,
                cfg=self.cfg,
                scores=scores,
                snap=snap,
                event_alive=self._event_alive_for_trade(snap, scores),
            )
        self.sm.state = st
        decision = "EXIT" if done else "MANAGE"
        if done or st.value != self._last_manage_state:
            self._last_manage_state = st.value
            self.log.decision(
                snap=snap,
                scores=scores,
                state=st.value,
                event=self.events.active,
                direction=self.paper.side,
                entry_decision=decision,
                rejection=RejectReason.NONE,
                extra={
                    "entryPrice": self.paper.entry,
                    "MFE": self.paper.mfe,
                    "MAE": self.paper.mae,
                    "exitReason": why,
                },
            )
            self._note_decision(
                tick.price,
                st.value,
                self.paper.side,
                decision,
                RejectReason.NONE,
                self.events.active,
                scores,
                exit_reason=why if decision == "EXIT" else "",
            )
        if done:
            self._close_position(tick, snap, scores, why)
            self._last_manage_state = ""
        else:
            self._push_levels()
        return {"state": st.value, "decision": decision, "reject": why}

    def _should_log_decision(
        self,
        decision: str,
        reject: RejectReason,
        state: str,
        event: EventRecord | None,
    ) -> bool:
        if decision in ("OBSERVE_EXECUTE", "PAPER_EXECUTE", "LIVE_EXECUTE", "EXIT"):
            return True
        key = (
            decision,
            reject.value if reject else "",
            state,
            event.event_id if event else 0,
        )
        if key == self._last_log_key:
            return False
        self._last_log_key = key
        return True

    def _close_position(
        self, tick: Tick, snap: MarketSnapshot, scores: ScoreBundle, why: str
    ) -> None:
        t = self.paper
        assert t is not None
        pts = (tick.price - t.entry) if t.side == Side.LONG else (t.entry - tick.price)
        pnl = pts * self.cfg.POINT_VALUE * t.qty - t.fees
        self.execution.flatten(why, ts=tick.ts)
        self.risk.note_fill_pnl(pnl)
        self.stats.record(
            side=t.side.value,
            pnl=pnl,
            points=pts,
            fees=t.fees,
            mfe=t.mfe,
            mae=t.mae,
            hold_sec=tick.ts - t.entry_ts,
            scratch=str(why).startswith("FAILED_EVENT"),
            hour=snap.hour_et,
            event_type=t.event_type,
            confidence=(
                scores.long_confidence if t.side == Side.LONG else scores.short_confidence
            ),
            opportunity=(
                scores.long_opportunity if t.side == Side.LONG else scores.short_opportunity
            ),
        )
        self.closed_trades.appendleft(
            _closed_trade_row(
                side=t.side.value,
                entry=t.entry,
                exit_px=tick.price,
                qty=int(t.qty),
                pts=pts,
                pnl=pnl,
                fees=t.fees,
                mfe=t.mfe,
                mae=t.mae,
                reason=why,
                ts=tick.ts,
            )
        )
        self.log.write(
            "MARK2_EXIT",
            exitPrice=tick.price,
            entryPrice=t.entry,
            realizedPnL=pnl,
            fees=t.fees,
            netPnL=pnl,
            MFE=t.mfe,
            MAE=t.mae,
            exitReason=why,
            direction=t.side.value,
            qty=int(t.qty),
            points=pts,
        )
        self._log_trade_telemetry(t, tick, snap, scores, pts=pts, pnl=pnl, why=why)
        # Free the consumed impulse so the next tick can arm a fresh event.
        self.events.release_after_trade(tick.ts, int(getattr(t, "event_id", 0) or 0))
        self.paper = None
        self._last_nt_stop = None
        self._ema_exit_armed = False
        self._ema_clear_setup()
        self._last_exit_wall = time.time()
        if bool(getattr(t, "ema_strategy", False)):
            cd = float(getattr(self.cfg, "EMA_REENTRY_COOLDOWN_SEC", 5.0) or 0.0)
        elif str(why).startswith("FAILED_EVENT"):
            cd = float(getattr(self.cfg, "THESIS_ABORT_COOLDOWN_SEC", 10.0))
        elif t.chaotic_bank:
            cd = float(getattr(self.cfg, "CHAOTIC_BANK_COOLDOWN_SEC", 2.0))
        elif t.chop_scalp:
            cd = float(getattr(self.cfg, "CHOP_SCALP_COOLDOWN_SEC", 1.5))
        else:
            cd = float(getattr(self.cfg, "DEFAULT_EXIT_COOLDOWN_SEC", 3.0))
        gp = self._goal_pressure(ts=tick.ts)
        if gp.active and gp.cooldown_sec is not None:
            # Mild hunt shorten only — never force instant re-entries.
            cd = min(cd, max(2.0, float(gp.cooldown_sec)))
        self.sm.begin_cooldown(tick.ts, cd)
        self._push_levels(clear=True)

    def clear_session(self) -> dict[str, Any]:
        """Zero live session PnL / W-L / closed-trade ledger. Open trade stays."""
        self.stats.clear_session()
        self.closed_trades.clear()
        self.risk.daily_pnl = 0.0
        self.goal_met = False
        self.risk.goal_met = False
        self._goal_started_ts = None
        if self.account.get("pnl_from_nt"):
            self._session_pnl_baseline = self._nt_total_pnl()
        else:
            self._session_pnl_baseline = None
        return self.stats.summary()

    def manual_order(self, side: str) -> dict[str, Any]:
        """HUD operator BUY/SELL — works while disarmed; blocked if already in a trade."""
        if self._ema_mode() or not bool(getattr(self.cfg, "ALLOW_LEGACY_ENTRIES", True)):
            return {"ok": False, "error": "EMA_ONLY"}
        raw = str(side or "").upper()
        if raw in ("BUY", "LONG"):
            direction = Side.LONG
        elif raw in ("SELL", "SHORT"):
            direction = Side.SHORT
        else:
            return {"ok": False, "error": "BAD_SIDE"}
        if self.paper is not None:
            return {"ok": False, "error": "IN_TRADE"}
        if self.goal_met:
            return {"ok": False, "error": "GOAL_HIT"}
        snap = self.last_snap
        if snap is None or float(snap.price) <= 0:
            return {"ok": False, "error": "NO_PRICE"}
        scores = self.last_scores or ScoreBundle()
        tick = Tick(ts=float(snap.ts), price=float(snap.price))
        atr = max(snap.atr, self.cfg.TICK_SIZE)
        hunt, hunt_bank = self._goal_bank_for_entry(ts=tick.ts)
        arm_usd = (
            float(hunt_bank)
            if hunt
            else float(getattr(self.cfg, "TRAIL_ARM_USD", 15.0) or 15.0)
        )
        stop = initial_stop(tick.price, direction, atr, self.cfg)
        qty = int(self.cfg.contracts())
        intent = Intent(
            side=direction,
            price=tick.price,
            stop=stop,
            quantity=qty,
            reason=f"HUD_MANUAL_{direction.value}",
            ts=tick.ts,
            attach_stop=False,
        )
        result = self.execution.manual_enter(intent)
        if result not in ("OBSERVE_EXECUTE", "PAPER_EXECUTE", "LIVE_EXECUTE"):
            self.recent_decisions.append(
                {
                    "price": round(tick.price, 2),
                    "state": self.sm.state.value,
                    "direction": direction.value,
                    "decision": "REJECT",
                    "reject": result,
                    "event": "MANUAL",
                    "eventId": 0,
                    "confidence": round(
                        scores.long_confidence
                        if direction == Side.LONG
                        else scores.short_confidence,
                        1,
                    ),
                    "opportunity": 0.0,
                    "extension": 0.0,
                }
            )
            return {"ok": False, "error": result}
        entry_conf = (
            scores.long_confidence
            if direction == Side.LONG
            else scores.short_confidence
        )
        health_at_entry = self.health.score(
            snap,
            scores,
            side=direction,
            entry_price=tick.price,
            mfe=0.0,
            mae=0.0,
            hold_sec=0.0,
        )
        self.paper = PaperTrade(
            side=direction,
            entry=tick.price,
            entry_ts=tick.ts,
            stop=stop,
            target=initial_target(
                tick.price,
                direction,
                atr,
                self.cfg,
                goal_hunt=True,
                bank_dollars_locked=arm_usd,
            ),
            peak=tick.price,
            trough=tick.price,
            qty=qty,
            event_id=0,
            event_type="HUD_MANUAL",
            fees=ROUND_TURN_FEES * qty,
            peak_health=health_at_entry,
            entry_confidence=entry_conf,
            manual_entry=True,
            goal_hunt=True,
            bank_dollars_locked=arm_usd,
            hard_stop=float(stop),
        )
        # Manual = operator owns the book. Disarm auto so it cannot re-enter
        # after a stop/flat and reverse or stack on top of the discretionary trade.
        self.set_enabled(False, persist=True)
        self.sm.enter_management()
        self.last_reject = RejectReason.NONE
        self._push_levels()
        self.log.write(
            f"MARK2_{result}",
            manual=True,
            price=tick.price,
            direction=direction.value,
            stop=stop,
            target=self.paper.target,
        )
        self.recent_decisions.append(
            {
                "price": round(tick.price, 2),
                "state": self.sm.state.value,
                "direction": direction.value,
                "decision": result,
                "reject": "",
                "event": "MANUAL",
                "eventId": 0,
                "confidence": round(entry_conf, 1),
                "opportunity": 0.0,
                "extension": 0.0,
            }
        )
        return {
            "ok": True,
            "action": result,
            "side": direction.value,
            "price": round(tick.price, 2),
            "qty": qty,
            "stop": round(stop, 2),
        }

    def flatten_now(self, reason: str = "HUD_FLAT") -> None:
        if self.paper is not None:
            snap = self.last_snap
            scores = self.last_scores or ScoreBundle()
            if snap is None:
                t = self.paper
                snap = MarketSnapshot(
                    ts=t.entry_ts,
                    price=t.entry,
                    completed_bars=[],
                    forming_bar=None,
                )
            tick = Tick(ts=float(snap.ts), price=float(snap.price))
            self._note_decision(
                tick.price,
                self.sm.state.value,
                self.paper.side,
                "EXIT",
                RejectReason.NONE,
                self.events.active,
                scores,
                exit_reason=reason,
            )
            self._close_position(tick, snap, scores, reason)
            self._last_manage_state = ""
        else:
            self.execution.flatten(reason, ts=0.0)
            self._push_levels(clear=True)
        self.risk.open_side = Side.NONE
        self.sm.reset()
        self.sm.on_ready()
        self.last_reject = RejectReason.NONE

    def _pnl_log_rows(
        self, paper: Optional[PaperTrade], snap: Optional[MarketSnapshot]
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        if paper is not None:
            px = float(snap.price) if snap else paper.entry
            ts = float(snap.ts) if snap else paper.entry_ts
            clock, day = _et_clock(ts)
            pts = _open_points(paper, px)
            pnl = _open_pnl(paper, px, self.cfg)
            rows.append(
                {
                    "key": f"open|{paper.entry_ts}|{paper.side.value}|{round(paper.entry, 2)}",
                    "ts": ts,
                    "clock": clock,
                    "day": day,
                    "side": paper.side.value,
                    "entry": round(paper.entry, 2),
                    "exit": None,
                    "qty": int(paper.qty),
                    "pts": round(pts, 2),
                    "pnl": round(pnl, 2),
                    "fees": 0.0,
                    "mfe": round(paper.mfe, 2),
                    "mae": round(paper.mae, 2),
                    "reason": "OPEN",
                    "open": True,
                }
            )
        rows.extend(self.closed_trades)
        return rows

    def _ema_mode(self) -> bool:
        return bool(getattr(self.cfg, "ENABLE_EMA_STRATEGY", False))

    def _ema_clear_setup(self) -> None:
        self._ema_pending_side = Side.NONE
        self._ema_pending_why = ""
        self._ema_pullback = None
        self._ema_pullback_fill = None

    def _ema_release_long_stack(self, stack) -> None:
        if stack_lock_should_clear(stack):
            self._ema_long_stack_taken = False
            self._scout_long_taken = False
            if self._scout_missed_side == Side.LONG:
                self._scout_missed_side = Side.NONE
                self._scout_missed_why = ""

    def _ema_release_short_stack(self, stack) -> None:
        if short_stack_lock_should_clear(stack):
            self._ema_short_stack_taken = False
            self._scout_short_taken = False
            if self._scout_missed_side == Side.SHORT:
                self._scout_missed_side = Side.NONE
                self._scout_missed_why = ""

    def _scout_note_miss(self, side: Side, why: str) -> None:
        if side not in (Side.LONG, Side.SHORT):
            return
        self._scout_missed_side = side
        self._scout_missed_why = str(why or "")

    def _trade_gap_left(self) -> float:
        need = float(getattr(self.cfg, "EMA_REENTRY_COOLDOWN_SEC", 5.0) or 0.0)
        if need <= 0 or self._last_exit_wall <= 0:
            return 0.0
        return max(0.0, need - (time.time() - self._last_exit_wall))

    def _trade_gap_blocked(self) -> bool:
        return self._trade_gap_left() > 1e-9

    def _scout_on_flat(
        self,
        tick: Tick,
        snap: MarketSnapshot,
        scores: ScoreBundle,
        stack,
    ) -> dict | None:
        if not bool(getattr(self.cfg, "ENABLE_AI_SCOUT", True)):
            self._scout_view = ScoutView(bullets=["SCOUT OFF"])
            return None
        if self.paper is not None:
            self._scout_view = ScoutView(
                action="HOLD",
                why="IN_TRADE",
                bullets=["IN TRADE · SCOUT STANDS DOWN"],
            )
            return None
        if self._trade_gap_blocked():
            left = self._trade_gap_left()
            self._scout_view = ScoutView(
                action="HOLD",
                why="COOLDOWN",
                bullets=[
                    f"PAUSE {left:.1f}S · NO BACK TO BACK",
                    "NEXT ENTRY MUST STILL FIT THE CROSS",
                ],
            )
            return None
        view = scout_opportunity(
            stack=stack,
            price=float(tick.price),
            atr=self._ema_atr(),
            bot_watch=str(getattr(self, "_ema_watch", "") or ""),
            missed_side=self._scout_missed_side,
            missed_why=self._scout_missed_why,
            cfg=self.cfg,
            in_trade=False,
            scout_long_taken=self._scout_long_taken,
            scout_short_taken=self._scout_short_taken,
        )
        self._scout_view = view
        if view.action not in ("TAKE", "OVERRIDE") or view.side == Side.NONE:
            return None
        event = EventRecord(
            event_id=self.events._next_id,
            event_type=EventType.EMA_CROSS,
            direction=view.side,
            started_ts=tick.ts,
            started_price=tick.price,
            started_bar_time=str((self.completed_bars[-1] or {}).get("time") or ""),
            peak_confidence=float(view.confidence),
            peak_opportunity=float(view.confidence),
        )
        self.events._next_id += 1
        self.events.active = event
        self._ema_pending_side = view.side
        self._ema_pending_why = view.why
        self._last_entry_tags = {
            "trigger": view.why,
            "book": "",
            "rsi": "AI SCOUT",
        }
        label = "ENTER LONG" if view.side == Side.LONG else "ENTER SHORT"
        extra = {
            "message": f"{label} · {view.action}",
            "reason": view.why,
            "scout": True,
            "miss": bool(view.miss),
        }
        print(f"AI SCOUT {view.action} {view.side.value}  {view.why}", flush=True)
        for line in view.bullets:
            print(f"AI SCOUT: {line}", flush=True)
        self._ema_write(
            "AI_SCOUT_ENTRY",
            side=view.side.value,
            action="ENTRY",
            price=tick.price,
            timestamp=tick.ts,
            trade_state="PROBATION",
            extra=extra,
        )
        self._ema_clear_setup()
        decision = self._arm_and_maybe_execute(tick, snap, scores, event)
        if self.paper is not None:
            if view.side == Side.LONG:
                self._scout_long_taken = True
                self._ema_long_stack_taken = True
            else:
                self._scout_short_taken = True
                self._ema_short_stack_taken = True
            self._scout_missed_side = Side.NONE
            self._scout_missed_why = ""
            self.paper.ema_entry_tag = view.why
        return {
            "state": self.sm.state.value,
            "decision": decision,
            "reject": "",
            "event": event.event_id,
            "scout": view.why,
        }

    def _ema_reset_long_stack(self) -> None:
        self._ema_long_stack_taken = False
        self._ema_short_stack_taken = False
        self._ema_rsi_peak = 0.0
        self._ema_fade_armed = False
        self._ema_fade_block_bar_i = -1
        self._scout_long_taken = False
        self._scout_short_taken = False
        self._scout_missed_side = Side.NONE
        self._scout_missed_why = ""

    def _ema_rsi_state(self, bars: list | None = None) -> tuple[float | None, float | None, float]:
        use = self.completed_bars if bars is None else bars
        if not use:
            return None, None, float(self._ema_rsi_peak or 0)
        ind = indicator_snapshot(use, self.cfg)
        now = float(ind.get("rsi") or 0)
        prev = now
        if len(use) >= 2:
            prev = float(indicator_snapshot(use[:-1], self.cfg).get("rsi") or 0)
        peak = max(float(self._ema_rsi_peak or 0), now, prev)
        self._ema_rsi_peak = peak
        return now, prev, peak

    def _ema_fade_ready(self) -> bool:
        if not bool(getattr(self.cfg, "EMA_BULL_FADE_SHORT", True)):
            return False
        bar_i = max(0, len(self.completed_bars) - 1)
        if self._ema_fade_armed:
            return bar_i > int(self._ema_fade_block_bar_i)
        # Hunt the down-punch even if we were not in the long.
        return True

    def _ema_entry_kwargs(self, bars: list | None = None) -> dict:
        rsi_now, rsi_prev, rsi_peak = self._ema_rsi_state(bars)
        return {
            "atr": self._ema_atr(),
            "bias": self._ema_bias(),
            "regime": self._ema_regime(),
            "rsi": rsi_now,
            "rsi_prev": rsi_prev,
            "rsi_peak": rsi_peak,
            "fade_ready": self._ema_fade_ready(),
        }

    def _ema_long_stack_blocked(self, stack) -> bool:
        self._ema_release_long_stack(stack)
        return bool(self._ema_long_stack_taken) and stack is not None and red_above_white_and_blue(stack)

    def _ema_fill_pending_from_bar(self) -> None:
        """Fill a just-armed sniper on the signal bar. Playback often has no flat tick."""
        if self._ema_pending_side == Side.NONE or self.paper is not None:
            return
        last = self.completed_bars[-1] if self.completed_bars else {}
        close = float(last.get("close") or 0)
        if close <= 0:
            return
        tick = Tick(
            ts=_bar_ts(last),
            price=close,
            volume=float(last.get("volume") or 0),
            bar_time=str(last.get("time") or ""),
            forming_open=float(last.get("open") or close),
            forming_high=float(last.get("high") or close),
            forming_low=float(last.get("low") or close),
            forming_volume=float(last.get("volume") or 0),
        )
        self.sm.on_ready()
        avg_vol = _avg_volume(self.completed_bars, self.cfg.VOLUME_LOOKBACK)
        atr_hint = float(self.context.last_atr or self.cfg.TICK_SIZE)
        fast = self.fast.update(tick, atr_hint, avg_vol)
        snap = self.context.snapshot(tick, self.completed_bars, self.forming_bar, fast)
        scores = self.confidence.update(snap)
        scores = self.opportunity.update(snap, scores)
        self.last_snap, self.last_scores = snap, scores
        self._ema_flat_tick(tick, snap, scores)

    def clear_ema_pending(self) -> None:
        if self._ema_pending_side != Side.NONE or self._ema_pullback is not None:
            last = self.completed_bars[-1] if self.completed_bars else {}
            px = float(last.get("close") or 0)
            side = self._ema_pending_side
            if side == Side.NONE and self._ema_pullback is not None:
                side = self._ema_pullback.direction
            print("IGNORED CROSS: INITIALIZATION / STALE STATE", flush=True)
            self._ema_write(
                "EMA_IGNORED",
                side=side.value if side != Side.NONE else "NONE",
                action="IGNORE",
                price=px,
                extra={"message": "IGNORED CROSS: INITIALIZATION / STALE STATE"},
            )
        self._ema_clear_setup()
        self._ema_reversal_side = Side.NONE
        self._ema_reset_long_stack()
        if read_ema_stack(self.completed_bars, self.cfg) is not None:
            self._ema_armed = True

    def _ema_live_bars(self) -> list[dict]:
        bars = list(self.completed_bars)
        form = self.forming_bar
        if form and float(form.get("close") or 0) > 0:
            same = bars and str(bars[-1].get("time") or "") == str(form.get("time") or "")
            if not same:
                bars.append(form)
        return bars

    def _hold_hud_locks_safe(self, paper: PaperTrade, snap: Any) -> dict[str, Any]:
        try:
            return self._hold_hud_locks(paper, snap)
        except Exception:
            return {
                "giveback": 0.30,
                "compress": 0.35,
                "floorPts": 0.0,
                "floorPx": 0.0,
                "spread": 0.0,
                "spreadNow": 0.0,
                "entrySpread": 0.0,
                "spreadExpanded": False,
                "warn9": False,
                "lost20": False,
                "confirmedAtr": 1.0,
                "runnerAtr": 2.0,
                "oneR": 0.0,
                "rMultiple": 0.0,
                "openPts": 0.0,
                "giveUsed": 0.0,
                "compressUsed": 0.0,
                "threat": "HOLD",
                "classic": bool(getattr(self.cfg, "EMA_CLASSIC_EXIT", False)),
                "hardStopPts": float(getattr(self.cfg, "EMA_HARD_STOP_POINTS", 10.0) or 10.0),
                "armUsd": float(getattr(self.cfg, "TIP_TRAIL_ARM_USD", 100.0) or 100.0),
                "trailPts": float(getattr(self.cfg, "TIP_TRAIL_START_POINTS", 7.5) or 7.5),
                "keepUsd": 0.0,
                "peakUsd": 0.0,
                "growMode": bool(getattr(self.cfg, "ENABLE_GROW_MODE", True)),
                "growGrab": 50.0,
                "stallBars": 0,
            }

    def _hold_hud_locks(self, paper: PaperTrade, snap: Any) -> dict[str, Any]:
        """Live hold meters for the EXIT HOLD panel."""
        px = float(snap.price) if snap is not None else float(paper.entry)
        pts = _open_points(paper, px)
        one_r = _one_r_points(paper, self.cfg)
        state = str(getattr(paper, "ema_trade_state", "") or "")
        mfe = float(getattr(paper, "mfe", 0) or 0)
        give = float(getattr(self.cfg, "MFE_GIVEBACK_FRAC", 0.30) or 0.30)
        compress = float(getattr(self.cfg, "SPREAD_COMPRESS_FRAC", 0.35) or 0.35)
        floor_pts = float(getattr(paper, "giveback_floor_pts", 0) or 0)
        if state == "RUNNER" and floor_pts <= 0 and mfe > 0:
            floor_pts = mfe_giveback_floor_pts(mfe, self.cfg)
        if paper.side == Side.LONG:
            floor_px = float(paper.entry) + floor_pts if floor_pts > 0 else 0.0
        else:
            floor_px = float(paper.entry) - floor_pts if floor_pts > 0 else 0.0
        given_frac = ((mfe - max(pts, 0.0)) / mfe) if mfe > 1e-9 else 0.0
        give_used = (given_frac / give) if give > 1e-9 else 0.0
        e9, e20, e50 = self._ema_hud_stack()
        spread_now = ema_cluster_spread(e9 or None, e20 or None, e50 or None) or 0.0
        entry_sp = float(getattr(paper, "entry_spread", 0) or 0)
        peak_sp = float(getattr(paper, "spread_peak", 0) or 0)
        if spread_now > 0:
            peak_sp = max(peak_sp, spread_now)
        expanded = peak_sp + 1e-12 >= max(entry_sp * 1.25, entry_sp + 4.0) if entry_sp > 0 else False
        compress_used = 0.0
        if peak_sp > 1e-9 and compress > 1e-9:
            compress_used = max(0.0, (peak_sp - spread_now) / peak_sp) / compress
        r_mult = pts / one_r if one_r > 1e-9 else 0.0
        warn9 = bool(getattr(paper, "ema9_warn", False))
        lost20 = bool(getattr(paper, "ema_lost_20", False))
        classic = bool(getattr(self.cfg, "EMA_CLASSIC_EXIT", False))
        qty = max(1, int(getattr(paper, "qty", 1) or 1))
        peak_usd = mfe * float(self.cfg.POINT_VALUE) * qty
        equity = float(self.account.get("equity") or self.account.get("cash") or 0)
        grow_on = grow_mode_active(self.cfg, equity)
        keep_usd = profit_keep_usd(peak_usd, self.cfg)
        stall_bars = int(getattr(paper, "stall_score", 0) or 0)
        tip_pts = float(getattr(paper, "tip_trail_pts", 0) or 0)
        if tip_pts <= 0 and peak_usd + 1e-9 >= float(
            getattr(self.cfg, "TIP_TRAIL_ARM_USD", 100.0) or 100.0
        ):
            tip_pts = runner_tip_trail_pts(peak_usd, stall_bars, self.cfg)
        start = int(getattr(self, "_ema_entry_bar_i", 0) or 0)
        post = self.completed_bars[start + 1 :]
        adverse_n = len(trailing_adverse_bars(paper.side, post))
        threat = "HARD STOP ONLY"
        if classic:
            threat = "5.5 TIP TRAIL" if state == "RUNNER" else "10 PT STOP"
        elif lost20:
            threat = "STRUCTURE CONFIRM"
        elif adverse_n >= int(getattr(self.cfg, "ADVERSE_STACK_BARS", 3) or 3) and peak_usd >= 20:
            threat = "ADVERSE STACK"
        elif state != "PROBATION" and expanded and compress_used >= 0.70:
            threat = "COMPRESSION"
        elif state == "RUNNER" and give_used >= 0.70:
            threat = "MFE GIVEBACK"
        elif grow_on and peak_usd + 1e-9 >= float(getattr(self.cfg, "GROW_GRAB_USD", 50.0) or 50.0) and stall_bars >= 2:
            threat = "GROW BANK"
        elif keep_usd > 0:
            threat = "DOLLAR FLOOR / TIP TRAIL"
        elif state == "RUNNER":
            threat = "70% MFE FLOOR"
        elif state in ("CONFIRMED", "CONFIRMED_TREND"):
            threat = "PROTECT / STRUCTURE"
        return {
            "giveback": give,
            "compress": compress,
            "floorPts": round(floor_pts, 2),
            "floorPx": round(floor_px, 2),
            "spread": round(peak_sp, 2),
            "spreadNow": round(spread_now, 2),
            "entrySpread": round(entry_sp, 2),
            "spreadExpanded": expanded,
            "warn9": warn9,
            "lost20": lost20,
            "confirmedAtr": 1.0,
            "runnerAtr": 2.0,
            "oneR": round(one_r, 2),
            "rMultiple": round(r_mult, 2),
            "openPts": round(pts, 2),
            "giveUsed": round(min(1.5, give_used), 2),
            "compressUsed": round(min(1.5, compress_used), 2),
            "threat": threat,
            "classic": bool(getattr(self.cfg, "EMA_CLASSIC_EXIT", False)),
            "hardStopPts": float(getattr(self.cfg, "EMA_HARD_STOP_POINTS", 10.0) or 10.0),
            "armUsd": float(
                getattr(self.cfg, "TRAIL_ARM_USD", 15.0) or 15.0
                if classic
                else getattr(self.cfg, "TIP_TRAIL_ARM_USD", 100.0) or 100.0
            ),
            "trailPts": round(
                float(getattr(self.cfg, "RUNNER_TRAIL_POINTS", 5.5) or 5.5)
                if classic
                else (tip_pts or float(getattr(self.cfg, "TIP_TRAIL_START_POINTS", 7.5) or 7.5)),
                2,
            ),
            "keepUsd": round(keep_usd, 2),
            "peakUsd": round(peak_usd, 2),
            "growMode": grow_on,
            "growGrab": float(getattr(self.cfg, "GROW_GRAB_USD", 50.0) or 50.0),
            "stallBars": stall_bars,
        }

    def _ema_hud_stack(self) -> tuple[float, float, float]:
        stack = read_ema_stack(self._ema_live_bars(), self.cfg)
        if stack is None:
            return 0.0, 0.0, 0.0
        return round(stack.ema9, 2), round(stack.ema20, 2), round(stack.ema50, 2)

    def _intersection_hud(self) -> dict[str, Any]:
        stack = read_ema_stack(self._ema_live_bars(), self.cfg)
        if stack is None:
            return IntersectionStatus(reject="BLUE_WARMUP", stage="WAIT_SEP").hud()
        ix = intersection_status(stack, self.cfg, self._ema_atr(), bars=self._ema_live_bars())
        return ix.hud()

    def _push_ema_overlay(self) -> None:
        sink = self.execution.sink
        if sink is None or not hasattr(sink, "send_ema_overlay"):
            return
        on = self._ema_mode()
        e9, e20, e50 = self._ema_hud_stack() if on else (0.0, 0.0, 0.0)
        key = (on, e9, e20, e50)
        if key == self._last_ema_overlay:
            return
        self._last_ema_overlay = key
        sink.send_ema_overlay(enabled=on, ema9=e9, ema20=e20, ema50=e50)

    def _ema_atr(self) -> float:
        """Completed-bar ATR14. Extension, pullback, and logged ATR14 all use this."""
        return max(atr14(self.completed_bars, self.cfg), float(self.cfg.TICK_SIZE))

    def _ema_stop_atr(self) -> float:
        """Wider ATR for catastrophic stop only. Never used to decide immediate vs pullback."""
        bar = self._ema_atr()
        ctx = float(getattr(self.context, "last_atr", 0) or 0)
        snap = float(self.last_snap.atr) if self.last_snap is not None else 0.0
        return max(bar, ctx, snap, float(self.cfg.TICK_SIZE))

    def _ema_note_cross_gap(self) -> int:
        now_i = max(0, len(self.completed_bars) - 1)
        prev_i = int(self._ema_last_cross_bar_i)
        if prev_i < -10**8:
            since = now_i
        else:
            since = max(0, now_i - prev_i)
        self._ema_log_bars_since = since
        self._ema_last_cross_bar_i = now_i
        return since

    def _ema_write(
        self,
        event: str,
        *,
        side: str,
        action: str,
        price: float,
        timestamp: float = 0.0,
        trade=None,
        trade_state: str = "",
        extra: dict | None = None,
    ) -> dict:
        snap = self.last_snap
        row = ema_dataset_row(
            self.completed_bars,
            self.cfg,
            side=side,
            action=action,
            price=float(price),
            timestamp=float(timestamp or 0.0),
            bias="" if snap is None else str(snap.trend_bias or ""),
            regime="" if snap is None else str(snap.trend_regime or ""),
            bars_since=int(self._ema_log_bars_since or 0),
            trade_state=trade_state or ("PROBATION" if action == "ENTRY" else "WAITING"),
            trade=trade,
            atr_used=self._ema_atr(),
            extra=extra,
        )
        self.log.write(event, **row)
        return row

    def _ema_print_row(self, title: str, row: dict) -> None:
        print(title, flush=True)
        print(
            f"Side:{row.get('side')}  Price:{row.get('price')}  "
            f"EMA9:{row.get('ema9')}  EMA20:{row.get('ema20')}  EMA50:{row.get('ema50')}  "
            f"EMA9-EMA20:{row.get('ema9MinusEMA20')}  ATR14:{row.get('atr14')}  "
            f"ExtPts:{row.get('extensionPoints')}  ExtATR:{row.get('extensionATR')}  "
            f"RSI14:{row.get('rsi14')}  MACD:{row.get('macdLine')}/"
            f"{row.get('macdSignal')}/{row.get('macdHistogram')}  "
            f"Bias:{row.get('bias')}  Regime:{row.get('regime')}  "
            f"BarsSinceCross:{row.get('barsSincePreviousCross')}  "
            f"State:{row.get('tradeState')}",
            flush=True,
        )

    def _ema_log_cross(self, side: Side, stack, *, valid: bool, extra: dict | None = None) -> dict:
        note = crossover_note(side, stack, self.cfg)
        last = self.completed_bars[-1] if self.completed_bars else {}
        close = float(last.get("close") or 0)
        atr_v = self._ema_atr()
        ok, ext = entry_extension_ok(close, stack.ema20, atr_v, self.cfg)
        if note:
            print(note, flush=True)
        payload = {"message": note, "signalValid": bool(valid), **(extra or {})}
        trade = (
            self.paper
            if self.paper is not None and bool(getattr(self.paper, "ema_strategy", False))
            else None
        )
        row = self._ema_write(
            "EMA_CROSS",
            side=side.value,
            action="CROSS",
            price=close,
            trade=trade,
            trade_state=str(getattr(trade, "ema_trade_state", "") or "") if trade else "WAITING",
            extra=payload,
        )
        self._ema_print_row(
            f"SignalValid:{bool(valid)}  MaximumAllowed:{ext.get('maxExtensionATR')}",
            row,
        )
        return row

    def _ema_bias(self) -> str:
        snap = self.last_snap
        return "" if snap is None else str(snap.trend_bias or "")

    def _ema_regime(self) -> str:
        snap = self.last_snap
        return "" if snap is None else str(snap.trend_regime or "")

    def _ema_ignore_long(self, why: str, stack) -> None:
        self._ema_ignore_sniper(why, stack)

    def _ema_ignore_sniper(self, why: str, stack) -> None:
        tag = str(why or "").upper()
        if tag not in (
            "WHITE_ONLY",
            "NOT_BEARISH",
            "NOT_BULLISH",
            "BLUE_WARMUP",
            "RED_FALLING",
            "STACK_STALE",
            "STACK_USED",
            "BULL_WAIT_CLOSE",
            "WAIT_CLOSE",
            "NO_SNIPER",
            "CHOPPY",
            "CHAOTIC",
            "RSI_NOT_DYING",
            "NO_INTERSECT",
            "RED_RISING",
            "FADE_SHORT_OFF",
            "WAIT_BREAK",
        ):
            return
        side_txt = "LONG"
        if tag in (
            "NOT_BULLISH",
            "RSI_NOT_DYING",
            "NO_INTERSECT",
            "RED_RISING",
            "FADE_SHORT_OFF",
            "WAIT_BREAK",
        ):
            side_txt = "SHORT"
        elif tag == "WHITE_ONLY" and stack is not None and cross_side(stack) == Side.SHORT:
            side_txt = "SHORT"
        last = self.completed_bars[-1] if self.completed_bars else {}
        close = float(last.get("close") or 0)
        msg = f"{side_txt} IGNORED: {tag}"
        print(msg, flush=True)
        print(
            f"EMA9:{stack.ema9:.2f}  EMA20:{stack.ema20:.2f}  EMA50:{stack.ema50:.2f}  "
            f"Bias:{self._ema_bias() or 'NA'}",
            flush=True,
        )
        self._ema_write(
            "EMA_IGNORED",
            side=side_txt,
            action="IGNORE",
            price=close,
            extra={"message": msg, "reason": tag},
        )

    def _ema_side_allowed(self, side: Side, why: str = "") -> bool:
        if side == Side.LONG:
            return bool(getattr(self.cfg, "EMA_ALLOW_LONG", True))
        if side == Side.SHORT:
            if str(why or "").upper() == "EMA_FADE_SHORT":
                return bool(getattr(self.cfg, "EMA_BULL_FADE_SHORT", True))
            return bool(getattr(self.cfg, "EMA_ALLOW_SHORT", False))
        return False

    def _ema_try_arm_signal(
        self, side: Side, stack, *, allow_entry: bool, why: str = "", fill_now: bool = True
    ) -> None:
        if not allow_entry or side == Side.NONE:
            return
        if self._trade_gap_blocked():
            return
        if side == Side.LONG:
            self._ema_release_long_stack(stack)
            ok, reason = ema_long_arm_ok(
                stack,
                why,
                self.cfg,
                stack_taken=self._ema_long_stack_taken,
                completed=read_ema_stack(self.completed_bars, self.cfg),
                live_tick=not fill_now,
                regime=self._ema_regime(),
            )
            if not ok:
                self._ema_ignore_sniper(reason, stack)
                return
        if side == Side.SHORT:
            self._ema_release_short_stack(stack)
            rsi_now, rsi_prev, rsi_peak = self._ema_rsi_state()
            ok, reason = ema_short_arm_ok(
                stack,
                why,
                self.cfg,
                stack_taken=self._ema_short_stack_taken,
                completed=read_ema_stack(self.completed_bars, self.cfg),
                live_tick=not fill_now,
                bias=self._ema_bias(),
                atr=self._ema_atr(),
                rsi=rsi_now,
                rsi_prev=rsi_prev,
                rsi_peak=rsi_peak,
                regime=self._ema_regime(),
                fade_ready=self._ema_fade_ready(),
            )
            if not ok:
                self._ema_ignore_sniper(reason, stack)
                return
        if not self._ema_side_allowed(side, why):
            last = self.completed_bars[-1] if self.completed_bars else {}
            close = float(last.get("close") or 0)
            print(
                f"SHORT IGNORED: LONG_ONLY" if side == Side.SHORT else f"{side.value} IGNORED: SIDE OFF",
                flush=True,
            )
            self._ema_write(
                "EMA_IGNORED",
                side=side.value,
                action="IGNORE",
                price=close,
                extra={
                    "message": "SHORT IGNORED: LONG_ONLY" if side == Side.SHORT else f"{side.value} IGNORED: SIDE OFF",
                },
            )
            return
        last = self.completed_bars[-1] if self.completed_bars else {}
        close = float(last.get("close") or 0)
        atr_v = self._ema_atr()
        min_gap = int(getattr(self.cfg, "MIN_BARS_SINCE_LAST_CROSS", 0) or 0)
        since = max(0, len(self.completed_bars) - 1 - int(self._ema_last_cross_bar_i))
        if min_gap > 0 and since < min_gap:
            self._ema_log_cross(side, stack, valid=False, extra={"reject": "MIN_BARS_SINCE_LAST_CROSS"})
            print(
                f"ENTRY REJECTED: MIN_BARS_SINCE_LAST_CROSS  bars={since}  need={min_gap}",
                flush=True,
            )
            return
        if why == "EMA_FADE_SHORT":
            print("INTERSECTION DOWN - FADE SHORT", flush=True)
            print(
                f"Side:{side.value}  SignalBarClose:{close}  "
                f"EMA9:{stack.ema9:.2f}  EMA20:{stack.ema20:.2f}  EMA50:{stack.ema50:.2f}",
                flush=True,
            )
            self._ema_write(
                "EMA_CROSS",
                side=side.value,
                action="CROSS",
                price=close,
                extra={
                    "message": "BULLISH FADE SHORT - IMMEDIATE ENTRY",
                    "signalValid": True,
                    "reason": why,
                },
            )
            self._ema_pullback = None
            self._ema_pending_side = side
            self._ema_pending_why = why
            if fill_now:
                self._ema_fill_pending_from_bar()
            return
        if why == "EMA_CHOP_LONG":
            print("CHOP WHITE CROSS - $150 TARGET, ELSE HOLD UNTIL RED CROSSES WHITE DOWN", flush=True)
            print(
                f"Side:{side.value}  SignalBarClose:{close}  "
                f"EMA9:{stack.ema9:.2f}  EMA20:{stack.ema20:.2f}  EMA50:{stack.ema50:.2f}",
                flush=True,
            )
            self._ema_write(
                "EMA_CROSS",
                side=side.value,
                action="CROSS",
                price=close,
                extra={
                    "message": "CHOP WHITE CROSS - IMMEDIATE ENTRY",
                    "signalValid": True,
                    "reason": why,
                },
            )
            self._ema_pullback = None
            self._ema_pending_side = side
            self._ema_pending_why = why
            if fill_now:
                self._ema_fill_pending_from_bar()
            return
        if why in ("EMA_RSI_LONG", "EMA_INTERSECT_SHORT"):
            label = "BULL LONG" if why == "EMA_RSI_LONG" else "BEAR INTERSECT SHORT"
            print(f"{label} - IMMEDIATE ENTRY  TARGET 5.5  TRAIL 3 AFTER +3", flush=True)
            print(
                f"Side:{side.value}  SignalBarClose:{close}  "
                f"EMA9:{stack.ema9:.2f}  EMA20:{stack.ema20:.2f}  EMA50:{stack.ema50:.2f}",
                flush=True,
            )
            self._ema_write(
                "EMA_CROSS",
                side=side.value,
                action="CROSS",
                price=close,
                extra={
                    "message": f"{label} - IMMEDIATE ENTRY",
                    "signalValid": True,
                    "reason": why,
                    "targetPoints": float(getattr(self.cfg, "BULL_LONG_TARGET_POINTS", 5.5) or 5.5),
                    "trailPoints": float(getattr(self.cfg, "BULL_TRAIL_POINTS", 3.0) or 3.0),
                    "trailArmPoints": float(getattr(self.cfg, "BULL_TRAIL_ARM_POINTS", 3.0) or 3.0),
                },
            )
            self._ema_pullback = None
            self._ema_pending_side = side
            self._ema_pending_why = why
            if fill_now:
                self._ema_fill_pending_from_bar()
            return
        ok, ext = entry_extension_ok(close, stack.ema20, atr_v, self.cfg)
        through_both = (
            side == Side.LONG and red_above_white_and_blue(stack)
        ) or (
            side == Side.SHORT and red_below_white_and_blue(stack)
        )
        if through_both:
            ok = True
        self._ema_log_cross(side, stack, valid=ok)
        label = "BULLISH" if side == Side.LONG else "BEARISH"
        if ok:
            if through_both:
                print("EMA_INTERSECTION - 9/50 CONFIRM THROUGH SPREAD STRUCTURE", flush=True)
            else:
                print("VALID CROSS - IMMEDIATE ENTRY", flush=True)
            print(
                f"Side:{side.value}  SignalBarClose:{close}  "
                f"EMA9:{stack.ema9:.2f}  EMA20:{stack.ema20:.2f}  EMA50:{stack.ema50:.2f}  "
                f"ATR14:{atr_v:.2f}  ExtensionPoints:{ext.get('extensionPoints')}  "
                f"ExtensionATR:{ext.get('extensionATR')}",
                flush=True,
            )
            self._ema_pullback = None
            self._ema_pending_side = side
            if why in ("EMA_SNIPER_LONG", "EMA_INTERSECTION_LONG") or (
                side == Side.LONG and bool(getattr(self.cfg, "EMA_LONG_SNIPER", True))
            ):
                self._ema_pending_why = str(why or "EMA_INTERSECTION_LONG")
            elif why in ("EMA_SNIPER_SHORT", "EMA_INTERSECTION_SHORT") or (
                side == Side.SHORT and bool(getattr(self.cfg, "EMA_SHORT_SNIPER", True))
            ):
                self._ema_pending_why = str(why or "EMA_INTERSECTION_SHORT")
            else:
                self._ema_pending_why = "EMA_CROSS_LONG" if side == Side.LONG else "EMA_CROSS_SHORT"
            if fill_now:
                self._ema_fill_pending_from_bar()
            return
        if not bool(getattr(self.cfg, "ENABLE_PULLBACK_ENTRY", True)):
            print(
                f"ENTRY REJECTED: EXTENDED  Side:{side.value}  Price:{close}  "
                f"EMA9:{stack.ema9:.2f}  EMA20:{stack.ema20:.2f}  ATR14:{atr_v:.2f}  "
                f"ExtensionPoints:{ext.get('extensionPoints')}  "
                f"ExtensionATR:{ext.get('extensionATR')}  MaximumAllowed:{ext.get('maxExtensionATR')}",
                flush=True,
            )
            self._ema_write(
                "EMA_REJECT",
                side=side.value,
                action="REJECT",
                price=close,
                extra={"message": "ENTRY REJECTED: EXTENDED", "maxExtensionATR": ext.get("maxExtensionATR")},
            )
            return
        print(f"{label} CROSS DETECTED - PRICE EXTENDED", flush=True)
        print("DO NOT CHASE", flush=True)
        print("WAITING FOR PULLBACK", flush=True)
        self._ema_pending_side = Side.NONE
        self._ema_pending_why = ""
        self._ema_pullback = EmaPullbackSetup(
            direction=side,
            signal_price=close,
            signal_bar_time=str(last.get("time") or ""),
            signal_ts=0.0,
            ema9=float(stack.ema9),
            ema20=float(stack.ema20),
            atr=float(atr_v),
            extension_atr=float(ext.get("extensionATR") or 0.0),
            bars_waited=0,
        )
        self._ema_write(
            "EMA_PULLBACK_ARM",
            side=side.value,
            action="ARM",
            price=close,
            trade_state="WAITING_FOR_PULLBACK",
            extra={
                "message": f"{label} CROSS DETECTED - PRICE EXTENDED",
                "signalBarClose": close,
                "originalExtensionATR": ext.get("extensionATR"),
                "pendingDirection": side.value,
            },
        )

    def _ema_cancel_pullback(self, *, why: str, close: float, extra: dict | None = None) -> None:
        setup = self._ema_pullback
        if setup is None:
            return
        side = setup.direction
        label = "LONG" if side == Side.LONG else "SHORT"
        if why == "STRUCTURE":
            msg = f"PENDING {label} CANCELLED - EMA STRUCTURE FAILED"
        else:
            msg = "PENDING SETUP EXPIRED"
        print(msg, flush=True)
        payload = {
            "message": msg,
            "pendingDirection": side.value,
            "barsSinceCross": setup.bars_waited,
            "originalCrossPrice": setup.signal_price,
            "originalExtensionATR": setup.extension_atr,
            **(extra or {}),
        }
        self._ema_write(
            "EMA_PULLBACK_CANCEL",
            side=side.value,
            action="CANCEL",
            price=close,
            trade_state="WAITING",
            extra=payload,
        )
        self._ema_pullback = None

    def _ema_qualify_pullback(self, setup: EmaPullbackSetup, close: float, stack, atr_v: float, zone: dict) -> None:
        side = setup.direction
        enter = "ENTER LONG" if side == Side.LONG else "ENTER SHORT"
        print("PULLBACK ENTRY QUALIFIED", flush=True)
        print(enter, flush=True)
        print(
            f"OriginalCrossPrice:{setup.signal_price}  CurrentPrice:{close}  "
            f"EMA9:{stack.ema9:.2f}  EMA20:{stack.ema20:.2f}  ATR:{atr_v:.2f}  "
            f"OriginalExtensionATR:{setup.extension_atr}  "
            f"CurrentExtensionATR:{zone.get('currentExtensionATR')}  "
            f"BarsSinceCross:{setup.bars_waited}",
            flush=True,
        )
        self._ema_write(
            "EMA_PULLBACK_QUALIFIED",
            side=side.value,
            action="QUALIFY",
            price=close,
            trade_state="WAITING_FOR_PULLBACK",
            extra={
                "message": "PULLBACK ENTRY QUALIFIED",
                "originalCrossPrice": setup.signal_price,
                "originalExtensionATR": setup.extension_atr,
                "currentExtensionATR": zone.get("currentExtensionATR"),
                "barsSinceCross": setup.bars_waited,
                "barsWaited": setup.bars_waited,
            },
        )
        self._ema_pending_side = side
        self._ema_pending_why = "EMA_PULLBACK_LONG" if side == Side.LONG else "EMA_PULLBACK_SHORT"
        self._ema_pullback_fill = {
            "originalCrossPrice": setup.signal_price,
            "originalExtensionATR": setup.extension_atr,
            "barsWaited": setup.bars_waited,
        }
        self._ema_pullback = None

    def _ema_pullback_on_bar(self, stack, new_cross: Side) -> bool:
        """Advance WAITING_FOR_PULLBACK. Returns True if the setup was cancelled/expired."""
        setup = self._ema_pullback
        if setup is None:
            return False
        last = self.completed_bars[-1] if self.completed_bars else {}
        close = float(last.get("close") or 0)
        atr_v = self._ema_atr()
        setup.bars_waited += 1
        still = ema_structure_valid(setup.direction, stack, self.cfg)
        if new_cross != Side.NONE and new_cross != setup.direction:
            still = False
        zone_ok, zone = pullback_zone_ok(setup.direction, close, stack.ema20, atr_v, self.cfg)
        print(
            f"PENDING  Direction:{setup.direction.value}  BarsSinceCross:{setup.bars_waited}  "
            f"Close:{close}  EMA9:{stack.ema9:.2f}  EMA20:{stack.ema20:.2f}  ATR:{atr_v:.2f}  "
            f"CurrentExtensionATR:{zone.get('currentExtensionATR')}  StillValid:{still}",
            flush=True,
        )
        self._ema_write(
            "EMA_PULLBACK_BAR",
            side=setup.direction.value,
            action="WAIT",
            price=close,
            trade_state="WAITING_FOR_PULLBACK",
            extra={
                "pendingDirection": setup.direction.value,
                "barsSinceCross": setup.bars_waited,
                "currentExtensionATR": zone.get("currentExtensionATR"),
                "stillValid": still,
                "originalCrossPrice": setup.signal_price,
            },
        )
        max_wait = int(getattr(self.cfg, "MAX_PULLBACK_WAIT_BARS", 10) or 10)
        if not still:
            self._ema_cancel_pullback(why="STRUCTURE", close=close, extra=zone)
            return True
        if setup.bars_waited > max_wait:
            self._ema_cancel_pullback(why="EXPIRED", close=close, extra=zone)
            return True
        if bool(getattr(self.cfg, "PULLBACK_ENTRY_ON_BAR_CLOSE", True)) and zone_ok:
            self._ema_qualify_pullback(setup, close, stack, atr_v, zone)
        return False

    def _ema_on_completed_bar(self, *, allow_entry: bool = True) -> None:
        if not self._ema_mode():
            self._ema_clear_setup()
            self._push_ema_overlay()
            return
        self._push_ema_overlay()
        stack = read_ema_stack(self.completed_bars, self.cfg)
        if stack is None:
            return
        last = self.completed_bars[-1] if self.completed_bars else {}
        cur_time = str(last.get("time") or "")
        sequential = bars_are_sequential(self._ema_last_bar_time, cur_time)
        prev_time = self._ema_last_bar_time
        if cur_time:
            self._ema_last_bar_time = cur_time
        if not sequential:
            self._ema_reset_long_stack()
        else:
            self._ema_release_long_stack(stack)
            self._ema_release_short_stack(stack)

        if not allow_entry:
            # Seed / historical bars: establish EMA state only. Never arm an entry.
            # Watch is live after seed so the first forming intersection can fill.
            self._ema_reset_long_stack()
            self._ema_armed = True
            return

        if not self._ema_armed:
            self._ema_armed = True

        if not sequential:
            side, why = ema_entry_signal(self.completed_bars, self.cfg, **self._ema_entry_kwargs())
            print(
                f"EMA BAR: {cur_time}  9:{stack.ema9:.2f}  20:{stack.ema20:.2f}  "
                f"50:{stack.ema50:.2f}  cluster:{stack.cluster:.1f}  "
                f"{side.value if side != Side.NONE else 'WAIT'}/{why or 'none'}",
                flush=True,
            )
            if side in (Side.LONG, Side.SHORT):
                self._ema_note_cross_gap()
                self._ema_try_arm_signal(side, stack, allow_entry=True, why=why)
                return
            if side != Side.NONE:
                last = self.completed_bars[-1] if self.completed_bars else {}
                print("IGNORED CROSS: INITIALIZATION / STALE STATE", flush=True)
                self._ema_write(
                    "EMA_IGNORED",
                    side=side.value,
                    action="IGNORE",
                    price=float(last.get("close") or 0),
                    extra={
                        "message": "IGNORED CROSS: INITIALIZATION / STALE STATE",
                        "prevBar": prev_time,
                    },
                )
            return

        if self.paper is not None and bool(getattr(self.paper, "ema_strategy", False)):
            if not bool(getattr(self.cfg, "EMA_OPPOSITE_CROSS_EXIT", False)):
                return
            flip = cross_side(stack)
            if flip != Side.NONE and flip != self.paper.side:
                state = str(getattr(self.paper, "ema_trade_state", "") or "PROBATION")
                require = bool(getattr(self.cfg, "OPPOSITE_CROSS_REQUIRES_CONFIRM", True))
                if require and state == "PROBATION":
                    last = self.completed_bars[-1] if self.completed_bars else {}
                    print("OPPOSITE CROSS IGNORED: STILL IN PROBATION", flush=True)
                    self._ema_write(
                        "EMA_CROSS_IGNORED",
                        side=flip.value,
                        action="IGNORE",
                        price=float(last.get("close") or 0),
                        trade=self.paper,
                        trade_state=state,
                        extra={"message": "OPPOSITE CROSS IGNORED: STILL IN PROBATION"},
                    )
                    return
                self._ema_note_cross_gap()
                self._ema_exit_armed = True
                self._ema_log_cross(flip, stack, valid=True)
                # Exit first. Opposite entry is evaluated only after flatten.
                self._ema_reversal_side = flip
            return

        side, why = ema_entry_signal(self.completed_bars, self.cfg, **self._ema_entry_kwargs())
        print(
            f"EMA BAR: {cur_time}  9:{stack.ema9:.2f}  20:{stack.ema20:.2f}  "
            f"50:{stack.ema50:.2f}  cluster:{stack.cluster:.1f}  "
            f"{side.value if side != Side.NONE else 'WAIT'}/{why or 'none'}",
            flush=True,
        )
        if self._ema_pullback is not None:
            if (
                self._ema_pullback.direction == Side.LONG
                and red_above_white_and_blue(stack)
                and red_rising(stack)
            ):
                self._ema_note_cross_gap()
                self._ema_try_arm_signal(Side.LONG, stack, allow_entry=True, why="EMA_SNIPER_LONG")
                return
            if self._ema_pullback.direction == Side.SHORT and red_below_white_and_blue(stack):
                self._ema_note_cross_gap()
                self._ema_try_arm_signal(Side.SHORT, stack, allow_entry=True, why="EMA_SNIPER_SHORT")
                return
            cancelled = self._ema_pullback_on_bar(stack, side)
            if cancelled and side != Side.NONE:
                self._ema_note_cross_gap()
                self._ema_try_arm_signal(side, stack, allow_entry=True, why=why)
            return
        if side == Side.NONE:
            self._ema_ignore_sniper(str(why or "").upper(), stack)
            return
        self._ema_note_cross_gap()
        self._ema_try_arm_signal(side, stack, allow_entry=True, why=why)

    def _ema_scan_live_long(self) -> None:
        """Keep watching while flat: red through white+blue must still get us in."""
        if not self._ema_mode():
            return
        if len(self.completed_bars) < 15:
            return
        self._ema_armed = True
        if self.paper is not None or self._ema_pending_side != Side.NONE:
            return
        live = self._ema_live_bars()
        side, why = ema_entry_signal(live, self.cfg, **self._ema_entry_kwargs(live))
        stack = read_ema_stack(live, self.cfg)
        self._ema_release_long_stack(stack)
        self._ema_release_short_stack(stack)
        if side not in (Side.LONG, Side.SHORT):
            tag = why
            if why == "CHOPPY":
                tag = "CHOPPY"
            elif stack is not None:
                long_tag = long_sniper_reason(
                    stack, self.cfg, bias=self._ema_bias(), atr=self._ema_atr()
                )
                short_tag = short_sniper_reason(
                    stack, self.cfg, bias=self._ema_bias(), atr=self._ema_atr()
                )
                if long_tag in (
                    "WHITE_ONLY",
                    "NOT_BEARISH",
                    "BLUE_WARMUP",
                    "RED_FALLING",
                    "STACK_STALE",
                    "STALE_BLUE",
                    "TIGHT",
                ):
                    tag = long_tag
                elif short_tag in (
                    "WHITE_ONLY",
                    "NOT_BULLISH",
                    "BLUE_WARMUP",
                    "STACK_STALE",
                    "STALE_BLUE",
                    "TIGHT",
                ):
                    tag = short_tag
                else:
                    tag = long_tag or short_tag or why
            self._ema_note_watch(str(tag or "no_cross"), stack)
            return
        if stack is None:
            return
        if side == Side.LONG:
            done = read_ema_stack(self.completed_bars, self.cfg)
            ok, reason = ema_long_arm_ok(
                stack,
                why,
                self.cfg,
                stack_taken=self._ema_long_stack_taken,
                completed=done,
                live_tick=True,
                regime=self._ema_regime(),
            )
            if not ok:
                self._ema_note_watch(reason, done or stack)
                return
        if side == Side.SHORT:
            done = read_ema_stack(self.completed_bars, self.cfg)
            rsi_now, rsi_prev, rsi_peak = self._ema_rsi_state(self.completed_bars)
            ok, reason = ema_short_arm_ok(
                stack,
                why,
                self.cfg,
                stack_taken=self._ema_short_stack_taken,
                completed=done,
                live_tick=True,
                bias=self._ema_bias(),
                atr=self._ema_atr(),
                rsi=rsi_now,
                rsi_prev=rsi_prev,
                rsi_peak=rsi_peak,
                regime=self._ema_regime(),
                fade_ready=self._ema_fade_ready(),
            )
            if not ok:
                self._ema_note_watch(reason, done or stack)
                return
        self._ema_watch = ""
        self._ema_note_cross_gap()
        self._ema_try_arm_signal(side, stack, allow_entry=True, why=why, fill_now=False)

    def _ema_note_watch(self, tag: str, stack) -> None:
        why = str(tag or "").strip() or "no_cross"
        self._ema_watch = why.upper() if why else ""
        last = self.completed_bars[-1] if self.completed_bars else {}
        key = f"{last.get('time') or ''}|{self._ema_watch}"
        if key == self._ema_watch_logged:
            return
        self._ema_watch_logged = key
        if self._ema_watch in (
            "WHITE_ONLY",
            "NOT_BEARISH",
            "NOT_BULLISH",
            "BLUE_WARMUP",
            "RED_FALLING",
            "STACK_STALE",
            "STALE_BLUE",
            "TIGHT",
            "STACK_USED",
            "BULL_WAIT_CLOSE",
            "WAIT_CLOSE",
            "NO_SNIPER",
            "CHOPPY",
            "RSI_NOT_DYING",
            "NO_INTERSECT",
            "RED_RISING",
            "FADE_SHORT_OFF",
            "WAIT_BREAK",
        ):
            if stack is not None:
                self._ema_ignore_sniper(self._ema_watch, stack)
            return
        if self._ema_watch in ("NO_SNIPER", "NO_CROSS"):
            print(f"EMA WATCH: {self._ema_watch}  (not choppy/regime — lines only)", flush=True)
            if stack is not None:
                print(
                    f"EMA9:{stack.ema9:.2f}  EMA20:{stack.ema20:.2f}  EMA50:{stack.ema50:.2f}",
                    flush=True,
                )

    def _ema_flat_tick(self, tick: Tick, snap: MarketSnapshot, scores: ScoreBundle) -> dict:
        if self._maybe_complete_goal(tick=tick):
            return {"state": self.sm.state.value, "decision": "GOAL_HIT"}
        if not self.cfg.MARK2_ENABLED:
            if self._ema_pending_side != Side.NONE:
                print("DISARMED - NO TRADE", flush=True)
                self._ema_write(
                    "EMA_IGNORED",
                    side=self._ema_pending_side.value,
                    action="IGNORE",
                    price=tick.price,
                    extra={"message": "DISARMED - NO TRADE"},
                )
                self._ema_clear_setup()
            return {"state": self.sm.state.value, "decision": "DISABLED"}
        if self.goal_met:
            return {"state": self.sm.state.value, "decision": "GOAL_HIT"}
        if self._trade_gap_blocked():
            left = self._trade_gap_left()
            if self._ema_pending_side != Side.NONE or self._ema_pullback is not None:
                self._ema_clear_setup()
            self._scout_view = ScoutView(
                action="HOLD",
                why="COOLDOWN",
                bullets=[
                    f"PAUSE {left:.1f}S · NO BACK TO BACK",
                    "NEXT ENTRY MUST STILL FIT THE CROSS",
                ],
            )
            self.sm.state = EngineState.WATCHING
            return {"state": self.sm.state.value, "decision": "WAIT", "reject": "COOLDOWN"}
        self._ema_scan_live_long()
        side = self._ema_pending_side
        stack = read_ema_stack(self._ema_live_bars(), self.cfg)
        atr_v = self._ema_atr()
        if (
            side == Side.NONE
            and self._ema_pullback is not None
            and not bool(getattr(self.cfg, "PULLBACK_ENTRY_ON_BAR_CLOSE", True))
            and stack is not None
        ):
            setup = self._ema_pullback
            if ema_structure_valid(setup.direction, stack, self.cfg):
                zone_ok, zone = pullback_zone_ok(
                    setup.direction, tick.price, stack.ema20, atr_v, self.cfg
                )
                if zone_ok:
                    self._ema_qualify_pullback(setup, tick.price, stack, atr_v, zone)
                    side = self._ema_pending_side
        if side == Side.NONE:
            scouted = self._scout_on_flat(tick, snap, scores, stack)
            if scouted is not None:
                return scouted
            self.sm.state = EngineState.WATCHING
            return {"state": self.sm.state.value, "decision": "WAIT", "reject": ""}
        if stack is not None and str(self._ema_pending_why) not in (
            "EMA_SNIPER_LONG",
            "EMA_SNIPER_SHORT",
            "EMA_INTERSECTION_LONG",
            "EMA_INTERSECTION_SHORT",
            "EMA_RSI_LONG",
            "EMA_INTERSECT_SHORT",
            "EMA_CHOP_LONG",
            "EMA_FADE_SHORT",
        ):
            fill_ok, fill_ext = entry_extension_ok(tick.price, stack.ema20, atr_v, self.cfg)
            # Seed / stale fill only. The through-both sniper fills on the signal bar.
            if (not fill_ok) and float(fill_ext.get("extensionATR") or 0) > 2.5:
                print(
                    f"ENTRY REJECTED: EXTENDED  Side:{side.value}  Price:{tick.price}  "
                    f"EMA9:{stack.ema9:.2f}  EMA20:{stack.ema20:.2f}  ATR14:{atr_v:.2f}  "
                    f"ExtensionPoints:{fill_ext.get('extensionPoints')}  "
                    f"ExtensionATR:{fill_ext.get('extensionATR')}  "
                    f"MaximumAllowed:{fill_ext.get('maxExtensionATR')}",
                    flush=True,
                )
                self._ema_write(
                    "EMA_REJECT",
                    side=side.value,
                    action="REJECT",
                    price=tick.price,
                    timestamp=tick.ts,
                    extra={
                        "message": "ENTRY REJECTED: EXTENDED",
                        "maxExtensionATR": fill_ext.get("maxExtensionATR"),
                    },
                )
                self._ema_clear_setup()
                self.sm.state = EngineState.WATCHING
                return {"state": self.sm.state.value, "decision": "REJECT", "reject": "EXTENDED"}
        event = EventRecord(
            event_id=self.events._next_id,
            event_type=EventType.EMA_CROSS,
            direction=side,
            started_ts=tick.ts,
            started_price=tick.price,
            started_bar_time=str((self.completed_bars[-1] or {}).get("time") or ""),
            peak_confidence=100.0,
            peak_opportunity=100.0,
        )
        self.events._next_id += 1
        self.events.active = event
        ind = indicator_snapshot(self.completed_bars, self.cfg)
        rsi_txt = f"RSI {ind['rsi']:.0f}"
        self._last_entry_tags = {
            "trigger": self._ema_pending_why or "EMA_CROSS",
            "book": "",
            "rsi": rsi_txt,
        }
        enter = "ENTER LONG" if side == Side.LONG else "ENTER SHORT"
        if str(self._ema_pending_why) == "EMA_CHOP_LONG":
            enter = "ENTER LONG CHOP"
        elif str(self._ema_pending_why) == "EMA_RSI_LONG":
            enter = "ENTER LONG BULL"
        elif str(self._ema_pending_why) in ("EMA_SNIPER_SHORT", "EMA_INTERSECTION_SHORT"):
            enter = "ENTER SHORT INTERSECTION"
        elif str(self._ema_pending_why) == "EMA_INTERSECTION_LONG":
            enter = "ENTER LONG INTERSECTION"
        elif str(self._ema_pending_why) == "EMA_INTERSECT_SHORT":
            enter = "ENTER SHORT INTERSECT"
        elif str(self._ema_pending_why) == "EMA_FADE_SHORT":
            enter = "ENTER SHORT FADE"
        pullback = str(self._ema_pending_why).startswith("EMA_PULLBACK")
        extra = {"message": "PULLBACK ENTRY" if pullback else enter}
        if pullback:
            fill_info = dict(self._ema_pullback_fill or {})
            extra.update(
                {
                    "entryMode": "PULLBACK",
                    "originalCrossPrice": fill_info.get("originalCrossPrice"),
                    "originalExtensionATR": fill_info.get("originalExtensionATR"),
                    "entryPrice": tick.price,
                    "barsWaited": fill_info.get("barsWaited"),
                }
            )
            print(
                f"PULLBACK ENTRY  OriginalCrossPrice:{fill_info.get('originalCrossPrice')}  "
                f"OriginalExtensionATR:{fill_info.get('originalExtensionATR')}  "
                f"EntryPrice:{tick.price}  BarsWaited:{fill_info.get('barsWaited')}",
                flush=True,
            )
        row = self._ema_write(
            "EMA_ENTRY",
            side=side.value,
            action="ENTRY",
            price=tick.price,
            timestamp=tick.ts,
            trade_state="PROBATION",
            extra=extra,
        )
        self._ema_print_row(enter, row)
        self._ema_clear_setup()
        decision = self._arm_and_maybe_execute(tick, snap, scores, event)
        if self.paper is None:
            self._scout_note_miss(side, str(self._last_entry_tags.get("trigger") or decision))
            scouted = self._scout_on_flat(tick, snap, scores, stack)
            if scouted is not None:
                return scouted
        elif bool(getattr(self.cfg, "ENABLE_AI_SCOUT", True)):
            self._scout_view = ScoutView(
                side=side,
                action="HOLD",
                why="BOT_HELD",
                bullets=["FILL STUCK · SCOUT STANDS DOWN"],
            )
            self._scout_missed_side = Side.NONE
            self._scout_missed_why = ""
        return {
            "state": self.sm.state.value,
            "decision": decision,
            "reject": "",
            "event": event.event_id,
        }

    def _manage_ema(self, tick: Tick, snap: MarketSnapshot, scores: ScoreBundle) -> dict:
        assert self.paper is not None
        prev_state = str(getattr(self.paper, "ema_trade_state", "") or "")
        prev_runner = bool(getattr(self.paper, "runner_trail_on", False))
        prev_stop = float(self.paper.stop)
        done_stack = read_ema_stack(self.completed_bars, self.cfg)
        start = int(getattr(self, "_ema_entry_bar_i", 0) or 0)
        post = self.completed_bars[start + 1 :]
        if self.paper.side == Side.LONG:
            highs = [float(b.get("high") or 0) for b in post]
            done_anchor = max(highs) if highs else 0.0
        else:
            lows = [float(b.get("low") or 0) for b in post if float(b.get("low") or 0) > 0]
            done_anchor = min(lows) if lows else 0.0
        ind = indicator_snapshot(self.completed_bars, self.cfg)
        rsi_now = float(ind.get("rsi") or 0)
        rsi_prev = rsi_now
        if len(self.completed_bars) > 20:
            prev_ind = indicator_snapshot(self.completed_bars[:-1], self.cfg)
            rsi_prev = float(prev_ind.get("rsi") or 0)
        done, why, st = manage_ema_hold(
            self.paper,
            price=tick.price,
            exit_armed=self._ema_exit_armed,
            cfg=self.cfg,
            ema9=None if done_stack is None else float(done_stack.ema9),
            ema20=None if done_stack is None else float(done_stack.ema20),
            ema50=None if done_stack is None else float(done_stack.ema50),
            atr=self._ema_atr(),
            regime=str(getattr(snap, "trend_regime", "") or ""),
            completed_anchor=done_anchor if done_anchor > 0 else None,
            bias=str(getattr(snap, "trend_bias", "") or ""),
            rsi=rsi_now,
            rsi_prev=rsi_prev,
            account_equity=float(self.account.get("equity") or self.account.get("cash") or 0),
            bars=post,
        )
        new_state = str(getattr(self.paper, "ema_trade_state", "") or "")
        if new_state and new_state != prev_state:
            print(f"TRADE STATE: {new_state}", flush=True)
            self._ema_write(
                "EMA_TRADE_STATE",
                side=self.paper.side.value,
                action="STATE",
                price=tick.price,
                timestamp=tick.ts,
                trade=self.paper,
                trade_state=new_state,
                extra={"message": f"TRADE STATE: {new_state}"},
            )
        if bool(getattr(self.paper, "runner_trail_on", False)):
            extreme = self.paper.peak if self.paper.side == Side.LONG else self.paper.trough
            trail_dist = abs(float(self.paper.stop) - float(extreme))
            if not prev_runner:
                print("RUNNER TRAIL ACTIVATED", flush=True)
                self._ema_write(
                    "EMA_RUNNER",
                    side=self.paper.side.value,
                    action="RUNNER",
                    price=tick.price,
                    timestamp=tick.ts,
                    trade=self.paper,
                    trade_state=new_state,
                    extra={
                        "message": "RUNNER TRAIL ACTIVATED",
                        "atrAtEntry": self.paper.atr_at_entry,
                        "trailDistance": trail_dist,
                        "trailATR": getattr(self.paper, "runner_trail_atr", None),
                        "spreadATR": getattr(self.paper, "runner_spread_atr", None),
                        "newStop": self.paper.stop,
                    },
                )
            elif abs(float(self.paper.stop) - prev_stop) > 1e-9:
                spread = getattr(self.paper, "runner_spread_atr", None)
                trail_atr = getattr(self.paper, "runner_trail_atr", None)
                print(
                    f"RUNNER TRAIL UPDATED  Highest/Lowest:{extreme}  "
                    f"ATRAtEntry:{self.paper.atr_at_entry:.4f}  "
                    f"SpreadATR:{'' if spread is None else f'{float(spread):.4f}'}  "
                    f"TrailATR:{'' if trail_atr is None else f'{float(trail_atr):.4f}'}  "
                    f"TrailDistance:{trail_dist:.2f}  NewStop:{self.paper.stop}",
                    flush=True,
                )
                self._ema_write(
                    "EMA_RUNNER",
                    side=self.paper.side.value,
                    action="RUNNER",
                    price=tick.price,
                    timestamp=tick.ts,
                    trade=self.paper,
                    trade_state=new_state,
                    extra={
                        "message": "RUNNER TRAIL UPDATED",
                        "atrAtEntry": self.paper.atr_at_entry,
                        "trailDistance": trail_dist,
                        "trailATR": trail_atr,
                        "spreadATR": spread,
                        "newStop": self.paper.stop,
                    },
                )
        self.sm.state = st
        decision = "EXIT" if done else "MANAGE"
        if done:
            if self.paper.side == Side.LONG:
                self._ema_rsi_peak = max(
                    float(self._ema_rsi_peak or 0),
                    float(getattr(self.paper, "rsi_peak", 0) or 0),
                )
            leave = "EXIT LONG" if self.paper.side == Side.LONG else "EXIT SHORT"
            if why in ("OPPOSITE_EMA_CROSS", "RED_WHITE_CROSS"):
                print(f"{leave} - OPPOSITE EMA CROSS", flush=True)
            elif why == "CATASTROPHIC_STOP":
                print("CATASTROPHIC STOP HIT", flush=True)
                print(leave, flush=True)
            elif why == "ATR_STOP":
                print("CATASTROPHIC STOP HIT", flush=True)
                print(leave, flush=True)
            elif why == "MFE_GIVEBACK":
                print("MFE GIVEBACK HIT - 30% OFF PEAK", flush=True)
                print(leave, flush=True)
            elif why == "PROTECT":
                print("PROTECT STOP HIT", flush=True)
                print(leave, flush=True)
            elif why == "COMPRESSION":
                print(f"{leave} - EMA SPREAD COMPRESSED", flush=True)
            elif why == "STRUCTURE_FAILURE":
                print(f"{leave} - 9/20 STRUCTURE BROKE", flush=True)
            elif why == "RUNNER_TRAIL":
                print("RUNNER TRAIL HIT", flush=True)
                print(leave, flush=True)
            elif why == "HARD_STOP":
                print("10 PT STOP HIT", flush=True)
                print(leave, flush=True)
            elif why == "TIP_TRAIL":
                print("5.5 TIP TRAIL HIT", flush=True)
                print(leave, flush=True)
            elif why == "TARGET":
                print("TARGET HIT", flush=True)
                print(leave, flush=True)
            elif why == "WHITE_CROSS":
                print(f"{leave} - RED CROSSED WHITE DOWN", flush=True)
            elif why == "WHITE_RSI":
                print(f"{leave} - RED CROSSED WHITE AND RSI FADING", flush=True)
            else:
                print(leave, flush=True)
            if self.paper.side == Side.LONG and str(self._ema_bias() or "").upper() == "BULLISH":
                self._ema_fade_armed = True
                self._ema_fade_block_bar_i = max(0, len(self.completed_bars) - 1)
                print("BULLISH FADE - WAITING FOR INTERSECTION DOWN", flush=True)
            atr_e = max(float(getattr(self.paper, "atr_at_entry", 0) or 0), 1e-9)
            pts = (
                tick.price - self.paper.entry
                if self.paper.side == Side.LONG
                else self.paper.entry - tick.price
            )
            bars_held = max(0, len(self.completed_bars) - 1 - int(self._ema_entry_bar_i or 0))
            row = self._ema_write(
                "EMA_EXIT",
                side=self.paper.side.value,
                action="EXIT",
                price=tick.price,
                timestamp=tick.ts,
                trade=self.paper,
                trade_state=new_state,
                extra={
                    "message": leave,
                    "reason": why,
                    "exitReason": why,
                    "entryPrice": self.paper.entry,
                    "exitPrice": tick.price,
                    "points": round(pts, 4),
                    "MFE": self.paper.mfe,
                    "MAE": self.paper.mae,
                    "MFE_ATR": round(self.paper.mfe / atr_e, 4),
                    "MAE_ATR": round(self.paper.mae / atr_e, 4),
                    "BarsHeld": bars_held,
                },
            )
            self._ema_print_row(leave, row)
            reversal = self._ema_reversal_side
            self._close_position(tick, snap, scores, why)
            self._last_manage_state = ""
            self._ema_maybe_queue_reversal(reversal)
        else:
            self._push_levels()
        return {"state": st.value, "decision": decision, "reject": why}

    def _ema_maybe_queue_reversal(self, flip: Side) -> None:
        self._ema_reversal_side = Side.NONE
        if flip == Side.NONE or not self._ema_mode() or self.paper is not None:
            return
        if not self._ema_side_allowed(flip):
            print(
                "SHORT IGNORED: LONG_ONLY" if flip == Side.SHORT else f"{flip.value} IGNORED: SIDE OFF",
                flush=True,
            )
            return
        stack = read_ema_stack(self.completed_bars, self.cfg)
        if stack is None:
            return
        self._ema_try_arm_signal(flip, stack, allow_entry=True)

    def _push_levels(self, *, clear: bool = False) -> None:
        sink = self.execution.sink
        if sink is None or not hasattr(sink, "send_levels"):
            return
        if clear or self.paper is None:
            sink.send_levels(clear=True)
            self._last_levels = None
            self._last_nt_stop = None
            return
        p = self.paper
        # Red = frozen hard stop. Purple = working protect (tip trail / $100 arm).
        hard = float(getattr(p, "hard_stop", 0) or 0) or float(p.stop)
        protect = float(p.stop) or hard
        tick = max(float(getattr(self.cfg, "TICK_SIZE", 0.25) or 0.25), 0.25)
        hard = _tick_align(hard, tick)
        protect = _tick_align(protect, tick)
        if bool(getattr(p, "ema_strategy", False)):
            trail_on = bool(getattr(p, "runner_trail_on", False)) or abs(protect - hard) > 1e-9
        else:
            trail_on = bool(p.target_touched) or bool(getattr(p, "flip_armed", False)) or bool(
                getattr(p, "early_trail", False)
            )
        trail_px = float(protect) if trail_on else 0.0
        key = (
            round(p.entry, 2),
            round(hard, 2),
            round(protect, 2),
            round(p.target, 2),
            round(trail_px, 2),
            trail_on,
            p.side.value,
        )
        if key == self._last_levels:
            return
        self._last_levels = key
        try:
            sink.send_levels(
                side=p.side.value,
                entry=p.entry,
                stop=hard,
                target=p.target,
                trail=trail_px,
                trail_active=trail_on,
                simulate=self.mode != RunMode.LIVE,
            )
        except Exception:
            pass
        if self.mode == RunMode.LIVE and hasattr(sink, "send_stop"):
            if bool(getattr(p, "ema_strategy", False)):
                if protect > 0 and (
                    self._last_nt_stop is None or abs(protect - self._last_nt_stop) >= tick - 1e-12
                ):
                    try:
                        sink.send_stop(protect, reason="ema_protect")
                        self._last_nt_stop = protect
                    except Exception:
                        pass
                return
            # Manual: only publish the frozen hard stop — never tip-trail the operator.
            if p.manual_entry:
                hard = float(getattr(p, "hard_stop", 0) or 0) or float(p.stop)
                if hard > 0:
                    sink.send_stop(hard, reason="manual_hard")
                return
            # Deep hold: keep the wide hard stop on NT until $300 bank or flip trail.
            if deep_hold_active(self.cfg, p) and not trail_on:
                if hard > 0:
                    sink.send_stop(hard, reason="deep_hold_wide")
                return
            px = float(self.last_snap.price) if self.last_snap else p.entry
            from .exits import early_lock_price

            if bool(getattr(p, "early_trail", False)) and not p.target_touched:
                lock = early_lock_price(p, self.cfg)
            else:
                lock = lock_price(p.entry, p.side, self.cfg, trade=p)
            broker_stop, flatten = broker_stop_for_nt(p, p.stop, px, lock, self.cfg)
            if (
                flatten
                and not p.target_touched
                and not p.manual_entry
                and not getattr(p, "flip_armed", False)
                and not getattr(p, "early_trail", False)
            ):
                snap = self.last_snap
                scores = self.last_scores or ScoreBundle()
                if snap is None:
                    snap = MarketSnapshot(
                        ts=p.entry_ts,
                        price=px,
                        completed_bars=[],
                        forming_bar=None,
                    )
                self._close_position(
                    Tick(ts=float(snap.ts), price=px),
                    snap,
                    scores,
                    "STOP",
                )
                self._last_manage_state = ""
                return
            if broker_stop is not None:
                sink.send_stop(broker_stop, reason="trail")

    def _log_trade_telemetry(
        self,
        t: PaperTrade,
        tick: Tick,
        snap: MarketSnapshot,
        scores: ScoreBundle,
        *,
        pts: float,
        pnl: float,
        why: str,
    ) -> None:
        hold = max(0.0, tick.ts - t.entry_ts)
        qty = int(t.qty)
        pv = float(self.cfg.POINT_VALUE)
        gross = pts * pv * qty
        mfe_d = t.mfe * pv * qty
        mae_d = t.mae * pv * qty
        capture_pct = (gross / mfe_d * 100.0) if mfe_d > 0 and gross > 0 else 0.0
        entry_eff = t.mfe / max(t.mfe + t.mae, 1e-9)
        exit_eff = (pts / t.mfe) if t.mfe > 0 and pts > 0 else 0.0
        stop_pts = abs(t.entry - t.stop)
        risk_d = stop_pts * pv * qty
        gap = abs(scores.long_confidence - scores.short_confidence)
        self.log.write(
            "MARK2_TRADE_TELEMETRY",
            tradeId=f"{t.entry_ts}|{t.side.value}|{round(t.entry, 2)}",
            timestampEntry=t.entry_ts,
            timestampExit=tick.ts,
            side=t.side.value,
            contracts=qty,
            sessionBucket=snap.session,
            eventType=t.event_type,
            eventId=t.event_id,
            regime=t.entry_regime or snap.trend_regime,
            bias=t.entry_bias or snap.trend_bias,
            experimental=bool(t.experimental),
            entryPrice=round(t.entry, 2),
            exitPrice=round(tick.price, 2),
            initialStop=round(t.stop, 2),
            initialRiskPts=round(stop_pts, 2),
            initialRiskDollars=round(risk_d, 2),
            confidenceEntry=round(t.entry_confidence, 2),
            opportunityEntry=round(t.entry_opportunity, 2),
            rvolEntry=round(t.entry_rvol, 4),
            impulseEntry=round(t.entry_impulse, 2),
            velocityEntry=round(t.entry_velocity, 4),
            extensionEntry=round(t.entry_extension, 2),
            longConf=round(scores.long_confidence, 2),
            shortConf=round(scores.short_confidence, 2),
            longOpp=round(scores.long_opportunity, 2),
            shortOpp=round(scores.short_opportunity, 2),
            directionGap=round(gap, 2),
            atr=round(float(snap.atr), 4),
            vwapDistance=round(float(snap.vwap_distance_atr), 4),
            emaDistance=round(float(snap.ema_distance_atr), 4),
            structure=snap.structure_state,
            mfePts=round(t.mfe, 2),
            maePts=round(t.mae, 2),
            mfeDollars=round(mfe_d, 2),
            maeDollars=round(mae_d, 2),
            timeToMfe2=t.time_to_mfe_2,
            timeToMfe4=t.time_to_mfe_4,
            timeToBank=t.time_to_bank,
            holdSec=round(hold, 2),
            bankReached=bool(t.target_touched),
            peakMfeAfterBank=round(t.peak_mfe_after_bank, 2),
            exitReason=why,
            stopAtExit=round(t.stop, 2),
            grossPnl=round(gross, 2),
            fees=round(t.fees, 2),
            netPnl=round(pnl, 2),
            mfeCapturePct=round(capture_pct, 2),
            entryEfficiency=round(entry_eff, 3),
            exitEfficiency=round(exit_eff, 3),
        )

    def _fold_forming(self, tick: Tick) -> None:
        bar_t = tick.bar_time or (
            str(self.forming_bar.get("time")) if self.forming_bar else ""
        )
        px = float(tick.price)
        if self.forming_bar is None or str(self.forming_bar.get("time") or "") != bar_t:
            self.forming_bar = {
                "time": bar_t,
                "open": float(tick.forming_open or px),
                "high": float(tick.forming_high or px),
                "low": float(tick.forming_low or px),
                "close": px,
                "volume": float(tick.forming_volume or tick.volume or 0),
            }
            return
        b = self.forming_bar
        hi = max(float(b["high"]), float(tick.forming_high or px), px)
        lo = min(float(b["low"]), float(tick.forming_low or px) or px, px)
        if tick.forming_low and tick.forming_low > 0:
            lo = min(lo, float(tick.forming_low))
        b["high"] = hi
        b["low"] = lo
        b["close"] = px
        if tick.forming_volume > 0:
            b["volume"] = float(tick.forming_volume)
        elif tick.volume > 0:
            b["volume"] = max(float(b.get("volume") or 0), float(tick.volume))


def _num(v: Any) -> float:
    try:
        n = float(v or 0)
    except (TypeError, ValueError):
        return 0.0
    return n if n == n else 0.0


def _pick_num(msg: dict[str, Any], *keys: str) -> float | None:
    for k in keys:
        if k not in msg or msg[k] is None or msg[k] == "":
            continue
        try:
            n = float(msg[k])
        except (TypeError, ValueError):
            continue
        if n == n:
            return n
    return None


def _positive_num(msg: dict[str, Any], *keys: str) -> float | None:
    n = _pick_num(msg, *keys)
    if n is None or n <= 0.01:
        return None
    return n


def _avg_volume(bars: list[dict], lookback: int) -> float:
    if not bars:
        return 0.0
    chunk = bars[-lookback:]
    vols = [float(b.get("volume") or 0) for b in chunk]
    return (sum(vols) / len(vols)) if vols else 0.0


def _bar_ts(bar: dict) -> float:
    t = bar.get("time")
    if isinstance(t, (int, float)):
        return float(t)
    return 0.0


def _tick_align(price: float, tick: float) -> float:
    step = max(float(tick), 0.25)
    return round(round(float(price) / step) * step, 2)


def _open_points(paper: PaperTrade, price: float) -> float:
    if paper.side == Side.LONG:
        return float(price) - paper.entry
    return paper.entry - float(price)


def _open_pnl(paper: PaperTrade, price: float, cfg: Mark2Config) -> float:
    return _open_points(paper, price) * float(cfg.POINT_VALUE) * int(paper.qty)


def _et_clock(ts: float) -> tuple[str, str]:
    try:
        dt = datetime.fromtimestamp(float(ts), ZoneInfo("America/New_York"))
    except Exception:
        dt = datetime.fromtimestamp(float(ts or 0), timezone.utc)
    return dt.strftime("%H:%M:%S"), dt.strftime("%m/%d")


def _closed_trade_row(
    *,
    side: str,
    entry: float,
    exit_px: float,
    qty: int,
    pts: float,
    pnl: float,
    fees: float,
    mfe: float,
    mae: float,
    reason: str,
    ts: float,
) -> dict[str, Any]:
    clock, day = _et_clock(ts)
    entry_r = round(float(entry), 2)
    exit_r = round(float(exit_px), 2)
    pnl_r = round(float(pnl), 2)
    return {
        "key": f"exit|{ts}|{side}|{entry_r}|{exit_r}|{pnl_r}",
        "ts": ts,
        "clock": clock,
        "day": day,
        "side": side,
        "entry": entry_r,
        "exit": exit_r,
        "qty": int(qty),
        "pts": round(float(pts), 2),
        "pnl": pnl_r,
        "fees": round(float(fees), 2),
        "mfe": round(float(mfe), 2),
        "mae": round(float(mae), 2),
        "reason": str(reason or ""),
        "open": False,
    }
