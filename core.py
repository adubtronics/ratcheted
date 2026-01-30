"""
core.py

Central shared definitions for the IBKR trading bot + GUI.

This file intentionally contains only:
- Enums and dataclasses that define the shared state model
- Runtime configuration schema + load/save helpers
- Logging/event ring buffer with spam suppression
- Intent and plan definitions passed between threads
- State machine helper enforcing "one transition per scan tick"

Python: 3.10.4
"""

from __future__ import annotations

import datetime
import json
import os
import threading
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple
from collections import deque
from zoneinfo import ZoneInfo


# =============================================================================
# SECTION: Time utilities (ET + monotonic)
# =============================================================================

_ET = ZoneInfo("America/New_York")


def now_ts() -> float:
    """Return current wall-clock timestamp (seconds since epoch)."""
    return time.time()


def fmt_ts_et(ts: float) -> str:
    """Format a Unix timestamp in Eastern Time as MM/DD/YYYY HH:MM:SS.MMM AM/PM."""
    dt = datetime.datetime.fromtimestamp(float(ts), tz=_ET)
    ms = int(dt.microsecond / 1000)
    return dt.strftime("%m/%d/%Y %I:%M:%S") + f".{ms:03d} " + dt.strftime("%p")


def now_et_hhmm() -> str:
    """Return current Eastern Time in HH:MM format."""
    return time.strftime("%H:%M", time.localtime(now_ts()))


def monotonic_s() -> float:
    """Return monotonic clock seconds for elapsed-time measurement."""
    return time.monotonic()


# =============================================================================
# SECTION: Enums (system-wide)
# =============================================================================

class AccountMode(str, Enum):
    """Selected IBKR account environment."""
    PAPER = "PAPER"
    LIVE = "LIVE"



class BudgetMode(str, Enum):
    """Budget sizing mode for strategy position sizing."""
    PCT_AVAILABLE_FUNDS = "PCT_AVAILABLE_FUNDS"
    FIXED_USD = "FIXED_USD"


class RunMode(str, Enum):
    """
    Execution mode:
    - REAL: place orders via IBKR
    - SIM: do not place orders; deterministic simulated fills based on quotes
    - REPLAY: run the same logic on recorded MarketSnapshot series
    """
    REAL = "REAL"
    SIM = "SIM"
    REPLAY = "REPLAY"


class TriggerChannel(str, Enum):
    """Straddle trigger channels (arbitration priority: OPEN > TIMED > GATED)."""
    OPEN = "OPEN"
    TIMED = "TIMED"
    GATED = "GATED"


class StrategyId(str, Enum):
    """Strategy identifiers used in trade registry and analytics."""
    STRADDLE = "STRADDLE"
    PCS_TAIL = "PCS_TAIL"


class GateType(str, Enum):
    """Gate classification chosen by gate logic."""
    NONE = "NONE"
    BULL = "BULL"
    BEAR = "BEAR"
    NEUTRAL = "NEUTRAL"


class GateState(str, Enum):
    """
    Gate evaluation/arming states:
    - DISARMED: not armable, not armed
    - ARMABLE: conditions met; can be armed (auto or manual)
    - ARMED: armed and waiting for PASS
    - PASS: pass condition met (may emit intent)
    - BLOCKED: gate cannot be used (e.g., deadline exceeded or day lock)
    """
    DISARMED = "DISARMED"
    ARMABLE = "ARMABLE"
    ARMED = "ARMED"
    PASS = "PASS"
    BLOCKED = "BLOCKED"


class TradeState(str, Enum):
    """Trade lifecycle state machine."""
    IDLE = "IDLE"
    ENTERING = "ENTERING"
    OPEN = "OPEN"
    EXITING = "EXITING"
    CLOSED = "CLOSED"


class LegState(str, Enum):
    """Per-leg lifecycle state machine."""
    IDLE = "IDLE"
    ENTERING = "ENTERING"
    OPEN = "OPEN"
    TRIMMING = "TRIMMING"
    EXITING = "EXITING"
    CLOSED = "CLOSED"


class TailStopState(str, Enum):
    """Tail stop activation/ratchet/hang state machine."""
    INACTIVE = "INACTIVE"
    ACTIVE = "ACTIVE"
    HANGING = "HANGING"
    EXITING = "EXITING"
    CLOSED = "CLOSED"


class LogLevel(str, Enum):
    """Severity for event log rows."""
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"


class DayLockType(str, Enum):
    """Day lock keys."""
    STRADDLE_TIMED = "STRADDLE_TIMED"
    STRADDLE_GATED = "STRADDLE_GATED"
    PCS = "PCS"


# =============================================================================
# SECTION: State machine transition guard
# =============================================================================

@dataclass
class TransitionGuard:
    """
    Enforces the rule: "at most one state change per scan tick" for a given
    state machine instance.

    A "scan tick" is any deterministic loop iteration within a supervisor or
    manager thread. The caller provides an integer tick_id that monotonically
    increases within that loop.

    Enforcement:
    - Each SM stores last_transition_tick_id.
    - transition(...) is allowed only if tick_id != last_transition_tick_id.
    - On success, last_transition_tick_id becomes tick_id.

    Deterministic priority:
    - The caller must evaluate possible transitions in a fixed order and call
      transition(...) at the first satisfied transition.
    - Subsequent satisfied transitions in the same tick will be rejected.
    """
    last_transition_tick_id: int = -1

    def can_transition(self, tick_id: int) -> bool:
        return tick_id != self.last_transition_tick_id

    def mark_transition(self, tick_id: int) -> None:
        self.last_transition_tick_id = tick_id


# =============================================================================
# SECTION: Market data models (immutable snapshots)
# =============================================================================

@dataclass(frozen=True)
class Bar1m:
    """1-minute bar since open (SPY)."""
    ts: float
    open: float
    high: float
    low: float
    close: float
    vwap: float  # running VWAP at the bar close


@dataclass(frozen=True)
class OptionKey:
    """
    Unique identifier for an option contract in snapshots/plans.

    For IBKR, conId is the stable unique identifier when known.
    If conId is unknown (e.g., in replay), key falls back to (symbol, expiry, strike, right).
    """
    symbol: str
    expiry: str  # YYYYMMDD
    strike: float
    right: str  # "C" or "P"
    con_id: int = 0


@dataclass(frozen=True)
class OptionQuote:
    """Option quote + greeks relevant to planning and management."""
    key: OptionKey
    bid: Optional[float]
    ask: Optional[float]
    last: Optional[float]
    mid: Optional[float]
    delta: Optional[float]  # signed
    quote_ts: float

    def spread(self) -> Optional[float]:
        if self.bid is None or self.ask is None:
            return None
        return max(0.0, self.ask - self.bid)

    def spread_pct_of_mid(self) -> Optional[float]:
        mid = self.mid
        spr = self.spread()
        if mid is None or spr is None or mid <= 0:
            return None
        return spr / mid


@dataclass(frozen=True)
class MarketSnapshot:
    """
    Immutable latest-value snapshot published by MarketDataHub.
    All consumers must treat snapshots as read-only.

    Heartbeat:
    - hub_heartbeat_ts updates whenever the hub publishes a new snapshot,
      even if quotes are unchanged.
    """
    ts: float
    hub_heartbeat_ts: float

    spy_last: Optional[float]
    spy_vwap: Optional[float]
    spy_hod: Optional[float]
    spy_lod: Optional[float]

    bars_1m: Tuple[Bar1m, ...]  # since open
    option_quotes: Dict[OptionKey, OptionQuote]  # planned + active only

    # 5-minute bars since open (built by MarketDataHub from real-time bars).
    # These are monitored in parallel with 1-minute bars for entry triggers.
    bars_5m: Tuple[Bar1m, ...] = ()

    # Derived indicators (None when inputs unavailable).
    # Note: VWAP/HOD/LOD are the single day-level truth and are computed from 1-minute bars.
    rsi_1m_14: Optional[float] = None
    sma_1m_10: Optional[float] = None
    rsi_5m_14: Optional[float] = None
    sma_5m_10: Optional[float] = None
    pct_change_since_close: Optional[float] = None  # fraction; GUI may display percent
    yesterday_close: Optional[float] = None
    
    def heartbeat_age_s(self) -> float:
        return max(0.0, now_ts() - self.hub_heartbeat_ts)

    def bars_count(self) -> int:
        return len(self.bars_1m)


# =============================================================================
# SECTION: Planner outputs (no trading)
# =============================================================================

@dataclass(frozen=True)
class StraddlePlan:
    """
    Planner output for STRADDLE.
    The trigger supervisors display this plan even when not armed.
    """
    ts: float
    call: Optional[OptionKey]
    put: Optional[OptionKey]
    call_delta: Optional[float]
    put_delta: Optional[float]


@dataclass(frozen=True)
class PCSTailPlan:
    """
    Planner output for PCS+TAIL.
    - short_put/long_put define the PCS spread
    - tail_put is the tail hedge
    """
    ts: float
    short_put: Optional[OptionKey]
    long_put: Optional[OptionKey]
    tail_put: Optional[OptionKey]
    short_delta: Optional[float]
    long_delta: Optional[float]
    tail_delta: Optional[float]


# =============================================================================
# SECTION: Intents (strategies -> execution)
# =============================================================================

@dataclass(frozen=True)
class Intent:
    """Base class for intent messages passed to the execution engine."""
    created_ts: float
    strategy: StrategyId
    reason: str


@dataclass(frozen=True)
class EntryIntent(Intent):
    """
    Request to open a new trade.

    Notes:
    - channel is only meaningful for STRADDLE.
    - ExecutionEngine must validate prerequisites at consumption time as well.
    """
    channel: Optional[TriggerChannel]
    legs: Tuple[OptionKey, ...]  # (call, put) for straddle; (short, long, tail?) for pcs
    qtys: Tuple[int, ...]        # per-leg contract quantities (positive integers)
    max_cost: float             # budget guard in dollars (premium * 100 * qty)
    deadline_ts: float          # after this, engine should refuse or cancel entry


@dataclass(frozen=True)
class ExitIntent(Intent):
    """Request to close an existing trade."""
    trade_id: str


@dataclass(frozen=True)
class FlattenIntent(Intent):
    """Request to flatten ALL positions and close all trades immediately."""
    pass


# =============================================================================
# SECTION: Trade + leg runtime state
# =============================================================================

@dataclass
class LegRuntime:
    """Mutable runtime state for a trade leg, managed by ExecutionEngine only."""
    key: OptionKey
    qty: int

    state: LegState = LegState.IDLE
    sm_guard: TransitionGuard = field(default_factory=TransitionGuard)

    # Order / fill tracking
    avg_fill_price: float = 0.0
    filled_qty: int = 0
    working_order_id: Optional[int] = None
    working_limit_price: Optional[float] = None
    entry_submit_ts: float = 0.0

    # Synthetic stop / ratchet / hang
    entry_cost_per_contract: float = 0.0  # premium at entry for P&L baseline
    stop_soft_pct: float = -0.15
    stop_hard_pct: float = -0.165
    stop_soft_hold_s: float = 15.0
    stop_soft_poked: bool = False
    stop_soft_poke_ts: float = 0.0
    stop_level_price: Optional[float] = None  # absolute premium level (bid-based trigger)

    ratchet_idx: int = 0
    last_high_bid: Optional[float] = None
    last_high_ts: float = 0.0
    hang_active: bool = False
    hang_start_ts: float = 0.0

    # Exit tracking
    exit_submit_ts: float = 0.0
    closed_ts: float = 0.0

    def is_filled(self) -> bool:
        return self.filled_qty >= self.qty and self.qty > 0


@dataclass
class TradeRuntime:
    """Mutable trade object stored in SharedState and managed by ExecutionEngine only."""
    trade_id: str
    strategy: StrategyId
    channel: Optional[TriggerChannel]
    opened_ts: float

    state: TradeState = TradeState.IDLE
    sm_guard: TransitionGuard = field(default_factory=TransitionGuard)

    legs: List[LegRuntime] = field(default_factory=list)

    # PCS credit bookkeeping (treated as a single spread leg for win-rate purposes).
    pcs_credit_received: float = 0.0
    pcs_debit_to_close: float = 0.0

    # Tail stop state machine fields
    tail_state: TailStopState = TailStopState.INACTIVE
    tail_guard: TransitionGuard = field(default_factory=TransitionGuard)
    tail_activation_profit: float = 0.0
    tail_stop_lock_profit: float = 0.0
    tail_last_high_bid: Optional[float] = None
    tail_last_high_ts: float = 0.0
    tail_hang_active: bool = False
    tail_hang_start_ts: float = 0.0

    # Administrative
    closed_ts: float = 0.0
    close_reason: str = ""


# =============================================================================
# SECTION: Ledger + analytics snapshots
# =============================================================================

@dataclass
class LedgerRow:
    """
    Append-only ledger row.

    Disk I/O is handled elsewhere; core only defines the schema.
    """
    ts: float
    trade_id: str
    strategy: StrategyId
    event: str  # e.g. "OPEN", "CLOSE", "TRIM", "STOP", "FILL_SUMMARY"

    gross_pnl: float = 0.0
    commissions: float = 0.0
    net_pnl: float = 0.0

    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AnalyticsSnapshot:
    """Computed analytics for GUI display (derived from ledger + open trades)."""
    ts: float
    per_strategy_win_rate: Dict[str, float] = field(default_factory=dict)
    per_leg_win_rate: Dict[str, float] = field(default_factory=dict)
    total_net_pnl: float = 0.0


# =============================================================================
# SECTION: Event log ring buffer with spam suppression
# =============================================================================

@dataclass(frozen=True)
class EventRow:
    """Single GUI-visible event row."""
    ts: float
    level: LogLevel
    text: str

    def ts_str(self) -> str:
        """Return the event timestamp formatted for GUI display."""
        return fmt_ts_et(self.ts)


@dataclass
class SpamEpisode:
    """
    Tracks "bad spread episode" style logging.

    A single episode logs once at start and then suppresses repeats until
    recovery occurs or cooldown expires.
    """
    active: bool = False
    last_log_ts: float = 0.0
    last_seen_ts: float = 0.0


class EventLog:
    """
    Thread-safe event ring buffer.

    GUI requirements:
    - Event window uses bullet prefix; core stores text without bullets.
    - Status window is separate; status strings are stored in SharedState.

    Spam suppression:
    - Caller provides a suppression_key for episodic conditions.
    - log_episode_start logs at most once per episode (active -> True).
    - log_episode_end clears the episode (active -> False) and allows future starts.
    """
    def __init__(self, capacity: int) -> None:
        self._cap = max(100, int(capacity))
        self._rows: Deque[EventRow] = deque(maxlen=self._cap)
        self._lock = threading.RLock()
        self._episodes: Dict[str, SpamEpisode] = {}

    def rows_snapshot(self) -> List[EventRow]:
        with self._lock:
            return list(self._rows)

    @property
    def rows(self) -> List[EventRow]:
        """Return a snapshot list of event rows for GUI display."""
        return self.rows_snapshot()


    def add(self, level: LogLevel, text: str) -> None:
        with self._lock:
            self._rows.append(EventRow(ts=now_ts(), level=level, text=text))

    def log_episode_start(self, key: str, level: LogLevel, text: str, cooldown_s: float) -> None:
        """
        Log an episodic event once per episode.

        - If key is inactive and cooldown is satisfied, log and activate it.
        - If already active, suppress.
        """
        with self._lock:
            ep = self._episodes.get(key)
            if ep is None:
                ep = SpamEpisode()
                self._episodes[key] = ep

            t = now_ts()
            ep.last_seen_ts = t
            if ep.active:
                return
            if (t - ep.last_log_ts) < max(0.0, float(cooldown_s)):
                return

            self._rows.append(EventRow(ts=t, level=level, text=text))
            ep.active = True
            ep.last_log_ts = t

    def log_episode_end(self, key: str, level: LogLevel, text: str) -> None:
        """End an episodic condition and log the recovery once."""
        with self._lock:
            ep = self._episodes.get(key)
            if ep is None or not ep.active:
                return
            t = now_ts()
            self._rows.append(EventRow(ts=t, level=level, text=text))
            ep.active = False
            ep.last_seen_ts = t


# =============================================================================
# SECTION: Runtime configuration schema
# =============================================================================

@dataclass
class ThreadConfig:
    """Tick rates and loop timing."""
    md_hub_tick_s: float = 0.25
    planners_tick_s: float = 0.50
    triggers_tick_s: float = 0.50
    execution_tick_s: float = 0.20
    gui_refresh_tick_s: float = 0.25


@dataclass
class AccountConfig:
    """Account selection and starting balance handling."""
    mode: AccountMode = AccountMode.PAPER.value
    paper_account_id: str = ""
    live_account_id: str = ""
    starting_balance_override: float = 0.0  # if >0, used until IBKR NetLiq read
    require_account_id_to_connect: bool = True


@dataclass
class UIConfig:
    """GUI behavior toggles."""
    allow_confirm_anytime_when_armable: bool = True
    popup_on_armable: bool = True
    sound_on_armable: bool = True
    event_log_capacity: int = 3000


@dataclass
class ExportConfig:
    """End-of-day persistence settings."""
    export_dir: str = "eod_exports"
    export_jsonl: bool = True
    export_csv: bool = True
    export_once_per_day: bool = True


@dataclass
class DebugConfig:
    """Debug/simulation/replay settings."""
    run_mode: RunMode = RunMode.REAL
    replay_file: str = ""
    replay_step_mode: bool = False  # if True, advance only by GUI step button
    sim_fill_assume_complete: bool = True  # if quote exists and qty requested, fill immediately
    sim_deterministic_slippage_ticks: int = 0  # 0 = fill at limit/mid per algo


@dataclass
class StraddleConfig:
    """STRADDLE strategy configuration (triggers, gates, sizing, entry, stops)."""
    enable_bull: bool = True
    enable_bear: bool = True
    enable_neutral: bool = True

    gate_arming_auto: bool = True  # False = manual confirm required
    timed_entry_enabled: bool = False
    timed_entry_time_et: str = "09:44"
    allow_multiple_gated_per_day: bool = False
    gate_deadline_time_et: str = "13:30"

    call_delta_target: float = 0.50
    put_delta_target: float = 0.50
    delta_tolerance: float = 0.05

    budget_mode: BudgetMode = BudgetMode.PCT_AVAILABLE_FUNDS
    # Budget inputs (both editable in GUI; budget_mode selects which is applied).
    budget_pct_available_funds: float = 1.00
    fixed_budget_usd: float = 0.0
    leg_cap_pct_each: float = 0.50

    entry_spread_max_pct_mid: float = 0.0002  # 0.02%
    entry_price_tick_size: float = 0.05
    entry_price_unchanged_s: float = 5.0
    entry_fill_timeout_s: float = 30.0

    # Gate thresholds
    gate_dev_armable_floor: float = 0.0010
    gate_dev_block_band: float = 0.0002
    gate_bars_min_for_rule_a: int = 4
    gate_count_min_for_rule_b: int = 8
    gate_count_ratio_mult: float = 2.0

    pass_abs_dev_bullbear: float = 0.0001
    neutral_need_above: int = 2
    neutral_need_below: int = 2
    neutral_dev_min: float = 0.0002
    neutral_bar0_hodlod_band: float = 0.00015
    neutral_bar1_close_band: float = 0.00005

    # Stops and management
    stop_soft_pct: float = -0.15
    stop_hard_pct: float = -0.165
    stop_soft_hold_s: float = 15.0
    soft_stop_poke_enables_hang: bool = True

    ratchet_ladder: List[Tuple[float, float]] = field(
        default_factory=lambda: [
            (0.15, 0.00),
            (0.30, 0.20),
            (0.45, 0.30),
            (0.60, 0.45),
        ]
    )
    ratchet_after_t4_step: float = 0.30  # +30% increments indefinitely

    hang_seconds: float = 120.0
    hang_starts_at_t1: bool = True
    imbalance_trim_loss_pct: float = -0.05  # -5% immediate loss rule


@dataclass
class PCSConfig:
    """PCS+TAIL strategy configuration (signals, structure, sizing, stops, tail)."""
    once_per_day_lock: bool = True
    min_bars_since_open: int = 29
    deadline_time_et: str = "13:30"

    # Signals
    rsi_length: int = 14
    rsi_max: float = 60.0
    sma_length: int = 10
    pct_change_min: float = 0.0025  # 0.25%

    # Structure
    short_put_delta_target: float = 0.15
    delta_tolerance: float = 0.05
    width_points: float = 50.0

    # Budget/sizing
    budget_mode: BudgetMode = BudgetMode.FIXED_USD
    # Budget inputs (both editable in GUI; budget_mode selects which is applied).
    fixed_budget: float = 2000.0
    budget_pct_available_funds: float = 0.0
    budget_credit_mult: float = 2.5  # 2.5 * credit_total <= budget

    # Spread filter
    entry_spread_max_pct_mid: float = 0.0050  # 0.50%

    # PCS stop (synthetic)
    stop_debit_mult: float = 2.0  # debit_to_close >= 2.0 * credit_received

    # Tail selection + sizing
    tail_delta_target: float = 0.05
    tail_delta_tolerance: float = 0.05
    tail_qty_mult_of_pcs: float = 0.7

    # Tail stop/ratchet/hang
    tail_activation_profit_mult_of_credit: float = 0.50
    tail_initial_lock_profit_mult_of_credit: float = 1.00  # "locks credit_total"
    tail_ratchet_ladder: List[Tuple[float, float]] = field(
        default_factory=lambda: [
            (1.00, 0.50),
        ]
    )
    tail_hang_seconds: float = 120.0


@dataclass
class RuntimeConfig:
    """Root runtime configuration container (loaded at startup, edited live, saved on demand)."""
    threads: ThreadConfig = field(default_factory=ThreadConfig)
    account: AccountConfig = field(default_factory=AccountConfig)
    ui: UIConfig = field(default_factory=UIConfig)
    export: ExportConfig = field(default_factory=ExportConfig)
    debug: DebugConfig = field(default_factory=DebugConfig)
    straddle: StraddleConfig = field(default_factory=StraddleConfig)
    pcs: PCSConfig = field(default_factory=PCSConfig)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "RuntimeConfig":
        """
        Strict-but-tolerant loader:
        - Unknown keys are ignored.
        - Missing keys use defaults.
        - Accepts legacy enum string forms like 'AccountMode.PAPER'.
        """
        cfg = RuntimeConfig()

        def _normalize_enum_str(v: Any) -> Any:
            if isinstance(v, str) and "." in v:
                return v.split(".")[-1]
            return v

        def apply(obj: Any, src: Dict[str, Any]) -> None:
            for k, v in src.items():
                if not hasattr(obj, k):
                    continue
                cur = getattr(obj, k)
                if dataclass_is_instance(cur) and isinstance(v, dict):
                    apply(cur, v)
                    continue

                if isinstance(cur, Enum):
                    vv = _normalize_enum_str(v)
                    setattr(obj, k, type(cur)(vv))
                else:
                    setattr(obj, k, v)

        apply(cfg, d)

        # Normalize nested enums after assignment as well (tolerant of legacy strings).
        cfg.account.mode = AccountMode(_normalize_enum_str(cfg.account.mode))
        cfg.debug.run_mode = RunMode(_normalize_enum_str(cfg.debug.run_mode))
        cfg.straddle.budget_mode = BudgetMode(_normalize_enum_str(cfg.straddle.budget_mode))
        cfg.pcs.budget_mode = BudgetMode(_normalize_enum_str(cfg.pcs.budget_mode))
        return cfg


def dataclass_is_instance(x: Any) -> bool:
    """Return True if x is an instance of a dataclass."""
    return hasattr(x, "__dataclass_fields__")


def load_config(path: str) -> RuntimeConfig:
    """
    Load runtime configuration from JSON file.

    If the file does not exist, returns defaults.
    """
    p = Path(path)
    if not p.exists():
        return RuntimeConfig()
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        return RuntimeConfig()
    return RuntimeConfig.from_dict(data)


def save_config(cfg: RuntimeConfig, path: str) -> None:
    """Save configuration as JSON (pretty, deterministic ordering)."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(cfg.to_dict(), f, indent=2, sort_keys=True)
    os.replace(tmp, p)


# =============================================================================
# SECTION: Shared state (single source of truth, lock-protected)
# =============================================================================

@dataclass
class GuiFlags:
    """GUI-controlled flags (written by GUI thread, read by workers)."""
    go: bool = False
    debug_mode: bool = False
    manual_confirm_pressed: bool = False
    straddle_open_pressed: bool = False
    pcs_open_pressed: bool = False
    shutdown_pressed: bool = False

    # Clear-lock requests set by GUI; supervisors consume and clear.
    clear_lock_straddle_timed: bool = False
    clear_lock_straddle_gated: bool = False
    clear_lock_pcs: bool = False


@dataclass
class GateRuntime:
    """Per-channel gate runtime state for STRADDLE GATED channel."""
    selected_gate: GateType = GateType.NONE
    state: GateState = GateState.DISARMED
    armable: bool = False
    armed: bool = False  # manual confirm state (explicit)
    last_transition_ts: float = 0.0


@dataclass
class SharedState:
    """
    Global shared state. This is the ONLY shared mutable storage between threads.

    Thread safety model:
    - All fields in SharedState are protected by a single re-entrant lock (self.lock).
      This ensures atomic multi-field updates for consistent GUI displays and avoids
      deadlocks from multiple locks.
    - Threads must copy needed values out while holding the lock, then operate without
      holding the lock where possible.
    - The GUI must never call directly into IBKR or worker-thread objects; it can only
      update GuiFlags and RuntimeConfig, and read snapshots/rows from SharedState.
    """
    lock: threading.RLock = field(default_factory=threading.RLock)

    # Runtime config (edited live by GUI)
    config: RuntimeConfig = field(default_factory=RuntimeConfig)
    config_path: str = "config/runtime_config.json"

    # Connectivity/account
    ibkr_connected: bool = False
    selected_account_mode: AccountMode = AccountMode.PAPER.value
    selected_account_id: str = ""
    net_liq: float = 0.0
    available_funds: float = 0.0
    starting_day_balance: float = 0.0

    # Latest-value market data + plans
    market: Optional[MarketSnapshot] = None
    straddle_plan: Optional[StraddlePlan] = None
    pcs_plan: Optional[PCSTailPlan] = None

    # Gates and arming (STRADDLE only)
    gate_by_channel: Dict[str, GateRuntime] = field(
        default_factory=lambda: {
            TriggerChannel.OPEN.value: GateRuntime(),
            TriggerChannel.TIMED.value: GateRuntime(),
            TriggerChannel.GATED.value: GateRuntime(),
        }
    )

    # Day locks (set/cleared explicitly)
    day_locks: Dict[str, bool] = field(
        default_factory=lambda: {
            DayLockType.STRADDLE_TIMED.value: False,
            DayLockType.STRADDLE_GATED.value: False,
            DayLockType.PCS.value: False,
        }
    )

    # Active trade registry (ExecutionEngine owns mutation; GUI reads)
    trades: Dict[str, TradeRuntime] = field(default_factory=dict)

    # Ledger and analytics
    ledger: List[LedgerRow] = field(default_factory=list)
    analytics: AnalyticsSnapshot = field(default_factory=lambda: AnalyticsSnapshot(ts=now_ts()))

    # Status strings (separate from event log)
    status_lines: List[str] = field(default_factory=list)

    # Event log ring buffer
    md_heartbeat_ts: float = 0.0
    planner_heartbeat_ts: float = 0.0
    straddle_supervisor_heartbeat_ts: float = 0.0
    pcs_supervisor_heartbeat_ts: float = 0.0
    execution_heartbeat_ts: float = 0.0
    event_log: EventLog = field(default_factory=lambda: EventLog(capacity=3000))

    # GUI flags
    gui: GuiFlags = field(default_factory=GuiFlags)

    # Shutdown coordination
    shutdown_event: threading.Event = field(default_factory=threading.Event)

    def set_status(self, lines: List[str]) -> None:
        """Replace status lines atomically."""
        with self.lock:
            self.status_lines = list(lines)

    def log_info(self, text: str) -> None:
        with self.lock:
            self.event_log.add(LogLevel.INFO, text)

    def log_warn(self, text: str) -> None:
        with self.lock:
            self.event_log.add(LogLevel.WARN, text)

    def log_error(self, text: str) -> None:
        with self.lock:
            self.event_log.add(LogLevel.ERROR, text)

    def log_spread_episode_start(self, key: str, text: str, cooldown_s: float = 30.0) -> None:
        """
        Convenience wrapper for spread filter failures.
        Logs once per "bad spread episode" until the episode ends.
        """
        with self.lock:
            self.event_log.log_episode_start(key=key, level=LogLevel.WARN, text=text, cooldown_s=cooldown_s)

    def log_spread_episode_end(self, key: str, text: str) -> None:
        with self.lock:
            self.event_log.log_episode_end(key=key, level=LogLevel.INFO, text=text)

    def request_shutdown(self) -> None:
        """Set shutdown flag in both GUI flags and the shared shutdown event."""
        with self.lock:
            self.gui.shutdown_pressed = True
            self.shutdown_event.set()


# =============================================================================
# SECTION: Minimal troubleshooting helpers
# =============================================================================

def ensure_export_dir(path: str) -> None:
    """Create export directory if missing."""
    Path(path).mkdir(parents=True, exist_ok=True)
