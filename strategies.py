"""\
strategies.py

Planners + gate logic + trigger supervisors.

This module contains **no trading** code.

Responsibilities:
- Planners: choose contracts for STRADDLE and PCS+TAIL and publish plans.
- Gate evaluation: determine ARMABLE / ARMED / PASS transitions.
- Trigger supervisors: emit EntryIntent objects into a queue.Queue.

Hard rules implemented here:
- State-machine style arming for gated STRADDLE channel.
- Deterministic arbitration per scan tick: STRADDLE OPEN > TIMED > GATED.
- One active trade per channel.
- Day locks enforced exactly.
- go blocks new entries only.

Threading:
- Each component is a dedicated daemon thread.
- Threads copy inputs from SharedState under lock, compute without holding the lock,
  then publish results back under lock.

Python: 3.10.4
"""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from queue import Queue
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from zoneinfo import ZoneInfo

from core import (
    AccountMode,
    BudgetMode,
    DayLockType,
    EntryIntent,
    GateRuntime,
    GateState,
    GateType,
    MarketSnapshot,
    OptionKey,
    OptionQuote,
    RunMode,
    SharedState,
    StrategyId,
    StraddlePlan,
    PCSTailPlan,
    TriggerChannel,
    now_ts,
)


# =============================================================================
# SECTION: Time helpers (ET)
# =============================================================================

_ET = ZoneInfo("America/New_York")


def _today_et() -> date:
    return datetime.now(tz=_ET).date()


def _parse_hhmm_et_to_ts_today(hhmm: str) -> float:
    """Parse HH:MM (ET) for *today* and return epoch seconds."""
    parts = (hhmm or "").strip().split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid HH:MM: {hhmm!r}")
    h = int(parts[0])
    m = int(parts[1])
    d = _today_et()
    dt = datetime(d.year, d.month, d.day, h, m, tzinfo=_ET)
    return dt.timestamp()


def _next_weekday_et(start: date, weekday: int) -> date:
    """Return the next date (>= start) that is the given weekday (Mon=0..Sun=6)."""
    if weekday < 0 or weekday > 6:
        raise ValueError("weekday must be 0..6")
    delta = (weekday - start.weekday()) % 7
    return start + timedelta(days=delta)


def _format_yyyymmdd(d: date) -> str:
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"


# =============================================================================
# SECTION: Numeric helpers
# =============================================================================


def _safe_mid(q: OptionQuote) -> Optional[float]:
    if q.mid is not None:
        return q.mid
    if q.bid is None or q.ask is None:
        return None
    return (q.bid + q.ask) / 2.0


def _spread_pct(q: OptionQuote) -> Optional[float]:
    return q.spread_pct_of_mid()


def _quote_age_s(q: OptionQuote) -> float:
    return max(0.0, now_ts() - q.quote_ts)


def _nearest_strike(price: float, step: float) -> float:
    if step <= 0:
        return float(price)
    return round(price / step) * step


# =============================================================================
# SECTION: Technical indicators (bar-based)
# =============================================================================


def _sma(closes: Sequence[float], length: int) -> Optional[float]:
    if length <= 0:
        return None
    if len(closes) < length:
        return None
    window = closes[-length:]
    return sum(window) / float(length)


def _rsi(closes: Sequence[float], length: int) -> Optional[float]:
    """Wilder-style RSI using simple averages over the last `length` deltas."""
    if length <= 0:
        return None
    if len(closes) < length + 1:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(-length, 0):
        d = closes[i] - closes[i - 1]
        if d >= 0:
            gains += d
        else:
            losses += -d
    if gains == 0.0 and losses == 0.0:
        return 50.0
    if losses == 0.0:
        return 100.0
    rs = gains / losses
    return 100.0 - (100.0 / (1.0 + rs))


# =============================================================================
# SECTION: Planner base
# =============================================================================


class PlannerBase(threading.Thread):
    """Common tick-based planner thread."""

    def __init__(self, shared: SharedState, name: str) -> None:
        super().__init__(daemon=True, name=name)
        self._shared = shared
        self._tick_id = 0

    def run(self) -> None:
        while not self._shared.shutdown_event.is_set():
            t0 = time.monotonic()
            try:
                self._tick_id += 1
                self._tick()
            except Exception as e:
                self._shared.log_error(f"{self.name} error: {e!r}")
            tick_s = self._get_tick_s()
            elapsed = time.monotonic() - t0
            remaining = max(0.0, tick_s - elapsed)
            if remaining > 0:
                time.sleep(min(remaining, 0.05))

    def _get_tick_s(self) -> float:
        with self._shared.lock:
            return float(self._shared.config.threads.planners_tick_s)

    def _tick(self) -> None:
        raise NotImplementedError


# =============================================================================
# SECTION: Straddle planner
# =============================================================================


class StraddlePlanner(PlannerBase):
    """Chooses SPX call/put contracts near target deltas and publishes StraddlePlan."""

    def __init__(self, shared: SharedState) -> None:
        super().__init__(shared=shared, name="StraddlePlanner")

    def _tick(self) -> None:
        with self._shared.lock:
            snap = self._shared.market
            cfg = self._shared.config

        plan = self._compute_plan(snap=snap, cfg=cfg)
        with self._shared.lock:
            self._shared.straddle_plan = plan

    def _compute_plan(self, snap: Optional[MarketSnapshot], cfg) -> StraddlePlan:
        ts = now_ts()
        if snap is None or snap.spy_last is None:
            return StraddlePlan(ts=ts, call=None, put=None, call_delta=None, put_delta=None)

        target_call = float(cfg.straddle.call_delta_target)
        target_put_abs = float(cfg.straddle.put_delta_target)
        tol = float(cfg.straddle.delta_tolerance)

        # Candidate expiry: next Friday (weekday=4) from today ET.
        expiry = _format_yyyymmdd(_next_weekday_et(_today_et(), 4))

        # Candidate universe around synthetic SPX underlying.
        spx_underlying = 10.0 * float(snap.spy_last)
        base = _nearest_strike(spx_underlying, step=25.0)
        strikes = [base + i * 25.0 for i in range(-8, 9)]  # +/- 200 points
        candidates: List[OptionKey] = []
        for k in strikes:
            candidates.append(OptionKey(symbol="SPX", expiry=expiry, strike=float(k), right="C"))
            candidates.append(OptionKey(symbol="SPX", expiry=expiry, strike=float(k), right="P"))

        # If quotes for any candidates exist, use delta-based selection.
        qmap = snap.option_quotes
        best_call: Optional[Tuple[float, OptionKey, float]] = None
        best_put: Optional[Tuple[float, OptionKey, float]] = None
        for key in candidates:
            q = qmap.get(key)
            if q is None or q.delta is None:
                continue
            d = float(q.delta)
            if key.right == "C":
                err = abs(d - target_call)
                if err <= tol:
                    if best_call is None or err < best_call[0]:
                        best_call = (err, key, d)
            else:
                # Put deltas are negative; compare absolute to target_put_abs
                err = abs(abs(d) - target_put_abs)
                if err <= tol:
                    if best_put is None or err < best_put[0]:
                        best_put = (err, key, d)

        if best_call is not None and best_put is not None:
            return StraddlePlan(
                ts=ts,
                call=best_call[1],
                put=best_put[1],
                call_delta=float(best_call[2]),
                put_delta=float(best_put[2]),
            )

        # Fallback: ATM straddle keys; deltas may be N/A until hub subscribes.
        atm = float(_nearest_strike(spx_underlying, step=25.0))
        return StraddlePlan(
            ts=ts,
            call=OptionKey(symbol="SPX", expiry=expiry, strike=atm, right="C"),
            put=OptionKey(symbol="SPX", expiry=expiry, strike=atm, right="P"),
            call_delta=None,
            put_delta=None,
        )


# =============================================================================
# SECTION: PCS + tail planner
# =============================================================================


class PCSTailPlanner(PlannerBase):
    """Chooses PCS short/long puts and a tail put and publishes PCSTailPlan."""

    def __init__(self, shared: SharedState) -> None:
        super().__init__(shared=shared, name="PCSTailPlanner")

    def _tick(self) -> None:
        with self._shared.lock:
            snap = self._shared.market
            cfg = self._shared.config

        plan = self._compute_plan(snap=snap, cfg=cfg)
        with self._shared.lock:
            self._shared.pcs_plan = plan

    def _compute_plan(self, snap: Optional[MarketSnapshot], cfg) -> PCSTailPlan:
        ts = now_ts()
        if snap is None or snap.spy_last is None:
            return PCSTailPlan(ts=ts, short_put=None, long_put=None, tail_put=None,
                               short_delta=None, long_delta=None, tail_delta=None)

        spx_underlying = 10.0 * float(snap.spy_last)
        expiry = _format_yyyymmdd(_next_weekday_et(_today_et(), 4))
        width = float(cfg.pcs.width_points)
        target_short_abs = float(cfg.pcs.short_put_delta_target)
        target_tail_abs = float(cfg.pcs.tail_delta_target)
        tol = float(cfg.pcs.delta_tolerance)

        # Candidate strikes below underlying.
        base = _nearest_strike(spx_underlying, step=25.0)
        strikes = [base - i * 25.0 for i in range(1, 21)]  # 25..500 points OTM
        candidates_puts = [OptionKey(symbol="SPX", expiry=expiry, strike=float(k), right="P") for k in strikes]
        qmap = snap.option_quotes

        best_short: Optional[Tuple[float, OptionKey, float]] = None
        best_tail: Optional[Tuple[float, OptionKey, float]] = None
        for key in candidates_puts:
            q = qmap.get(key)
            if q is None or q.delta is None:
                continue
            d = float(q.delta)
            absd = abs(d)
            err_short = abs(absd - target_short_abs)
            if err_short <= tol:
                if best_short is None or err_short < best_short[0]:
                    best_short = (err_short, key, d)
            err_tail = abs(absd - target_tail_abs)
            if err_tail <= tol:
                if best_tail is None or err_tail < best_tail[0]:
                    best_tail = (err_tail, key, d)

        if best_short is not None:
            short_key = best_short[1]
            long_strike = float(short_key.strike - width)
            long_key = OptionKey(symbol="SPX", expiry=expiry, strike=long_strike, right="P")
            long_q = qmap.get(long_key)
            long_d = float(long_q.delta) if (long_q is not None and long_q.delta is not None) else None
            tail_key = best_tail[1] if best_tail is not None else None
            tail_d = float(best_tail[2]) if best_tail is not None else None
            return PCSTailPlan(
                ts=ts,
                short_put=short_key,
                long_put=long_key,
                tail_put=tail_key,
                short_delta=float(best_short[2]),
                long_delta=long_d,
                tail_delta=tail_d,
            )

        # Fallback: approximate short at ~2% OTM and long by width.
        short_strike = float(_nearest_strike(spx_underlying * 0.98, step=25.0))
        long_strike = float(short_strike - width)
        return PCSTailPlan(
            ts=ts,
            short_put=OptionKey(symbol="SPX", expiry=expiry, strike=short_strike, right="P"),
            long_put=OptionKey(symbol="SPX", expiry=expiry, strike=long_strike, right="P"),
            tail_put=OptionKey(symbol="SPX", expiry=expiry, strike=float(_nearest_strike(spx_underlying * 0.95, 25.0)), right="P"),
            short_delta=None,
            long_delta=None,
            tail_delta=None,
        )


# =============================================================================
# SECTION: Gate evaluation logic (STRADDLE)
# =============================================================================


@dataclass(frozen=True)
class GateEval:
    selected: GateType
    bull_armable: bool
    bear_armable: bool
    neutral_armable: bool
    bull_pass: bool
    bear_pass: bool
    neutral_pass: bool


def evaluate_straddle_gate(snap: MarketSnapshot, cfg) -> GateEval:
    """Evaluate ARMABLE and PASS conditions for bull/bear/neutral gates."""
    bars = list(snap.bars_1m)
    if len(bars) == 0 or snap.spy_vwap is None:
        return GateEval(
            selected=GateType.NONE,
            bull_armable=False,
            bear_armable=False,
            neutral_armable=False,
            bull_pass=False,
            bear_pass=False,
            neutral_pass=False,
        )

    vwap = float(snap.spy_vwap)
    closes = [float(b.close) for b in bars]
    devs = [(c - vwap) / vwap for c in closes if vwap > 0]
    above_count = sum(1 for d in devs if d > 0)
    below_count = sum(1 for d in devs if d < 0)

    dev_floor = float(cfg.straddle.gate_dev_armable_floor)   # 0.0010
    block_band = float(cfg.straddle.gate_dev_block_band)     # 0.0002
    bars_min = int(cfg.straddle.gate_bars_min_for_rule_a)    # 4
    cnt_min = int(cfg.straddle.gate_count_min_for_rule_b)    # 8
    ratio_mult = float(cfg.straddle.gate_count_ratio_mult)   # 2.0

    def _no_close_against_vwap_bull() -> bool:
        if len(devs) < bars_min:
            return False
        return all(d > -block_band for d in devs)

    def _no_close_against_vwap_bear() -> bool:
        if len(devs) < bars_min:
            return False
        return all(d < block_band for d in devs)

    bull_rule_a = _no_close_against_vwap_bull()
    bull_rule_b = (any(d >= dev_floor for d in devs) and above_count >= cnt_min and above_count >= ratio_mult * below_count)
    bull_armable = bool(cfg.straddle.enable_bull) and (bull_rule_a or bull_rule_b)

    bear_rule_a = _no_close_against_vwap_bear()
    bear_rule_b = (any(d <= -dev_floor for d in devs) and below_count >= cnt_min and below_count >= ratio_mult * above_count)
    bear_armable = bool(cfg.straddle.enable_bear) and (bear_rule_a or bear_rule_b)

    neutral_armable = False
    if bool(cfg.straddle.enable_neutral):
        need_above = int(cfg.straddle.neutral_need_above)
        need_below = int(cfg.straddle.neutral_need_below)
        neutral_dev_min = float(cfg.straddle.neutral_dev_min)
        neutral_armable = (
            above_count >= need_above
            and below_count >= need_below
            and any(d > neutral_dev_min for d in devs)
            and any(d < -neutral_dev_min for d in devs)
        )

    # Selection: neutral only if not bull and not bear.
    selected = GateType.NONE
    if bull_armable:
        selected = GateType.BULL
    elif bear_armable:
        selected = GateType.BEAR
    elif neutral_armable:
        selected = GateType.NEUTRAL

    # PASS conditions depend on selected gate type.
    pass_abs_dev = float(cfg.straddle.pass_abs_dev_bullbear)
    last_close = float(bars[-1].close)
    last_dev = (last_close - vwap) / vwap if vwap > 0 else 0.0
    bull_pass = abs(last_dev) <= pass_abs_dev
    bear_pass = abs(last_dev) <= pass_abs_dev

    neutral_pass = False
    if len(bars) >= 2 and snap.spy_hod is not None and snap.spy_lod is not None:
        bar0 = bars[-1]
        bar1 = bars[-2]
        hod = float(snap.spy_hod)
        lod = float(snap.spy_lod)
        band0 = float(cfg.straddle.neutral_bar0_hodlod_band)
        band1 = float(cfg.straddle.neutral_bar1_close_band)
        c0 = float(bar0.close)
        c1 = float(bar1.close)
        near_hod = abs(c0 - hod) <= (band0 * vwap)
        near_lod = abs(c0 - lod) <= (band0 * vwap)
        created_new_hod = c0 >= hod
        created_new_lod = c0 <= lod
        cond0 = near_hod or near_lod or created_new_hod or created_new_lod
        cond1 = abs(c1 - c0) <= (band1 * vwap)
        neutral_pass = bool(cond0 and cond1)

    return GateEval(
        selected=selected,
        bull_armable=bull_armable,
        bear_armable=bear_armable,
        neutral_armable=neutral_armable,
        bull_pass=bull_pass,
        bear_pass=bear_pass,
        neutral_pass=neutral_pass,
    )


# =============================================================================
# SECTION: Trigger prerequisites (shared checks)
# =============================================================================


def _has_account_id(cfg) -> bool:
    if cfg.account.mode == AccountMode.PAPER:
        return (cfg.account.paper_account_id or "").strip() != ""
    return (cfg.account.live_account_id or "").strip() != ""


def _is_before_deadline(deadline_hhmm: str) -> bool:
    try:
        deadline_ts = _parse_hhmm_et_to_ts_today(deadline_hhmm)
    except Exception:
        return True
    return now_ts() <= deadline_ts


def _plan_has_deltas(plan: StraddlePlan) -> bool:
    return (plan.call is not None and plan.put is not None and plan.call_delta is not None and plan.put_delta is not None)


def _spread_filter_ok(
    shared: SharedState,
    snap: MarketSnapshot,
    keys: Sequence[OptionKey],
    max_spread_pct: float,
    episode_key_prefix: str,
) -> bool:
    """Return True if all legs pass spread filter; episodic logging on failures."""
    all_ok = True
    for idx, k in enumerate(keys):
        q = snap.option_quotes.get(k)
        if q is None or q.bid is None or q.ask is None:
            all_ok = False
            shared.log_spread_episode_start(
                key=f"{episode_key_prefix}:{idx}:MISSING",
                text=f"Spread filter blocked (missing quote) for {k.symbol} {k.expiry} {k.right}{k.strike}.",
                cooldown_s=30.0,
            )
            continue
        spr_pct = _spread_pct(q)
        if spr_pct is None:
            all_ok = False
            shared.log_spread_episode_start(
                key=f"{episode_key_prefix}:{idx}:NOMID",
                text=f"Spread filter blocked (no mid) for {k.symbol} {k.expiry} {k.right}{k.strike} bid={q.bid} ask={q.ask}.",
                cooldown_s=30.0,
            )
            continue
        if spr_pct > max_spread_pct:
            all_ok = False
            shared.log_spread_episode_start(
                key=f"{episode_key_prefix}:{idx}:WIDE",
                text=(
                    "Spread filter blocked (wide spread) "
                    f"{k.symbol} {k.expiry} {k.right}{k.strike}: "
                    f"bid={q.bid} ask={q.ask} mid={_safe_mid(q)} spread%={spr_pct:.6f} limit={max_spread_pct:.6f}"
                ),
                cooldown_s=30.0,
            )
        else:
            # Recovery ends episode for this leg.
            shared.log_spread_episode_end(
                key=f"{episode_key_prefix}:{idx}:WIDE",
                text=(
                    f"Spread episode recovered for {k.symbol} {k.expiry} {k.right}{k.strike} "
                    f"spread%={spr_pct:.6f}."
                ),
            )
    return all_ok


def _compute_straddle_qtys(
    snap: MarketSnapshot,
    cfg,
    shared_available_funds: float,
    call_key: OptionKey,
    put_key: OptionKey,
) -> Tuple[Tuple[int, int], float]:
    """
    Compute (call_qty, put_qty) and strategy budget (USD).

    Budget selection (Option B):
    - If cfg.straddle.budget_mode == PCT_AVAILABLE_FUNDS, budget = pct_available_funds * AvailableFunds.
    - If cfg.straddle.budget_mode == FIXED_USD, budget = fixed_budget_usd.

    Notes:
    - This sizing uses ask as a conservative proxy for the working limit price.
    - Execution is the final authority: it must enforce per-leg budget caps on any repricing.
    """
    mode = getattr(cfg.straddle, "budget_mode", BudgetMode.PCT_AVAILABLE_FUNDS)
    if mode == BudgetMode.FIXED_USD:
        budget = max(0.0, float(getattr(cfg.straddle, "fixed_budget_usd", 0.0)))
    else:
        budget = max(0.0, float(getattr(cfg.straddle, "budget_pct_available_funds", 0.0)) * float(shared_available_funds))

    call_q = snap.option_quotes.get(call_key)
    put_q = snap.option_quotes.get(put_key)
    call_ask = call_q.ask if (call_q is not None and call_q.ask is not None) else None
    put_ask = put_q.ask if (put_q is not None and put_q.ask is not None) else None
    if call_ask is None or put_ask is None:
        return (0, 0), budget

    leg_cap = max(0.0, float(getattr(cfg.straddle, "leg_cap_pct_each", 0.0)))
    call_cap = budget * leg_cap
    put_cap = budget * leg_cap

    call_qty = int(max(0.0, math.floor(call_cap / (float(call_ask) * 100.0))))
    put_qty = int(max(0.0, math.floor(put_cap / (float(put_ask) * 100.0))))
    return (call_qty, put_qty), budget

def _compute_pcs_qtys(
    snap: MarketSnapshot,
    cfg,
    shared_available_funds: float,
    short_key: OptionKey,
    long_key: OptionKey,
) -> Tuple[int, float, float, float]:
    """
    Compute PCS qty and credit values.

    Returns:
        (qty, credit_per_spread, credit_total, budget)

    Budget selection (Option B):
    - If cfg.pcs.budget_mode == PCT_AVAILABLE_FUNDS, budget = pct_available_funds * AvailableFunds.
    - If cfg.pcs.budget_mode == FIXED_USD, budget = fixed_budget.

    Budget sizing rule:
        qty * credit_total_one * budget_credit_mult <= budget
    where:
        credit_total_one = credit_per_spread * 100
    """
    short_q = snap.option_quotes.get(short_key)
    long_q = snap.option_quotes.get(long_key)
    if short_q is None or long_q is None:
        return 0, 0.0, 0.0, 0.0
    short_mid = _safe_mid(short_q)
    long_mid = _safe_mid(long_q)
    if short_mid is None or long_mid is None:
        return 0, 0.0, 0.0, 0.0

    credit_per_spread = max(0.0, float(short_mid) - float(long_mid))
    credit_total_one = credit_per_spread * 100.0
    if credit_total_one <= 0:
        return 0, credit_per_spread, 0.0, 0.0

    mode = getattr(cfg.pcs, "budget_mode", BudgetMode.FIXED_USD)
    if mode == BudgetMode.PCT_AVAILABLE_FUNDS:
        budget = max(0.0, float(getattr(cfg.pcs, "budget_pct_available_funds", 0.0)) * float(shared_available_funds))
    else:
        budget = max(0.0, float(getattr(cfg.pcs, "fixed_budget", 0.0)))

    mult = max(0.0, float(getattr(cfg.pcs, "budget_credit_mult", 0.0)))
    denom = max(1e-9, mult * credit_total_one)
    q = int(max(0.0, math.floor(budget / denom)))
    credit_total = credit_total_one * q
    return q, credit_per_spread, credit_total, budget

class StraddleTriggerSupervisor(threading.Thread):
    """Maintains STRADDLE channels and emits EntryIntents only."""

    def __init__(self, shared: SharedState, intents_q: Queue) -> None:
        super().__init__(daemon=True, name="StraddleTriggerSupervisor")
        self._shared = shared
        self._q: Queue = intents_q
        self._tick_id = 0

    def run(self) -> None:
        while not self._shared.shutdown_event.is_set():
            t0 = time.monotonic()
            try:
                self._tick_id += 1
                self._scan_once(self._tick_id)
            except Exception as e:
                self._shared.log_error(f"StraddleTriggerSupervisor error: {e!r}")
            tick_s = self._get_tick_s()
            elapsed = time.monotonic() - t0
            remaining = max(0.0, tick_s - elapsed)
            if remaining > 0:
                time.sleep(min(remaining, 0.05))

    def _get_tick_s(self) -> float:
        with self._shared.lock:
            return float(self._shared.config.threads.triggers_tick_s)

    # ---------------------------------------------------------------------
    # SECTION: Tick scan
    # ---------------------------------------------------------------------

    def _scan_once(self, tick_id: int) -> None:
        # Snapshot inputs
        with self._shared.lock:
            cfg = self._shared.config
            snap = self._shared.market
            plan = self._shared.straddle_plan
            gui = self._shared.gui
            locks = dict(self._shared.day_locks)
            trades = list(self._shared.trades.values())
            ibkr_connected = self._shared.ibkr_connected
            available_funds = float(self._shared.available_funds)
            gate_rt = self._shared.gate_by_channel.get(TriggerChannel.GATED.value, GateRuntime())

        # Handle clear-lock requests (GUI is assumed to have confirmed).
        self._handle_clear_lock_requests(gui_flags=gui)

        if snap is None or plan is None:
            return

        # Active trades per channel
        active_by_channel = self._active_straddle_trade_by_channel(trades)

        # Arbitration per scan tick: OPEN > TIMED > GATED
        if self._try_open_channel(tick_id, cfg, snap, plan, gui, locks, active_by_channel, ibkr_connected, available_funds):
            return
        if self._try_timed_channel(tick_id, cfg, snap, plan, gui, locks, active_by_channel, ibkr_connected, available_funds):
            return
        self._try_gated_channel(tick_id, cfg, snap, plan, gui, locks, active_by_channel, ibkr_connected, available_funds, gate_rt)

    def _handle_clear_lock_requests(self, gui_flags) -> None:
        with self._shared.lock:
            if gui_flags.clear_lock_straddle_timed:
                self._shared.day_locks[DayLockType.STRADDLE_TIMED.value] = False
                gui_flags.clear_lock_straddle_timed = False
                self._shared.log_info("Cleared STRADDLE TIMED day lock.")
            if gui_flags.clear_lock_straddle_gated:
                self._shared.day_locks[DayLockType.STRADDLE_GATED.value] = False
                gui_flags.clear_lock_straddle_gated = False
                self._shared.log_info("Cleared STRADDLE GATED day lock.")

    def _active_straddle_trade_by_channel(self, trades) -> Dict[str, bool]:
        out = {TriggerChannel.OPEN.value: False, TriggerChannel.TIMED.value: False, TriggerChannel.GATED.value: False}
        for tr in trades:
            if tr.strategy != StrategyId.STRADDLE:
                continue
            ch = tr.channel.value if tr.channel is not None else ""
            if ch in out and tr.state.value != "CLOSED":
                out[ch] = True
        return out

    # ---------------------------------------------------------------------
    # SECTION: Channel attempts
    # ---------------------------------------------------------------------

    def _common_entry_prereqs_ok(self, cfg, gui, ibkr_connected: bool) -> bool:
        if not gui.go:
            return False
        if cfg.debug.run_mode == RunMode.REPLAY:
            # Replay mode runs SMs but does not emit new entries.
            return False
        if cfg.account.require_account_id_to_connect and not _has_account_id(cfg):
            return False
        # In SIM mode, connectivity is not required for orders, but we still
        # require market data connectivity to reduce ambiguity.
        if cfg.debug.run_mode != RunMode.SIM and not ibkr_connected:
            return False
        return True

    def _try_open_channel(
        self,
        tick_id: int,
        cfg,
        snap: MarketSnapshot,
        plan: StraddlePlan,
        gui,
        locks: Dict[str, bool],
        active_by_channel: Dict[str, bool],
        ibkr_connected: bool,
        available_funds: float,
    ) -> bool:
        if not gui.straddle_open_pressed:
            return False
        # Consume button press regardless of success.
        with self._shared.lock:
            self._shared.gui.straddle_open_pressed = False
        if active_by_channel.get(TriggerChannel.OPEN.value, False):
            self._shared.log_warn("STRADDLE OPEN ignored: existing OPEN-channel trade active.")
            return True
        # OPEN bypasses gates but still obeys prerequisites (spread/budget, deadline).
        if not self._common_entry_prereqs_ok(cfg, gui, ibkr_connected):
            self._shared.log_warn("STRADDLE OPEN blocked: go/account/connect prerequisites not satisfied.")
            return True
        if not _is_before_deadline(cfg.straddle.gate_deadline_time_et):
            self._shared.log_warn("STRADDLE OPEN blocked: deadline exceeded.")
            return True
        if plan.call is None or plan.put is None:
            self._shared.log_warn("STRADDLE OPEN blocked: planner has no contracts.")
            return True

        keys = (plan.call, plan.put)
        if not _spread_filter_ok(
            shared=self._shared,
            snap=snap,
            keys=keys,
            max_spread_pct=float(cfg.straddle.entry_spread_max_pct_mid),
            episode_key_prefix="STRADDLE_OPEN",
        ):
            return True
        qtys, budget = _compute_straddle_qtys(snap, cfg, available_funds, plan.call, plan.put)
        if qtys[0] <= 0 or qtys[1] <= 0:
            self._shared.log_warn("STRADDLE OPEN blocked: budget/leg-cap produced zero qty.")
            return True

        deadline_ts = _parse_hhmm_et_to_ts_today(cfg.straddle.gate_deadline_time_et)
        intent = EntryIntent(
            created_ts=now_ts(),
            strategy=StrategyId.STRADDLE,
            reason="MANUAL_OPEN",
            channel=TriggerChannel.OPEN,
            legs=tuple(keys),
            qtys=tuple(qtys),
            max_cost=float(budget),
            deadline_ts=float(deadline_ts),
        )
        self._q.put(intent)
        self._shared.log_info("STRADDLE EntryIntent created (OPEN channel).")
        return True

    def _try_timed_channel(
        self,
        tick_id: int,
        cfg,
        snap: MarketSnapshot,
        plan: StraddlePlan,
        gui,
        locks: Dict[str, bool],
        active_by_channel: Dict[str, bool],
        ibkr_connected: bool,
        available_funds: float,
    ) -> bool:
        if not bool(cfg.straddle.timed_entry_enabled):
            return False
        if active_by_channel.get(TriggerChannel.TIMED.value, False):
            return False
        if locks.get(DayLockType.STRADDLE_TIMED.value, False):
            return False

        # Time check
        t_target = _parse_hhmm_et_to_ts_today(cfg.straddle.timed_entry_time_et)
        if now_ts() < t_target:
            return False
        if not _is_before_deadline(cfg.straddle.gate_deadline_time_et):
            return False
        if not self._common_entry_prereqs_ok(cfg, gui, ibkr_connected):
            return False
        if plan.call is None or plan.put is None:
            return False

        keys = (plan.call, plan.put)
        if not _spread_filter_ok(
            shared=self._shared,
            snap=snap,
            keys=keys,
            max_spread_pct=float(cfg.straddle.entry_spread_max_pct_mid),
            episode_key_prefix="STRADDLE_TIMED",
        ):
            return False

        qtys, budget = _compute_straddle_qtys(snap, cfg, available_funds, plan.call, plan.put)
        if qtys[0] <= 0 or qtys[1] <= 0:
            return False

        intent = EntryIntent(
            created_ts=now_ts(),
            strategy=StrategyId.STRADDLE,
            reason="TIMED_ENTRY",
            channel=TriggerChannel.TIMED,
            legs=tuple(keys),
            qtys=tuple(qtys),
            max_cost=float(budget),
            deadline_ts=float(_parse_hhmm_et_to_ts_today(cfg.straddle.gate_deadline_time_et)),
        )
        self._q.put(intent)
        self._shared.log_info("STRADDLE EntryIntent created (TIMED channel).")
        with self._shared.lock:
            self._shared.day_locks[DayLockType.STRADDLE_TIMED.value] = True
        self._shared.log_info("STRADDLE TIMED day lock set.")
        return True

    def _try_gated_channel(
        self,
        tick_id: int,
        cfg,
        snap: MarketSnapshot,
        plan: StraddlePlan,
        gui,
        locks: Dict[str, bool],
        active_by_channel: Dict[str, bool],
        ibkr_connected: bool,
        available_funds: float,
        gate_rt: GateRuntime,
    ) -> bool:
        if active_by_channel.get(TriggerChannel.GATED.value, False):
            # Still keep gate state updated for GUI display.
            self._update_gate_runtime(tick_id, cfg, snap, gui, locks, gate_rt)
            return False

        if locks.get(DayLockType.STRADDLE_GATED.value, False) and not bool(cfg.straddle.allow_multiple_gated_per_day):
            self._update_gate_blocked(gate_rt, reason="day lock")
            return False

        if not _is_before_deadline(cfg.straddle.gate_deadline_time_et):
            self._update_gate_blocked(gate_rt, reason="deadline")
            return False

        # Update gate runtime based on bars and arming rules.
        self._update_gate_runtime(tick_id, cfg, snap, gui, locks, gate_rt)

        with self._shared.lock:
            gate_state = self._shared.gate_by_channel[TriggerChannel.GATED.value].state
            selected_gate = self._shared.gate_by_channel[TriggerChannel.GATED.value].selected_gate

        if gate_state != GateState.PASS:
            return False

        if not self._common_entry_prereqs_ok(cfg, gui, ibkr_connected):
            return False
        if plan.call is None or plan.put is None:
            return False

        keys = (plan.call, plan.put)
        if not _spread_filter_ok(
            shared=self._shared,
            snap=snap,
            keys=keys,
            max_spread_pct=float(cfg.straddle.entry_spread_max_pct_mid),
            episode_key_prefix=f"STRADDLE_GATED_{selected_gate.value}",
        ):
            return False

        qtys, budget = _compute_straddle_qtys(snap, cfg, available_funds, plan.call, plan.put)
        if qtys[0] <= 0 or qtys[1] <= 0:
            return False

        intent = EntryIntent(
            created_ts=now_ts(),
            strategy=StrategyId.STRADDLE,
            reason=f"GATED_{selected_gate.value}",
            channel=TriggerChannel.GATED,
            legs=tuple(keys),
            qtys=tuple(qtys),
            max_cost=float(budget),
            deadline_ts=float(_parse_hhmm_et_to_ts_today(cfg.straddle.gate_deadline_time_et)),
        )
        self._q.put(intent)
        self._shared.log_info(f"STRADDLE EntryIntent created (GATED channel, {selected_gate.value}).")

        with self._shared.lock:
            if not bool(cfg.straddle.allow_multiple_gated_per_day):
                self._shared.day_locks[DayLockType.STRADDLE_GATED.value] = True
                self._shared.log_info("STRADDLE GATED day lock set.")
            # Reset gate state after intent emission.
            gr = self._shared.gate_by_channel[TriggerChannel.GATED.value]
            gr.state = GateState.DISARMED
            gr.armable = False
            gr.armed = False
            gr.last_transition_ts = now_ts()
        return True

    # ---------------------------------------------------------------------
    # SECTION: Gate runtime updates
    # ---------------------------------------------------------------------

    def _update_gate_blocked(self, gate_rt: GateRuntime, reason: str) -> None:
        with self._shared.lock:
            gr = self._shared.gate_by_channel[TriggerChannel.GATED.value]
            if gr.state != GateState.BLOCKED:
                gr.state = GateState.BLOCKED
                gr.selected_gate = GateType.NONE
                gr.armable = False
                gr.armed = False
                gr.last_transition_ts = now_ts()
                self._shared.log_info(f"Gate state -> BLOCKED ({reason}).")

    
    def _compute_pass_for_bars(
        self,
        selected: GateType,
        bars: List[Any],
        day_vwap: Optional[float],
        day_hod: Optional[float],
        day_lod: Optional[float],
    ) -> bool:
        """
        Evaluate PASS conditions for the *selected* gate type on an arbitrary bar stream.

        Notes:
        - Gate selection (Bull/Bear/Neutral) remains a 1-minute truth computed by evaluate_straddle_gate().
        - PASS may be satisfied by either 1-minute bars or 5-minute bars; this helper allows the same
          PASS logic to be evaluated on both streams without duplicating day-level truths.
        - day_vwap/day_hod/day_lod are canonical session truths published by MarketDataHub.
        """
        if selected == GateType.NONE:
            return False
        if not bars:
            return False
        last_close = getattr(bars[-1], "close", None)
        if not isinstance(last_close, (int, float)):
            return False

        # Bull/Bear PASS: bar close within 0.01% of VWAP (abs(dev) <= 0.0001).
        if selected in (GateType.BULL, GateType.BEAR):
            if not isinstance(day_vwap, (int, float)) or day_vwap == 0:
                return False
            dev = (float(last_close) - float(day_vwap)) / float(day_vwap)
            return abs(dev) <= 0.0001

        # Neutral PASS: two-bar sequence evaluated on the supplied timeframe:
        # bar0 close within 0.00015 of HOD/LOD OR creates new HOD/LOD; bar1 close within 0.00005 of bar0 close
        if selected == GateType.NEUTRAL:
            if len(bars) < 2:
                return False
            bar0 = bars[-2]
            bar1 = bars[-1]
            c0 = getattr(bar0, "close", None)
            c1 = getattr(bar1, "close", None)
            if not isinstance(c0, (int, float)) or not isinstance(c1, (int, float)):
                return False

            # Use canonical day HOD/LOD for "near extremum" checks.
            hod = float(day_hod) if isinstance(day_hod, (int, float)) else None
            lod = float(day_lod) if isinstance(day_lod, (int, float)) else None
            c0f = float(c0)
            c1f = float(c1)

            near_hod = (hod is not None) and (abs(c0f - hod) <= 0.00015 * max(1.0, hod))
            near_lod = (lod is not None) and (abs(c0f - lod) <= 0.00015 * max(1.0, lod))
            makes_new_hod = (hod is not None) and (c0f >= hod)
            makes_new_lod = (lod is not None) and (c0f <= lod)
            cond0 = near_hod or near_lod or makes_new_hod or makes_new_lod
            cond1 = abs(c1f - c0f) <= 0.00005 * max(1.0, abs(c0f))
            return bool(cond0 and cond1)

        return False

    def _update_gate_runtime(self, tick_id: int, cfg, snap: MarketSnapshot, gui, locks: Dict[str, bool], gate_rt: GateRuntime) -> None:
        ge = evaluate_straddle_gate(snap, cfg)
        selected = ge.selected
        armable = selected != GateType.NONE

        # PASS evaluation: satisfied if either 1-minute or 5-minute timeframe PASS is true.
        # PASS_1m is computed by evaluate_straddle_gate() from the 1-minute truth stream.
        pass_1m = False
        if selected == GateType.BULL:
            pass_1m = ge.bull_pass
        elif selected == GateType.BEAR:
            pass_1m = ge.bear_pass
        elif selected == GateType.NEUTRAL:
            pass_1m = ge.neutral_pass

        with self._shared.lock:
            bars_5m = list(getattr(self._shared, 'bars_5m', []))
        pass_5m = self._compute_pass_for_bars(
            selected=selected,
            bars=bars_5m,
            day_vwap=snap.spy_vwap,
            day_hod=snap.spy_hod,
            day_lod=snap.spy_lod,
        )
        passed = bool(pass_1m or pass_5m)

        with self._shared.lock:
            gr = self._shared.gate_by_channel[TriggerChannel.GATED.value]
            prev_state = gr.state
            prev_armable = gr.armable
            prev_armed = gr.armed
            prev_sel = gr.selected_gate

            # Update selection and armable flag (does not count as a state transition).
            gr.selected_gate = selected
            gr.armable = armable

            # Manual confirm press is one-shot and consumed here.
            manual_confirm = bool(self._shared.gui.manual_confirm_pressed)
            if manual_confirm:
                self._shared.gui.manual_confirm_pressed = False

            # State transitions: at most one per tick.
            # Priority order (deterministic):
            # BLOCKED handled externally; here we handle DISARMED/ARMABLE/ARMED/PASS.
            def _set_state(new_state: GateState) -> None:
                gr.state = new_state
                gr.last_transition_ts = now_ts()
            if prev_state == GateState.DISARMED:
                if armable:
                    _set_state(GateState.ARMABLE)
            elif prev_state == GateState.ARMABLE:
                if not armable:
                    _set_state(GateState.DISARMED)
                    gr.armed = False
                else:
                    if bool(cfg.straddle.gate_arming_auto):
                        gr.armed = True
                        _set_state(GateState.ARMED)
                    else:
                        if manual_confirm:
                            gr.armed = True
                            _set_state(GateState.ARMED)
            elif prev_state == GateState.ARMED:
                if not armable:
                    _set_state(GateState.DISARMED)
                    gr.armed = False
                elif passed:
                    _set_state(GateState.PASS)
            elif prev_state == GateState.PASS:
                # PASS is transient; if conditions no longer pass, return to ARMED.
                if not passed and armable:
                    _set_state(GateState.ARMED)
                elif not armable:
                    _set_state(GateState.DISARMED)
                    gr.armed = False

            # Logging (infrequent events only)
            if prev_sel != gr.selected_gate:
                self._shared.log_info(f"Gate selected: {gr.selected_gate.value}.")
            if prev_armable != gr.armable and gr.armable:
                self._shared.log_info(f"Gate -> ARMABLE ({gr.selected_gate.value}).")
                if not bool(cfg.straddle.gate_arming_auto):
                    if bool(cfg.ui.popup_on_armable):
                        self._shared.log_info("Gate ARMABLE: manual confirmation required.")
            if prev_armed != gr.armed and gr.armed:
                self._shared.log_info("Gate -> ARMED.")
            if prev_state != gr.state:
                self._shared.log_info(f"Gate state transition: {prev_state.value} -> {gr.state.value}.")
                if gr.state == GateState.PASS:
                    # If both timeframes satisfy PASS on the same scan tick, log both with no preference.
                    if pass_1m:
                        self._shared.log_info("Gate PASS satisfied on 1m bars.")
                    if pass_5m:
                        self._shared.log_info("Gate PASS satisfied on 5m bars.")


# =============================================================================
# SECTION: PCS Trigger Supervisor
# =============================================================================


class PCSTriggerSupervisor(threading.Thread):
    """Emits PCS+TAIL EntryIntent and enforces once/day lock."""

    def __init__(self, shared: SharedState, intents_q: Queue) -> None:
        super().__init__(daemon=True, name="PCSTriggerSupervisor")
        self._shared = shared
        self._q: Queue = intents_q
        self._tick_id = 0

    def run(self) -> None:
        while not self._shared.shutdown_event.is_set():
            t0 = time.monotonic()
            try:
                self._tick_id += 1
                self._scan_once(self._tick_id)
            except Exception as e:
                self._shared.log_error(f"PCSTriggerSupervisor error: {e!r}")
            with self._shared.lock:
                tick_s = float(self._shared.config.threads.triggers_tick_s)
            elapsed = time.monotonic() - t0
            remaining = max(0.0, tick_s - elapsed)
            if remaining > 0:
                time.sleep(min(remaining, 0.05))

    def _scan_once(self, tick_id: int) -> None:
        with self._shared.lock:
            cfg = self._shared.config
            snap = self._shared.market
            plan = self._shared.pcs_plan
            gui = self._shared.gui
            lock_pcs = bool(self._shared.day_locks.get(DayLockType.PCS.value, False))
            trades = list(self._shared.trades.values())
            ibkr_connected = self._shared.ibkr_connected
            available_funds = float(self._shared.available_funds)

        # Clear-lock request
        with self._shared.lock:
            if self._shared.gui.clear_lock_pcs:
                self._shared.day_locks[DayLockType.PCS.value] = False
                self._shared.gui.clear_lock_pcs = False
                self._shared.log_info("Cleared PCS day lock.")

        if snap is None or plan is None:
            return

        if self._active_pcs_trade(trades):
            return

        if lock_pcs and bool(cfg.pcs.once_per_day_lock):
            return

        if not _is_before_deadline(cfg.pcs.deadline_time_et):
            return

        if cfg.debug.run_mode == RunMode.REPLAY:
            return

        # Manual OPEN button bypasses signals but not go/prereqs.
        manual_open = bool(gui.pcs_open_pressed)
        if manual_open:
            with self._shared.lock:
                self._shared.gui.pcs_open_pressed = False

        if not gui.go:
            return

        if cfg.account.require_account_id_to_connect and not _has_account_id(cfg):
            return
        if cfg.debug.run_mode != RunMode.SIM and not ibkr_connected:
            return

        # Bar-based prerequisites.
        if len(snap.bars_1m) < int(cfg.pcs.min_bars_since_open):
            return

        if plan.short_put is None or plan.long_put is None:
            return

        # Automatic mode requires signals.
        if not manual_open:
            if not self._signals_pass(snap, cfg):
                return

        # Spread filter on both legs
        keys = (plan.short_put, plan.long_put)
        if not _spread_filter_ok(
            shared=self._shared,
            snap=snap,
            keys=keys,
            max_spread_pct=float(cfg.pcs.entry_spread_max_pct_mid),
            episode_key_prefix="PCS",
        ):
            return

        pcs_qty, credit_per, credit_total, budget = _compute_pcs_qtys(snap, cfg, available_funds, plan.short_put, plan.long_put)
        if pcs_qty <= 0:
            return

        tail_qty = 0
        tail_key: Optional[OptionKey] = None
        if plan.tail_put is not None:
            tail_qty = int(max(0.0, math.floor(float(cfg.pcs.tail_qty_mult_of_pcs) * pcs_qty)))
            tail_key = plan.tail_put

        legs: List[OptionKey] = [plan.short_put, plan.long_put]
        qtys: List[int] = [pcs_qty, pcs_qty]
        if tail_key is not None and tail_qty > 0:
            legs.append(tail_key)
            qtys.append(tail_qty)

        intent = EntryIntent(
            created_ts=now_ts(),
            strategy=StrategyId.PCS_TAIL,
            reason="PCS_MANUAL_OPEN" if manual_open else "PCS_SIGNAL_ENTRY",
            channel=None,
            legs=tuple(legs),
            qtys=tuple(qtys),
            max_cost=float(budget),
            deadline_ts=float(_parse_hhmm_et_to_ts_today(cfg.pcs.deadline_time_et)),
        )
        self._q.put(intent)
        self._shared.log_info("PCS+TAIL EntryIntent created.")
        with self._shared.lock:
            if bool(cfg.pcs.once_per_day_lock):
                self._shared.day_locks[DayLockType.PCS.value] = True
                self._shared.log_info("PCS day lock set.")

    def _active_pcs_trade(self, trades) -> bool:
        for tr in trades:
            if tr.strategy == StrategyId.PCS_TAIL and tr.state.value != "CLOSED":
                return True
        return False

    def _signals_pass(self, snap: MarketSnapshot, cfg) -> bool:
        if snap.spy_last is None:
            return False
        closes = [float(b.close) for b in snap.bars_1m]
        rsi_val = _rsi(closes, int(cfg.pcs.rsi_length))
        if rsi_val is None or rsi_val >= float(cfg.pcs.rsi_max):
            return False
        sma_val = _sma(closes, int(cfg.pcs.sma_length))
        if sma_val is None or float(snap.spy_last) <= sma_val:
            return False

        # % change since last close: approximate using first bar close as "prior".
        # A real implementation would use yesterday close; this is deterministic and
        # sufficient for consistent trigger behavior with the available snapshot data.
        if len(closes) < 2:
            return False
        prior = closes[0]
        if prior <= 0:
            return False
        pct = (float(snap.spy_last) - prior) / prior
        return pct >= float(cfg.pcs.pct_change_min)


# =============================================================================
# SECTION: Troubleshooting notes (for novices)
# =============================================================================
# - If no plans appear in the GUI, ensure MarketDataHub is publishing snapshots.
# - If plans appear but deltas are N/A, the hub may not yet have subscribed to
#   the candidate option keys. After a few ticks, the stub provider will
#   generate quotes/deltas for requested keys.
# - If no intents are emitted, check go is ON and day locks are clear.
