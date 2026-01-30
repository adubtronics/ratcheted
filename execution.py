"""
execution.py

Execution engine: the only component permitted to trade in REAL mode.

This module implements:
- Intent consumption (single consumer queue)
- Trade lifecycle state machines (IDLE→ENTERING→OPEN→EXITING→CLOSED)
- Per-leg lifecycle state machines (ENTERING/OPEN/TRIMMING/EXITING/CLOSED)
- Synthetic stop logic, ratchet ladder, hanging stop exits
- Partial fill rules and cost-match trim behavior
- Shutdown/flatten priority that ignores go and immediately exits positions

IMPORTANT:
- This implementation is compile-safe without external IBKR libraries.
- In RunMode.SIM, fills are simulated deterministically from bid/ask presence.
- In RunMode.REAL, order placement is represented as a stub adapter point.

Python: 3.10.4
"""

from __future__ import annotations

import time
import threading
import queue
import uuid
import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from core import (
    BudgetMode,
    EntryIntent,
    ExitIntent,
    FlattenIntent,
    Intent,
    LegRuntime,
    LegState,
    LogLevel,
    MarketSnapshot,
    OptionKey,
    OptionQuote,
    RunMode,
    SharedState,
    StrategyId,
    TailStopState,
    TradeRuntime,
    TradeState,
    TransitionGuard,
    now_ts,
)


# =============================================================================
# SECTION: Order/Fills model (internal lightweight representation)
# =============================================================================

@dataclass
class WorkingOrder:
    """
    Minimal working order representation.

    REAL mode would map this to an IBKR Order + OrderId.
    SIM mode uses it to determine when fills occur.
    """
    order_id: str
    contract: OptionKey
    qty: int
    limit_price: float
    submitted_ts: float
    action: str = 'BUY'
    last_mid: Optional[float] = None
    last_mid_change_ts: float = 0.0
    last_reprice_ts: float = 0.0
    filled_qty: int = 0
    cancelled: bool = False



def _infer_action(trade: TradeRuntime, leg: LegRuntime) -> str:
    """Infer order action (BUY/SELL) for entry orders for journaling and repricing."""
    # Prefer an explicit attribute if present.
    for attr in ("action", "side", "order_action"):
        try:
            v = getattr(leg, attr)
            if isinstance(v, str) and v.upper() in ("BUY", "SELL"):
                return v.upper()
        except Exception:
            continue

    try:
        strat = getattr(trade, "strategy", None)
    except Exception:
        strat = None

    if strat == StrategyId.PCS_TAIL:
        # Heuristic: PCS short leg is a SELL; others are BUY.
        try:
            lid = str(getattr(leg, "leg_id", "")).lower()
            if "short" in lid:
                return "SELL"
        except Exception:
            pass
        return "BUY"

    # Straddles and tails are debit buys.
    return "BUY"



# =============================================================================
# SECTION: Execution Engine
# =============================================================================

class ExecutionEngine:
    """
    ExecutionEngine consumes EntryIntent objects and manages trades.

    Hard rules:
    - Only trading component in REAL mode.
    - Must remain responsive (no long blocking).
    - Shutdown/flatten always processed immediately.
    - go flag blocks new entries only, never exits.
    """

    def __init__(self, shared: SharedState, intent_queue: "queue.Queue[Intent]" | None = None) -> None:
        """Create an execution engine.

        Parameters
        ----------
        shared:
            Shared state container.
        intent_queue:
            Optional intent queue (single-consumer). If not provided, the engine will
            fall back to `shared.intent_queue` when present, otherwise it will create
            a private queue.
        """
        self._shared = shared
        self._working_orders: Dict[str, WorkingOrder] = {}
        self._tick_id: int = 0
        self._flattening: bool = False

        self._shutdown_event: threading.Event = getattr(shared, "shutdown_event", threading.Event())
        if intent_queue is not None:
            self._intent_queue: "queue.Queue[Intent]" = intent_queue
        else:
            self._intent_queue = getattr(shared, "intent_queue", queue.Queue(maxsize=1000))

    # -------------------------------------------------------------------------
    # SECTION: Main loop entrypoint
    # -------------------------------------------------------------------------

# -------------------------------------------------------------------------
    # SECTION: Budgeting helpers (strategy-specific sizing modes)
    # -------------------------------------------------------------------------

    def _budget_usd_for_strategy(self, strategy: StrategyId, available_funds: float) -> float:
        """
        Resolve the active budget in USD for the given strategy based on RuntimeConfig.

        Notes:
        - GUI edits keep both budget fields editable; the budget_mode selects which is applied.
        - Percent inputs are entered as human percents in the GUI, but stored as fractions in config.
          (e.g., 0.25% is stored as 0.0025). Therefore, percent-of-funds budgets use the stored
          fractional form directly.
        """
        cfg = self._shared.config

        if strategy == StrategyId.STRADDLE:
            if cfg.straddle.budget_mode == BudgetMode.PCT_AVAILABLE_FUNDS:
                pct = float(cfg.straddle.budget_pct_available_funds)
                return max(0.0, pct * float(available_funds))
            return max(0.0, float(cfg.straddle.fixed_budget_usd))

        if strategy == StrategyId.PCS_TAIL:
            if cfg.pcs.budget_mode == BudgetMode.PCT_AVAILABLE_FUNDS:
                pct = float(cfg.pcs.budget_pct_available_funds)
                return max(0.0, pct * float(available_funds))
            return max(0.0, float(cfg.pcs.fixed_budget))

        return 0.0

    def run(self) -> None:
        """
        Main execution loop.

        This is intended to run in its own daemon thread.
        Tick pacing is controlled by config.execution_tick_s.
        """
        self._shared.log_info("Execution: Starting")
        while not self._shutdown_event.is_set():
            tick_start = time.monotonic()
            self._tick_id += 1

            # Heartbeat for GUI diagnostics
            try:
                with self._shared.lock:
                    setattr(self._shared, "execution_heartbeat_ts", time.time())
            except Exception:
                pass

            # Shutdown/flatten priority
            if self._is_shutdown_pressed():
                self._flatten_all()
                try:
                    self._shutdown_event.set()
                except Exception:
                    pass
                break

            # Consume at most one intent per tick to preserve determinism.
            self._consume_one_intent()

            # Manage existing trades every tick.
            self._manage_trades()

            # Tick pacing (short sleep only)
            tick_s = self._shared.config.threads.execution_tick_s
            elapsed = time.monotonic() - tick_start
            remaining = max(0.0, tick_s - elapsed)
            if remaining > 0:
                time.sleep(min(remaining, 0.05))

        self._shared.log_info("Execution: Stopped")
        # Final safety flatten
        self._flatten_all()

    # -------------------------------------------------------------------------
    # SECTION: Intent consumption
    # -------------------------------------------------------------------------

    def _consume_one_intent(self) -> None:
        try:
            intent: Intent = self._intent_queue.get_nowait()
        except Exception:
            return

        if isinstance(intent, EntryIntent):
            self._handle_entry_intent(intent)
        elif isinstance(intent, ExitIntent):
            self._handle_exit_intent(intent)
        elif isinstance(intent, FlattenIntent):
            self._flatten_all()

    def _handle_entry_intent(self, intent: EntryIntent) -> None:
        """
        EntryIntent creates a new TradeRuntime and submits entry orders.

        Entry prerequisites are enforced upstream in trigger supervisors,
        but execution still validates shutdown and connectivity.
        """
        if self._is_shutdown_pressed():
            return

        with self._shared.lock:
            run_mode = self._shared.config.debug.run_mode
            connected = self._shared.ibkr_connected
            go_flag = self._shared.gui.go

        # go blocks new entries only
        if not go_flag:
            return

        if run_mode == RunMode.REAL and not connected:
            self._shared.log_warn("REAL entry blocked: IBKR not connected.")
            return


        with self._shared.lock:
            available_funds = float(self._shared.available_funds)

        # Enforce budget guard at consumption time (protects against stale upstream sizing).
        budget_usd = self._budget_usd_for_strategy(intent.strategy, available_funds)
        if budget_usd <= 0.0:
            self._shared.log_warn(
                f"Entry blocked: budget is 0 for {intent.strategy.value}."
            )
            return

        if float(intent.max_cost) > budget_usd:
            self._shared.log_warn(
                f"Entry blocked: intent cost ${float(intent.max_cost):.2f} exceeds budget ${budget_usd:.2f} "
                f"for {intent.strategy.value}."
            )
            return

        trade_id = str(uuid.uuid4())[:8]
        trade = TradeRuntime.create_from_intent(trade_id, intent)

        with self._shared.lock:
            self._shared.trades[trade_id] = trade

        self._shared.log_info(
            f"Intent accepted: ENTER {intent.strategy.value} channel={intent.channel.value}"
        )

        # Submit entry orders immediately.
        self._submit_entry_orders(trade_id, trade)

    # -------------------------------------------------------------------------
    # SECTION: Order submission + fills
    # -------------------------------------------------------------------------

    def _submit_entry_orders(self, trade_id: str, trade: TradeRuntime) -> None:
        """
        Submit entry orders for each leg.

        Price algorithm:
        - Use mid initially.
        - Execution management will ratchet +1 tick after unchanged window.
        """
        snap = self._latest_snapshot()
        if snap is None:
            return

        for leg in trade.legs:
            if leg.contract is None or leg.qty <= 0:
                continue
            quote = snap.option_quotes.get(leg.contract)
            if quote is None or quote.mid is None:
                continue

            order_id = f"{trade_id}:{leg.leg_id}"
            wo = WorkingOrder(
                order_id=order_id,
                contract=leg.contract,
                qty=leg.qty,
                limit_price=float(quote.mid),
                submitted_ts=now_ts(),
                action=_infer_action(trade, leg),
                last_mid=float(quote.mid) if quote.mid is not None else None,
                last_mid_change_ts=now_ts(),
                last_reprice_ts=0.0,
            )
            self._working_orders[order_id] = wo
            leg.state = LegState.ENTERING
            leg.sm_guard.mark_transition(self._tick_id)

            self._shared.log_info(
                f"Order submitted ({trade.strategy.value}) leg={leg.leg_id} "
                f"qty={leg.qty} limit={wo.limit_price:.2f}"
            )

        trade.state = TradeState.ENTERING
        trade.sm_guard.mark_transition(self._tick_id)

    def _simulate_fill_if_possible(self, wo: WorkingOrder, quote: OptionQuote) -> None:
        """
        Deterministic SIM fill model:
        - If bid/ask exist, assume immediate fill at limit price.
        """
        if wo.cancelled:
            return
        if quote.bid is None or quote.ask is None:
            return
        wo.filled_qty = wo.qty

    # -------------------------------------------------------------------------
    # SECTION: Trade management loop
    # -------------------------------------------------------------------------

    def _manage_trades(self) -> None:
        snap = self._latest_snapshot()
        if snap is None:
            return

        with self._shared.lock:
            run_mode = self._shared.config.debug.run_mode

        # Step 1: process working orders (fills/timeouts)
        self._process_working_orders(snap, run_mode)

        # Step 2: manage open legs (stops/ratchets/hang)
        trade_ids = list(self._shared.trades.keys())
        for tid in trade_ids:
            trade = self._shared.trades.get(tid)
            if trade is None:
                continue
            self._manage_trade(trade, snap)



    # -------------------------------------------------------------------------
    # SECTION: Entry repricing (mid + stale-improve) with budget-capped quantity
    # -------------------------------------------------------------------------

    def _compute_leg_budget_usd(self, trade: TradeRuntime, leg: LegRuntime) -> float:
        """
        Compute the USD budget cap for a single leg.

        Rules enforced:
        - Straddle uses `leg_cap_pct_each` as the per-leg share of the strategy budget.
        - Other strategies default to full strategy budget unless future per-leg caps are added.
        """
        try:
            trade_budget = float(getattr(trade, "max_cost", 0.0) or 0.0)
        except Exception:
            trade_budget = 0.0

        if trade_budget <= 0.0:
            return 0.0

        if getattr(trade, "strategy", None) == StrategyId.STRADDLE:
            cap_frac = float(self._shared.config.straddle.leg_cap_pct_each)
            return max(0.0, trade_budget * cap_frac)

        return trade_budget

    def _budget_cap_qty(self, wo: WorkingOrder, new_limit: float, leg_budget_usd: float) -> int:
        """
        Return the maximum total order quantity (filled + remaining) allowed at `new_limit`
        given the leg budget cap.

        Hard requirements implemented:
        - Budget cap uses the *working limit price*.
        - Budget cap applies to the *unfilled quantity*.
        - Quantity reprices both ways: if limit increases, qty may need to drop; if limit decreases,
          qty may increase up to the original requested qty.
        """
        if wo.cancelled:
            return 0
        if new_limit <= 0.0 or leg_budget_usd <= 0.0:
            return wo.qty

        # Budget already consumed by any filled qty (best-effort; partial fills are rare in SIM).
        consumed = float(wo.filled_qty) * float(new_limit) * 100.0
        remaining_budget = max(0.0, float(leg_budget_usd) - consumed)

        allowed_remaining = int(math.floor(remaining_budget / (float(new_limit) * 100.0)))
        return max(wo.filled_qty, wo.filled_qty + allowed_remaining)

    def _reprice_working_order(self, trade: TradeRuntime, leg: LegRuntime, wo: WorkingOrder, quote: OptionQuote) -> None:
        """
        Reprice a working entry order using the configured algorithm:

        Algorithm:
        - Base target is current mid when available.
        - If mid has remained unchanged for `entry_price_stale_s`, improve the limit by one tick:
          - BUY: +tick_size, capped at ask
          - SELL: -tick_size, floored at bid
        - Budget-capped quantity adjustment is enforced against the *unfilled* quantity using the
          *working limit price* at all times. If the new limit would violate budget, the order is
          replaced with a reduced quantity. If the new limit frees budget, quantity may increase
          up to the original intended qty.
        """
        if wo.cancelled:
            return

        mid = quote.mid
        if mid is None:
            return

        now = now_ts()
        if wo.last_mid is None or abs(float(mid) - float(wo.last_mid)) > 1e-12:
            wo.last_mid = float(mid)
            wo.last_mid_change_ts = now

        stale_s = float(self._shared.config.straddle.entry_price_stale_s)
        tick = float(self._shared.config.straddle.entry_price_tick_size)

        # Determine the candidate limit.
        new_limit = float(mid)
        mid_unchanged_s = now - float(wo.last_mid_change_ts or now)
        if mid_unchanged_s >= stale_s:
            if wo.action.upper() == "SELL":
                new_limit = float(mid) - tick
                if quote.bid is not None:
                    new_limit = max(new_limit, float(quote.bid))
            else:
                new_limit = float(mid) + tick
                if quote.ask is not None:
                    new_limit = min(new_limit, float(quote.ask))

        # If unchanged (within rounding), skip.
        if abs(float(new_limit) - float(wo.limit_price)) < 1e-9:
            return

        # Enforce per-leg budget cap (for entry orders).
        leg_budget = self._compute_leg_budget_usd(trade, leg)
        capped_total_qty = self._budget_cap_qty(wo, new_limit, leg_budget)

        # Never exceed the intended qty for this leg.
        intended_qty = int(getattr(leg, "qty", wo.qty) or wo.qty)
        capped_total_qty = min(capped_total_qty, intended_qty)

        if capped_total_qty <= wo.filled_qty:
            wo.cancelled = True
            self._shared.log_warn(
                f"Entry order cancelled (budget cap): trade={trade.trade_id} leg={leg.leg_id} "
                f"limit={new_limit:.2f} budget={leg_budget:.2f}"
            )
            return

        # Replace (update) order: new limit + possibly adjusted qty.
        prior_qty = wo.qty
        prior_limit = wo.limit_price
        wo.qty = int(capped_total_qty)
        wo.limit_price = float(new_limit)
        wo.last_reprice_ts = now

        if wo.qty != prior_qty:
            self._shared.log_info(
                f"Order replaced (repriced + qty cap): trade={trade.trade_id} leg={leg.leg_id} "
                f"limit {prior_limit:.2f}->{wo.limit_price:.2f} qty {prior_qty}->{wo.qty}"
            )
        else:
            self._shared.log_info(
                f"Order replaced (repriced): trade={trade.trade_id} leg={leg.leg_id} "
                f"limit {prior_limit:.2f}->{wo.limit_price:.2f}"
            )
    def _process_working_orders(self, snap: MarketSnapshot, run_mode: RunMode) -> None:
        now = now_ts()
        timeout_s = self._shared.config.straddle.entry_fill_timeout_s

        for oid, wo in list(self._working_orders.items()):
            if wo.cancelled:
                continue

            quote = snap.option_quotes.get(wo.contract)
            if quote is None:
                continue

            # Repricing (entry orders only): apply mid/stale algorithm and budget-capped qty adjustment.
            try:
                trade_id, leg_id = wo.order_id.split(":")
                trade = self._shared.trades.get(trade_id)
                if trade is not None:
                    leg = next((l for l in trade.legs if l.leg_id == leg_id), None)
                    if leg is not None and leg.state == LegState.ENTERING:
                        self._reprice_working_order(trade, leg, wo, quote)
                        if wo.cancelled:
                            continue
            except Exception:
                pass

            # SIM fills
            if run_mode == RunMode.SIM:
                self._simulate_fill_if_possible(wo, quote)

            # If fully filled, apply to leg runtime
            if wo.filled_qty >= wo.qty:
                self._apply_fill(wo, fill_price=wo.limit_price)
                continue

            # Timeout handling
            if now - wo.submitted_ts >= timeout_s:
                wo.cancelled = True
                self._shared.log_warn(
                    f"Entry timeout: cancelling order {oid}"
                )

    def _apply_fill(self, wo: WorkingOrder, fill_price: float) -> None:
        """
        Apply a fill into the associated leg runtime.
        """
        trade_id, leg_id = wo.order_id.split(":")
        trade = self._shared.trades.get(trade_id)
        if trade is None:
            return

        leg = next((l for l in trade.legs if l.leg_id == leg_id), None)
        if leg is None:
            return

        leg.filled_qty = wo.filled_qty
        leg.avg_fill_price = fill_price
        leg.open_ts = now_ts()
        leg.state = LegState.OPEN
        leg.sm_guard.mark_transition(self._tick_id)

        self._shared.log_info(
            f"Fill: trade={trade_id} leg={leg_id} "
            f"qty={leg.filled_qty} price={fill_price:.2f}"
        )

        # Remove working order
        wo.cancelled = True

        # Trade state update
        if trade.state == TradeState.ENTERING:
            trade.state = TradeState.OPEN
            trade.sm_guard.mark_transition(self._tick_id)

    # -------------------------------------------------------------------------
    # SECTION: Stops, ratchets, hanging exits
    # -------------------------------------------------------------------------

    def _manage_trade(self, trade: TradeRuntime, snap: MarketSnapshot) -> None:
        """
        Manage each open leg:
        - Synthetic stops (bid-based)
        - Ratchet ladder
        - Hanging stop exits
        """
        for leg in trade.legs:
            if leg.state != LegState.OPEN:
                continue
            if leg.contract is None or leg.avg_fill_price is None:
                continue

            quote = snap.option_quotes.get(leg.contract)
            if quote is None or quote.bid is None:
                continue

            pnl_pct = (quote.bid - leg.avg_fill_price) / leg.avg_fill_price

            # Soft/hard stop thresholds
            soft = self._shared.config.straddle.stop_soft_pct
            hard = self._shared.config.straddle.stop_hard_pct

            # Hard stop immediate
            if pnl_pct <= hard:
                self._exit_leg(trade, leg, reason="HARD STOP")
                continue

            # Soft stop poke enables hang immediately
            if pnl_pct <= soft and not leg.hang_active:
                leg.hang_active = True
                leg.hang_start_ts = now_ts()
                self._shared.log_warn(
                    f"Soft stop poked: enabling hang leg={leg.leg_id}"
                )

            # Ratchet ladder
            self._apply_ratchet(trade, leg, pnl_pct)

            # Hang exit
            if leg.hang_active:
                self._apply_hang_exit(trade, leg, quote.bid)

    def _apply_ratchet(self, trade: TradeRuntime, leg: LegRuntime, pnl_pct: float) -> None:
        ladder = self._shared.config.straddle.ratchet_ladder
        if leg.max_pnl_pct is None or pnl_pct > leg.max_pnl_pct:
            leg.max_pnl_pct = pnl_pct
            leg.last_new_high_ts = now_ts()

        for threshold, lock_level in ladder:
            if pnl_pct >= threshold and leg.stop_lock_pct < lock_level:
                leg.stop_lock_pct = lock_level
                self._shared.log_info(
                    f"Ratchet: leg={leg.leg_id} lock={lock_level:.2f}"
                )

    def _apply_hang_exit(self, trade: TradeRuntime, leg: LegRuntime, bid: float) -> None:
        hang_s = self._shared.config.straddle.hang_seconds
        if leg.last_new_high_ts is None:
            leg.last_new_high_ts = now_ts()
            return

        elapsed = now_ts() - leg.last_new_high_ts
        if elapsed >= hang_s:
            self._exit_leg(trade, leg, reason="HANG EXIT")

    def _exit_leg(self, trade: TradeRuntime, leg: LegRuntime, reason: str) -> None:
        """
        Exit leg immediately (synthetic market exit in SIM).

        REAL mode would submit closing orders here.
        """
        if leg.state != LegState.OPEN:
            return

        leg.state = LegState.EXITING
        leg.sm_guard.mark_transition(self._tick_id)

        self._shared.log_warn(
            f"Exit leg={leg.leg_id} reason={reason}"
        )

        leg.state = LegState.CLOSED
        leg.close_ts = now_ts()
        leg.sm_guard.mark_transition(self._tick_id)

        # If all legs closed, close trade
        if all(l.state == LegState.CLOSED for l in trade.legs):
            trade.state = TradeState.CLOSED
            trade.sm_guard.mark_transition(self._tick_id)
            self._shared.log_info(
                f"Trade closed trade_id={trade.trade_id}"
            )

    # -------------------------------------------------------------------------
    # SECTION: Flatten/shutdown
    # -------------------------------------------------------------------------

    def _flatten_all(self) -> None:
        """Flatten all open trades immediately.

        This ignores go and is always processed first on shutdown.
        """
        if self._flattening:
            return
        self._flattening = True
        self._shared.log_warn("Shutdown flatten start.")
        for trade in list(self._shared.trades.values()):
            for leg in trade.legs:
                if leg.state == LegState.OPEN:
                    self._exit_leg(trade, leg, reason="SHUTDOWN FLATTEN")
        self._shared.log_warn("Shutdown flatten complete.")

        
        self._flattening = False

    def _is_shutdown_pressed(self) -> bool:
        with self._shared.lock:
            return bool(self._shared.gui.shutdown_pressed)

    # -------------------------------------------------------------------------
    # SECTION: Utilities
    # -------------------------------------------------------------------------

    def _latest_snapshot(self) -> Optional[MarketSnapshot]:
        with self._shared.lock:
            return self._shared.market
