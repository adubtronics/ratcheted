"""
execution.py

Execution module (ONLY trader). HARD RULE: imports only from core.py (plus stdlib).

Contains:
- ExecutionEngine thread (only trader)
- Order pricing logic (mid → tick → ask)
- Synthetic stop management
- Ratchet + hanging stop logic
- Partial fill trim logic
- Flatten/shutdown logic

NOTE:
- This project is designed to run in SIMULATED mode (GUI Debug tab sim fills only).
- LIVE execution wiring to IBKR is intentionally not here (per import constraints).
"""

from __future__ import annotations

import math
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from core import (
    BulletLogger,
    ContractKey,
    EntryIntent,
    ExitIntent,
    FlattenIntent,
    Intent,
    IntentBus,
    IntentType,
    LockName,
    LogLevel,
    MarketSnapshot,
    OptionQuote,
    OptionRight,
    Position,
    RatchetStep,
    RunMode,
    RuntimeConfig,
    SharedState,
    Side,
    StrategyType,
    WorkingOrder,
    clamp,
    safe_mid,
)


# =========================
# Pricing helpers
# =========================

def tick_size(price: float) -> float:
    """
    Simple option tick rules (approx):
    - < 3.00 => 0.01
    - >=3.00 => 0.05
    """
    if price < 3.0:
        return 0.01
    return 0.05


def round_to_tick(price: float, tick: float, direction: str) -> float:
    """
    direction:
      - "NEAREST"
      - "UP"
      - "DOWN"
    """
    if tick <= 0.0:
        return price
    x = price / tick
    if direction == "UP":
        return math.ceil(x) * tick
    if direction == "DOWN":
        return math.floor(x) * tick
    return round(x) * tick


def choose_working_price(bid: float, ask: float) -> float:
    """
    Pricing ladder for entries:
      mid -> round to tick -> if too far/invalid, use ask.
    Caller decides side. For BUY, mid/tick then may progress to ask.
    For SELL, mid/tick then may progress to bid.
    """
    mid = safe_mid(bid, ask)
    if mid <= 0.0:
        # fallback to the best known side price
        return ask if ask > 0.0 else bid
    t = tick_size(mid)
    return round_to_tick(mid, t, "NEAREST")


def now_ts() -> float:
    return time.time()


# =========================
# Synthetic stop management
# =========================

@dataclass
class LegStopState:
    key: ContractKey
    direction: int  # +1 long, -1 short (for profit sign)
    entry_price: float
    # stop lock is a profit_pct threshold; exit if profit_pct <= stop_lock
    soft_stop_pct: float
    hard_stop_pct: float
    ratchet_ladder: List[RatchetStep]
    hang_seconds: int
    current_stop_lock: float = field(default=-999.0)
    hang_until_ts: float = field(default=0.0)
    last_profit_pct: float = field(default=0.0)

    def update_from_profit(self, profit_pct: float) -> None:
        self.last_profit_pct = profit_pct
        # ratchet: if profit exceeds trigger, raise stop lock
        for step in self.ratchet_ladder:
            if profit_pct >= float(step.profit_trigger):
                self.current_stop_lock = max(self.current_stop_lock, float(step.stop_lock))

    def stop_should_exit(self, profit_pct: float) -> Tuple[bool, str]:
        """
        Returns (should_exit, reason).
        - Hard stop: immediate
        - Soft stop: hang timer
        - Ratchet lock: hang timer
        """
        # hard stop (immediate)
        if profit_pct <= self.hard_stop_pct:
            return True, f"HardStop {profit_pct:.3f}<= {self.hard_stop_pct:.3f}"

        # choose active stop threshold: max of soft and ratchet lock (both are profit_pct thresholds)
        active_lock = max(self.soft_stop_pct, self.current_stop_lock)
        if profit_pct <= active_lock:
            # hang logic
            if self.hang_seconds <= 0:
                return True, f"Stop {profit_pct:.3f}<= {active_lock:.3f} (no hang)"
            if self.hang_until_ts <= 0.0:
                self.hang_until_ts = now_ts() + float(self.hang_seconds)
                return False, f"HangStart {profit_pct:.3f}<= {active_lock:.3f}"
            if now_ts() >= self.hang_until_ts:
                return True, f"HangExit {profit_pct:.3f}<= {active_lock:.3f}"
            return False, f"Hanging {profit_pct:.3f}<= {active_lock:.3f}"
        else:
            # recovered: clear hang
            self.hang_until_ts = 0.0
            return False, "OK"


@dataclass
class OrderFillState:
    order_id: str
    key: ContractKey
    side: Side
    target_qty: int
    filled_qty: int = 0
    avg_fill_price: float = 0.0
    created_ts: float = field(default_factory=now_ts)
    last_update_ts: float = field(default_factory=now_ts)

    def fill_ratio(self) -> float:
        if self.target_qty <= 0:
            return 1.0
        return float(self.filled_qty) / float(self.target_qty)

    def is_done(self) -> bool:
        return self.filled_qty >= self.target_qty


@dataclass
class PCSGroupState:
    """
    Tracks a PCS spread (short put + long put) + tail put.
    We manage the PCS stop using spread value vs entry credit.
    Tail activation based on credit decay.
    """
    group_id: str
    short_put: ContractKey
    long_put: ContractKey
    tail_put: ContractKey

    entry_credit: float = 0.0  # positive
    pcs_stop_debit: float = 0.0  # if spread value (debit) >= this, stop out
    tail_activated: bool = False

    tail_hang_seconds: int = 120
    tail_ratchet_threshold_frac: float = 1.0
    tail_ratchet_lock_frac: float = 0.5
    tail_stop_lock: float = -999.0
    tail_hang_until_ts: float = 0.0


# =========================
# Execution Engine
# =========================


@dataclass
class PCSComboOrderState:
    """Working state for a PCS spread entry submitted as a combo (BAG) credit order."""

    group_id: str
    short_put: ContractKey
    long_put: ContractKey
    qty_total: int
    qty_filled: int
    limit_credit: float
    tick_credit: float
    budget_usd: float
    budget_credit_mult: float
    last_mid: float
    last_reprice_ts: float
    created_ts: float
    stale_since_ts: float
    done: bool = False

    @property
    def qty_remaining(self) -> int:
        return max(0, int(self.qty_total) - int(self.qty_filled))

class ExecutionEngine(threading.Thread):
    """
    The ONLY trader thread.

    Responsibilities:
    - Consume intents from IntentBus
    - Create/cancel "working orders" in SharedState
    - Apply SIMULATED fills based on market snapshot
    - Maintain synthetic stops (ratchet + hanging stops)
    - Partial fill trim rules (best-effort; primarily relevant in LIVE)
    - Flatten/shutdown flatten (ignores go)
    """

    def __init__(self, shared: SharedState, bus: IntentBus, poll_s: float = 0.25) -> None:
        super().__init__(name="ExecutionEngine", daemon=True)
        self._shared = shared
        self._bus = bus
        self._poll_s = float(poll_s)
        self._stop_evt = threading.Event()
        self._log = self._shared.logger

        # active working order fill states
        self._fills: Dict[str, OrderFillState] = {}
        # per-leg stop states (primarily for straddle legs)
        self._stops: Dict[ContractKey, LegStopState] = {}
        # pcs groups
        self._pcs_groups: Dict[str, PCSGroupState] = {}
        # pcs combo working orders (spread entered as BAG credit)
        self._pcs_combo_orders: Dict[str, PCSComboOrderState] = {}


        self._shutdown_requested: bool = False

    def stop(self) -> None:
        self._stop_evt.set()

    # -------------------------
    # Market lookups
    # -------------------------

    def _find_option_quote(self, market: Optional[MarketSnapshot], key: ContractKey) -> Optional[OptionQuote]:
        if market is None:
            return None
        for oq in market.option_quotes:
            if (
                oq.symbol == key.symbol
                and oq.expiry == key.expiry
                and float(oq.strike) == float(key.strike)
                and oq.right == key.right
            ):
                return oq
        return None

    def _mark_price(self, market: Optional[MarketSnapshot], key: ContractKey) -> float:
        oq = self._find_option_quote(market, key)
        if oq is None:
            return 0.0
        q = oq.quote
        # prefer mid if valid, else last/bid/ask
        if q.mid > 0.0:
            return float(q.mid)
        if q.last > 0.0:
            return float(q.last)
        if q.bid > 0.0 and q.ask > 0.0:
            return float(safe_mid(q.bid, q.ask))
        return float(q.ask or q.bid or 0.0)

    # -------------------------
    # Order creation & fills
    # -------------------------


    def _combo_bid_ask_mid_credit(self, market: Optional[MarketSnapshot], short_put: ContractKey, long_put: ContractKey) -> Tuple[float, float, float]:
        """Return (combo_bid, combo_ask, combo_mid) for a PCS credit spread using option quotes.

        Credit spread definition:
        - Sell short_put, buy long_put (both PUT).
        - Combo credit approximations:
            bid_credit = short_bid - long_ask
            ask_credit = short_ask - long_bid
            mid_credit = (bid_credit + ask_credit) / 2
        Missing data yields zeros.
        """
        oq_s = self._find_option_quote(market, short_put)
        oq_l = self._find_option_quote(market, long_put)
        if oq_s is None or oq_l is None:
            return 0.0, 0.0, 0.0
        sb = float(oq_s.quote.bid or 0.0)
        sa = float(oq_s.quote.ask or 0.0)
        lb = float(oq_l.quote.bid or 0.0)
        la = float(oq_l.quote.ask or 0.0)
        bid = max(0.0, sb - la)
        ask = max(0.0, sa - lb)
        mid = 0.0
        if bid > 0.0 and ask > 0.0:
            mid = 0.5 * (bid + ask)
        elif bid > 0.0:
            mid = bid
        elif ask > 0.0:
            mid = ask
        return float(bid), float(ask), float(mid)

    def _new_order_id(self) -> str:
        return uuid.uuid4().hex[:12]

    def _place_working_order(self, key: ContractKey, side: Side, qty: int, limit_price: Optional[float]) -> str:
        oid = self._new_order_id()
        wo = WorkingOrder(order_id=oid, key=key, side=side, qty=int(qty), limit_price=limit_price, ts=now_ts())
        self._shared.set_working_order(wo)
        self._fills[oid] = OrderFillState(order_id=oid, key=key, side=side, target_qty=int(qty))
        self._log.info(f"Placed {side.value} {qty} {key.display()} @ {limit_price if limit_price is not None else 'MKT'}")
        return oid


    def _apply_simulated_fill(self, key: ContractKey, side: Side, qty: int, price: float) -> None:
        """Apply a synthetic fill into SharedState positions for SIMULATED mode."""
        if qty == 0:
            return
        snap = self._shared.snapshot()
        pos = snap.positions.get(key)
        cur_qty = int(pos.qty) if pos is not None else 0
        cur_avg = float(pos.avg_price or 0.0) if pos is not None else 0.0
        signed = int(qty) if side == Side.BUY else -int(qty)
        new_qty = cur_qty + signed
        if new_qty == 0:
            new_avg = 0.0
        else:
            # Weighted average for adds; simplistic but deterministic.
            if cur_qty == 0:
                new_avg = float(price)
            else:
                total_cost = (cur_avg * abs(cur_qty)) + (float(price) * abs(signed))
                new_avg = total_cost / max(1, (abs(cur_qty) + abs(signed)))
        self._shared.apply_sim_fill(key, new_qty, new_avg)

    def _cancel_working_order(self, order_id: str, reason: str) -> None:
        self._shared.remove_working_order(order_id)
        self._fills.pop(order_id, None)
        self._log.warn(f"Canceled order {order_id}: {reason}", key="exec.cancel")

    def _cancel_all_orders(self, reason: str) -> None:
        snap = self._shared.snapshot()
        for oid in list(snap.working_orders.keys()):
            self._cancel_working_order(oid, reason)

    def _apply_fill_to_position(self, key: ContractKey, side: Side, qty: int, price: float) -> None:
        snap = self._shared.snapshot()
        pos = snap.positions.get(key)
        signed_qty = int(qty) if side == Side.BUY else -int(qty)

        if pos is None:
            new_pos = Position(key=key, qty=signed_qty, avg_price=float(price), last_update_ts=now_ts())
            self._shared.set_position(new_pos)
            return

        # average price update only if increasing absolute exposure in same direction
        old_qty = int(pos.qty)
        new_qty = old_qty + signed_qty

        if old_qty == 0:
            pos.qty = new_qty
            pos.avg_price = float(price)
        elif (old_qty > 0 and signed_qty > 0) or (old_qty < 0 and signed_qty < 0):
            # weighted avg
            total_abs = abs(old_qty) + abs(signed_qty)
            if total_abs > 0:
                pos.avg_price = (pos.avg_price * abs(old_qty) + float(price) * abs(signed_qty)) / float(total_abs)
            pos.qty = new_qty
        else:
            # reducing or flipping: keep avg_price for remaining, simplistic realized handling
            pos.qty = new_qty
            if new_qty == 0:
                pos.avg_price = 0.0

        pos.last_update_ts = now_ts()
        self._shared.set_position(pos)

    def _try_simulated_fill(self, cfg: RuntimeConfig, market: Optional[MarketSnapshot], fill: OrderFillState) -> None:
        """
        SIMULATED fills only (as required by GUI Debug).
        - BUY fills at ask if ask exists else at mid.
        - SELL fills at bid if bid exists else at mid.
        - If limit_price provided, respect it:
            BUY fills if fill_price <= limit
            SELL fills if fill_price >= limit
        """
        if cfg.general.mode != RunMode.SIMULATED:
            return

        oq = self._find_option_quote(market, fill.key)
        if oq is None:
            return

        bid = float(oq.quote.bid)
        ask = float(oq.quote.ask)
        mid = float(oq.quote.mid or safe_mid(bid, ask))

        if fill.side == Side.BUY:
            px = ask if ask > 0.0 else mid
            if fill.avg_fill_price <= 0.0:
                fill.avg_fill_price = px
            if fill_key_limit_ok(fill.side, px, self._shared.snapshot().working_orders.get(fill.order_id)):
                self._finalize_full_fill(fill, px)
        else:
            px = bid if bid > 0.0 else mid
            if fill.avg_fill_price <= 0.0:
                fill.avg_fill_price = px
            if fill_key_limit_ok(fill.side, px, self._shared.snapshot().working_orders.get(fill.order_id)):
                self._finalize_full_fill(fill, px)

    def _finalize_full_fill(self, fill: OrderFillState, price: float) -> None:
        remaining = fill.target_qty - fill.filled_qty
        if remaining <= 0:
            return
        fill.filled_qty += remaining
        fill.avg_fill_price = float(price)
        fill.last_update_ts = now_ts()

        # Remove working order, update positions
        self._shared.remove_working_order(fill.order_id)
        self._apply_fill_to_position(fill.key, fill.side, remaining, float(price))
        self._log.info(f"FILLED {fill.side.value} {remaining} {fill.key.display()} @ {price:.2f}")
        # clean fill state
        self._fills.pop(fill.order_id, None)

    # -------------------------
    # Intent handlers
    # -------------------------

    def _handle_entry_intent(self, intent: EntryIntent) -> None:
        cfg = self._shared.get_config()

        # go semantics: blocks new entries only
        if not cfg.general.go_enabled:
            self._log.warn("Entry blocked (go is OFF).", key="exec.go.off")
            return

        snap = self._shared.snapshot()
        market = snap.market

        # Build working orders with mid->tick pricing, then step toward ask/bid if needed (SIM fills usually instant).
        # "Call/put cap 50% each enforced using working limit price": best-effort by limiting per-leg notional vs budget.
        budget = float(intent.budget)
        if budget <= 0.0:
            budget = float(snap.account.available_funds or 0.0)

        per_leg_cap = budget
        cap_call = per_leg_cap
        cap_put = per_leg_cap

        # if straddle, split caps 50/50
        if intent.strategy == StrategyType.STRADDLE:
            cap_call = budget * float(cfg.straddle.call_cap_pct)
            cap_put = budget * float(cfg.straddle.put_cap_pct)


        # PCS entries: place the short+long put spread as a single combo (BAG) credit order,
        # and place the tail as an independent order.
        if intent.strategy == StrategyType.PCS_TAIL:
            sp_k: Optional[ContractKey] = None
            lp_k: Optional[ContractKey] = None
            tp_k: Optional[ContractKey] = None
            sp_qty = 0
            lp_qty = 0
            tp_qty = 0
            for (k, s, q, _cap) in intent.legs:
                if k.right != OptionRight.PUT:
                    continue
                if s == Side.SELL and sp_k is None:
                    sp_k = k
                    sp_qty = int(q)
                elif s == Side.BUY and lp_k is None:
                    lp_k = k
                    lp_qty = int(q)
                elif s == Side.BUY:
                    tp_k = k
                    tp_qty = int(q)

            if sp_k is None or lp_k is None:
                self._log.warn("PCS entry skipped (spread legs not detected).", key="pcs.entry.skip")
                return

            # Combo order sizing + repricing constraints:
            # - The combo order is a SELL credit with minimum credit = limit_credit.
            # - The minimum credit will never be set below current combo_bid (no benefit below bid).
            # - Budget constraint uses working limit credit and applies to the *unfilled* quantity:
            #     remaining_qty * limit_credit * 100 * budget_credit_mult <= budget_usd
            combo_bid, combo_ask, combo_mid = self._combo_bid_ask_mid_credit(market, sp_k, lp_k)
            if combo_bid <= 0.0 and combo_ask <= 0.0 and combo_mid <= 0.0:
                self._log.warn("PCS entry blocked (missing combo quotes).", key="pcs.entry.noquotes")
                return

            # Start at mid if available, else bid; clamp to [combo_bid, combo_ask] where possible.
            start_credit = combo_mid if combo_mid > 0.0 else combo_bid
            if combo_ask > 0.0:
                start_credit = min(start_credit, combo_ask)
            start_credit = max(start_credit, combo_bid)

            # Budget constraint (qty can move up/down as credit changes).
            budget_mult = float(cfg.pcs_tail.budget_credit_mult)
            tick = max(0.01, float(cfg.general.price_tick))
            max_qty = 0
            if budget > 0.0 and start_credit > 0.0 and budget_mult > 0.0:
                max_qty = int(max(0.0, math.floor(budget / (start_credit * 100.0 * budget_mult))))
            desired_qty = abs(int(sp_qty)) if sp_qty != 0 else abs(int(lp_qty))
            if desired_qty <= 0:
                desired_qty = max_qty
            if max_qty > 0:
                desired_qty = min(desired_qty, max_qty) if desired_qty > 0 else max_qty
            desired_qty = max(1, int(desired_qty))

            gid = uuid.uuid4().hex[:10]
            self._pcs_combo_orders[gid] = PCSComboOrderState(
                group_id=gid,
                short_put=sp_k,
                long_put=lp_k,
                qty_total=int(desired_qty),
                qty_filled=0,
                limit_credit=float(start_credit),
                tick_credit=float(tick),
                budget_usd=float(budget),
                budget_credit_mult=float(budget_mult),
                last_mid=float(combo_mid),
                last_reprice_ts=now_ts(),
                created_ts=now_ts(),
                stale_since_ts=now_ts(),
            )
            self._log.info(f"PCS BAG submit {gid}: qty={desired_qty} credit={start_credit:.2f} bid={combo_bid:.2f} ask={combo_ask:.2f}", key="pcs.bag.submit")

            # Tail leg order remains a normal single-leg order (BUY).
            if tp_k is not None and tp_qty != 0:
                oq = self._find_option_quote(market, tp_k)
                bid = float(oq.quote.bid) if oq is not None else 0.0
                ask = float(oq.quote.ask) if oq is not None else 0.0
                px0 = choose_working_price(bid, ask)
                self._place_working_order(tp_k, Side.BUY, int(tp_qty), px0 if px0 > 0.0 else None)

            # Initialize PCS management state based on the intent legs (spread+tail keys).
            self._init_pcs_group_from_intent(intent)
            return

        # Place orders
        for (key, side, qty, _limit_cap) in intent.legs:
            oq = self._find_option_quote(market, key)
            bid = float(oq.quote.bid) if oq is not None else 0.0
            ask = float(oq.quote.ask) if oq is not None else 0.0
            px0 = choose_working_price(bid, ask)

            # cap logic (very conservative): if price * qty > cap, skip
            cap = cap_call if key.right == OptionRight.CALL else cap_put
            if cap > 0.0 and px0 > 0.0 and (px0 * abs(int(qty)) * 100.0) > cap:
                self._log.warn(f"Skipped {key.display()} due to cap. px={px0:.2f} cap={cap:.2f}", key="exec.cap")
                continue

            limit = px0 if px0 > 0.0 else None
            self._place_working_order(key, side, int(qty), limit)

        # Initialize synthetic stops for STRADDLE legs using config + intent meta
        if intent.strategy == StrategyType.STRADDLE:
            self._init_straddle_stops_from_config()

        # Initialize PCS group stop/tail rules (best-effort)
        if intent.strategy == StrategyType.PCS_TAIL:
            self._init_pcs_group_from_intent(intent)

    def _init_straddle_stops_from_config(self) -> None:
        cfg = self._shared.get_config()
        snap = self._shared.snapshot()
        market = snap.market
        if market is None:
            return

        for key, pos in snap.positions.items():
            # Only attach stops to option positions (CALL/PUT) for underlying symbol
            if key.symbol != cfg.general.underlying_symbol:
                continue
            if key.right not in (OptionRight.CALL, OptionRight.PUT):
                continue
            if abs(int(pos.qty)) == 0:
                continue
            # Attach if not present
            if key in self._stops:
                continue

            entry = float(pos.avg_price or 0.0)
            if entry <= 0.0:
                entry = self._mark_price(market, key)

            direction = 1 if int(pos.qty) > 0 else -1
            st = LegStopState(
                key=key,
                direction=direction,
                entry_price=max(0.0001, entry),
                soft_stop_pct=float(cfg.straddle.soft_stop_pct),
                hard_stop_pct=float(cfg.straddle.hard_stop_pct),
                ratchet_ladder=list(cfg.straddle.ratchet_ladder),
                hang_seconds=int(cfg.straddle.hang_seconds),
                current_stop_lock=-999.0,
            )
            self._stops[key] = st
            self._log.info(f"Stop armed for {key.display()} entry={entry:.2f}")

    def _init_pcs_group_from_intent(self, intent: EntryIntent) -> None:
        cfg = self._shared.get_config()
        # Expect legs: SELL short put, BUY long put, BUY tail put
        short_put: Optional[ContractKey] = None
        long_put: Optional[ContractKey] = None
        tail_put: Optional[ContractKey] = None
        for (k, s, _q, _cap) in intent.legs:
            if k.right != OptionRight.PUT:
                continue
            if s == Side.SELL and short_put is None:
                short_put = k
            elif s == Side.BUY and long_put is None:
                long_put = k
            elif s == Side.BUY:
                tail_put = k

        if short_put is None or long_put is None or tail_put is None:
            self._log.warn("PCS group init skipped (legs not detected).", key="pcs.init.skip")
            return

        gid = uuid.uuid4().hex[:10]
        gs = PCSGroupState(
            group_id=gid,
            short_put=short_put,
            long_put=long_put,
            tail_put=tail_put,
            tail_hang_seconds=int(cfg.pcs_tail.tail_hang_seconds),
            tail_ratchet_threshold_frac=float(cfg.pcs_tail.tail_ratchet_threshold_frac),
            tail_ratchet_lock_frac=float(cfg.pcs_tail.tail_ratchet_lock_frac),
        )
        # entry credit/stop will be computed once we have marks
        self._pcs_groups[gid] = gs
        self._log.info(f"PCS group armed {gid}: short={short_put.display()} long={long_put.display()} tail={tail_put.display()}")

    def _handle_exit_intent(self, intent: ExitIntent) -> None:
        snap = self._shared.snapshot()
        keys = list(intent.keys)
        if not keys:
            keys = list(snap.positions.keys())
        self._submit_flatten_for_keys(keys, reason=intent.note or "ExitIntent")

    def _handle_flatten(self, reason: str) -> None:
        # ignores go
        snap = self._shared.snapshot()
        self._cancel_all_orders(reason=f"flatten: {reason}")
        self._submit_flatten_for_keys(list(snap.positions.keys()), reason=reason)

    def _submit_flatten_for_keys(self, keys: List[ContractKey], reason: str) -> None:
        cfg = self._shared.get_config()
        snap = self._shared.snapshot()
        market = snap.market

        for key in keys:
            pos = snap.positions.get(key)
            if pos is None or int(pos.qty) == 0:
                continue
            qty = abs(int(pos.qty))
            side = Side.SELL if int(pos.qty) > 0 else Side.BUY

            oq = self._find_option_quote(market, key)
            bid = float(oq.quote.bid) if oq is not None else 0.0
            ask = float(oq.quote.ask) if oq is not None else 0.0
            mid_tick = choose_working_price(bid, ask)
            # For flatten we are more aggressive: BUY uses ask, SELL uses bid when available
            if side == Side.BUY:
                px = ask if ask > 0.0 else mid_tick
            else:
                px = bid if bid > 0.0 else mid_tick

            limit = px if px > 0.0 else None
            self._place_working_order(key, side, qty, limit)

        self._log.warn(f"Flatten submitted: {reason}", key="exec.flatten")

        # Clear stop/group state for flattened keys
        for k in keys:
            self._stops.pop(k, None)
        # If PCS group legs are flattened, drop group
        self._garbage_collect_pcs_groups()

    def _garbage_collect_pcs_groups(self) -> None:
        snap = self._shared.snapshot()
        keep: Dict[str, PCSGroupState] = {}
        for gid, g in self._pcs_groups.items():
            # keep if any of the keys still has a position
            if (g.short_put in snap.positions and snap.positions[g.short_put].qty != 0) or \
               (g.long_put in snap.positions and snap.positions[g.long_put].qty != 0) or \
               (g.tail_put in snap.positions and snap.positions[g.tail_put].qty != 0):
                keep[gid] = g
        self._pcs_groups = keep

    # -------------------------
    # Management loops
    # -------------------------

    def _manage_partial_fills(self, cfg: RuntimeConfig) -> None:
        """
        Partial fill trim logic (as specified): best-effort implementation.
        - Only acts on STRADDLE entries, driven by config.
        - If after wait_s, fill_ratio < min_fill_ratio -> cancel remainder.
        """
        if not cfg.straddle.partial_trim_enabled:
            return
        wait_s = float(cfg.straddle.partial_trim_wait_s)
        min_ratio = float(cfg.straddle.partial_trim_min_fill_ratio)

        now = now_ts()
        snap = self._shared.snapshot()
        for oid, fs in list(self._fills.items()):
            wo = snap.working_orders.get(oid)
            if wo is None:
                continue
            # Only trim buy orders (entry legs) and only after wait
            if wo.side != Side.BUY:
                continue
            if (now - fs.created_ts) < wait_s:
                continue
            ratio = fs.fill_ratio()
            if ratio < min_ratio:
                self._cancel_working_order(oid, reason=f"partial_trim ratio={ratio:.2f} < {min_ratio:.2f}")

    def _manage_straddle_stops(self, cfg: RuntimeConfig, market: Optional[MarketSnapshot]) -> None:
        """
        Synthetic stop management for legs in self._stops.
        - Computes profit_pct in "directional" terms:
            profit_pct = ((mark - entry)/entry) * direction
          where direction = +1 long, -1 short.
        - Applies soft/hard stops + ratchet + hanging stop.
        """
        if market is None:
            return

        snap = self._shared.snapshot()
        for key, st in list(self._stops.items()):
            pos = snap.positions.get(key)
            if pos is None or int(pos.qty) == 0:
                self._stops.pop(key, None)
                continue

            mark = self._mark_price(market, key)
            if mark <= 0.0:
                continue

            entry = max(0.0001, float(st.entry_price))
            profit_pct = ((mark - entry) / entry) * float(st.direction)

            st.update_from_profit(profit_pct)
            should_exit, reason = st.stop_should_exit(profit_pct)
            if should_exit:
                self._log.warn(f"Stop EXIT {key.display()} {reason}", key="stop.exit")
                self._submit_flatten_for_keys([key], reason=f"Stop:{reason}")


    def _manage_pcs_combo_orders(self, cfg: RuntimeConfig, market: Optional[MarketSnapshot]) -> None:
        """Manage PCS combo (BAG) spread entry orders with credit repricing and budget-capped quantity."""
        if market is None:
            return

        now = now_ts()
        for gid, st in list(self._pcs_combo_orders.items()):
            if st.done:
                continue
            if st.qty_remaining <= 0:
                st.done = True
                continue

            combo_bid, combo_ask, combo_mid = self._combo_bid_ask_mid_credit(market, st.short_put, st.long_put)
            if combo_bid <= 0.0 and combo_ask <= 0.0 and combo_mid <= 0.0:
                continue

            # If mid moved, re-anchor credit at mid (bounded by bid/ask).
            mid = combo_mid if combo_mid > 0.0 else combo_bid
            if combo_ask > 0.0:
                mid = min(mid, combo_ask)
            mid = max(mid, combo_bid)

            mid_changed = (st.last_mid <= 0.0) or (abs(mid - st.last_mid) >= 1e-9)
            if mid_changed:
                st.last_mid = float(mid)
                st.stale_since_ts = now

            # Repricing cadence: if mid is stale beyond cfg.straddle.entry_price_stale_s, improve odds by lowering credit.
            stale_s = float(cfg.straddle.entry_price_stale_s)
            can_reprice = (now - st.last_reprice_ts) >= max(0.10, stale_s)

            new_credit = float(st.limit_credit)
            if can_reprice:
                # Lower credit by one tick, but never below combo_bid.
                new_credit = max(float(combo_bid), float(st.limit_credit) - float(st.tick_credit))
                st.last_reprice_ts = now

            # Always keep credit within observed [bid, ask] bounds when ask exists.
            if combo_ask > 0.0:
                new_credit = min(new_credit, float(combo_ask))
            new_credit = max(new_credit, float(combo_bid))

            # Budget cap applies to remaining qty using working limit credit.
            max_qty = st.qty_remaining
            if st.budget_usd > 0.0 and new_credit > 0.0 and st.budget_credit_mult > 0.0:
                allowed = int(max(0.0, math.floor(st.budget_usd / (new_credit * 100.0 * st.budget_credit_mult))))
                if allowed > 0:
                    max_qty = min(max_qty, allowed)
                else:
                    max_qty = 0

            if max_qty <= 0:
                self._log.warn(f"PCS BAG {gid}: budget constraint prevents any qty at credit={new_credit:.2f}", key="pcs.bag.budget.zero")
                continue

            # If allowed qty differs from remaining qty, adjust total qty accordingly (can move up/down).
            target_total = int(st.qty_filled) + int(max_qty)
            if target_total != st.qty_total:
                st.qty_total = int(target_total)
                self._log.info(f"PCS BAG {gid}: qty_adj total={st.qty_total} filled={st.qty_filled} credit={new_credit:.2f}", key="pcs.bag.qty")

            if abs(new_credit - st.limit_credit) >= 1e-9:
                st.limit_credit = float(new_credit)
                self._log.info(f"PCS BAG {gid}: credit_adj={st.limit_credit:.2f} bid={combo_bid:.2f} ask={combo_ask:.2f}", key="pcs.bag.credit")

            # SIM fills: fill immediately when combo_bid >= limit_credit (credit SELL).
            if cfg.general.run_mode == RunMode.SIMULATED:
                if combo_bid >= st.limit_credit:
                    fill_qty = st.qty_remaining
                    if fill_qty <= 0:
                        continue
                    # Apply atomic fills to both legs (short SELL, long BUY).
                    self._apply_simulated_fill(st.short_put, Side.SELL, fill_qty, st.limit_credit)
                    self._apply_simulated_fill(st.long_put, Side.BUY, fill_qty, 0.0)
                    st.qty_filled += int(fill_qty)
                    st.done = True
                    self._log.info(f"PCS BAG FILLED {gid}: qty={fill_qty} credit={st.limit_credit:.2f}", key="pcs.bag.filled")

    def _manage_pcs_groups(self, cfg: RuntimeConfig, market: Optional[MarketSnapshot]) -> None:
        """
        PCS stop management:
        - entry_credit computed as (short_put_mark - long_put_mark) at first opportunity.
        - current_spread_debit = (short_put_mark - long_put_mark) (could be negative if inverted; clamp).
        - Stop if current_spread_debit >= entry_credit * pcs_stop_mult (debit expansion vs credit).
        Tail:
        - activate when spread value <= entry_credit * (1 - tail_activation_credit_frac)
          (i.e., captured >= tail_activation_credit_frac of credit).
        - once active, apply tail ratchet lock based on tail mark profit relative to entry tail price:
            threshold = entry_tail_price * tail_ratchet_threshold_frac
            lock = entry_tail_price * tail_ratchet_lock_frac (as "profit lock" in profit_pct space)
          Implemented as stop lock in profit_pct:
            stop_lock_pct = (lock - entry_tail)/entry_tail = (tail_ratchet_lock_frac - 1)
          This is simplistic but stable.
        """
        if market is None:
            return

        snap = self._shared.snapshot()
        for gid, g in list(self._pcs_groups.items()):
            # Require positions exist
            sp = snap.positions.get(g.short_put)
            lp = snap.positions.get(g.long_put)
            tp = snap.positions.get(g.tail_put)
            if sp is None or lp is None or tp is None:
                continue
            if int(sp.qty) == 0 and int(lp.qty) == 0 and int(tp.qty) == 0:
                continue

            short_mark = self._mark_price(market, g.short_put)
            long_mark = self._mark_price(market, g.long_put)
            tail_mark = self._mark_price(market, g.tail_put)

            if short_mark <= 0.0 or long_mark <= 0.0:
                continue

            spread_val = short_mark - long_mark
            # For a credit spread, entry_credit should be positive; spread_val also positive as a "credit price".
            # In live, spread value is usually quoted as a debit to close; we keep consistent here.
            spread_val = max(0.0, float(spread_val))

            if g.entry_credit <= 0.0:
                g.entry_credit = spread_val
                g.pcs_stop_debit = g.entry_credit * float(cfg.pcs_tail.pcs_stop_mult)
                self._log.info(f"PCS {gid} entry_credit={g.entry_credit:.2f} stop_debit={g.pcs_stop_debit:.2f}")

            # PCS stop (debit expansion)
            if g.entry_credit > 0.0 and spread_val >= g.pcs_stop_debit:
                self._log.warn(f"PCS STOP {gid}: spread={spread_val:.2f} >= {g.pcs_stop_debit:.2f}", key="pcs.stop")
                self._submit_flatten_for_keys([g.short_put, g.long_put], reason="PCS stop")
                # tail stays (can remain as hedge), do not force flatten tail here
                continue

            # Tail activation based on captured credit fraction
            # captured = (entry_credit - current_spread) / entry_credit
            captured = 0.0
            if g.entry_credit > 0.0:
                captured = (g.entry_credit - spread_val) / g.entry_credit

            if (not g.tail_activated) and captured >= float(cfg.pcs_tail.tail_activation_credit_frac):
                g.tail_activated = True
                self._log.info(f"Tail ACTIVATED {gid}: captured={captured:.2f}", key="tail.activated")

            # Tail ratchet + hanging stop once activated
            if g.tail_activated and tail_mark > 0.0 and tp is not None and int(tp.qty) != 0:
                entry_tail = float(tp.avg_price or 0.0)
                if entry_tail <= 0.0:
                    entry_tail = tail_mark

                # "threshold=100% lock=50%" (defaults):
                # When tail profit_pct >= threshold_frac (i.e., +100%), set stop lock to +50%.
                direction = 1 if int(tp.qty) > 0 else -1
                profit_pct = ((tail_mark - entry_tail) / max(0.0001, entry_tail)) * float(direction)

                # Threshold is interpreted as profit_pct >= threshold_frac
                if profit_pct >= float(cfg.pcs_tail.tail_ratchet_threshold_frac):
                    g.tail_stop_lock = max(g.tail_stop_lock, float(cfg.pcs_tail.tail_ratchet_lock_frac))

                # Apply hanging stop on tail lock (profit_pct <= lock_frac)
                # lock_frac in config is "50%" meaning profit_pct lock at +50%
                active_lock = float(g.tail_stop_lock)
                if active_lock > -998.0 and profit_pct <= active_lock:
                    if g.tail_hang_seconds <= 0:
                        self._log.warn(f"Tail EXIT {gid} (no hang) profit={profit_pct:.2f}<=lock={active_lock:.2f}", key="tail.exit")
                        self._submit_flatten_for_keys([g.tail_put], reason="Tail ratchet stop")
                    else:
                        if g.tail_hang_until_ts <= 0.0:
                            g.tail_hang_until_ts = now_ts() + float(g.tail_hang_seconds)
                            self._log.warn(f"Tail hang start {gid}", key="tail.hang.start")
                        elif now_ts() >= g.tail_hang_until_ts:
                            self._log.warn(f"Tail EXIT {gid} after hang", key="tail.exit")
                            self._submit_flatten_for_keys([g.tail_put], reason="Tail ratchet stop (hang)")
                else:
                    g.tail_hang_until_ts = 0.0

        self._garbage_collect_pcs_groups()

    # -------------------------
    # Thread loop
    # -------------------------

    def run(self) -> None:
        self._log.info("ExecutionEngine starting...", key="exec.start")
        while not self._stop_evt.is_set():
            cfg = self._shared.get_config()
            snap = self._shared.snapshot()
            market = snap.market

            # consume intents (drain quickly)
            for _ in range(50):
                intent = self._bus.try_get()
                if intent is None:
                    break
                self._dispatch_intent(intent)

            # simulated fills
            for oid, fs in list(self._fills.items()):
                self._try_simulated_fill(cfg, market, fs)

            # partial fill trim (mostly LIVE-relevant)
            self._manage_partial_fills(cfg)

            # pcs combo (BAG) spread entry management + SIM fills
            self._manage_pcs_combo_orders(cfg, market)

            # management (exits/ratchets are not blocked by go)
            self._manage_straddle_stops(cfg, market)
            self._manage_pcs_groups(cfg, market)

            # shutdown handling
            if self._shutdown_requested:
                # keep trying until positions/orders are cleared
                self._handle_flatten("shutdown requested")
                # if no positions and no orders, stop thread
                snap2 = self._shared.snapshot()
                if not snap2.working_orders and all(int(p.qty) == 0 for p in snap2.positions.values()):
                    self._log.warn("Shutdown flatten complete; stopping engine.", key="exec.shutdown.done")
                    break

            time.sleep(self._poll_s)

        self._log.info("ExecutionEngine stopped.", key="exec.stop")

    def _dispatch_intent(self, intent: Intent) -> None:
        if intent.type == IntentType.ENTRY and isinstance(intent, EntryIntent):
            self._handle_entry_intent(intent)
            return
        if intent.type == IntentType.EXIT and isinstance(intent, ExitIntent):
            self._handle_exit_intent(intent)
            return
        if intent.type == IntentType.FLATTEN and isinstance(intent, FlattenIntent):
            self._handle_flatten(intent.note or "FlattenIntent")
            return
        if intent.type == IntentType.SHUTDOWN_FLATTEN and isinstance(intent, ShutdownFlattenIntent):
            # shutdown flatten ignores go
            self._shutdown_requested = True
            self._log.warn("Shutdown flatten requested.", key="exec.shutdown.req")
            return
        if intent.type == IntentType.CANCEL_ALL:
            self._cancel_all_orders(reason=intent.note or "CancelAll")
            return
        if intent.type == IntentType.CLEAR_LOCKS:
            self._shared.locks_clear_all()
            self._log.info("Locks cleared.", key="exec.locks.cleared")
            return
        if intent.type == IntentType.UPDATE_CONFIG:
            # config updates should be applied by GUI layer; no-op here for safety
            self._log.warn("UpdateConfigIntent received in execution; ignored.", key="exec.cfg.ignored")
            return

        self._log.debug(f"Unknown/ignored intent {intent.type}", key="exec.intent.unknown")


# =========================
# Internal helpers (module-level)
# =========================

def fill_key_limit_ok(side: Side, fill_px: float, wo: Optional[WorkingOrder]) -> bool:
    if wo is None:
        return True
    lim = wo.limit_price
    if lim is None or lim <= 0.0:
        return True
    if side == Side.BUY:
        return float(fill_px) <= float(lim) + 1e-9
    return float(fill_px) >= float(lim) - 1e-9
