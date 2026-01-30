"""
ibkr_md.py

Market data hub and IBKR market data adapter.

Design goals:
- Non-blocking, tick-based publisher that owns all market data retrieval.
- Publishes immutable MarketSnapshot objects into SharedState as the single
  source of truth for all consumers (planners, triggers, execution, GUI).
- Avoids hard dependencies on third-party IBKR libraries so the program
  compiles in a clean Python 3.10.4 environment. If an IBKR client library is
  available at runtime, it can be wired in via the optional adapter.

This module implements a robust fallback "stub" provider that produces:
- SPY last, HOD/LOD, and 1-minute bars with a running VWAP estimate
- Synthetic SPX option quotes/deltas for any requested OptionKey instances
  (planned + active only)
- A heartbeat timestamp on every publish tick

Python: 3.10.4
"""

from __future__ import annotations

import math
import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from core import (
    AccountMode,
    Bar1m,
    MarketSnapshot,
    OptionKey,
    OptionQuote,
    PCSTailPlan,
    RunMode,
    SharedState,
    StraddlePlan,
    now_ts,
)


# =============================================================================
# SECTION: Optional IBKR API dependency (ib_insync)
# =============================================================================
# The program can run without ib_insync installed (GUI + state machines + SIM).
# When ib_insync is available and TWS/IB Gateway is running, MarketDataHub will
# pull real SPY/SPX market data. Otherwise, it will publish heartbeat-only
# snapshots with no changing prices (unless stub market data is enabled).

try:
    from ib_insync import IB, Stock, Option, util  # type: ignore
    _HAVE_IB_INSYNC = True
except Exception:
    IB = None  # type: ignore
    Stock = None  # type: ignore
    Option = None  # type: ignore
    util = None  # type: ignore
    _HAVE_IB_INSYNC = False

# =============================================================================
# SECTION: Shutdown helpers
# =============================================================================

def _should_stop(shared: "SharedState") -> bool:
    """Return True when any shutdown signal is active."""
    try:
        with shared.lock:
            if getattr(shared.gui, "shutdown_pressed", False):
                return True
    except Exception:
        pass
    ev = getattr(shared, "shutdown_event", None)
    if isinstance(ev, threading.Event) and ev.is_set():
        return True
    return False



# =============================================================================
# SECTION: Optional IBKR adapter import
# =============================================================================
# This project intentionally avoids a hard dependency on a specific IBKR library
# (ibapi / ib_insync). If a compatible adapter is present in the environment,
# it may be used by replacing StubMarketDataProvider with a real provider.
#
# The remainder of the system is designed to operate even without a live IBKR
# connection (SIM / REPLAY / disconnected GUI mode).
# =============================================================================


# =============================================================================
# SECTION: Provider interface
# =============================================================================

class MarketDataProvider:
    """
    Abstract market data provider used by MarketDataHub.

    A provider is responsible for producing:
    - SPY underlying last price (and optionally intraday bar updates)
    - Option quotes for requested OptionKey contracts

    The hub remains the sole publisher into SharedState.
    """

    def connect(self, shared: SharedState) -> None:
        """Connect and initialize provider. Must be non-blocking or quick."""
        raise NotImplementedError

    def disconnect(self) -> None:
        """Disconnect provider."""
        raise NotImplementedError

    def is_connected(self) -> bool:
        """Return True if provider considers itself connected."""
        raise NotImplementedError

    def update_requested_options(self, keys: Set[OptionKey]) -> None:
        """Inform provider which option contracts are needed (planned + active)."""
        raise NotImplementedError

    def poll_underlying_spy_last(self) -> Optional[float]:
        """Return latest SPY last price, or None if unavailable."""
        raise NotImplementedError

    def poll_option_quotes(self) -> Dict[OptionKey, OptionQuote]:
        """Return latest option quotes for requested keys (may be partial)."""
        raise NotImplementedError


# =============================================================================
# SECTION: Stub provider (no external dependencies)
# =============================================================================

@dataclass
class _StubBarBuilder:
    """
    Stateful 1-minute bar builder.

    The hub owns the bar series; this helper only tracks the in-progress minute.
    """
    minute_start_ts: float
    open: float
    high: float
    low: float
    close: float
    pseudo_volume: float



# =============================================================================
# SECTION: IBKR Market Data Provider (ib_insync)
# =============================================================================

class IBInsyncMarketDataProvider(MarketDataProvider):
    """Real IBKR market data provider using ib_insync.

    Behavior:
    - Connects to 127.0.0.1 and selects port based on selected account mode:
      PAPER -> 7497, LIVE -> 7496.
    - clientId is randomized per connect attempt (to avoid collisions).
    - Subscribes to:
      * SPY underlying tick data
      * SPY 1-minute bars since open (best-effort, refreshed periodically)
      * SPX option quotes for requested OptionKey set

    Safety:
    - If ib_insync is not installed, this provider will never connect.
    - If TWS/IB Gateway is not running, connect attempts will fail and be handled
      by MarketDataHub connection state machine/backoff.
    """

    def __init__(self) -> None:
        self._ib = IB() if _HAVE_IB_INSYNC and IB is not None else None
        self._spy_contract = None
        self._spy_ticker = None
        self._bars_last_refresh_ts: float = 0.0
        self._bars_cache: Tuple[Bar1m, ...] = tuple()
        self._requested: Set[OptionKey] = set()
        self._opt_tickers: Dict[OptionKey, Any] = {}
        self._connected_host: str = "127.0.0.1"
        self._connected_port: int = 0

    def connect(self, shared: SharedState) -> None:
        if not _HAVE_IB_INSYNC or self._ib is None:
            raise RuntimeError("ib_insync not available")

        with shared.lock:
            mode = shared.config.account.mode
        port = 7497 if mode == AccountMode.PAPER else 7496
        client_id = random.randint(1000, 9999)

        # ib.connect() raises if it cannot connect.
        self._ib.connect(self._connected_host, port, clientId=client_id, readonly=True, timeout=2.0)
        self._connected_port = port

        # Subscribe SPY once.
        if self._spy_contract is None:
            self._spy_contract = Stock("SPY", "SMART", "USD")
        if self._spy_ticker is None:
            self._spy_ticker = self._ib.reqMktData(self._spy_contract, "", False, False)

        # Prime option subscriptions for any already-requested keys.
        if self._requested:
            self._subscribe_options(self._requested)

        # Expose IB object for other components (execution) without hard coupling.
        try:
            setattr(shared, "ib", self._ib)
        except Exception:
            pass

    def disconnect(self) -> None:
        if self._ib is None:
            return
        try:
            if self._spy_ticker is not None and self._spy_contract is not None:
                self._ib.cancelMktData(self._spy_contract)
        except Exception:
            pass
        try:
            for k, tkr in list(self._opt_tickers.items()):
                try:
                    c = tkr.contract
                    self._ib.cancelMktData(c)
                except Exception:
                    pass
            self._opt_tickers.clear()
        except Exception:
            pass
        try:
            self._ib.disconnect()
        except Exception:
            pass

    def is_connected(self) -> bool:
        if self._ib is None:
            return False
        try:
            return bool(self._ib.isConnected())
        except Exception:
            return False

    def update_requested_options(self, keys: Set[OptionKey]) -> None:
        self._requested = set(keys)
        if not self.is_connected() or self._ib is None:
            return
        self._subscribe_options(keys)

    def _subscribe_options(self, keys: Set[OptionKey]) -> None:
        if self._ib is None:
            return
        for k in keys:
            if k in self._opt_tickers:
                continue
            c = Option("SPX", k.expiry, float(k.strike), k.right, "SMART", tradingClass="SPX", multiplier="100")
            tkr = self._ib.reqMktData(c, "", False, False)
            self._opt_tickers[k] = tkr

    def poll_underlying_spy_last(self) -> Optional[float]:
        if self._spy_ticker is None:
            return None
        try:
            # marketPrice() falls back to last/bid/ask.
            p = float(self._spy_ticker.marketPrice())
            return p if p > 0 else None
        except Exception:
            return None

    def poll_bars_1m_since_open(self) -> Tuple[Bar1m, ...]:
        if self._ib is None or self._spy_contract is None or not self.is_connected():
            return tuple()

        now = now_ts()
        # Refresh bars at most every 15 seconds.
        if (now - self._bars_last_refresh_ts) < 15.0 and self._bars_cache:
            return self._bars_cache

        try:
            bars = self._ib.reqHistoricalData(
                self._spy_contract,
                endDateTime="",
                durationStr="1 D",
                barSizeSetting="1 min",
                whatToShow="TRADES",
                useRTH=True,
                formatDate=1,
                keepUpToDate=False,
            )
        except Exception:
            return self._bars_cache

        out: List[Bar1m] = []
        try:
            # ib_insync bars have .date, .open, .high, .low, .close, .average (vwap-ish)
            for b in bars:
                ts = float(getattr(b, "date", 0.0))
                if ts == 0.0:
                    # attempt parse datetime
                    dt = getattr(b, "date", None)
                    try:
                        ts = dt.timestamp() if dt is not None else 0.0
                    except Exception:
                        ts = 0.0
                vw = float(getattr(b, "average", getattr(b, "wap", 0.0)) or 0.0)
                out.append(Bar1m(ts=ts, open=float(b.open), high=float(b.high), low=float(b.low), close=float(b.close), vwap=vw))
        except Exception:
            return self._bars_cache

        self._bars_cache = tuple(out)
        self._bars_last_refresh_ts = now
        return self._bars_cache

    def poll_option_quotes(self) -> Dict[OptionKey, OptionQuote]:
        if self._ib is None or not self.is_connected():
            return {}

        out: Dict[OptionKey, OptionQuote] = {}
        now = now_ts()
        for k, tkr in list(self._opt_tickers.items()):
            try:
                bid = float(tkr.bid) if tkr.bid is not None else None
                ask = float(tkr.ask) if tkr.ask is not None else None
                last = float(tkr.last) if tkr.last is not None else None
                mid = None
                if bid is not None and ask is not None and bid > 0 and ask > 0:
                    mid = (bid + ask) / 2.0
                delta = None
                mg = getattr(tkr, "modelGreeks", None)
                if mg is not None:
                    d = getattr(mg, "delta", None)
                    if isinstance(d, (int, float)):
                        delta = float(d)
                out[k] = OptionQuote(
                    key=k,
                    bid=bid,
                    ask=ask,
                    last=last,
                    mid=mid,
                    delta=delta,
                    quote_ts=now,
                )
            except Exception:
                continue
        return out


class StubMarketDataProvider(MarketDataProvider):
    """
    Dependency-free fallback provider.

    Notes:
    - Uses a deterministic pseudo-random walk for SPY last.
    - Generates synthetic option quotes for SPX options based on strike distance.
    - Intended for GUI bring-up, SIM testing, and as a compile-safe default.

    The simulation is intentionally simple but stable:
    - SPY price evolves smoothly.
    - Options have a mid price derived from distance to strike and time-to-expiry
      heuristic; bid/ask built around mid using a conservative spread.
    - Delta is approximated via a logistic moneyness curve.
    """

    def __init__(self) -> None:
        self._connected = False
        self._rng = random.Random(7)  # deterministic across runs
        self._spy_last = 500.0  # reasonable modern SPY neighborhood
        self._requested: Set[OptionKey] = set()
        self._last_quote_ts = 0.0

    def connect(self, shared: SharedState) -> None:
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def update_requested_options(self, keys: Set[OptionKey]) -> None:
        self._requested = set(keys)

    def poll_underlying_spy_last(self) -> Optional[float]:
        if not self._connected:
            return None
        # Smooth drift + bounded noise.
        noise = (self._rng.random() - 0.5) * 0.15
        drift = 0.01 * math.sin(time.time() / 60.0)
        self._spy_last = max(1.0, self._spy_last * (1.0 + (drift + noise) / 100.0))
        return self._spy_last

    def poll_option_quotes(self) -> Dict[OptionKey, OptionQuote]:
        if not self._connected:
            return {}
        qts = now_ts()
        self._last_quote_ts = qts
        out: Dict[OptionKey, OptionQuote] = {}
        # Synthetic SPX underlying assumed near 5000 with mild coupling to SPY.
        spx_underlying = 10.0 * self._spy_last
        for key in self._requested:
            out[key] = self._synth_option_quote(key, spx_underlying, qts)
        return out

    def _synth_option_quote(self, key: OptionKey, underlying: float, qts: float) -> OptionQuote:
        # Heuristic time value: more for longer-dated expiries.
        # Expiry format YYYYMMDD; tolerate errors.
        t_days = 1.0
        try:
            y = int(key.expiry[0:4])
            m = int(key.expiry[4:6])
            d = int(key.expiry[6:8])
            expiry_ts = time.mktime((y, m, d, 16, 0, 0, 0, 0, -1))
            t_days = max(0.25, (expiry_ts - qts) / 86400.0)
        except Exception:
            t_days = 1.0

        # Moneyness and intrinsic value.
        k = float(key.strike)
        is_call = key.right.upper() == "C"
        intrinsic = max(0.0, underlying - k) if is_call else max(0.0, k - underlying)

        # Simple time value model.
        # Wider for further OTM and longer time to expiry.
        moneyness = (underlying - k) / max(1.0, underlying)
        time_value = (0.18 * math.sqrt(t_days)) * underlying * math.exp(-abs(moneyness) * 10.0) * 0.01
        mid = max(0.05, intrinsic + time_value)

        # Spread model: small absolute spread, larger for low-priced options.
        spread = max(0.05, min(2.0, mid * 0.02))
        bid = max(0.0, mid - spread / 2.0)
        ask = mid + spread / 2.0

        # Delta approximation: logistic curve on moneyness.
        # Calls: 0..1, Puts: -1..0
        s = 12.0  # steepness
        if is_call:
            delta = 1.0 / (1.0 + math.exp(-s * moneyness))
        else:
            delta = -(1.0 / (1.0 + math.exp(s * moneyness)))

        return OptionQuote(
            key=key,
            bid=round(bid, 2),
            ask=round(ask, 2),
            last=None,
            mid=round(mid, 2),
            delta=float(round(delta, 3)),
            quote_ts=qts,
        )


# =============================================================================
# SECTION: MarketDataHub (single owner/publisher)
# =============================================================================


# =============================================================================
# SECTION: Null provider (no data when IBKR/TWS is unavailable)
# =============================================================================

class NullMarketDataProvider(MarketDataProvider):
    """Market data provider that never returns prices or quotes.

    Use this provider when IBKR/TWS is not connected and stub market data has not
    been explicitly enabled. The hub will still publish heartbeats so the GUI
    can verify liveness, but all market fields remain None/empty.
    """

    def __init__(self) -> None:
        self._requested: Set[OptionKey] = set()

    def connect(self, shared: SharedState) -> None:
        return

    def disconnect(self) -> None:
        return

    def is_connected(self) -> bool:
        return False

    def update_requested_options(self, keys: Set[OptionKey]) -> None:
        self._requested = set(keys)

    def poll_underlying_spy_last(self) -> Optional[float]:
        return None

    def poll_option_quotes(self) -> Dict[OptionKey, OptionQuote]:
        return {}


class MarketDataHub(threading.Thread):
    """
    MarketDataHub owns all market data polling and publishes MarketSnapshot.

    Responsibilities:
    - Enforce account connection rules (do not connect without account id).
    - Publish immutable snapshots at a fixed tick rate (no long blocking).
    - Maintain SPY 1-minute bars since open + running VWAP/HOD/LOD.
    - Track and publish option quotes for planned + active contracts only.
    - Update SharedState.ibkr_connected to reflect provider connection.

    Threading:
    - This thread is a producer only. It never consumes intents or places orders.
    - It must be safe to start even when disconnected, so GUI can run.
    """

    def __init__(self, shared: SharedState, provider: Optional[MarketDataProvider] = None) -> None:
        super().__init__(daemon=True, name="MarketDataHub")
        self._shared = shared
        self._provider: MarketDataProvider = provider if provider is not None else self._default_provider()
        if (not _HAVE_IB_INSYNC) and (not isinstance(self._provider, StubMarketDataProvider)):
            self._shared.log_warn("MD: ib_insync is not installed; real IBKR market data disabled")
        # Track stub-market-data enablement and log mode changes only when toggled.
        with self._shared.lock:
            dbg = getattr(self._shared.config, "debug", None)
            self._last_stub_md_enabled = bool(getattr(dbg, "stub_market_data", False)) if dbg is not None else False

        # One-time startup log describing market data source.
        if isinstance(self._provider, StubMarketDataProvider):
            self._shared.log_warn("MD: Stub Market Data ENABLED (synthetic moving prices/quotes)")
        elif isinstance(self._provider, NullMarketDataProvider):
            self._shared.log_info("MD: Stub Market Data DISABLED (heartbeat only until IBKR/TWS connects)")

        self._connected: bool = False
        self._last_conn_state: bool = False
        self._last_conn_error: str = ""
        self._no_acct_id_episode: bool = False
        with self._shared.lock:
            setattr(self._shared, "ibkr_connected", False)
            setattr(self._shared, "ibkr_last_error", "")

        # Bar tracking
        self._bars: List[Bar1m] = []
        self._builder: Optional[_StubBarBuilder] = None
        self._hod: Optional[float] = None
        self._lod: Optional[float] = None
        self._vwap_running: Optional[float] = None
        self._vwap_pv: float = 0.0  # sum(price*vol)
        self._vwap_v: float = 0.0   # sum(vol)

        # Option quotes cache (latest known)
        self._option_quotes: Dict[OptionKey, OptionQuote] = {}

    def _maybe_update_provider(self, cfg: Any) -> None:
        """Swap between NullMarketDataProvider and StubMarketDataProvider when toggled.

        Logs only on change. Disabling stub while using StubMarketDataProvider immediately
        stops synthetic prices by switching to NullMarketDataProvider.
        """
        enabled = bool(getattr(getattr(cfg, "debug", None), "stub_market_data", False))
        if enabled == getattr(self, "_last_stub_md_enabled", None):
            return
        self._last_stub_md_enabled = enabled

        if enabled:
            if isinstance(self._provider, NullMarketDataProvider):
                self._shared.log_warn("MD: Stub Market Data ENABLED (synthetic moving prices/quotes)")
                self._provider = StubMarketDataProvider()
        else:
            if isinstance(self._provider, StubMarketDataProvider):
                self._shared.log_info("MD: Stub Market Data DISABLED (heartbeat only until IBKR/TWS connects)")
                try:
                    self._provider.disconnect()
                except Exception:
                    pass
                self._provider = NullMarketDataProvider()
                self._set_conn_state(False, "")

    def _stub_md_enabled(self, cfg: Any) -> bool:
        """Return True if stub/synthetic market data is enabled for this session.

        This flag is intentionally *non-persistent*; GUI should not save it to disk.
        """
        try:
            dbg = getattr(cfg, "debug", None)
            return bool(getattr(dbg, "stub_market_data", False))
        except Exception:
            return False

    def _default_provider(self) -> MarketDataProvider:
        """Select the safest default market data provider.

        Rules:
        - If debug.stub_market_data is True -> StubMarketDataProvider (synthetic moving prices/quotes)
        - Else if ib_insync is available -> IBInsyncMarketDataProvider (real IBKR market data)
        - Else -> NullMarketDataProvider (heartbeat only; no moving prices)
        """
        with self._shared.lock:
            cfg = self._shared.config
        if self._stub_md_enabled(cfg):
            return StubMarketDataProvider()
        if _HAVE_IB_INSYNC:
            return IBInsyncMarketDataProvider()
        return NullMarketDataProvider()


# -------------------------------------------------------------------------
    # SECTION: Public controls
    # -------------------------------------------------------------------------

    def request_stop(self) -> None:
        """Signal the hub to stop via SharedState.shutdown_event."""
        self._shared.shutdown_event.set()

    # -------------------------------------------------------------------------
    # SECTION: Main loop
    # -------------------------------------------------------------------------
    def run(self) -> None:
        """
        Publisher loop:
        - Evaluate desired connectivity from config + account rules.
        - Poll provider quickly; never block long.
        - Publish MarketSnapshot every tick.
        """
        self._shared.log_info("MD: Starting")
        while True:
            # Heartbeat for GUI diagnostics (updated every tick).
            with self._shared.lock:
                self._shared.md_heartbeat_ts = time.time()
            if _should_stop(self._shared):
                self._shared.log_info("MD: Stopped")
                return
            tick_start = time.monotonic()
            cfg, run_mode, acct_mode, acct_id, tick_s = self._snapshot_config()

            # Apply Debug toggle for stub market data without requiring restart.
            self._maybe_update_provider(cfg)

            # Connect/disconnect decision logging (rate-limited per episode).
            require_id = bool(getattr(cfg.account, "require_account_id_to_connect", True))
            if require_id and (acct_id or "").strip() == "":
                if not self._no_acct_id_episode:
                    self._no_acct_id_episode = True
                    self._shared.log_warn(
                        f"MD: Not connecting (account id missing) mode={acct_mode} run_mode={run_mode}"
                    )
            else:
                self._no_acct_id_episode = False

            # Connect/disconnect rules:
            should_connect = self._should_connect(cfg, run_mode, acct_mode, acct_id)
            self._apply_connectivity(should_connect)

            spy_last = self._provider.poll_underlying_spy_last()
            if spy_last is not None:
                self._update_bars_and_levels(spy_last)

            req_keys = self._collect_requested_option_keys()
            self._provider.update_requested_options(req_keys)

            quotes = self._provider.poll_option_quotes()
            if quotes:
                self._option_quotes.update(quotes)
            # Drop quotes for keys no longer requested to keep snapshot concise.
            self._option_quotes = {k: v for k, v in self._option_quotes.items() if k in req_keys}

            snapshot = MarketSnapshot(
                ts=now_ts(),
                hub_heartbeat_ts=now_ts(),
                spy_last=spy_last,
                spy_vwap=self._vwap_running,
                spy_hod=self._hod,
                spy_lod=self._lod,
                bars_1m=tuple(self._bars),
                option_quotes=dict(self._option_quotes),
            )
            self._publish(snapshot)

            # Tick pacing (non-blocking; short sleep only)
            elapsed = time.monotonic() - tick_start
            remaining = max(0.0, tick_s - elapsed)
            if remaining > 0:
                time.sleep(min(remaining, 0.05))

        # Best-effort disconnect
        try:
            self._provider.disconnect()
        except Exception:
            pass
        with self._shared.lock:
            self._shared.ibkr_connected = False

    # -------------------------------------------------------------------------
    # SECTION: Connectivity + config snapshot
    # -------------------------------------------------------------------------

    def _snapshot_config(self) -> Tuple:
        with self._shared.lock:
            cfg = self._shared.config
            run_mode = cfg.debug.run_mode
            acct_mode = cfg.account.mode
            acct_id = cfg.account.paper_account_id if acct_mode == AccountMode.PAPER else cfg.account.live_account_id
            tick_s = float(cfg.threads.md_hub_tick_s)
        return cfg, run_mode, acct_mode, acct_id, tick_s

    def _set_conn_state(self, connected: bool, error: str = "") -> None:
        """Persist connectivity state into SharedState (for GUI display)."""
        if connected == self._last_conn_state and error == self._last_conn_error:
            return
        self._last_conn_state = connected
        self._last_conn_error = error
        with self._shared.lock:
            setattr(self._shared, "ibkr_connected", connected)
            setattr(self._shared, "ibkr_last_error", error)

    def _should_connect(self, cfg, run_mode: RunMode, acct_mode: AccountMode, acct_id: str) -> bool:
        # REPLAY mode never connects.
        if run_mode == RunMode.REPLAY:
            return False
        if cfg.account.require_account_id_to_connect and (acct_id or "").strip() == "":
            return False
        return True

    def _apply_connectivity(self, should_connect: bool) -> None:
        """
        Apply desired connectivity state.

        This is called on every hub tick. It emits infrequent lifecycle events:
        - "MD: Provider connecting" once per connect attempt
        - "MD: Provider connected" on success
        - "IBKR: Connect failed" on failure (once per failure episode)
        - "MD: Provider disconnected" when disconnecting due to config/mode change
        """
        currently = self._provider.is_connected()

        if should_connect and not currently:
            # Connect attempt (log once per attempt).
            try:
                with self._shared.lock:
                    acct_mode = self._shared.config.account.mode
                    acct_id = (
                        self._shared.config.account.paper_account_id
                        if acct_mode == AccountMode.PAPER
                        else self._shared.config.account.live_account_id
                    )
                self._shared.log_info(
                    f"MD: Provider connecting (mode={acct_mode.value}, account_id={acct_id})"
                )
            except Exception:
                # If config cannot be read, still attempt connect; provider will handle errors.
                self._shared.log_info("MD: Provider connecting")

            try:
                self._provider.connect(self._shared)
                with self._shared.lock:
                    self._shared.ibkr_connected = True
                    self._shared.selected_account_mode = self._shared.config.account.mode
                    self._shared.selected_account_id = (
                        self._shared.config.account.paper_account_id
                        if self._shared.config.account.mode == AccountMode.PAPER
                        else self._shared.config.account.live_account_id
                    )
                self._shared.log_info("MD: Provider connected")
                self._set_conn_state(True, "")
            except Exception as e:
                with self._shared.lock:
                    self._shared.ibkr_connected = False
                self._shared.log_warn(f"IBKR: Connect failed: {e!r}")
                self._set_conn_state(False, str(e))

        elif (not should_connect) and currently:
            try:
                self._provider.disconnect()
            except Exception:
                pass
            with self._shared.lock:
                self._shared.ibkr_connected = False
            self._shared.log_info("MD: Provider disconnected (account id missing or mode changed)")


    # -------------------------------------------------------------------------
    # SECTION: Requested contract collection (plans + active trades)
    # -------------------------------------------------------------------------

    def _collect_requested_option_keys(self) -> Set[OptionKey]:
        keys: Set[OptionKey] = set()
        with self._shared.lock:
            sp: Optional[StraddlePlan] = self._shared.straddle_plan
            pp: Optional[PCSTailPlan] = self._shared.pcs_plan
            trades = list(self._shared.trades.values())

        if sp is not None:
            if sp.call is not None:
                keys.add(sp.call)
            if sp.put is not None:
                keys.add(sp.put)

        if pp is not None:
            if pp.short_put is not None:
                keys.add(pp.short_put)
            if pp.long_put is not None:
                keys.add(pp.long_put)
            if pp.tail_put is not None:
                keys.add(pp.tail_put)

        for tr in trades:
            for leg in tr.legs:
                if leg.contract is not None:
                    keys.add(leg.contract)
        return keys

    # -------------------------------------------------------------------------
    # SECTION: Bar/VWAP/HOD/LOD maintenance
    # -------------------------------------------------------------------------

    def _update_bars_and_levels(self, spy_last: float) -> None:
        now = now_ts()
        if self._hod is None or spy_last > self._hod:
            self._hod = spy_last
        if self._lod is None or spy_last < self._lod:
            self._lod = spy_last

        # Initialize bar builder if needed.
        if self._builder is None:
            self._builder = _StubBarBuilder(
                minute_start_ts=now - (now % 60.0),
                open=spy_last,
                high=spy_last,
                low=spy_last,
                close=spy_last,
                pseudo_volume=1.0,
            )
            self._update_vwap(price=spy_last, vol=1.0)
            return

        # Update current minute builder.
        b = self._builder
        if b is None:
            return

        minute_start = b.minute_start_ts
        cur_minute_start = now - (now % 60.0)
        if cur_minute_start > minute_start:
            # Close the previous bar and start a new one.
            bar = Bar1m(
                ts=minute_start,
                open=b.open,
                high=b.high,
                low=b.low,
                close=b.close,
                vwap=self._vwap_running if self._vwap_running is not None else b.close,
            )
            self._bars.append(bar)
            self._builder = _StubBarBuilder(
                minute_start_ts=cur_minute_start,
                open=spy_last,
                high=spy_last,
                low=spy_last,
                close=spy_last,
                pseudo_volume=1.0,
            )
            self._update_vwap(price=spy_last, vol=1.0)
            return

        # Same minute update.
        b.high = max(b.high, spy_last)
        b.low = min(b.low, spy_last)
        b.close = spy_last
        b.pseudo_volume += 1.0
        self._update_vwap(price=spy_last, vol=1.0)

    def _update_vwap(self, price: float, vol: float) -> None:
        self._vwap_pv += price * vol
        self._vwap_v += vol
        if self._vwap_v > 0:
            self._vwap_running = self._vwap_pv / self._vwap_v

    # -------------------------------------------------------------------------
    # SECTION: Publish into SharedState
    # -------------------------------------------------------------------------

    def _publish(self, snap: MarketSnapshot) -> None:
        with self._shared.lock:
            self._shared.market = snap
            # Convenience heartbeat mirror for GUI/status; snapshot remains source of truth.
            self._shared.md_heartbeat_ts = snap.hub_heartbeat_ts
        # Status lines are handled by other components; hub only publishes data.
