"""
gui_main.py

PySide6 GUI for the IBKR trading bot.

Design rules enforced:
- Config file is loaded ONLY on application startup (launch).
- Editable UI fields are initialized from config ONCE at launch.
- After launch, runtime config is updated ONLY from UI events.
- Periodic GUI refresh NEVER writes into editable fields.
- Save writes runtime config to disk ONLY when Save is pressed.

Hard requirements preserved:
- Exactly five tabs: Real Time, Straddle Settings, PCS+Tail Settings, Account Balance, Debug.
- Real Time shows snapshot metrics, planned+active SPX option selections, and status/event panes together.
- Controls present on Real Time: go toggle, Straddle OPEN, Straddle Confirm, Clear locks, PCS OPEN, Shutdown/Flatten.

Python: 3.10.4
"""

from __future__ import annotations

import ast
import inspect
from dataclasses import fields, is_dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from PySide6.QtCore import QTimer, Qt
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDoubleSpinBox,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QRadioButton,
    QScrollArea,
    QSizePolicy,
    QSpinBox,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from core import (
    AccountMode,
    DayLockType,
    MarketSnapshot,
    RunMode,
    SharedState,
    load_config,
    now_ts,
    save_config,
)

# =============================================================================
# SECTION: Config path
# =============================================================================

CONFIG_PATH = "config/runtime_config.json"

# =============================================================================
# SECTION: Formatting utilities
# =============================================================================


def _fmt_float(x: Optional[float], nd: int = 2) -> str:
    if x is None:
        return "N/A"
    return f"{x:.{nd}f}"


def _age_s(ts: Optional[float]) -> Optional[float]:
    if ts is None:
        return None
    return max(0.0, now_ts() - ts)


def _vwap_dev(last: Optional[float], vwap: Optional[float]) -> Optional[float]:
    if last is None or vwap is None or vwap == 0:
        return None
    return (last - vwap) / vwap


# =============================================================================
# SECTION: Logging adapter (supports either shared.event_log or shared.log_*)
# =============================================================================


def _log_info(shared: SharedState, msg: str) -> None:
    if hasattr(shared, "event_log") and hasattr(shared.event_log, "log_info"):
        shared.event_log.log_info(msg)
        return
    if hasattr(shared, "log_info"):
        shared.log_info(msg)  # type: ignore[attr-defined]


def _log_warn(shared: SharedState, msg: str) -> None:
    if hasattr(shared, "event_log") and hasattr(shared.event_log, "log_warn"):
        shared.event_log.log_warn(msg)
        return
    if hasattr(shared, "log_warn"):
        shared.log_warn(msg)  # type: ignore[attr-defined]


def _get_event_rows_text(shared: SharedState) -> str:
    if hasattr(shared, "event_log") and hasattr(shared.event_log, "rows"):
        rows = list(shared.event_log.rows)  # type: ignore[attr-defined]
        return "\n".join([getattr(r, "text", str(r)) for r in rows])
    return ""


# =============================================================================
# SECTION: Config metadata (description + tab routing)
# =============================================================================

# Every displayed config row includes a one-line description.
# Unknown keys fall back to a default description to satisfy the requirement.
CONFIG_META: Dict[str, Tuple[str, str]] = {
    # (description, tab_name)

    # Threads (Debug)
    "threads.md_hub_tick_s": ("MarketDataHub publish tick (seconds).", "Debug"),
    "threads.planners_tick_s": ("Planner tick rate (seconds).", "Debug"),
    "threads.triggers_tick_s": ("Trigger supervisor scan tick (seconds).", "Debug"),
    "threads.execution_tick_s": ("ExecutionEngine management tick (seconds).", "Debug"),
    "threads.gui_refresh_tick_s": ("GUI refresh timer interval (seconds).", "Debug"),

    # Account (Account Balance)
    "account.mode": ("Select PAPER vs LIVE.", "Account Balance"),
    "account.paper_account_id": ("IBKR paper account id; required to connect in PAPER.", "Account Balance"),
    "account.live_account_id": ("IBKR live account id; required to connect in LIVE.", "Account Balance"),
    "account.starting_balance_override": ("Starting balance used until NetLiq read (0 disables).", "Account Balance"),
    "account.require_account_id_to_connect": ("Prevent connect unless selected mode id is set.", "Account Balance"),

    # UI (Straddle/Debug)
    "ui.allow_confirm_anytime_when_armable": ("Allow Confirm button anytime while armable is true.", "Straddle Settings"),
    "ui.popup_on_armable": ("Show popup when gate becomes ARMABLE (manual mode).", "Straddle Settings"),
    "ui.sound_on_armable": ("Play sound when gate becomes ARMABLE (manual mode).", "Straddle Settings"),
    "ui.event_log_capacity": ("Max number of event rows retained in ring buffer.", "Debug"),

    # Export (Account Balance)
    "export.export_dir": ("Directory for EOD exports.", "Account Balance"),
    "export.export_jsonl": ("Enable JSONL export at EOD.", "Account Balance"),
    "export.export_csv": ("Enable CSV export at EOD.", "Account Balance"),
    "export.export_once_per_day": ("Write EOD files once per day only.", "Account Balance"),

    # Debug (Debug)
    "debug.run_mode": ("REAL places orders; SIM simulates; REPLAY uses snapshot file.", "Debug"),
    "debug.replay_file": ("Path to replay snapshot series file.", "Debug"),
    "debug.replay_step_mode": ("If true, advance replay only via GUI step.", "Debug"),
    "debug.sim_fill_assume_complete": ("SIM fills assumed complete if quotes exist.", "Debug"),
    "debug.sim_deterministic_slippage_ticks": ("SIM deterministic slippage in ticks.", "Debug"),

    # Straddle (Straddle Settings) - include any additional keys as needed
    "straddle.enable_bull": ("Enable bull gate evaluation.", "Straddle Settings"),
    "straddle.enable_bear": ("Enable bear gate evaluation.", "Straddle Settings"),
    "straddle.enable_neutral": ("Enable neutral gate evaluation.", "Straddle Settings"),
    "straddle.gate_arming_auto": ("Auto-arm when ARMABLE; otherwise manual confirm required.", "Straddle Settings"),
    "straddle.timed_entry_enabled": ("Enable TIMED channel.", "Straddle Settings"),
    "straddle.timed_entry_time_et": ("TIMED entry time (ET, HH:MM).", "Straddle Settings"),
    "straddle.allow_multiple_gated_per_day": ("Allow multiple gated entries per day (else lock).", "Straddle Settings"),
    "straddle.gate_deadline_time_et": ("Gate/entry deadline time (ET, HH:MM).", "Straddle Settings"),
    "straddle.call_delta_target": ("Call delta target for selection.", "Straddle Settings"),
    "straddle.put_delta_target": ("Put delta target (absolute) for selection.", "Straddle Settings"),
    "straddle.delta_tolerance": ("Delta tolerance around target.", "Straddle Settings"),
    "straddle.budget_pct_available_funds": ("Budget as % of AvailableFunds.", "Straddle Settings"),
    "straddle.leg_cap_pct_each": ("Per-leg cap as % of budget (limit-price based).", "Straddle Settings"),
    "straddle.entry_spread_max_pct_mid": ("Max spread% of mid allowed for entry (each leg).", "Straddle Settings"),
    "straddle.entry_price_tick_size": ("Tick size used for entry price improvement.", "Straddle Settings"),
    "straddle.entry_price_unchanged_s": ("Seconds mid must remain unchanged before +1 tick.", "Straddle Settings"),
    "straddle.entry_fill_timeout_s": ("Entry timeout seconds before cancel remainder.", "Straddle Settings"),
    "straddle.stop_soft_pct": ("Soft stop P&L percent (bid-based).", "Straddle Settings"),
    "straddle.stop_hard_pct": ("Hard stop P&L percent immediate trigger.", "Straddle Settings"),
    "straddle.stop_soft_hold_s": ("Soft stop must persist this long before exit.", "Straddle Settings"),
    "straddle.ratchet_ladder": ("Profit thresholds mapped to stop lock levels.", "Straddle Settings"),
    "straddle.hang_seconds": ("No-new-high hang seconds before hang exit.", "Straddle Settings"),

    # PCS + Tail (PCS+Tail Settings) - include any additional keys as needed
    "pcs.once_per_day_lock": ("Enforce PCS once/day hard lock.", "PCS+Tail Settings"),
    "pcs.min_bars_since_open": ("Minimum bars since open required.", "PCS+Tail Settings"),
    "pcs.deadline_time_et": ("PCS deadline time (ET, HH:MM).", "PCS+Tail Settings"),
    "pcs.rsi_length": ("RSI lookback length.", "PCS+Tail Settings"),
    "pcs.rsi_max": ("RSI must be below this to allow entry.", "PCS+Tail Settings"),
    "pcs.sma_length": ("SMA length for bullish filter.", "PCS+Tail Settings"),
    "pcs.pct_change_min": ("Min % change since last close (0.0025=0.25%).", "PCS+Tail Settings"),
    "pcs.short_put_delta_target": ("Short put delta target.", "PCS+Tail Settings"),
    "pcs.delta_tolerance": ("Delta tolerance for PCS/tail selection.", "PCS+Tail Settings"),
    "pcs.width_points": ("Spread width in points.", "PCS+Tail Settings"),
    "pcs.fixed_budget": ("Fixed PCS budget in dollars.", "PCS+Tail Settings"),
    "pcs.budget_credit_mult": ("Require credit_total * mult <= budget.", "PCS+Tail Settings"),
    "pcs.entry_spread_max_pct_mid": ("Max spread% of mid allowed for entry (each leg).", "PCS+Tail Settings"),
    "pcs.stop_debit_mult": ("Stop when debit_to_close >= mult * credit_received.", "PCS+Tail Settings"),
    "pcs.tail_delta_target": ("Tail delta target.", "PCS+Tail Settings"),
    "pcs.tail_qty_mult_of_pcs": ("Tail qty multiplier (floor).", "PCS+Tail Settings"),
    "pcs.tail_activation_profit_mult_of_credit": ("Tail stop activates after this profit multiple.", "PCS+Tail Settings"),
    "pcs.tail_ratchet_ladder": ("Tail ratchet ladder thresholds/locks (credit multiples).", "PCS+Tail Settings"),
    "pcs.tail_hang_seconds": ("Tail hang seconds once stops active.", "PCS+Tail Settings"),
}


# =============================================================================
# SECTION: Dataclass flattening and mutation
# =============================================================================

def _flatten_dataclass(obj: Any, prefix: str = "") -> Dict[str, Any]:
    """Flatten nested dataclasses into {dotted_path: value} mapping."""
    out: Dict[str, Any] = {}
    if not is_dataclass(obj):
        return out
    for f in fields(obj):
        val = getattr(obj, f.name)
        path = f"{prefix}{f.name}" if prefix == "" else f"{prefix}.{f.name}"
        if is_dataclass(val):
            out.update(_flatten_dataclass(val, path))
        else:
            out[path] = val
    return out


def _set_by_path(root: Any, path: str, value: Any) -> None:
    """Set nested attribute by dotted path into dataclass tree."""
    parts = path.split(".")
    cur = root
    for p in parts[:-1]:
        cur = getattr(cur, p)
    setattr(cur, parts[-1], value)


# =============================================================================
# SECTION: Event + Status text views
# =============================================================================

class EventTextView(QTextEdit):
    """Event window with bullet formatting + conditional autoscroll."""

    def __init__(self) -> None:
        super().__init__()
        self.setReadOnly(True)
        self.setLineWrapMode(QTextEdit.WidgetWidth)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

    def set_text_preserve_autoscroll(self, text: str) -> None:
        sb = self.verticalScrollBar()
        at_bottom = sb.value() >= (sb.maximum() - 2)
        self.setPlainText(text)
        if at_bottom:
            sb.setValue(sb.maximum())


class StatusTextView(QTextEdit):
    """Status window with word wrap and no forced autoscroll."""

    def __init__(self) -> None:
        super().__init__()
        self.setReadOnly(True)
        self.setLineWrapMode(QTextEdit.WidgetWidth)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)


# =============================================================================
# SECTION: Main Window
# =============================================================================

class MainWindow(QMainWindow):
    """Main GUI window with exactly five tabs."""

    TAB_REALTIME = "Real Time"
    TAB_STRADDLE = "Straddle Settings"
    TAB_PCS = "PCS+Tail Settings"
    TAB_ACCOUNT = "Account Balance"
    TAB_DEBUG = "Debug"

    def __init__(self, shared: SharedState) -> None:
        super().__init__()
        self._shared = shared
        self.setWindowTitle("IBKR Trading Bot GUI")

        self._tabs = QTabWidget()
        self.setCentralWidget(self._tabs)

        # Build tabs
        self._rt_tab = self._build_realtime_tab()
        self._straddle_tab = self._build_config_tab(self.TAB_STRADDLE)
        self._pcs_tab = self._build_config_tab(self.TAB_PCS)
        self._account_tab = self._build_account_tab()
        self._debug_tab = self._build_config_tab(self.TAB_DEBUG)

        self._tabs.addTab(self._rt_tab, self.TAB_REALTIME)
        self._tabs.addTab(self._straddle_tab, self.TAB_STRADDLE)
        self._tabs.addTab(self._pcs_tab, self.TAB_PCS)
        self._tabs.addTab(self._account_tab, self.TAB_ACCOUNT)
        self._tabs.addTab(self._debug_tab, self.TAB_DEBUG)

        # Initialize editable fields ONCE from config (launch only)
        self._init_account_inputs_from_config()

        # Refresh timer
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._refresh_all)  # type: ignore[attr-defined]
        self._timer.start(int(self._shared.config.threads.gui_refresh_tick_s * 1000))

        self._refresh_all()

    # -------------------------------------------------------------------------
    # Real Time tab
    # -------------------------------------------------------------------------

    def _build_realtime_tab(self) -> QWidget:
        w = QWidget()
        root = QVBoxLayout(w)

        # Market snapshot labels
        box = QGroupBox("Market Snapshot")
        grid = QGridLayout(box)

        self._lbl_spy = QLabel("SPY last: N/A")
        self._lbl_vwap = QLabel("VWAP: N/A")
        self._lbl_dev = QLabel("VWAP dev: N/A")
        self._lbl_hodlod = QLabel("HOD/LOD: N/A")
        self._lbl_bars = QLabel("Bars since open: N/A")
        self._lbl_md = QLabel("Heartbeat: N/A")

        grid.addWidget(self._lbl_spy, 0, 0)
        grid.addWidget(self._lbl_vwap, 0, 1)
        grid.addWidget(self._lbl_dev, 0, 2)
        grid.addWidget(self._lbl_hodlod, 1, 0)
        grid.addWidget(self._lbl_bars, 1, 1)
        grid.addWidget(self._lbl_md, 1, 2)

        root.addWidget(box)

        # Option table (planned + active)
        self._opt_table = QTableWidget(0, 11)
        self._opt_table.setHorizontalHeaderLabels(
            [
                "Strategy/Role",
                "Expiry",
                "Strike",
                "Right",
                "Delta",
                "Bid",
                "Ask",
                "Mid",
                "Spread",
                "Spread%",
                "Quote Age(s)",
            ]
        )
        root.addWidget(QLabel("Selected SPX Options (planned + active)"))
        root.addWidget(self._opt_table)

        # Controls
        controls = QGroupBox("Controls")
        hb = QHBoxLayout(controls)

        self._chk_go = QCheckBox("GO (new entries)")
        self._chk_go.stateChanged.connect(self._on_go_toggled)  # type: ignore[attr-defined]

        self._btn_straddle_open = QPushButton("STRADDLE OPEN")
        self._btn_straddle_open.clicked.connect(self._on_straddle_open)  # type: ignore[attr-defined]

        self._btn_straddle_confirm = QPushButton("STRADDLE CONFIRM")
        self._btn_straddle_confirm.clicked.connect(self._on_straddle_confirm)  # type: ignore[attr-defined]

        self._btn_clear_straddle_timed = QPushButton("Clear Straddle Timed Lock")
        self._btn_clear_straddle_timed.clicked.connect(self._on_clear_straddle_timed)  # type: ignore[attr-defined]

        self._btn_clear_straddle_gated = QPushButton("Clear Straddle Gated Lock")
        self._btn_clear_straddle_gated.clicked.connect(self._on_clear_straddle_gated)  # type: ignore[attr-defined]

        self._btn_pcs_open = QPushButton("PCS OPEN")
        self._btn_pcs_open.clicked.connect(self._on_pcs_open)  # type: ignore[attr-defined]

        self._btn_clear_pcs = QPushButton("Clear PCS Lock")
        self._btn_clear_pcs.clicked.connect(self._on_clear_pcs)  # type: ignore[attr-defined]

        self._btn_shutdown = QPushButton("Shutdown / Flatten")
        self._btn_shutdown.clicked.connect(self._on_shutdown)  # type: ignore[attr-defined]

        for x in [
            self._chk_go,
            self._btn_straddle_open,
            self._btn_straddle_confirm,
            self._btn_clear_straddle_timed,
            self._btn_clear_straddle_gated,
            self._btn_pcs_open,
            self._btn_clear_pcs,
            self._btn_shutdown,
        ]:
            hb.addWidget(x)

        root.addWidget(controls)

        # Status + Events
        row = QHBoxLayout()
        self._status = StatusTextView()
        self._events = EventTextView()
        row.addWidget(self._status, 1)
        row.addWidget(self._events, 1)
        root.addLayout(row)

        return w

    # -------------------------------------------------------------------------
    # Settings/config tab builder
    # -------------------------------------------------------------------------

    def _build_config_tab(self, tab_name: str) -> QWidget:
        w = QWidget()
        root = QVBoxLayout(w)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)

        inner = QWidget()
        form = QFormLayout(inner)
        form.setLabelAlignment(Qt.AlignLeft)
        form.setFormAlignment(Qt.AlignTop)

        flat = _flatten_dataclass(self._shared.config)
        for path in sorted(flat.keys()):
            meta = CONFIG_META.get(path)
            if meta is None:
                desc, tab_for_key = ("(Description missing: add to CONFIG_META.)", self.TAB_DEBUG)
            else:
                desc, tab_for_key = meta
            if tab_for_key != tab_name:
                continue

            label = QLabel(path)
            label.setToolTip(desc)

            editor, getter = self._make_editor_for_value(flat[path])
            editor.setToolTip(desc)

            row = QWidget()
            hb = QHBoxLayout(row)
            hb.setContentsMargins(0, 0, 0, 0)

            hb.addWidget(editor, 0)

            desc_lbl = QLabel(desc)
            desc_lbl.setWordWrap(True)
            desc_lbl.setStyleSheet("color: #666;")
            desc_lbl.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
            hb.addWidget(desc_lbl, 1)

            form.addRow(label, row)
            self._wire_live_apply(editor, path, getter)

        scroll.setWidget(inner)
        root.addWidget(scroll)

        btn_save = QPushButton("Save Config")
        btn_save.clicked.connect(self._on_save_config)  # type: ignore[attr-defined]
        root.addWidget(btn_save)

        return w

    def _wire_live_apply(self, editor: QWidget, path: str, getter: Callable[[], Any]) -> None:
        """Connect appropriate signals so edits apply live to SharedState.config."""
        if isinstance(editor, QCheckBox):
            editor.stateChanged.connect(lambda _=None: self._apply_config_value(path, getter()))  # type: ignore[attr-defined]
        elif isinstance(editor, QComboBox):
            editor.currentTextChanged.connect(lambda _=None: self._apply_config_value(path, getter()))  # type: ignore[attr-defined]
        elif isinstance(editor, QSpinBox):
            editor.valueChanged.connect(lambda _=None: self._apply_config_value(path, getter()))  # type: ignore[attr-defined]
        elif isinstance(editor, QDoubleSpinBox):
            editor.valueChanged.connect(lambda _=None: self._apply_config_value(path, getter()))  # type: ignore[attr-defined]
        elif isinstance(editor, QLineEdit):
            editor.editingFinished.connect(lambda: self._apply_config_value(path, getter()))  # type: ignore[attr-defined]

    def _make_editor_for_value(self, value: Any) -> Tuple[QWidget, Callable[[], Any]]:
        """
        Create a compact editor for a config leaf value.

        Width policy:
        - Editors are constrained so the description column consumes extra space.
        """
        if isinstance(value, AccountMode):
            cb = QComboBox()
            cb.addItems([m.value for m in AccountMode])
            cb.setCurrentText(value.value)
            cb.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
            cb.setMaximumWidth(220)

            def getter() -> Any:
                return AccountMode(cb.currentText())

            return cb, getter

        if isinstance(value, RunMode):
            cb = QComboBox()
            cb.addItems([m.value for m in RunMode])
            cb.setCurrentText(value.value)
            cb.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
            cb.setMaximumWidth(220)

            def getter() -> Any:
                return RunMode(cb.currentText())

            return cb, getter

        if isinstance(value, bool):
            chk = QCheckBox()
            chk.setChecked(bool(value))
            chk.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

            def getter() -> Any:
                return bool(chk.isChecked())

            return chk, getter

        if isinstance(value, int) and not isinstance(value, bool):
            sp = QSpinBox()
            sp.setRange(-10_000_000, 10_000_000)
            sp.setValue(int(value))
            sp.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
            sp.setMaximumWidth(140)

            def getter() -> Any:
                return int(sp.value())

            return sp, getter

        if isinstance(value, float):
            dsp = QDoubleSpinBox()
            dsp.setRange(-1e9, 1e9)
            dsp.setDecimals(6)
            dsp.setSingleStep(0.01)
            dsp.setValue(float(value))
            dsp.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
            dsp.setMaximumWidth(160)

            def getter() -> Any:
                return float(dsp.value())

            return dsp, getter

        le = QLineEdit(str(value))
        le.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        le.setMaximumWidth(280)

        def getter() -> Any:
            txt = le.text()
            if isinstance(value, (list, tuple, dict)):
                try:
                    return ast.literal_eval(txt)
                except Exception:
                    return value
            return txt

        return le, getter

    def _apply_config_value(self, path: str, value: Any) -> None:
        with self._shared.lock:
            _set_by_path(self._shared.config, path, value)

    # -------------------------------------------------------------------------
    # Account Balance tab (mode + ids)
    # -------------------------------------------------------------------------

    def _build_account_tab(self) -> QWidget:
        w = QWidget()
        root = QVBoxLayout(w)

        # Account configuration group
        acct_cfg = QGroupBox("Account Configuration")
        form = QFormLayout(acct_cfg)
        form.setLabelAlignment(Qt.AlignLeft)
        form.setFormAlignment(Qt.AlignTop)

        # Mode selector (radio buttons)
        mode_row = QWidget()
        mode_hb = QHBoxLayout(mode_row)
        mode_hb.setContentsMargins(0, 0, 0, 0)

        self._rb_paper = QRadioButton("PAPER")
        self._rb_live = QRadioButton("LIVE")

        self._rb_paper.toggled.connect(self._on_mode_changed)  # type: ignore[attr-defined]
        self._rb_live.toggled.connect(self._on_mode_changed)  # type: ignore[attr-defined]

        mode_hb.addWidget(self._rb_paper)
        mode_hb.addWidget(self._rb_live)

        mode_desc = QLabel("Select which account mode is active. Connection requires the selected mode id.")
        mode_desc.setWordWrap(True)
        mode_desc.setStyleSheet("color: #666;")
        mode_desc.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        mode_wrap = QWidget()
        mode_wrap_hb = QHBoxLayout(mode_wrap)
        mode_wrap_hb.setContentsMargins(0, 0, 0, 0)
        mode_wrap_hb.addWidget(mode_row, 0)
        mode_wrap_hb.addWidget(mode_desc, 1)
        form.addRow(QLabel("account.mode"), mode_wrap)

        # Paper account id
        self._le_paper_id = QLineEdit()
        self._le_paper_id.setMaximumWidth(300)
        self._le_paper_id.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self._le_paper_id.editingFinished.connect(self._on_paper_id_committed)  # type: ignore[attr-defined]

        paper_desc = QLabel("IBKR paper account id used for connection when mode=PAPER.")
        paper_desc.setWordWrap(True)
        paper_desc.setStyleSheet("color: #666;")
        paper_desc.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        paper_wrap = QWidget()
        paper_hb = QHBoxLayout(paper_wrap)
        paper_hb.setContentsMargins(0, 0, 0, 0)
        paper_hb.addWidget(self._le_paper_id, 0)
        paper_hb.addWidget(paper_desc, 1)
        form.addRow(QLabel("account.paper_account_id"), paper_wrap)

        # Live account id
        self._le_live_id = QLineEdit()
        self._le_live_id.setMaximumWidth(300)
        self._le_live_id.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self._le_live_id.editingFinished.connect(self._on_live_id_committed)  # type: ignore[attr-defined]

        live_desc = QLabel("IBKR live account id used for connection when mode=LIVE.")
        live_desc.setWordWrap(True)
        live_desc.setStyleSheet("color: #666;")
        live_desc.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        live_wrap = QWidget()
        live_hb = QHBoxLayout(live_wrap)
        live_hb.setContentsMargins(0, 0, 0, 0)
        live_hb.addWidget(self._le_live_id, 0)
        live_hb.addWidget(live_desc, 1)
        form.addRow(QLabel("account.live_account_id"), live_wrap)

        # Save button (explicit, disk write only on press)
        btn_save = QPushButton("Save Config")
        btn_save.clicked.connect(self._on_save_config)  # type: ignore[attr-defined]
        form.addRow(QLabel(""), btn_save)

        root.addWidget(acct_cfg)

        # Account status (read-only)
        self._acct_lbl = QLabel("Account: N/A")
        self._bal_lbl = QLabel("Balances: N/A")
        root.addWidget(self._acct_lbl)
        root.addWidget(self._bal_lbl)

        self._ledger = QTextEdit()
        self._ledger.setReadOnly(True)
        self._ledger.setLineWrapMode(QTextEdit.WidgetWidth)
        self._ledger.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        root.addWidget(self._ledger, 1)

        return w

    def _init_account_inputs_from_config(self) -> None:
        """
        One-time initialization of account inputs from the config loaded on launch.
        After this point, periodic refresh must not write into these inputs.
        """
        with self._shared.lock:
            mode = self._shared.config.account.mode
            paper_id = self._shared.config.account.paper_account_id
            live_id = self._shared.config.account.live_account_id

        self._rb_paper.blockSignals(True)
        self._rb_live.blockSignals(True)
        self._rb_paper.setChecked(mode == AccountMode.PAPER)
        self._rb_live.setChecked(mode == AccountMode.LIVE)
        self._rb_paper.blockSignals(False)
        self._rb_live.blockSignals(False)

        self._le_paper_id.blockSignals(True)
        self._le_live_id.blockSignals(True)
        self._le_paper_id.setText(paper_id)
        self._le_live_id.setText(live_id)
        self._le_paper_id.blockSignals(False)
        self._le_live_id.blockSignals(False)

    def _on_mode_changed(self) -> None:
        new_mode = AccountMode.PAPER if self._rb_paper.isChecked() else AccountMode.LIVE
        with self._shared.lock:
            self._shared.config.account.mode = new_mode

    def _on_paper_id_committed(self) -> None:
        with self._shared.lock:
            self._shared.config.account.paper_account_id = self._le_paper_id.text().strip()

    def _on_live_id_committed(self) -> None:
        with self._shared.lock:
            self._shared.config.account.live_account_id = self._le_live_id.text().strip()

    # -------------------------------------------------------------------------
    # Controls callbacks
    # -------------------------------------------------------------------------

    def _on_go_toggled(self) -> None:
        with self._shared.lock:
            self._shared.gui.go = bool(self._chk_go.isChecked())

    def _on_straddle_open(self) -> None:
        with self._shared.lock:
            self._shared.gui.straddle_open_pressed = True
        _log_info(self._shared, "• GUI: STRADDLE OPEN pressed.")

    def _on_straddle_confirm(self) -> None:
        with self._shared.lock:
            self._shared.gui.manual_confirm_pressed = True
        _log_info(self._shared, "• GUI: STRADDLE CONFIRM pressed.")

    def _on_pcs_open(self) -> None:
        with self._shared.lock:
            self._shared.gui.pcs_open_pressed = True
        _log_info(self._shared, "• GUI: PCS OPEN pressed.")

    def _on_shutdown(self) -> None:
        with self._shared.lock:
            self._shared.gui.shutdown_pressed = True
        self._shared.shutdown_event.set()
        _log_warn(self._shared, "• GUI: Shutdown/Flatten pressed.")

    def _confirm(self, title: str, text: str) -> bool:
        r = QMessageBox.question(self, title, text, QMessageBox.Yes | QMessageBox.No)
        return r == QMessageBox.Yes

    def _on_clear_straddle_timed(self) -> None:
        if not self._confirm("Confirm", "Clear STRADDLE TIMED day lock?"):
            return
        with self._shared.lock:
            self._shared.gui.clear_lock_straddle_timed = True
        _log_info(self._shared, "• GUI: Clear STRADDLE TIMED lock requested.")

    def _on_clear_straddle_gated(self) -> None:
        if not self._confirm("Confirm", "Clear STRADDLE GATED day lock?"):
            return
        with self._shared.lock:
            self._shared.gui.clear_lock_straddle_gated = True
        _log_info(self._shared, "• GUI: Clear STRADDLE GATED lock requested.")

    def _on_clear_pcs(self) -> None:
        if not self._confirm("Confirm", "Clear PCS once/day lock?"):
            return
        with self._shared.lock:
            self._shared.gui.clear_lock_pcs = True
        _log_info(self._shared, "• GUI: Clear PCS lock requested.")

    # -------------------------------------------------------------------------
    # Save config (disk write only on press)
    # -------------------------------------------------------------------------

    def _on_save_config(self) -> None:
        with self._shared.lock:
            cfg = self._shared.config
        try:
            sig = inspect.signature(save_config)
            if len(sig.parameters) >= 2:
                save_config(cfg, CONFIG_PATH)  # type: ignore[misc]
            else:
                save_config(cfg)  # type: ignore[misc]
            _log_info(self._shared, "• Config saved to disk.")
        except Exception as e:
            _log_warn(self._shared, f"• Config save failed: {e!r}")

    # -------------------------------------------------------------------------
    # Refresh loop (read-only updates only)
    # -------------------------------------------------------------------------

    def _refresh_all(self) -> None:
        self._refresh_realtime()
        self._refresh_account_readonly()
        self._refresh_text_panels()

    def _refresh_realtime(self) -> None:
        with self._shared.lock:
            snap: Optional[MarketSnapshot] = self._shared.market
            sp = self._shared.straddle_plan
            pp = self._shared.pcs_plan
            trades = list(self._shared.trades.values())
            go_flag = bool(self._shared.gui.go)

        self._chk_go.blockSignals(True)
        self._chk_go.setChecked(go_flag)
        self._chk_go.blockSignals(False)

        if snap is None:
            self._opt_table.setRowCount(0)
            return

        dev = _vwap_dev(snap.spy_last, snap.spy_vwap)
        hb_age = _age_s(snap.hub_heartbeat_ts)

        self._lbl_spy.setText(f"SPY last: {_fmt_float(snap.spy_last, 2)}")
        self._lbl_vwap.setText(f"VWAP: {_fmt_float(snap.spy_vwap, 2)}")
        self._lbl_dev.setText(f"VWAP dev: {_fmt_float(dev, 6)}")
        self._lbl_hodlod.setText(f"HOD={_fmt_float(snap.spy_hod, 2)}  LOD={_fmt_float(snap.spy_lod, 2)}")
        self._lbl_bars.setText(f"Bars since open: {len(snap.bars_1m)}")
        self._lbl_md.setText(f"Heartbeat age: {_fmt_float(hb_age, 2)}s")

        rows: List[Tuple[str, Any]] = []
        if sp is not None and getattr(sp, "call", None) is not None:
            rows.append(("STRADDLE planned CALL", sp.call))
        if sp is not None and getattr(sp, "put", None) is not None:
            rows.append(("STRADDLE planned PUT", sp.put))
        if pp is not None and getattr(pp, "short_put", None) is not None:
            rows.append(("PCS planned SHORT", pp.short_put))
        if pp is not None and getattr(pp, "long_put", None) is not None:
            rows.append(("PCS planned LONG", pp.long_put))
        if pp is not None and getattr(pp, "tail_put", None) is not None:
            rows.append(("TAIL planned", pp.tail_put))

        for tr in trades:
            for leg in tr.legs:
                if leg.contract is None:
                    continue
                rows.append((f"ACTIVE {tr.strategy.value} leg={leg.leg_id}", leg.contract))

        seen = set()
        uniq: List[Tuple[str, Any]] = []
        for role, k in rows:
            if k in seen:
                continue
            seen.add(k)
            uniq.append((role, k))

        self._opt_table.setRowCount(len(uniq))
        for r, (role, key) in enumerate(uniq):
            q = snap.option_quotes.get(key)
            delta = "N/A" if q is None or q.delta is None else f"{q.delta:+.3f}"
            bid = "N/A" if q is None or q.bid is None else f"{q.bid:.2f}"
            ask = "N/A" if q is None or q.ask is None else f"{q.ask:.2f}"
            mid = "N/A" if q is None or q.mid is None else f"{q.mid:.2f}"

            spread_v = None
            spread_pct = None
            if q is not None and q.bid is not None and q.ask is not None and q.mid is not None and q.mid > 0:
                spread_v = q.ask - q.bid
                spread_pct = spread_v / q.mid

            spread = "N/A" if spread_v is None else f"{spread_v:.2f}"
            spreadp = "N/A" if spread_pct is None else f"{spread_pct * 100.0:.3f}%"
            qage = "N/A" if q is None else _fmt_float(_age_s(q.quote_ts), 2)

            values = [
                role,
                key.expiry,
                str(key.strike),
                key.right,
                delta,
                bid,
                ask,
                mid,
                spread,
                spreadp,
                qage,
            ]
            for c, v in enumerate(values):
                item = QTableWidgetItem(v)
                item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                self._opt_table.setItem(r, c, item)

        self._opt_table.resizeColumnsToContents()

    def _refresh_account_readonly(self) -> None:
        """
        Read-only refresh of account status/ledger.
        Editable account fields must not be written during refresh.
        """
        with self._shared.lock:
            connected = bool(self._shared.ibkr_connected)
            acct_mode = self._shared.selected_account_mode
            acct_id = self._shared.selected_account_id
            netliq = self._shared.net_liq
            avail = self._shared.available_funds
            ledger = list(self._shared.ledger)

        self._acct_lbl.setText(f"Connected={connected}  Mode={acct_mode}  ID={acct_id}")
        self._bal_lbl.setText(f"NetLiq={_fmt_float(netliq)}  Available={_fmt_float(avail)}")
        self._ledger.setPlainText("\n".join(str(x) for x in ledger[-30:]))

    def _refresh_text_panels(self) -> None:
        with self._shared.lock:
            snap = self._shared.market
            trades = list(self._shared.trades.values())
            locks = dict(self._shared.day_locks)

        status_lines: List[str] = []
        if snap is not None:
            status_lines.append(
                f"SPY last={_fmt_float(snap.spy_last,2)} vwap={_fmt_float(snap.spy_vwap,2)} "
                f"dev={_fmt_float(_vwap_dev(snap.spy_last, snap.spy_vwap),6)} bars={len(snap.bars_1m)}"
            )
        status_lines.append(
            f"Day locks: timed={locks.get(DayLockType.STRADDLE_TIMED, False)} "
            f"gated={locks.get(DayLockType.STRADDLE_GATED, False)} "
            f"pcs={locks.get(DayLockType.PCS, False)}"
        )
        status_lines.append(f"Active trades: {len(trades)}")
        self._status.setPlainText("\n".join(status_lines))

        self._events.set_text_preserve_autoscroll(_get_event_rows_text(self._shared))


# =============================================================================
# SECTION: GUI Entrypoint
# =============================================================================

def _call_load_config() -> Any:
    """Support either load_config() or load_config(path)."""
    sig = inspect.signature(load_config)
    if len(sig.parameters) >= 1:
        return load_config(CONFIG_PATH)  # type: ignore[misc]
    return load_config()  # type: ignore[misc]


def run_gui(shared: SharedState) -> int:
    app = QApplication([])
    win = MainWindow(shared)
    win.resize(1500, 860)
    win.show()
    return app.exec()


def main() -> int:
    cfg = _call_load_config()
    shared = SharedState(config=cfg)
    return run_gui(shared)


if __name__ == "__main__":
    raise SystemExit(main())
