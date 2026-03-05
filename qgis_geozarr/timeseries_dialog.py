"""Time series search + band picker dialog."""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

from qgis.PyQt.QtCore import QDate, QThread, pyqtSignal
from qgis.PyQt.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDateEdit,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from . import band_presets, geozarr_metadata
from .geozarr_dialog import populate_band_checkboxes
from .geozarr_metadata import ZarrRootInfo
from .stac_search import TimeSeriesItem, query_stac_items

log = logging.getLogger(__name__)


class _SearchThread(QThread):
    """Background STAC search + metadata probe from first result."""

    finished = pyqtSignal(object, list)  # (ZarrRootInfo | None, [TimeSeriesItem])
    error = pyqtSignal(str)

    def __init__(self, base_url, collection_id, bbox, datetime_range,
                 grid_code, limit):
        super().__init__()
        self.base_url = base_url
        self.collection_id = collection_id
        self.bbox = bbox
        self.datetime_range = datetime_range
        self.grid_code = grid_code
        self.limit = limit

    def run(self):
        try:
            items = query_stac_items(
                self.base_url, self.collection_id,
                bbox=self.bbox,
                datetime_range=self.datetime_range,
                grid_code=self.grid_code,
                limit=self.limit,
            )
            if not items:
                self.error.emit("No items found matching search criteria.")
                return

            # Probe metadata from first item (reuses 3-tier cache)
            info, _ = geozarr_metadata.fetch_resolved(items[0].zarr_url)
            if not info:
                self.error.emit(
                    f"Failed to read metadata from {items[0].zarr_url}"
                )
                return

            self.finished.emit(info, items)
        except Exception as e:
            self.error.emit(str(e))


class TimeSeriesDialog(QDialog):
    """Date range search + band/resolution picker for time series loading."""

    def __init__(
        self,
        parent=None,
        base_url: str = "",
        collection_id: str = "",
        default_bbox: Tuple[float, ...] | None = None,
        default_grid_code: str = "",
    ):
        super().__init__(parent)
        self.setWindowTitle("Load Time Series")
        self.setMinimumWidth(540)
        self.setMinimumHeight(550)

        self._base_url = base_url
        self._collection_id = collection_id
        self._default_bbox = default_bbox
        self._default_grid_code = default_grid_code
        self._info: ZarrRootInfo | None = None
        self._items: list[TimeSeriesItem] = []
        self._band_checks: Dict[str, QCheckBox] = {}
        self._preset_buttons: List[QPushButton] = []
        self._satellite = (
            band_presets.detect_satellite(collection_id) if collection_id else None
        )
        self._search_thread: _SearchThread | None = None
        self._last_preset_bands: Optional[Tuple[str, ...]] = None

        layout = QVBoxLayout(self)
        layout.setSpacing(8)

        self._build_search_section(layout)
        self._build_results_section(layout)
        self._build_band_section(layout)
        self._build_stretch_controls(layout)
        self._build_footer(layout)

        # Band section hidden until search completes
        self._band_group.setVisible(False)
        self._stretch_row.setVisible(False)
        self._ok_btn.setEnabled(False)

    # -- Search section -------------------------------------------------------

    def _build_search_section(self, layout):
        group = QGroupBox("Search")
        g_layout = QVBoxLayout(group)

        # Date range
        date_row = QHBoxLayout()
        date_row.addWidget(QLabel("From:"))
        self._date_from = QDateEdit()
        self._date_from.setCalendarPopup(True)
        self._date_from.setDate(QDate.currentDate().addMonths(-6))
        date_row.addWidget(self._date_from)
        date_row.addWidget(QLabel("To:"))
        self._date_to = QDateEdit()
        self._date_to.setCalendarPopup(True)
        self._date_to.setDate(QDate.currentDate())
        date_row.addWidget(self._date_to)
        g_layout.addLayout(date_row)

        # Limit + grid code
        opts_row = QHBoxLayout()
        opts_row.addWidget(QLabel("Max items:"))
        self._limit_spin = QSpinBox()
        self._limit_spin.setRange(2, 200)
        self._limit_spin.setValue(24)
        opts_row.addWidget(self._limit_spin)
        opts_row.addWidget(QLabel("Max cloud %:"))
        self._cloud_spin = QSpinBox()
        self._cloud_spin.setRange(0, 100)
        self._cloud_spin.setValue(30)
        self._cloud_spin.setToolTip("Filter out items with cloud cover above this %")
        opts_row.addWidget(self._cloud_spin)
        if self._default_grid_code:
            opts_row.addWidget(QLabel(f"Grid: {self._default_grid_code}"))
        opts_row.addStretch()
        self._search_btn = QPushButton("Search")
        self._search_btn.clicked.connect(self._on_search)
        opts_row.addWidget(self._search_btn)
        g_layout.addLayout(opts_row)

        layout.addWidget(group)

    def _build_results_section(self, layout):
        self._results_label = QLabel("")
        self._results_label.setWordWrap(True)
        self._results_label.setStyleSheet("color: #555; font-size: 11px;")
        layout.addWidget(self._results_label)

    # -- Band section (shown after search) ------------------------------------

    def _build_band_section(self, layout):
        self._band_group = QGroupBox("Bands && Resolution")

        band_layout = QVBoxLayout(self._band_group)

        # Resolution combo
        self._res_combo = QComboBox()
        self._res_combo.currentIndexChanged.connect(self._on_resolution_changed)
        self._res_row = QHBoxLayout()
        self._res_row.addWidget(QLabel("Resolution:"))
        self._res_row.addWidget(self._res_combo)
        band_layout.addLayout(self._res_row)

        # Preset buttons
        self._preset_layout = QHBoxLayout()
        band_layout.addLayout(self._preset_layout)

        # Band checkboxes
        bands_header = QHBoxLayout()
        self._bands_label = QLabel("<b>Bands</b>")
        bands_header.addWidget(self._bands_label)
        bands_header.addStretch()
        btn_all = QPushButton("All")
        btn_all.setFixedWidth(40)
        btn_all.clicked.connect(lambda: self._set_all_bands(True))
        btn_none = QPushButton("None")
        btn_none.setFixedWidth(46)
        btn_none.clicked.connect(lambda: self._set_all_bands(False))
        bands_header.addWidget(btn_all)
        bands_header.addWidget(btn_none)
        band_layout.addLayout(bands_header)

        self._band_widget = QWidget()
        self._band_layout = QVBoxLayout(self._band_widget)
        self._band_layout.setContentsMargins(4, 4, 4, 4)
        self._band_layout.setSpacing(2)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(self._band_widget)
        scroll.setFrameShape(scroll.Shape.NoFrame)
        scroll.setMinimumHeight(150)
        band_layout.addWidget(scroll, stretch=1)

        layout.addWidget(self._band_group)

    def _build_stretch_controls(self, layout):
        self._stretch_row = QWidget()
        stretch_layout = QHBoxLayout(self._stretch_row)
        stretch_layout.setContentsMargins(0, 0, 0, 0)
        stretch_layout.addWidget(QLabel("Stretch:"))
        self._stretch_min = QDoubleSpinBox()
        self._stretch_min.setDecimals(4)
        self._stretch_min.setRange(-1e9, 1e9)
        self._stretch_max = QDoubleSpinBox()
        self._stretch_max.setDecimals(4)
        self._stretch_max.setRange(-1e9, 1e9)
        stretch_layout.addWidget(QLabel("Min"))
        stretch_layout.addWidget(self._stretch_min)
        stretch_layout.addWidget(QLabel("Max"))
        stretch_layout.addWidget(self._stretch_max)
        stretch_layout.addStretch()
        layout.addWidget(self._stretch_row)

    def _build_footer(self, layout):
        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel,
        )
        self._ok_btn = buttons.button(QDialogButtonBox.StandardButton.Ok)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    # -- Lifecycle ------------------------------------------------------------

    def reject(self):
        """Clean up running search thread before closing."""
        if self._search_thread and self._search_thread.isRunning():
            self._search_thread.finished.disconnect()
            self._search_thread.error.disconnect()
            self._search_thread.requestInterruption()
            self._search_thread.quit()
            self._search_thread.wait(2000)
        super().reject()

    # -- Search logic ---------------------------------------------------------

    def _on_search(self):
        if self._date_from.date() > self._date_to.date():
            self._results_label.setText(
                "<span style='color:red'>From date must be before To date.</span>"
            )
            return
        d_from = self._date_from.date().toString("yyyy-MM-dd") + "T00:00:00Z"
        d_to = self._date_to.date().toString("yyyy-MM-dd") + "T23:59:59Z"
        dt_range = f"{d_from}/{d_to}"

        self._search_btn.setEnabled(False)
        self._search_btn.setText("Searching...")
        self._results_label.setText("Searching...")

        self._search_thread = _SearchThread(
            self._base_url, self._collection_id,
            bbox=self._default_bbox,
            datetime_range=dt_range,
            grid_code=self._default_grid_code,
            limit=self._limit_spin.value(),
        )
        self._search_thread.finished.connect(self._on_search_finished)
        self._search_thread.error.connect(self._on_search_error)
        self._search_thread.start()

    def _on_search_finished(self, info: ZarrRootInfo, items: list):
        self._info = info
        self._search_btn.setEnabled(True)
        self._search_btn.setText("Search")

        # Cloud cover filter
        max_cc = self._cloud_spin.value()
        if max_cc < 100:
            filtered = [
                i for i in items
                if i.cloud_cover is None or i.cloud_cover <= max_cc
            ]
            if filtered:
                items = filtered
        self._items = items

        # Date range summary
        dates = [i.datetime_str[:10] for i in items]
        cc_note = f" (cloud <= {max_cc}%)" if max_cc < 100 else ""
        self._results_label.setText(
            f"Found {len(items)} items: {dates[0]} to {dates[-1]}{cc_note}"
        )

        # Populate resolution combo
        self._res_combo.clear()
        for res in info.resolutions:
            self._res_combo.addItem(res, res)

        # Build preset buttons
        for btn in self._preset_buttons:
            self._preset_layout.removeWidget(btn)
            btn.deleteLater()
        self._preset_buttons.clear()
        if self._satellite:
            presets = band_presets.get_presets(self._satellite)
            for name in list(presets.keys())[:4]:
                btn = QPushButton(name.replace("_", " ").title())
                tooltip = band_presets.get_preset_tooltip(self._satellite, name)
                if tooltip:
                    btn.setToolTip(tooltip)
                btn.clicked.connect(
                    lambda checked, n=name: self._apply_preset(n),
                )
                btn.setProperty("preset_bands", presets[name])
                self._preset_layout.addWidget(btn)
                self._preset_buttons.append(btn)

        # Populate bands
        self._populate_bands(self._current_resolution())

        # Show band section
        self._band_group.setVisible(True)
        self._stretch_row.setVisible(True)
        self._ok_btn.setEnabled(True)

        # Stretch defaults
        self._update_stretch_defaults()

    def _on_search_error(self, msg: str):
        self._search_btn.setEnabled(True)
        self._search_btn.setText("Search")
        self._results_label.setText(f"<span style='color:red'>{msg}</span>")

    # -- Band selection -------------------------------------------------------

    def _populate_bands(self, resolution: str):
        if not self._info:
            return
        populate_band_checkboxes(
            self._info, resolution, self._band_layout,
            self._band_checks, self._satellite, self._preset_buttons,
        )
        count = len(self._band_checks)
        self._bands_label.setText(f"<b>Bands ({count})</b>")
        for cb in self._band_checks.values():
            cb.toggled.connect(self._update_ok_state)

    def _on_resolution_changed(self, _index: int):
        if self._info:
            self._populate_bands(self._current_resolution())
            self._update_stretch_defaults()

    def _current_resolution(self) -> str:
        if self._res_combo and self._res_combo.currentData():
            return self._res_combo.currentData()
        if self._info and self._info.resolutions:
            return self._info.resolutions[0]
        return ""

    def _apply_preset(self, preset_name: str):
        presets = band_presets.get_presets(self._satellite)
        bands = presets.get(preset_name, ())
        if bands:
            self._last_preset_bands = bands
            band_set = {b.upper() for b in bands}
            for name, cb in self._band_checks.items():
                cb.setChecked(name.upper() in band_set)

    def _set_all_bands(self, checked: bool):
        for cb in self._band_checks.values():
            cb.setChecked(checked)

    def _update_ok_state(self):
        any_checked = any(cb.isChecked() for cb in self._band_checks.values())
        self._ok_btn.setEnabled(any_checked and self._info is not None)

    def _update_stretch_defaults(self):
        res = self._current_resolution()
        if not self._info:
            return
        dtype = self._info.dtype_per_resolution.get(res, "")
        from .geozarr_provider import _STRETCH_DEFAULTS, _DTYPE_DEFAULTS
        lo, hi = None, None
        if self._satellite and dtype:
            lo, hi = _STRETCH_DEFAULTS.get((self._satellite, dtype), (None, None))
        if lo is None and dtype:
            lo, hi = _DTYPE_DEFAULTS.get(dtype, (None, None))
        if lo is not None:
            self._stretch_min.setValue(lo)
            self._stretch_max.setValue(hi)
        else:
            self._stretch_min.setValue(0.0)
            self._stretch_max.setValue(1.0)

    # -- Public accessors -----------------------------------------------------

    def selected_resolution(self) -> str:
        return self._current_resolution()

    def selected_bands(self) -> List[str]:
        checked = [
            cb.property("band_id")
            for cb in self._band_checks.values()
            if cb.isChecked()
        ]
        if hasattr(self, "_last_preset_bands") and self._last_preset_bands:
            preset_set = {b.upper() for b in self._last_preset_bands}
            checked_set = {b.upper() for b in checked}
            if preset_set == checked_set:
                upper_to_actual = {b.upper(): b for b in checked}
                return [upper_to_actual[b.upper()] for b in self._last_preset_bands]
        return checked

    def stretch_range(self) -> Optional[Tuple[float, float]]:
        lo = self._stretch_min.value()
        hi = self._stretch_max.value()
        return (lo, hi) if hi > lo else None

    def search_results(self) -> Tuple[Optional[ZarrRootInfo], List[TimeSeriesItem]]:
        return self._info, self._items
