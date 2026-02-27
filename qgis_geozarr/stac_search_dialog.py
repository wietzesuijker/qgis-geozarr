"""STAC time series search dialog: bbox, date range, band picker."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Optional

from qgis.PyQt.QtCore import QDate, Qt
from qgis.PyQt.QtWidgets import (
    QCheckBox,
    QDateEdit,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from . import band_presets
from .geozarr_dialog import populate_band_checkboxes
from .geozarr_metadata import ZarrRootInfo


class StacSearchDialog(QDialog):
    """Dialog for STAC time series search + band selection."""

    def __init__(
        self,
        collection_id: str,
        parent=None,
        item_datetime: str = "",
        map_extent: tuple = None,  # (xmin, ymin, xmax, ymax) in EPSG:4326
    ):
        super().__init__(parent)
        self.setWindowTitle("Load Time Series")
        self.setMinimumWidth(480)
        self.setMinimumHeight(500)

        self._collection_id = collection_id
        self._satellite = band_presets.detect_satellite(collection_id) if collection_id else None
        self._band_checks: Dict[str, QCheckBox] = {}
        self._preset_buttons: List[QPushButton] = []
        self._info: Optional[ZarrRootInfo] = None
        self._items: list = []

        layout = QVBoxLayout(self)
        layout.setSpacing(8)

        self._build_collection_header(layout, collection_id)
        self._build_bbox_group(layout, map_extent)
        self._build_date_group(layout, item_datetime)
        self._build_search_section(layout)
        self._build_band_section(layout)
        self._build_footer(layout)

        # Callbacks set by provider
        self._search_callback = None

    def _build_collection_header(self, layout, collection_id: str) -> None:
        """Collection ID (read-only)."""
        coll_row = QHBoxLayout()
        coll_row.addWidget(QLabel("Collection:"))
        coll_edit = QLineEdit(collection_id)
        coll_edit.setReadOnly(True)
        coll_edit.setFrame(False)
        coll_edit.setStyleSheet(
            "QLineEdit { background: transparent; font-weight: bold; }"
        )
        coll_row.addWidget(coll_edit, stretch=1)
        layout.addLayout(coll_row)

    def _build_bbox_group(self, layout, map_extent) -> None:
        """Bounding box coordinate spinners."""
        bbox_group = QGroupBox("Area")
        bbox_layout = QVBoxLayout(bbox_group)
        self._use_extent = QCheckBox("Use map extent")
        self._use_extent.setChecked(map_extent is not None)
        self._use_extent.toggled.connect(self._on_extent_toggled)
        bbox_layout.addWidget(self._use_extent)

        coord_row = QHBoxLayout()
        defaults = map_extent or (-180.0, -90.0, 180.0, 90.0)
        self._bbox_spins = []
        for label_text, val, lo, hi in [
            ("W", defaults[0], -180, 180), ("S", defaults[1], -90, 90),
            ("E", defaults[2], -180, 180), ("N", defaults[3], -90, 90),
        ]:
            coord_row.addWidget(QLabel(label_text))
            spin = QDoubleSpinBox()
            spin.setRange(lo, hi)
            spin.setDecimals(2)
            spin.setValue(val)
            spin.setEnabled(map_extent is None)
            coord_row.addWidget(spin)
            self._bbox_spins.append(spin)
        bbox_layout.addLayout(coord_row)
        layout.addWidget(bbox_group)

    def _build_date_group(self, layout, item_datetime: str) -> None:
        """Date range pickers + max items spinner."""
        date_group = QGroupBox("Date range")
        date_layout = QHBoxLayout(date_group)

        center_date = None
        if item_datetime:
            try:
                center_date = datetime.fromisoformat(
                    item_datetime.replace("Z", "+00:00")
                )
            except (ValueError, TypeError):
                pass

        if center_date:
            date_start = (center_date - timedelta(days=183)).date()
            date_end = (center_date + timedelta(days=183)).date()
        else:
            date_end = datetime.now().date()
            date_start = (datetime.now() - timedelta(days=365)).date()

        self._date_start = QDateEdit()
        self._date_start.setCalendarPopup(True)
        self._date_start.setDate(QDate(date_start.year, date_start.month, date_start.day))
        self._date_end = QDateEdit()
        self._date_end.setCalendarPopup(True)
        self._date_end.setDate(QDate(date_end.year, date_end.month, date_end.day))

        date_layout.addWidget(self._date_start)
        date_layout.addWidget(QLabel("to"))
        date_layout.addWidget(self._date_end)
        date_layout.addStretch()

        date_layout.addWidget(QLabel("Max:"))
        self._max_items = QSpinBox()
        self._max_items.setRange(1, 500)
        self._max_items.setValue(24)
        date_layout.addWidget(self._max_items)
        layout.addWidget(date_group)

    def _build_search_section(self, layout) -> None:
        """Search button + results label."""
        search_row = QHBoxLayout()
        self._search_btn = QPushButton("Search")
        self._search_btn.clicked.connect(self._on_search)
        search_row.addWidget(self._search_btn)
        self._results_label = QLabel("")
        self._results_label.setStyleSheet("color: #555;")
        search_row.addWidget(self._results_label, stretch=1)
        layout.addLayout(search_row)

    def _build_band_section(self, layout) -> None:
        """Band selection group with presets + scrollable checkboxes (hidden until search)."""
        self._band_group = QGroupBox("Band")
        band_group_layout = QVBoxLayout(self._band_group)

        if self._satellite:
            presets = band_presets.get_presets(self._satellite)
            if presets:
                preset_row = QHBoxLayout()
                for name in list(presets.keys())[:4]:
                    btn = QPushButton(name.replace("_", " ").title())
                    tooltip = band_presets.get_preset_tooltip(self._satellite, name)
                    if tooltip:
                        btn.setToolTip(tooltip)
                    btn.clicked.connect(
                        lambda checked, n=name: self._apply_preset(n)
                    )
                    btn.setProperty("preset_bands", presets[name])
                    preset_row.addWidget(btn)
                    self._preset_buttons.append(btn)
                band_group_layout.addLayout(preset_row)

        self._band_widget = QWidget()
        self._band_layout = QVBoxLayout(self._band_widget)
        self._band_layout.setContentsMargins(4, 4, 4, 4)
        self._band_layout.setSpacing(2)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(self._band_widget)
        scroll.setFrameShape(scroll.Shape.NoFrame)
        scroll.setMinimumHeight(120)
        band_group_layout.addWidget(scroll, stretch=1)

        self._band_group.setVisible(False)
        layout.addWidget(self._band_group, stretch=1)

    def _build_footer(self, layout) -> None:
        """Layer name field + OK/Cancel buttons."""
        name_row = QHBoxLayout()
        name_row.addWidget(QLabel("Layer name:"))
        self._name_edit = QLineEdit()
        name_row.addWidget(self._name_edit)
        layout.addLayout(name_row)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        self._ok_btn = buttons.button(QDialogButtonBox.StandardButton.Ok)
        self._ok_btn.setText("Load")
        self._ok_btn.setEnabled(False)
        layout.addWidget(buttons)

    def set_search_callback(self, callback) -> None:
        """Set callback for search: callback(dialog, bbox, datetime_range, limit)."""
        self._search_callback = callback

    def on_search_error(self, msg: str) -> None:
        """Called when background search fails."""
        self._search_btn.setEnabled(True)
        self._search_btn.setText("Search")
        self._results_label.setText(f"Search failed: {msg}")
        self._results_label.setStyleSheet("color: #c00;")

    def on_search_results(self, info: Optional[ZarrRootInfo], items: list) -> None:
        """Called when background search completes."""
        self._search_btn.setEnabled(True)
        self._search_btn.setText("Search")
        self._results_label.setStyleSheet("color: #555;")

        if not items:
            self._results_label.setText("No items found")
            self._band_group.setVisible(False)
            self._ok_btn.setEnabled(False)
            return

        self._info = info
        self._items = items

        # Date range + cloud cover summary
        dates = sorted(it["datetime"][:10] for it in items)
        first = dates[0] if dates else ""
        last = dates[-1] if dates else ""
        summary = f"Found {len(items)} items ({first} to {last})"
        cc_values = [it["cloud_cover"] for it in items if "cloud_cover" in it]
        if cc_values:
            avg_cc = sum(cc_values) / len(cc_values)
            summary += f", avg {avg_cc:.0f}% cloud"
        self._results_label.setText(summary)

        # Populate bands
        if info and info.resolutions:
            res = info.resolutions[0]  # time series uses finest resolution
            populate_band_checkboxes(
                info, res, self._band_layout,
                self._band_checks, self._satellite, self._preset_buttons,
                auto_select=False,
            )
            # For time series, default to first band only
            if self._band_checks:
                first_cb = next(iter(self._band_checks.values()))
                first_cb.setChecked(True)
            self._band_group.setVisible(True)
            self._ok_btn.setEnabled(True)

            # Default layer name
            self._name_edit.setText(
                f"{self._collection_id} ({first[:7]} to {last[:7]}, "
                f"{len(items)} dates)"
            )

    def _on_extent_toggled(self, checked: bool) -> None:
        for spin in self._bbox_spins:
            spin.setEnabled(not checked)

    def _on_search(self) -> None:
        if not self._search_callback:
            return

        bbox = None
        if self._use_extent.isChecked() or any(
            s.value() != d for s, d in
            zip(self._bbox_spins, [-180.0, -90.0, 180.0, 90.0])
        ):
            bbox = tuple(s.value() for s in self._bbox_spins)

        ds = self._date_start.date()
        de = self._date_end.date()
        dt_range = (
            f"{ds.year():04d}-{ds.month():02d}-{ds.day():02d}T00:00:00Z/"
            f"{de.year():04d}-{de.month():02d}-{de.day():02d}T23:59:59Z"
        )

        self._search_btn.setEnabled(False)
        self._search_btn.setText("Searching...")
        self._results_label.setText("")

        self._search_callback(self, bbox, dt_range, self._max_items.value())

    def _apply_preset(self, preset_name: str) -> None:
        presets = band_presets.get_presets(self._satellite)
        if not presets:
            return
        bands = presets.get(preset_name)
        if bands:
            band_set = {b.upper() for b in bands}
            for name, cb in self._band_checks.items():
                cb.setChecked(name.upper() in band_set)

    # --- Public accessors ---

    def selected_band(self) -> str:
        """Return the first checked band ID (time series = single band)."""
        for cb in self._band_checks.values():
            if cb.isChecked():
                return cb.property("band_id")
        return ""

    def selected_bands(self) -> List[str]:
        """Return all checked band IDs."""
        return [
            cb.property("band_id")
            for cb in self._band_checks.values()
            if cb.isChecked()
        ]

    def selected_resolution(self) -> str:
        if self._info and self._info.resolutions:
            return self._info.resolutions[0]
        return ""

    def layer_name(self) -> str:
        return self._name_edit.text() or f"{self._collection_id} time series"

    def search_items(self) -> list:
        return self._items

    def search_info(self) -> Optional[ZarrRootInfo]:
        return self._info
