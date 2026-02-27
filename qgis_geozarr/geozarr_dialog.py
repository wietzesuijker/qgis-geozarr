"""Band/resolution picker dialog for GeoZarr loading."""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

from qgis.core import QgsSettings
from qgis.PyQt.QtCore import Qt
from qgis.PyQt.QtGui import QPixmap
from qgis.PyQt.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)

from . import band_presets
from .geozarr_metadata import ZarrRootInfo


def populate_band_checkboxes(
    info: ZarrRootInfo,
    resolution: str,
    band_layout: QVBoxLayout,
    band_checks: Dict[str, QCheckBox],
    satellite: Optional[str],
    preset_buttons: Optional[List[QPushButton]] = None,
    auto_select: bool = True,
    on_change=None,
) -> None:
    """Populate band checkboxes into a layout. Shared by load + time series dialogs."""
    # Clear existing
    for cb in band_checks.values():
        band_layout.removeWidget(cb)
        cb.deleteLater()
    band_checks.clear()

    bands = info.bands_per_resolution.get(resolution, ())
    for b in bands:
        desc = info.band_descriptions.get(b, "")
        label = band_presets.get_band_label(satellite, b, desc)
        cb = QCheckBox(label)
        tooltip = band_presets.get_band_tooltip(satellite, b)
        if tooltip != b:
            cb.setToolTip(tooltip)
        cb.setProperty("band_id", b)
        if on_change:
            cb.toggled.connect(on_change)
        band_checks[b] = cb
        band_layout.addWidget(cb)

    band_layout.addStretch()

    # Enable/disable preset buttons
    if preset_buttons:
        available = {b.upper() for b in bands}
        for btn in preset_buttons:
            pbands = btn.property("preset_bands")
            btn.setEnabled(all(x.upper() in available for x in pbands))

    # Auto-select default preset or first 3
    if auto_select:
        if satellite:
            default = band_presets.default_preset(satellite)
            if default:
                band_set = {x.upper() for x in default}
                for name, cb in band_checks.items():
                    cb.setChecked(name.upper() in band_set)
                return
        for i, cb in enumerate(band_checks.values()):
            if i < 3:
                cb.setChecked(True)


class GeoZarrLoadDialog(QDialog):
    """Dialog for selecting bands and resolution from a GeoZarr dataset."""

    def __init__(
        self,
        info: ZarrRootInfo,
        parent=None,
        collection_id: str = "",
        zarr_url: str = "",
        item_name: str = "",
        stac_properties: dict = None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Load GeoZarr")
        self.setMinimumWidth(540)
        self.setMinimumHeight(500)
        self._info = info
        self._item_name = item_name
        self._stac_props = stac_properties or {}
        self._band_checks: Dict[str, QCheckBox] = {}
        self._preset_buttons: List[QPushButton] = []
        self._res_combo = None
        self._thumb_label: Optional[QLabel] = None
        self._last_preset_bands: Optional[Tuple[str, ...]] = None
        self._satellite = (
            band_presets.detect_satellite(collection_id) if collection_id else None
        )

        layout = QVBoxLayout(self)
        layout.setSpacing(8)

        self._build_header(layout, zarr_url, info)
        self._build_resolution_selector(layout, info)
        self._build_preset_buttons(layout)
        self._build_band_area(layout)
        self._build_stretch_controls(layout)
        self._build_footer(layout, zarr_url)

        # Populate initial bands
        self._populate_bands(self._current_resolution())

    def set_thumbnail(self, data: bytes) -> None:
        """Called async when thumbnail arrives from fetch thread."""
        if not data or not self._header_row:
            return
        pm = QPixmap()
        pm.loadFromData(data)
        if pm.isNull():
            return
        pm = pm.scaledToHeight(120, Qt.TransformationMode.SmoothTransformation)
        self._thumb_label = QLabel()
        self._thumb_label.setPixmap(pm)
        self._thumb_label.setFixedSize(pm.size())
        self._header_row.insertWidget(0, self._thumb_label, 0, Qt.AlignmentFlag.AlignTop)

    def _build_header(self, layout, zarr_url, info):
        """Source URL + thumbnail + dataset info summary."""
        self._header_row = QHBoxLayout()

        # Text info (thumbnail inserted async via set_thumbnail)
        text_col = QVBoxLayout()
        if zarr_url:
            url_edit = QLineEdit(zarr_url)
            url_edit.setReadOnly(True)
            url_edit.setFrame(False)
            url_edit.setStyleSheet(
                "QLineEdit { background: transparent; color: #666; font-size: 11px; }"
            )
            url_edit.setToolTip("Source URL (select to copy)")
            text_col.addWidget(url_edit)

        info_parts = self._build_info_lines(info, self._stac_props)
        if info_parts:
            info_label = QLabel(
                "<span style='color:#555; font-size:11px'>"
                + " &middot; ".join(info_parts)
                + "</span>"
            )
            info_label.setWordWrap(True)
            info_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
            text_col.addWidget(info_label)

        text_col.addStretch()
        self._header_row.addLayout(text_col, 1)
        layout.addLayout(self._header_row)

    def _build_resolution_selector(self, layout, info):
        """Resolution combo box (shown only when > 1 resolution)."""
        if len(info.resolutions) <= 1:
            return
        res_group = QGroupBox("Resolution")
        res_layout = QHBoxLayout(res_group)
        self._res_combo = QComboBox()
        for res in info.resolutions:
            label = self._resolution_label(res)
            self._res_combo.addItem(label, res)
        self._res_combo.currentIndexChanged.connect(self._on_resolution_changed)
        res_layout.addWidget(self._res_combo)
        layout.addWidget(res_group)

    def _build_preset_buttons(self, layout):
        """Satellite band preset buttons."""
        if not self._satellite:
            return
        presets = band_presets.get_presets(self._satellite)
        if not presets:
            return
        preset_group = QGroupBox(f"Presets ({self._satellite})")
        preset_layout = QHBoxLayout(preset_group)
        for name in list(presets.keys())[:4]:
            btn = QPushButton(name.replace("_", " ").title())
            tooltip = band_presets.get_preset_tooltip(self._satellite, name)
            if tooltip:
                btn.setToolTip(tooltip)
            btn.clicked.connect(
                lambda checked, n=name: self._apply_preset(n)
            )
            btn.setProperty("preset_bands", presets[name])
            preset_layout.addWidget(btn)
            self._preset_buttons.append(btn)
        layout.addWidget(preset_group)

    def _build_band_area(self, layout):
        """Scrollable band checkbox area with All/None buttons."""
        bands_header = QHBoxLayout()
        self._bands_label = QLabel("<b>Bands</b>")
        bands_header.addWidget(self._bands_label)
        bands_header.addStretch()
        btn_all = QPushButton("All")
        btn_all.setFixedWidth(40)
        btn_all.setToolTip("Select all bands")
        btn_all.clicked.connect(lambda: self._set_all_bands(True))
        btn_none = QPushButton("None")
        btn_none.setFixedWidth(46)
        btn_none.setToolTip("Clear band selection")
        btn_none.clicked.connect(lambda: self._set_all_bands(False))
        bands_header.addWidget(btn_all)
        bands_header.addWidget(btn_none)
        layout.addLayout(bands_header)

        self._band_widget = QWidget()
        self._band_layout = QVBoxLayout(self._band_widget)
        self._band_layout.setContentsMargins(4, 4, 4, 4)
        self._band_layout.setSpacing(2)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(self._band_widget)
        scroll.setFrameShape(scroll.Shape.NoFrame)
        scroll.setMinimumHeight(180)
        layout.addWidget(scroll, stretch=1)

    def _build_stretch_controls(self, layout):
        """Min/max stretch range for RGB display."""
        stretch_layout = QHBoxLayout()
        stretch_layout.addWidget(QLabel("Stretch:"))
        self._stretch_min = QDoubleSpinBox()
        self._stretch_min.setDecimals(4)
        self._stretch_min.setRange(-1e9, 1e9)
        self._stretch_min.setToolTip("Minimum value for RGB stretch")
        self._stretch_max = QDoubleSpinBox()
        self._stretch_max.setDecimals(4)
        self._stretch_max.setRange(-1e9, 1e9)
        self._stretch_max.setToolTip("Maximum value for RGB stretch")
        stretch_layout.addWidget(QLabel("Min"))
        stretch_layout.addWidget(self._stretch_min)
        stretch_layout.addWidget(QLabel("Max"))
        stretch_layout.addWidget(self._stretch_max)
        stretch_layout.addStretch()
        layout.addLayout(stretch_layout)
        self._update_stretch_defaults()

    def _update_stretch_defaults(self):
        """Pre-populate stretch range from saved settings, metadata, satellite, or dtype."""
        res = self._current_resolution()
        info = self._info
        dtype = info.dtype_per_resolution.get(res, "")

        # Priority 0: restore last user-set stretch for this (satellite, dtype)
        sat_key = self._satellite or "generic"
        settings_key = f"GeoZarr/stretch/{sat_key}/{dtype}"
        s = QgsSettings()
        saved_lo = s.value(f"{settings_key}/min")
        saved_hi = s.value(f"{settings_key}/max")
        if saved_lo is not None and saved_hi is not None:
            try:
                self._stretch_min.setValue(float(saved_lo))
                self._stretch_max.setValue(float(saved_hi))
                return
            except (ValueError, TypeError):
                pass

        # Try metadata valid_range (first band with data wins)
        bands = info.bands_per_resolution.get(res, ())
        for band in bands[:3]:
            vr = info.valid_range_per_band.get(band)
            if vr:
                self._stretch_min.setValue(vr[0])
                # Use 30% of max for reflectance-like display range
                sf = info.scale_per_band.get(band)
                if sf and sf > 1:
                    self._stretch_max.setValue(vr[1] * 0.3)
                else:
                    self._stretch_max.setValue(vr[1])
                return

        # Try satellite+dtype defaults, then dtype defaults
        from .geozarr_provider import _STRETCH_DEFAULTS, _DTYPE_DEFAULTS
        lo, hi = None, None
        if self._satellite and dtype:
            lo, hi = _STRETCH_DEFAULTS.get((self._satellite, dtype), (None, None))
        if lo is None and dtype:
            lo, hi = _DTYPE_DEFAULTS.get(dtype, (None, None))
        if lo is not None:
            self._stretch_min.setValue(lo)
            self._stretch_max.setValue(hi)
            return

        # No info - leave at 0/1
        self._stretch_min.setValue(0.0)
        self._stretch_max.setValue(1.0)

    def stretch_range(self) -> Optional[Tuple[float, float]]:
        """Return user-set (min, max) stretch range."""
        lo = self._stretch_min.value()
        hi = self._stretch_max.value()
        if hi > lo:
            return (lo, hi)
        return None

    def _build_footer(self, layout, zarr_url):
        """Layer name field + OK/Cancel buttons."""
        name_layout = QHBoxLayout()
        name_layout.addWidget(QLabel("Layer name:"))
        self._name_edit = QLineEdit()
        self._name_edit.setText(self._default_layer_name(zarr_url))
        name_layout.addWidget(self._name_edit)
        layout.addLayout(name_layout)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    @staticmethod
    def _build_info_lines(info: ZarrRootInfo, stac_props: dict = None) -> list:
        """Build compact info strings for the dataset header."""
        parts = []
        if info.epsg:
            parts.append(f"EPSG:{info.epsg}")
        if info.geotransform and info.shape_per_resolution:
            gt = info.geotransform
            shape = max(
                info.shape_per_resolution.values(), key=lambda s: s[0] * s[1],
            )
            x_min = gt[0]
            y_max = gt[3]
            x_max = x_min + gt[1] * shape[1]
            y_min = y_max + gt[5] * shape[0]
            fmt = ".4f" if info.epsg == 4326 else ".0f"
            parts.append(
                f"{x_min:{fmt}}, {y_min:{fmt}} - {x_max:{fmt}}, {y_max:{fmt}}"
            )
        total_bands = sum(len(b) for b in info.bands_per_resolution.values())
        parts.append(f"{len(info.resolutions)} res, {total_bands} bands")
        if info.conventions:
            parts.append(", ".join(info.conventions))
        # STAC quality properties
        if stac_props:
            cc = stac_props.get("eo:cloud_cover")
            if cc is not None:
                if cc < 20:
                    cc_color = "#2a7"
                elif cc < 50:
                    cc_color = "#c90"
                else:
                    cc_color = "#c33"
                parts.append(
                    f"<span style='color:{cc_color}; font-weight:bold'>"
                    f"Cloud: {cc:.0f}%</span>"
                )
            proc = stac_props.get("processing:level") or stac_props.get(
                "processing_level"
            )
            if proc:
                parts.append(proc)
            sun_el = stac_props.get("view:sun_elevation")
            if sun_el is not None:
                parts.append(f"Sun el: {sun_el:.0f}\u00b0")
            orbit = stac_props.get("sat:orbit_state")
            if orbit:
                parts.append(orbit.capitalize())
        return parts

    def _resolution_label(self, res: str) -> str:
        """Format resolution for display: 'r10m - 10980 x 10980 (10 m/px, ~450 MB)'."""
        shape = self._info.shape_per_resolution.get(res)
        m = re.search(r"(\d+)", res)
        px_size = m.group(1) if m else ""
        parts = [res]
        if shape:
            parts.append(f"{shape[1]} x {shape[0]}")
        if px_size:
            parts.append(f"{px_size} m/px")
        if shape:
            dtype = self._info.dtype_per_resolution.get(res, "")
            bpp = {"Byte": 1, "UInt16": 2, "Int16": 2, "UInt32": 4,
                    "Int32": 4, "Float32": 4, "Float64": 8}.get(dtype, 2)
            size_mb = shape[0] * shape[1] * bpp * 3 / 1e6
            if size_mb >= 1000:
                parts.append(f"~{size_mb / 1000:.1f} GB/3 bands")
            else:
                parts.append(f"~{size_mb:.0f} MB/3 bands")
        return " - ".join(parts)

    def _current_resolution(self) -> str:
        if self._res_combo:
            return self._res_combo.currentData()
        return self._info.resolutions[0] if self._info.resolutions else ""

    def _populate_bands(self, resolution: str) -> None:
        populate_band_checkboxes(
            self._info, resolution, self._band_layout,
            self._band_checks, self._satellite, self._preset_buttons,
            on_change=self._update_preset_highlight,
        )
        count = len(self._band_checks)
        self._bands_label.setText(f"<b>Bands ({count})</b>")
        self._update_preset_highlight()
        # Record default preset order so selected_bands() returns R,G,B
        if self._satellite:
            default = band_presets.default_preset(self._satellite)
            if default:
                self._last_preset_bands = default

    def _on_resolution_changed(self, _index: int) -> None:
        self._populate_bands(self._current_resolution())
        self._update_stretch_defaults()

    def _update_preset_highlight(self, _checked=None) -> None:
        """Highlight the preset button matching current band selection."""
        selected = {b.upper() for b in self.selected_bands()}
        for btn in self._preset_buttons:
            pbands = btn.property("preset_bands")
            if pbands and {b.upper() for b in pbands} == selected:
                btn.setStyleSheet(
                    "QPushButton { font-weight: bold; border: 2px solid #4a90d9; }"
                )
            else:
                btn.setStyleSheet("")

    def _apply_preset(self, preset_name: str) -> None:
        presets = band_presets.get_presets(self._satellite)
        if not presets:
            return
        bands = presets.get(preset_name)
        if bands:
            self._last_preset_bands = bands  # preserve R,G,B order
            self._select_bands(bands)

    def _select_bands(self, bands: tuple) -> None:
        """Check only the bands in the tuple, uncheck others."""
        band_set = {b.upper() for b in bands}
        for name, cb in self._band_checks.items():
            cb.setChecked(name.upper() in band_set)

    def _set_all_bands(self, checked: bool) -> None:
        for cb in self._band_checks.values():
            cb.setChecked(checked)

    def _default_layer_name(self, url: str) -> str:
        if self._item_name:
            return self._item_name
        if not url:
            return "GeoZarr"
        parts = url.rstrip("/").split("/")
        for part in reversed(parts):
            if part and not part.startswith("http"):
                return part.replace(".zarr", "")
        return "GeoZarr"

    def selected_resolution(self) -> str:
        return self._current_resolution()

    def selected_bands(self) -> List[str]:
        checked = [
            cb.property("band_id")
            for cb in self._band_checks.values()
            if cb.isChecked()
        ]
        # If a preset was applied and selection still matches, preserve
        # preset order (e.g. B04,B03,B02 for True Color = R,G,B).
        # Use original metadata case (server paths may be case-sensitive).
        if self._last_preset_bands:
            preset_set = {b.upper() for b in self._last_preset_bands}
            checked_set = {b.upper() for b in checked}
            if preset_set == checked_set:
                upper_to_actual = {b.upper(): b for b in checked}
                return [upper_to_actual[b.upper()] for b in self._last_preset_bands]
        return checked

    def accept(self):
        """Save stretch range to settings before closing."""
        res = self._current_resolution()
        dtype = self._info.dtype_per_resolution.get(res, "")
        if dtype:
            sat_key = self._satellite or "generic"
            key = f"GeoZarr/stretch/{sat_key}/{dtype}"
            QgsSettings().setValue(f"{key}/min", self._stretch_min.value())
            QgsSettings().setValue(f"{key}/max", self._stretch_max.value())
        super().accept()

    def layer_name(self) -> str:
        return self._name_edit.text() or "GeoZarr"
