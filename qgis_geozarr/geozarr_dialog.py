"""Band/resolution picker dialog for GeoZarr loading."""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

from qgis.PyQt.QtCore import Qt
from qgis.PyQt.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
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
    ):
        super().__init__(parent)
        self.setWindowTitle("Load GeoZarr")
        self.setMinimumWidth(460)
        self.setMinimumHeight(400)
        self._info = info
        self._item_name = item_name
        self._band_checks: Dict[str, QCheckBox] = {}
        self._preset_buttons: List[QPushButton] = []
        self._res_combo = None
        self._satellite = (
            band_presets.detect_satellite(collection_id) if collection_id else None
        )

        layout = QVBoxLayout(self)
        layout.setSpacing(8)

        self._build_header(layout, zarr_url, info)
        self._build_resolution_selector(layout, info)
        self._build_preset_buttons(layout)
        self._build_band_area(layout)
        self._build_footer(layout, zarr_url)

        # Populate initial bands
        self._populate_bands(self._current_resolution())

    def _build_header(self, layout, zarr_url, info):
        """Source URL + dataset info summary."""
        if zarr_url:
            url_edit = QLineEdit(zarr_url)
            url_edit.setReadOnly(True)
            url_edit.setFrame(False)
            url_edit.setStyleSheet(
                "QLineEdit { background: transparent; color: #666; font-size: 11px; }"
            )
            url_edit.setToolTip("Source URL (select to copy)")
            layout.addWidget(url_edit)

        info_parts = self._build_info_lines(info)
        if info_parts:
            info_label = QLabel(
                "<span style='color:#555; font-size:11px'>"
                + " &middot; ".join(info_parts)
                + "</span>"
            )
            info_label.setWordWrap(True)
            info_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
            layout.addWidget(info_label)

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
        bands_header.addWidget(QLabel("<b>Bands</b>"))
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
        scroll.setFrameShape(scroll.NoFrame)
        scroll.setMinimumHeight(180)
        layout.addWidget(scroll, stretch=1)

    def _build_footer(self, layout, zarr_url):
        """Layer name field + OK/Cancel buttons."""
        name_layout = QHBoxLayout()
        name_layout.addWidget(QLabel("Layer name:"))
        self._name_edit = QLineEdit()
        self._name_edit.setText(self._default_layer_name(zarr_url))
        name_layout.addWidget(self._name_edit)
        layout.addLayout(name_layout)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    @staticmethod
    def _build_info_lines(info: ZarrRootInfo) -> list:
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
        return parts

    def _resolution_label(self, res: str) -> str:
        """Format resolution for display: 'r10m - 10980 x 10980 (10 m/px)'."""
        shape = self._info.shape_per_resolution.get(res)
        m = re.search(r"(\d+)", res)
        px_size = m.group(1) if m else ""
        parts = [res]
        if shape:
            parts.append(f"{shape[1]} x {shape[0]}")
        if px_size:
            parts.append(f"{px_size} m/px")
        return " - ".join(parts)

    def _current_resolution(self) -> str:
        if self._res_combo:
            return self._res_combo.currentData()
        return self._info.resolutions[0] if self._info.resolutions else ""

    def _populate_bands(self, resolution: str) -> None:
        populate_band_checkboxes(
            self._info, resolution, self._band_layout,
            self._band_checks, self._satellite, self._preset_buttons,
        )

    def _on_resolution_changed(self, _index: int) -> None:
        self._populate_bands(self._current_resolution())

    def _apply_preset(self, preset_name: str) -> None:
        presets = band_presets.get_presets(self._satellite)
        if not presets:
            return
        bands = presets.get(preset_name)
        if bands:
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
        return [
            cb.property("band_id")
            for cb in self._band_checks.values()
            if cb.isChecked()
        ]

    def layer_name(self) -> str:
        return self._name_edit.text() or "GeoZarr"
