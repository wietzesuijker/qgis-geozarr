"""Band/resolution picker dialog for GeoZarr loading."""

from __future__ import annotations

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
    QVBoxLayout,
)

from . import band_presets
from .geozarr_metadata import ZarrRootInfo


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
        self.setMinimumWidth(400)
        self._info = info
        self._item_name = item_name
        self._band_checks: Dict[str, QCheckBox] = {}
        self._preset_buttons: List[QPushButton] = []
        self._satellite = band_presets.detect_satellite(collection_id) if collection_id else None

        layout = QVBoxLayout(self)

        # URL display
        if zarr_url:
            url_label = QLabel(f"<b>Source:</b> {zarr_url}")
            url_label.setWordWrap(True)
            layout.addWidget(url_label)

        # Resolution selector
        if len(info.resolutions) > 1:
            res_group = QGroupBox("Resolution")
            res_layout = QHBoxLayout(res_group)
            self._res_combo = QComboBox()
            for res in info.resolutions:
                shape = info.shape_per_resolution.get(res)
                label = f"{res} ({shape[1]}x{shape[0]})" if shape else res
                self._res_combo.addItem(label, res)
            self._res_combo.currentIndexChanged.connect(self._on_resolution_changed)
            res_layout.addWidget(self._res_combo)
            layout.addWidget(res_group)
        else:
            self._res_combo = None

        # Band checkboxes
        self._band_group = QGroupBox("Bands")
        self._band_layout = QVBoxLayout(self._band_group)
        layout.addWidget(self._band_group)

        # Preset buttons
        if self._satellite:
            presets = band_presets.get_presets(self._satellite)
            if presets:
                preset_group = QGroupBox(f"Presets ({self._satellite})")
                preset_layout = QHBoxLayout(preset_group)
                for name in list(presets.keys())[:4]:
                    btn = QPushButton(name.replace("_", " ").title())
                    btn.clicked.connect(lambda checked, n=name: self._apply_preset(n))
                    btn.setProperty("preset_bands", presets[name])
                    preset_layout.addWidget(btn)
                    self._preset_buttons.append(btn)
                layout.addWidget(preset_group)

        # Layer name
        name_layout = QHBoxLayout()
        name_layout.addWidget(QLabel("Layer name:"))
        self._name_edit = QLineEdit()
        self._name_edit.setText(self._default_layer_name(zarr_url))
        name_layout.addWidget(self._name_edit)
        layout.addLayout(name_layout)

        # OK/Cancel
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        # Populate initial bands
        self._populate_bands(self._current_resolution())

    def _current_resolution(self) -> str:
        if self._res_combo:
            return self._res_combo.currentData()
        return self._info.resolutions[0] if self._info.resolutions else ""

    def _populate_bands(self, resolution: str) -> None:
        # Clear existing
        for cb in self._band_checks.values():
            self._band_layout.removeWidget(cb)
            cb.deleteLater()
        self._band_checks.clear()

        bands = self._info.bands_per_resolution.get(resolution, ())
        for band in bands:
            cb = QCheckBox(band)
            self._band_checks[band] = cb
            self._band_layout.addWidget(cb)

        # Enable/disable preset buttons based on available bands
        available = {b.upper() for b in bands}
        for btn in self._preset_buttons:
            preset_bands = btn.property("preset_bands")
            btn.setEnabled(all(b.upper() in available for b in preset_bands))

        # Auto-select default preset or first 3 bands
        if self._satellite:
            default = band_presets.default_preset(self._satellite)
            if default:
                self._select_bands(default)
                return
        for i, cb in enumerate(self._band_checks.values()):
            if i < 3:
                cb.setChecked(True)

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

    def _default_layer_name(self, url: str) -> str:
        if self._item_name:
            return self._item_name
        if not url:
            return "GeoZarr"
        # Extract meaningful name from URL path
        parts = url.rstrip("/").split("/")
        for part in reversed(parts):
            if part and not part.startswith("http"):
                return part.replace(".zarr", "")
        return "GeoZarr"

    def selected_resolution(self) -> str:
        return self._current_resolution()

    def selected_bands(self) -> List[str]:
        return [name for name, cb in self._band_checks.items() if cb.isChecked()]

    def layer_name(self) -> str:
        return self._name_edit.text() or "GeoZarr"
