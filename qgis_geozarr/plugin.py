"""GeoZarr plugin: GeoZarr-aware loading for QGIS STAC browser."""

from __future__ import annotations

import logging
import re

from qgis.core import Qgis, QgsSettings
from qgis.gui import QgisInterface, QgsGui
from qgis.PyQt.QtCore import Qt, QThread, pyqtSignal
from qgis.PyQt.QtWidgets import (
    QAction,
    QApplication,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
)

import os

from . import gdal_config, geozarr_metadata
from .geozarr_provider import GeoZarrDataItemGuiProvider, cleanup_temp_files

log = logging.getLogger(__name__)

# Minimum GDAL version for Zarr v3 sharding
_MIN_GDAL = (3, 13)
_SETTINGS_KEY = "GeoZarr/recent_urls"
_MAX_RECENT = 10
_URL_RE = re.compile(r"^https?://|^s3://", re.IGNORECASE)


class _FetchThread(QThread):
    """Background thread for zarr.json fetch."""

    finished = pyqtSignal(object, str)  # (ZarrRootInfo | None, final_url)

    def __init__(self, url: str):
        super().__init__()
        self.url = url

    def run(self):
        info, url = geozarr_metadata.fetch_resolved(self.url)
        self.finished.emit(info, url)


class GeoZarrPlugin:
    def __init__(self, iface: QgisInterface):
        self._iface = iface
        self._provider = None
        self._toolbar = None
        self._action = None
        self._fetch_thread = None

    def initGui(self) -> None:
        from osgeo import gdal

        ver = gdal.VersionInfo("VERSION_NUM")
        major = int(ver) // 1000000
        minor = (int(ver) // 10000) % 100
        if (major, minor) < _MIN_GDAL:
            log.warning(
                "GeoZarr: GDAL %d.%d detected, %d.%d+ required for Zarr v3 sharding",
                major,
                minor,
                *_MIN_GDAL,
            )

        gdal_config.apply()

        # Register browser context menu provider with iface for message bar
        self._provider = GeoZarrDataItemGuiProvider(iface=self._iface)
        QgsGui.dataItemGuiProviderRegistry().addProvider(self._provider)

        # Toolbar with standalone URL entry
        self._toolbar = self._iface.addToolBar("GeoZarr")
        self._toolbar.setObjectName("GeoZarrToolbar")

        from qgis.PyQt.QtGui import QIcon

        icon_path = os.path.join(os.path.dirname(__file__), "icon.svg")
        self._action = QAction(
            QIcon(icon_path), "Load GeoZarr URL...", self._toolbar
        )
        self._action.setToolTip(
            "Load a GeoZarr dataset from a Zarr v3 URL"
        )
        self._action.triggered.connect(self._load_from_url)
        self._toolbar.addAction(self._action)
        self._iface.addPluginToRasterMenu("GeoZarr", self._action)

    def unload(self) -> None:
        if self._fetch_thread and self._fetch_thread.isRunning():
            self._fetch_thread.finished.disconnect()
            self._fetch_thread.wait(3000)
        self._fetch_thread = None

        if self._provider:
            self._provider.stop_fetch()
            QgsGui.dataItemGuiProviderRegistry().removeProvider(self._provider)
            self._provider = None

        if self._action:
            self._iface.removePluginRasterMenu("GeoZarr", self._action)
        if self._toolbar:
            del self._toolbar
            self._toolbar = None

        gdal_config.restore()
        geozarr_metadata.clear_cache()
        cleanup_temp_files()

    def _load_from_url(self) -> None:
        """Standalone entry: paste a Zarr URL to load."""
        dlg = _UrlDialog(self._iface.mainWindow())
        if dlg.exec_() != QDialog.Accepted:
            return

        url = dlg.url().strip()
        if not url:
            return

        _save_recent_url(url)

        QApplication.setOverrideCursor(Qt.WaitCursor)
        self._iface.messageBar().pushMessage(
            "GeoZarr", "Fetching zarr.json...", Qgis.Info, 0
        )

        self._fetch_thread = _FetchThread(url)
        self._fetch_thread.finished.connect(self._on_url_fetch_done)
        self._fetch_thread.start()

    def _on_url_fetch_done(self, info, url) -> None:
        """Handle completed metadata fetch."""
        QApplication.restoreOverrideCursor()
        self._iface.messageBar().clearWidgets()

        if info is None:
            QMessageBox.warning(
                self._iface.mainWindow(),
                "GeoZarr",
                f"Could not read zarr.json from:\n{url}\n\n"
                "Check that the URL points to a Zarr v3 store root.",
            )
            return

        if not info.resolutions:
            QMessageBox.warning(
                self._iface.mainWindow(),
                "GeoZarr",
                "No resolutions/bands found in zarr.json.\n\n"
                "Expected resolution groups (r10m, r20m) with band arrays.",
            )
            return

        from .geozarr_dialog import GeoZarrLoadDialog

        band_dlg = GeoZarrLoadDialog(
            info,
            parent=self._iface.mainWindow(),
            zarr_url=url,
        )
        if band_dlg.exec_() != band_dlg.Accepted:
            return

        bands = band_dlg.selected_bands()
        resolution = band_dlg.selected_resolution()
        layer_name = band_dlg.layer_name()

        if not bands:
            return

        QApplication.setOverrideCursor(Qt.WaitCursor)
        self._iface.messageBar().pushMessage(
            "GeoZarr", "Building VRT and loading...", Qgis.Info, 0
        )
        try:
            from .geozarr_provider import _create_layer

            _create_layer(url, bands, resolution, layer_name, info)
        finally:
            QApplication.restoreOverrideCursor()
            self._iface.messageBar().clearWidgets()


def _load_recent_urls() -> list:
    """Load recent URLs from QgsSettings."""
    s = QgsSettings()
    urls = s.value(_SETTINGS_KEY, [])
    if isinstance(urls, str):
        return [urls] if urls else []
    return list(urls) if urls else []


def _save_recent_url(url: str) -> None:
    """Add URL to front of recent list, dedup, cap at _MAX_RECENT."""
    urls = _load_recent_urls()
    if url in urls:
        urls.remove(url)
    urls.insert(0, url)
    urls = urls[:_MAX_RECENT]
    QgsSettings().setValue(_SETTINGS_KEY, urls)


class _UrlDialog(QDialog):
    """URL entry dialog with recent URLs and paste button."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Load GeoZarr URL")
        self.setMinimumWidth(500)

        layout = QVBoxLayout(self)

        layout.addWidget(QLabel("Zarr v3 dataset URL:"))

        url_row = QHBoxLayout()
        self._combo = QComboBox()
        self._combo.setEditable(True)
        self._combo.setInsertPolicy(QComboBox.NoInsert)
        self._combo.setSizePolicy(
            self._combo.sizePolicy().horizontalPolicy(),
            self._combo.sizePolicy().verticalPolicy(),
        )

        # Populate with recent URLs
        for url in _load_recent_urls():
            self._combo.addItem(url)
        self._combo.setCurrentText("")

        url_row.addWidget(self._combo, stretch=1)

        paste_btn = QPushButton("Paste")
        paste_btn.setToolTip("Paste URL from clipboard")
        paste_btn.clicked.connect(self._paste)
        url_row.addWidget(paste_btn)

        layout.addLayout(url_row)

        # Validation hint
        self._hint = QLabel("")
        self._hint.setStyleSheet("color: #888; font-size: 11px;")
        layout.addWidget(self._hint)
        self._combo.editTextChanged.connect(self._validate)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        self._ok_btn = buttons.button(QDialogButtonBox.Ok)
        self._ok_btn.setEnabled(False)
        layout.addWidget(buttons)

    def _paste(self) -> None:
        clipboard = QApplication.clipboard()
        text = clipboard.text().strip()
        if text:
            self._combo.setCurrentText(text)

    def _validate(self, text: str) -> None:
        text = text.strip()
        if not text:
            self._hint.setText("")
            self._ok_btn.setEnabled(False)
        elif _URL_RE.match(text):
            self._hint.setText("")
            self._ok_btn.setEnabled(True)
        else:
            self._hint.setText("URL should start with https:// or s3://")
            self._ok_btn.setEnabled(False)

    def url(self) -> str:
        return self._combo.currentText()
