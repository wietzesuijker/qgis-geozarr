"""GeoZarr plugin: GeoZarr-aware loading for QGIS STAC browser."""

from __future__ import annotations

import logging

from qgis.gui import QgisInterface, QgsGui
from qgis.PyQt.QtWidgets import QAction, QInputDialog, QMessageBox

from . import gdal_config, geozarr_metadata
from .geozarr_provider import GeoZarrDataItemGuiProvider

log = logging.getLogger(__name__)

# Minimum GDAL version for Zarr v3 sharding
_MIN_GDAL = (3, 13)


class GeoZarrPlugin:
    def __init__(self, iface: QgisInterface):
        self._iface = iface
        self._provider = None
        self._toolbar = None
        self._action = None

    def initGui(self) -> None:
        # Check GDAL version
        from osgeo import gdal
        ver = gdal.VersionInfo("VERSION_NUM")
        major = int(ver) // 1000000
        minor = (int(ver) // 10000) % 100
        if (major, minor) < _MIN_GDAL:
            log.warning(
                "GeoZarr: GDAL %d.%d detected, %d.%d+ required for Zarr v3 sharding",
                major, minor, *_MIN_GDAL,
            )

        # Apply cloud-optimized GDAL config
        gdal_config.apply()

        # Register browser context menu provider
        self._provider = GeoZarrDataItemGuiProvider()
        QgsGui.dataItemGuiProviderRegistry().addProvider(self._provider)

        # Toolbar with standalone URL entry
        self._toolbar = self._iface.addToolBar("GeoZarr")
        self._toolbar.setObjectName("GeoZarrToolbar")

        self._action = QAction("Load GeoZarr URL...", self._toolbar)
        self._action.setToolTip("Load a GeoZarr dataset from a URL")
        self._action.triggered.connect(self._load_from_url)
        self._toolbar.addAction(self._action)
        self._iface.addPluginToRasterMenu("GeoZarr", self._action)

    def unload(self) -> None:
        # Remove browser provider
        if self._provider:
            QgsGui.dataItemGuiProviderRegistry().removeProvider(self._provider)
            self._provider = None

        # Remove toolbar/menu
        if self._action:
            self._iface.removePluginRasterMenu("GeoZarr", self._action)
        if self._toolbar:
            del self._toolbar
            self._toolbar = None

        # Restore GDAL config
        gdal_config.restore()

        # Clear metadata cache
        geozarr_metadata.clear_cache()

    def _load_from_url(self) -> None:
        """Standalone entry: paste a Zarr URL to load."""
        url, ok = QInputDialog.getText(
            self._iface.mainWindow(),
            "Load GeoZarr",
            "Zarr v3 dataset URL:",
        )
        if not ok or not url.strip():
            return

        url = url.strip()

        info = geozarr_metadata.fetch(url)

        # If parser found bands under a sub-group, re-fetch from there
        # for full CRS/multiscales metadata
        if info and info.sub_group and not info.epsg:
            sub_url = f"{url}/{info.sub_group}"
            sub_info = geozarr_metadata.fetch(sub_url)
            if sub_info and sub_info.resolutions:
                url = sub_url
                info = sub_info

        if info is None:
            QMessageBox.warning(
                self._iface.mainWindow(),
                "GeoZarr",
                f"Could not read zarr.json from:\n{url}",
            )
            return

        if not info.resolutions:
            QMessageBox.warning(
                self._iface.mainWindow(),
                "GeoZarr",
                "No resolutions/bands found in zarr.json.",
            )
            return

        from .geozarr_dialog import GeoZarrLoadDialog

        dlg = GeoZarrLoadDialog(
            info,
            parent=self._iface.mainWindow(),
            zarr_url=url,
        )
        if dlg.exec_() != dlg.Accepted:
            return

        bands = dlg.selected_bands()
        resolution = dlg.selected_resolution()
        layer_name = dlg.layer_name()

        if not bands:
            return

        from .geozarr_provider import _create_layer
        _create_layer(url, bands, resolution, layer_name, info)
