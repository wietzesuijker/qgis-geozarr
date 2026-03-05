"""Time series controller: progressive layer creation with native temporal navigation."""

from __future__ import annotations

import logging
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass

from osgeo import gdal

from qgis.core import (
    Qgis,
    QgsDateTimeRange,
    QgsInterval,
    QgsMessageLog,
    QgsProject,
    QgsRasterLayer,
    QgsRasterLayerTemporalProperties,
    QgsTemporalNavigationObject,
)
from qgis.PyQt.QtCore import QDateTime, QObject, Qt, QTimer
from qgis.PyQt.QtWidgets import QDockWidget

from . import geozarr_metadata
from .geozarr_provider import (
    _auto_style,
    _build_multiband_vrt_xml,
    _untrack_and_remove,
)
from .stac_search import TimeSeriesItem, parse_datetime

log = logging.getLogger(__name__)

_QGIS_DTYPE_NAMES = {
    Qgis.DataType.Byte: "Byte",
    Qgis.DataType.UInt16: "UInt16",
    Qgis.DataType.Int16: "Int16",
    Qgis.DataType.UInt32: "UInt32",
    Qgis.DataType.Int32: "Int32",
    Qgis.DataType.Float32: "Float32",
    Qgis.DataType.Float64: "Float64",
}


def _gdal_dtype_name(qgis_dtype) -> str:
    return _QGIS_DTYPE_NAMES.get(qgis_dtype, "")


@dataclass
class TimeSeriesState:
    """Configuration for a time series session."""

    items: list[TimeSeriesItem]
    info: geozarr_metadata.ZarrRootInfo
    bands: list[str]
    resolution: str
    satellite: str = ""
    stretch_range: tuple[float, float] | None = None


class TimeSeriesController(QObject):
    """Time series with progressive layer loading and native temporal navigation.

    First layer loads immediately for fast feedback. Remaining layers are
    created progressively as their pre-warms complete. QGIS temporal controller
    frame count updates automatically as layers are added.
    """

    def __init__(self, state: TimeSeriesState, iface, parent=None):
        super().__init__(parent)
        self._state = state
        self._iface = iface
        self._layers: list[QgsRasterLayer] = []
        self._vrt_paths: dict[int, str] = {}
        self._group = None
        self._next_index = 0  # next layer to create

        # Background thread pool for pre-warming vsicurl cache
        self._pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="ts-warm")

        # Filter items with valid dates, dedup by calendar day
        self._items: list[TimeSeriesItem] = []
        self._dates = []
        self._qdatetimes = []
        seen_dates: set[str] = set()
        n_parse_fail = 0
        n_qdt_fail = 0
        n_dedup = 0
        for item in state.items:
            dt = parse_datetime(item.datetime_str)
            if not dt:
                n_parse_fail += 1
                continue
            date_key = dt.strftime("%Y-%m-%d")
            if date_key in seen_dates:
                n_dedup += 1
                continue
            seen_dates.add(date_key)
            # UTC + no microseconds for Qt compatibility
            iso_str = dt.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
            qdt = QDateTime.fromString(iso_str, Qt.DateFormat.ISODate)
            if not qdt.isValid():
                n_qdt_fail += 1
                log.warning("Invalid QDateTime for %s (iso=%s)", item.datetime_str, iso_str)
                continue
            self._items.append(item)
            self._dates.append(dt)
            self._qdatetimes.append(qdt)

        _log_msg(
            f"Pipeline: {len(state.items)} input -> "
            f"{n_parse_fail} parse_fail, {n_dedup} dedup, "
            f"{n_qdt_fail} qdt_fail -> {len(self._items)} frames",
        )

        # Pre-build all VRTs (fast - pure XML string work, no HTTP)
        for i, item in enumerate(self._items):
            vrt = _build_vrt(item, state)
            if vrt:
                self._vrt_paths[i] = vrt

        # Pre-warm in background threads
        self._warm_futures: dict[int, Future] = {}
        for i in range(min(20, self.count)):
            if i in self._vrt_paths:
                self._warm_futures[i] = self._pool.submit(self._prewarm_item, i)

    @property
    def count(self) -> int:
        return len(self._dates)

    def start(self) -> None:
        """Configure temporal controller, then progressively add layers."""
        if not self._dates:
            return

        # Create layer group
        root = QgsProject.instance().layerTreeRoot()
        self._group = root.insertGroup(0, "Time Series")

        self._configure_temporal_controller()

        # Show temporal controller panel
        for dock in self._iface.mainWindow().findChildren(QDockWidget):
            title = dock.windowTitle().lower()
            name = dock.objectName().lower()
            if "temporal" in title or "temporal" in name:
                dock.setVisible(True)
                dock.raise_()
                break

        self._iface.messageBar().pushMessage(
            "GeoZarr", f"Loading {self.count} layers...",
            Qgis.MessageLevel.Info, 0,
        )

        # All layers load via timer - never block main thread
        self._next_index = 0
        self._load_timer = QTimer(self)
        self._load_timer.timeout.connect(self._load_next_batch)
        self._load_timer.start(100)

    def _configure_temporal_controller(self) -> None:
        """Set temporal controller to Animated + IrregularStep with full extents."""
        tc = self._iface.mapCanvas().temporalController()
        first, last = self._qdatetimes[0], self._qdatetimes[-1]

        tc.setNavigationMode(
            QgsTemporalNavigationObject.NavigationMode.Animated,
        )
        if Qgis.versionInt() >= 33600:
            tc.setFrameDuration(QgsInterval(1, Qgis.TemporalUnit.IrregularStep))
        else:
            tc.setFrameDuration(QgsInterval(1, Qgis.TemporalUnit.Days))

        tc.setTemporalExtents(QgsDateTimeRange(first, last.addSecs(1)))
        tc.rewindToStart()
        tc.setCurrentFrameNumber(0)

    def _load_next_batch(self) -> None:
        """Create next layer if its pre-warm is done, or force-create if no future."""
        if self._next_index >= self.count:
            self._load_timer.stop()
            # Re-apply temporal controller settings now that all layers are present.
            # Each addMapLayer triggered onLayersAdded which recalculated ranges
            # incrementally - now we have the complete set.
            self._configure_temporal_controller()
            tc = self._iface.mapCanvas().temporalController()
            n = len(self._layers)
            self._iface.messageBar().clearWidgets()
            self._iface.messageBar().pushMessage(
                "GeoZarr",
                f"Time series ready: {n} layers, {tc.totalFrameCount()} frames",
                Qgis.MessageLevel.Success, 5,
            )
            _log_msg(f"Ready: {n}/{self.count} layers, totalFrameCount={tc.totalFrameCount()}")
            return

        i = self._next_index
        self._build_layer(i)
        self._next_index += 1

    def _prewarm_item(self, index: int) -> None:
        """Pre-warm vsicurl cache by opening VRT (triggers band URI fetches)."""
        vrt_path = self._vrt_paths.get(index)
        if not vrt_path:
            return
        try:
            ds = gdal.Open(vrt_path)
            if ds:
                del ds
        except Exception:
            pass

    def _build_layer(self, index: int) -> None:
        """Create QgsRasterLayer for index and add to project."""
        if index not in self._vrt_paths:
            return

        vrt_path = self._vrt_paths[index]
        item = self._items[index]
        dt_label = item.datetime_str[:10]
        layer_name = f"{self._state.satellite or 'zarr'}_{dt_label}"

        layer = QgsRasterLayer(vrt_path, layer_name, "gdal")
        if not layer.isValid():
            _log_msg(f"Skipping {dt_label}: data unavailable")
            return

        # Style
        s = self._state
        dtype = _gdal_dtype_name(layer.dataProvider().dataType(1))
        if not dtype:
            dtype = s.info.dtype_per_resolution.get(s.resolution, "")
        stretch = s.stretch_range
        if stretch and dtype.startswith("Float") and stretch[1] > 10:
            stretch = None
        _auto_style(
            layer, len(s.bands), dtype=dtype, satellite=s.satellite,
            stretch_range=stretch,
        )

        # Temporal properties - QGIS uses these to derive frame ranges
        qdt = self._qdatetimes[index]
        props = layer.temporalProperties()
        props.setIsActive(True)
        props.setMode(
            QgsRasterLayerTemporalProperties.TemporalMode.ModeFixedTemporalRange,
        )
        props.setFixedTemporalRange(QgsDateTimeRange(qdt, qdt.addSecs(1)))

        self._layers.append(layer)
        QgsProject.instance().addMapLayer(layer, False)
        if self._group:
            self._group.addLayer(layer)

        _log_msg(f"Loaded {len(self._layers)}/{self.count}: {layer_name}")

    def cleanup(self) -> None:
        """Remove all layers and temp VRT files."""
        if hasattr(self, "_load_timer") and self._load_timer.isActive():
            self._load_timer.stop()
        self._pool.shutdown(wait=False)

        for layer in self._layers:
            try:
                QgsProject.instance().removeMapLayer(layer.id())
            except Exception:
                pass
        self._layers.clear()

        for vrt_path in self._vrt_paths.values():
            _untrack_and_remove(vrt_path)
        self._vrt_paths.clear()

        if self._group:
            try:
                root = QgsProject.instance().layerTreeRoot()
                root.removeChildNode(self._group)
            except RuntimeError:
                pass  # C++ object already deleted
            self._group = None


def _build_vrt(item: TimeSeriesItem, state: TimeSeriesState) -> str:
    """Build VRT for a single item. Pure string/XML work, no HTTP."""
    zarr_url = item.zarr_url.rstrip("/")
    if state.info.sub_group:
        zarr_url = f"{zarr_url}/{state.info.sub_group}"
    return _build_multiband_vrt_xml(
        zarr_url, state.bands, state.resolution, state.info, state.satellite,
    )


def _log_msg(msg: str) -> None:
    QgsMessageLog.logMessage(msg, "GeoZarr", Qgis.MessageLevel.Info)
