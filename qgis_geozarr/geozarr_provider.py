"""QgsDataItemGuiProvider: adds 'Load GeoZarr...' to STAC Zarr assets."""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import threading
import xml.etree.ElementTree as ET

from osgeo import gdal, osr

from qgis.core import (
    Qgis,
    QgsCoordinateReferenceSystem,
    QgsDataItem,
    QgsMessageLog,
    QgsProject,
    QgsRasterLayer,
    QgsStacConnection,
)
from qgis.gui import QgsDataItemGuiProvider
from qgis.PyQt.QtCore import Qt, QThread, pyqtSignal
from qgis.PyQt.QtWidgets import QAction, QApplication, QMessageBox

from . import geozarr_metadata
from .geozarr_dialog import GeoZarrLoadDialog

TAG = "GeoZarr"
log = logging.getLogger(__name__)

# Track temp VRT files for cleanup
_temp_files: set = set()
_temp_lock = threading.Lock()


def cleanup_temp_files() -> None:
    """Remove all tracked temp VRT files."""
    with _temp_lock:
        for path in list(_temp_files):
            try:
                if os.path.exists(path):
                    os.unlink(path)
            except OSError:
                pass
        _temp_files.clear()


def _find_zarr_root(url: str) -> str:
    """Find the Zarr store root from a deep asset URL."""
    m = re.search(r"(https?://[^?#]*\.zarr)", url)
    if m:
        return m.group(1)
    return url


def _fetch_zarr_href(stac_item_url: str) -> tuple:
    """Fetch a STAC item and find its Zarr asset href. Thread-safe."""
    try:
        vsi_path = f"/vsicurl/{stac_item_url}"
        fp = gdal.VSIFOpenL(vsi_path, "rb")
        if fp is None:
            return ("", "")
        data = b""
        while True:
            chunk = gdal.VSIFReadL(1, 65536, fp)
            if not chunk:
                break
            data += chunk
        gdal.VSIFCloseL(fp)
        stac_item = json.loads(data)
    except Exception:
        log.debug("STAC item fetch failed: %s", stac_item_url, exc_info=True)
        return ("", "")

    assets = stac_item.get("assets", {})
    for key, asset in assets.items():
        if not isinstance(asset, dict):
            continue
        href = asset.get("href", "")
        media = asset.get("type", "")
        if "zarr" in media.lower() or ".zarr" in href.lower():
            return (_find_zarr_root(href), key)

    return ("", "")


class _ProviderFetchThread(QThread):
    """Background thread for STAC resolve + zarr.json fetch."""

    finished = pyqtSignal(object, str)  # (ZarrRootInfo | None, zarr_url)

    def __init__(self, zarr_url: str, stac_api_url: str = ""):
        super().__init__()
        self.zarr_url = zarr_url
        self.stac_api_url = stac_api_url

    def run(self):
        zarr_url = self.zarr_url
        if self.stac_api_url:
            zarr_url, _ = _fetch_zarr_href(self.stac_api_url)
            if not zarr_url:
                self.finished.emit(None, "")
                return
        info, url = geozarr_metadata.fetch_resolved(zarr_url)
        self.finished.emit(info, url)


class GeoZarrDataItemGuiProvider(QgsDataItemGuiProvider):
    """Injects 'Load GeoZarr...' context menu on STAC Zarr assets."""

    def __init__(self, iface=None):
        super().__init__()
        self._iface = iface
        self._fetch_thread = None
        self._pending = {}

    def name(self) -> str:
        return "GeoZarr"

    def populateContextMenu(self, item, menu, selectedItems, context):
        zarr_url = self._detect_zarr(item)
        if not zarr_url:
            return
        action = QAction("Load GeoZarr...", menu)
        action.triggered.connect(lambda: self._load_geozarr(item, zarr_url))
        menu.addAction(action)

    def _msg(self, text: str, level=Qgis.Info, duration: int = 0) -> None:
        """Push a message to the QGIS message bar if iface available."""
        if self._iface:
            self._iface.messageBar().pushMessage(TAG, text, level, duration)

    def _msg_clear(self) -> None:
        """Clear the message bar."""
        if self._iface:
            self._iface.messageBar().clearWidgets()

    def _detect_zarr(self, item: QgsDataItem) -> str:
        """Detect if item is Zarr and return store root URL."""
        try:
            for uri in item.mimeUris():
                raw = uri.uri or ""
                if "ZARR:" in raw.upper():
                    return self._zarr_root_from_gdal_uri(raw)
                if ".zarr" in raw.lower():
                    return _find_zarr_root(raw)
        except Exception:
            log.debug("Zarr detection via mimeUris failed", exc_info=True)

        try:
            path = item.path() or ""
            name = item.name() or ""
            if ".zarr" in path.lower() or ".zarr" in name.lower():
                return self._zarr_from_parent(item)
        except Exception:
            log.debug("Zarr detection via path failed", exc_info=True)

        # Strategy 4: any STAC item - resolve via API when clicked
        try:
            path = item.path() or ""
            if "/items/" in path and "stac" in path.lower():
                return f"STAC:{path}"
        except Exception:
            log.debug("STAC item detection failed", exc_info=True)

        return ""

    def _zarr_root_from_gdal_uri(self, gdal_uri: str) -> str:
        raw = gdal_uri
        if raw.upper().startswith("ZARR:"):
            raw = raw[5:]
        if raw.startswith('"') and raw.endswith('"'):
            raw = raw[1:-1]
        if raw.startswith("/vsicurl/"):
            raw = raw[9:]
        return _find_zarr_root(raw)

    def _zarr_from_parent(self, item: QgsDataItem) -> str:
        parent = item.parent()
        while parent:
            try:
                for uri in parent.mimeUris():
                    raw = uri.uri or ""
                    if ".zarr" in raw.lower():
                        cleaned = raw
                        if cleaned.upper().startswith("ZARR:"):
                            cleaned = cleaned[5:]
                        if cleaned.startswith('"') and cleaned.endswith('"'):
                            cleaned = cleaned[1:-1]
                        if cleaned.startswith("/vsicurl/"):
                            cleaned = cleaned[9:]
                        return _find_zarr_root(cleaned)
            except Exception:
                log.debug("Zarr parent walk failed", exc_info=True)
            parent = parent.parent()
        return ""

    def _extract_collection_id(self, item: QgsDataItem) -> str:
        parent = item.parent()
        while parent:
            name = parent.name() if hasattr(parent, "name") else ""
            path = parent.path() if hasattr(parent, "path") else ""
            if "collection" in path.lower():
                return name
            parent = parent.parent()
        return ""

    def _build_stac_api_url(self, item: QgsDataItem) -> str:
        """Extract STAC API URL from data item tree (main thread only)."""
        path = item.path() or ""
        item_id, collection_id, conn_name = "", "", ""

        parts = path.split("/")
        for i, part in enumerate(parts):
            if part == "items" and i + 1 < len(parts):
                item_id = parts[i + 1]
            elif part == "collections" and i + 1 < len(parts):
                collection_id = parts[i + 1]

        if not item_id:
            item_id = item.name()
        if not collection_id:
            p = item.parent()
            collection_id = p.name() if p else ""

        parent = item.parent()
        while parent:
            gp = parent.parent()
            if gp and not gp.parent():
                conn_name = parent.name()
                break
            parent = gp

        if not all([item_id, collection_id, conn_name]):
            QgsMessageLog.logMessage(
                f"STAC resolve failed: item={item_id} coll={collection_id} "
                f"conn={conn_name} path={path}",
                TAG,
                Qgis.Warning,
            )
            return ""

        try:
            conn_data = QgsStacConnection.connection(conn_name)
        except Exception:
            log.debug("STAC connection lookup failed: %s", conn_name, exc_info=True)
            return ""
        if not conn_data or not conn_data.url:
            return ""

        return (
            f"{conn_data.url.rstrip('/')}"
            f"/collections/{collection_id}/items/{item_id}"
        )

    def _stac_item_name(self, item: QgsDataItem) -> str:
        path = item.path() or ""
        parts = path.split("/")
        for i, part in enumerate(parts):
            if part == "items" and i + 1 < len(parts):
                return parts[i + 1]
        return item.name() or ""

    def _load_geozarr(self, item: QgsDataItem, zarr_url: str) -> None:
        """Fetch metadata, show dialog, create layer (non-blocking)."""
        item_name = ""
        stac_api_url = ""

        if zarr_url.startswith("STAC:"):
            item_name = self._stac_item_name(item)
            stac_api_url = self._build_stac_api_url(item)
            if not stac_api_url:
                QMessageBox.warning(
                    None,
                    TAG,
                    "No Zarr assets found in this STAC item.\n\n"
                    "The item may use a different format (COG, NetCDF).",
                )
                return
            zarr_url = ""

        self._pending = {
            "item_name": item_name,
            "collection_id": self._extract_collection_id(item),
        }

        self._msg("Fetching zarr.json...")
        QApplication.setOverrideCursor(Qt.WaitCursor)

        self._fetch_thread = _ProviderFetchThread(zarr_url, stac_api_url)
        self._fetch_thread.finished.connect(self._on_fetch_done)
        self._fetch_thread.start()

    def _on_fetch_done(self, info, zarr_url) -> None:
        """Handle completed metadata fetch (main thread)."""
        QApplication.restoreOverrideCursor()
        self._msg_clear()

        item_name = self._pending.get("item_name", "")
        collection_id = self._pending.get("collection_id", "")

        if info is None:
            if not zarr_url:
                QMessageBox.warning(
                    None,
                    TAG,
                    "No Zarr assets found in this STAC item.\n\n"
                    "The item may use a different format (COG, NetCDF).",
                )
            else:
                QMessageBox.warning(
                    None,
                    TAG,
                    f"Could not read zarr.json from:\n{zarr_url}\n\n"
                    "Check that the URL points to a Zarr v3 store root "
                    "(.zarr directory).",
                )
            return

        if not info.resolutions or not any(info.bands_per_resolution.values()):
            QMessageBox.warning(
                None,
                TAG,
                "No bands found in zarr.json.\n\n"
                "Expected resolution groups (r10m, r20m) containing "
                "band arrays. The dataset may not follow GeoZarr conventions.",
            )
            return

        dlg = GeoZarrLoadDialog(
            info,
            parent=None,
            collection_id=collection_id,
            zarr_url=zarr_url,
            item_name=item_name,
        )
        if dlg.exec_() != dlg.Accepted:
            return

        bands = dlg.selected_bands()
        resolution = dlg.selected_resolution()
        layer_name = dlg.layer_name()

        if not bands:
            QMessageBox.warning(None, TAG, "No bands selected.")
            return

        self._msg("Building VRT and loading...")
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            _create_layer(zarr_url, bands, resolution, layer_name, info)
        finally:
            QApplication.restoreOverrideCursor()
            self._msg_clear()

    def stop_fetch(self) -> None:
        """Cancel any running fetch thread."""
        if self._fetch_thread and self._fetch_thread.isRunning():
            self._fetch_thread.finished.disconnect()
            self._fetch_thread.wait(3000)
        self._fetch_thread = None


def _vsi_prefix(url: str) -> str:
    if url.startswith("s3://"):
        return "/vsis3/"
    return "/vsicurl/"


def _band_uri(zarr_url: str, resolution: str, band: str) -> str:
    path = f"{zarr_url}/{resolution}/{band}" if resolution else f"{zarr_url}/{band}"
    prefix = _vsi_prefix(path)
    if prefix == "/vsis3/":
        path = path[5:]
    return f'ZARR:"{prefix}{path}"'


def _res_pixel_size(name: str) -> int:
    m = re.search(r"(\d+)", name)
    return int(m.group(1)) if m else 0


def _overview_resolutions(base_res, bands, info) -> list:
    """Find coarser resolutions where all requested bands exist."""
    base_size = _res_pixel_size(base_res)
    if not base_size:
        return []
    result = []
    for res in info.resolutions:
        if res == base_res:
            continue
        if _res_pixel_size(res) <= base_size:
            continue
        available = set(b.upper() for b in info.bands_per_resolution.get(res, ()))
        matched = [b for b in bands if b.upper() in available]
        if matched:
            result.append((res, matched))
    result.sort(key=lambda x: _res_pixel_size(x[0]))
    return result


def _track_temp(path: str) -> str:
    """Register a temp file for cleanup and return the path."""
    with _temp_lock:
        _temp_files.add(path)
    return path


def _build_overview_vrt(zarr_url, bands, resolution, info) -> str:
    """Build a VRT for one overview level. Returns temp file path."""
    band_uris = [_band_uri(zarr_url, resolution, b) for b in bands]

    vrt_file = tempfile.NamedTemporaryFile(
        suffix=".vrt",
        prefix=f"geozarr_ovr_{resolution}_",
        delete=False,
    )
    vrt_path = _track_temp(vrt_file.name)
    vrt_file.close()

    vrt_opts = gdal.BuildVRTOptions(separate=True)
    vrt_ds = gdal.BuildVRT(vrt_path, band_uris, options=vrt_opts)
    if vrt_ds is None:
        return ""

    gt = info.transform_per_resolution.get(resolution)
    if gt:
        vrt_ds.SetGeoTransform(gt)
    elif info.geotransform and info.shape_per_resolution.get(resolution):
        base_gt = info.geotransform
        ovr_shape = info.shape_per_resolution[resolution]
        base_shape = max(
            info.shape_per_resolution.values(), key=lambda s: s[0] * s[1]
        )
        if base_shape[0] > 0 and base_shape[1] > 0:
            sx = (base_gt[1] * base_shape[1]) / ovr_shape[1]
            sy = (base_gt[5] * base_shape[0]) / ovr_shape[0]
            vrt_ds.SetGeoTransform(
                (base_gt[0], sx, base_gt[2], base_gt[3], base_gt[4], sy)
            )

    if info.epsg:
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(info.epsg)
        vrt_ds.SetProjection(srs.ExportToWkt())

    vrt_ds.FlushCache()
    vrt_ds = None
    return vrt_path


def _inject_overviews(base_vrt_path, ovr_vrt_paths, bands, ovr_bands) -> None:
    """Insert <Overview> elements into base VRT XML."""
    tree = ET.parse(base_vrt_path)
    root = tree.getroot()

    for band_elem in root.findall("VRTRasterBand"):
        band_idx = int(band_elem.get("band", "0")) - 1
        if band_idx < 0 or band_idx >= len(bands):
            continue
        band_name = bands[band_idx]

        for ovr_path, ovr_band_list in zip(ovr_vrt_paths, ovr_bands):
            if not ovr_path:
                continue
            try:
                ovr_idx = [b.upper() for b in ovr_band_list].index(
                    band_name.upper()
                )
            except ValueError:
                continue
            ovr = ET.SubElement(band_elem, "Overview")
            sf = ET.SubElement(ovr, "SourceFilename")
            sf.set("relativeToVRT", "1")
            sf.text = os.path.basename(ovr_path)
            sb = ET.SubElement(ovr, "SourceBand")
            sb.text = str(ovr_idx + 1)

    tree.write(base_vrt_path, xml_declaration=True, encoding="utf-8")


def _create_layer(zarr_url, bands, resolution, layer_name, info) -> None:
    """Create a QgsRasterLayer from selected bands."""
    if len(bands) == 1 and not _overview_resolutions(resolution, bands, info):
        uri = _band_uri(zarr_url, resolution, bands[0])
        layer = QgsRasterLayer(uri, layer_name, "gdal")
        if not layer.isValid():
            QMessageBox.warning(
                None,
                TAG,
                f"Failed to load layer:\n{layer.error().message()}",
            )
            return
        if info.epsg and not layer.crs().isValid():
            layer.setCrs(QgsCoordinateReferenceSystem(f"EPSG:{info.epsg}"))
        QgsProject.instance().addMapLayer(layer)
        return

    # Multi-band or single-band with overviews: build VRT
    band_uris = [_band_uri(zarr_url, resolution, b) for b in bands]

    vrt_file = tempfile.NamedTemporaryFile(
        suffix=".vrt",
        prefix="geozarr_",
        delete=False,
    )
    vrt_path = _track_temp(vrt_file.name)
    vrt_file.close()

    vrt_opts = gdal.BuildVRTOptions(separate=True)
    vrt_ds = gdal.BuildVRT(vrt_path, band_uris, options=vrt_opts)

    if vrt_ds is None:
        QMessageBox.warning(
            None,
            TAG,
            f"Failed to build VRT:\n{gdal.GetLastErrorMsg()}",
        )
        return

    if info.epsg:
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(info.epsg)
        vrt_ds.SetProjection(srs.ExportToWkt())

    if info.geotransform:
        vrt_ds.SetGeoTransform(info.geotransform)

    vrt_ds.FlushCache()
    vrt_ds = None

    # Add multiscale overviews
    ovr_levels = _overview_resolutions(resolution, bands, info)
    if ovr_levels:
        ovr_paths = []
        ovr_band_lists = []
        for ovr_res, ovr_bands_at_level in ovr_levels:
            ovr_path = _build_overview_vrt(
                zarr_url, ovr_bands_at_level, ovr_res, info
            )
            ovr_paths.append(ovr_path)
            ovr_band_lists.append(ovr_bands_at_level)
        _inject_overviews(vrt_path, ovr_paths, bands, ovr_band_lists)

    layer = QgsRasterLayer(vrt_path, layer_name, "gdal")
    if not layer.isValid():
        QMessageBox.warning(
            None,
            TAG,
            f"Failed to load VRT:\n{layer.error().message()}",
        )
        return

    QgsProject.instance().addMapLayer(layer)
    QgsMessageLog.logMessage(
        f"Loaded: {layer_name} ({len(bands)} bands, {resolution})",
        TAG,
        Qgis.Info,
    )
