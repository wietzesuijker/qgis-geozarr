"""QgsDataItemGuiProvider: adds 'Load GeoZarr...' to STAC Zarr assets."""

from __future__ import annotations

import atexit
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
    QgsContrastEnhancement,
    QgsCoordinateReferenceSystem,
    QgsCoordinateTransform,
    QgsDataItem,
    QgsDateTimeRange,
    QgsInterval,
    QgsMessageLog,
    QgsMultiBandColorRenderer,
    QgsProject,
    QgsRasterLayer,
    QgsRasterMinMaxOrigin,
    QgsSingleBandGrayRenderer,
    QgsStacConnection,
    QgsTemporalNavigationObject,
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


atexit.register(cleanup_temp_files)


def _find_zarr_root(url: str) -> str:
    """Find the Zarr store root from a deep asset URL."""
    m = re.search(r"(https?://[^?#]*\.zarr)", url)
    if m:
        return m.group(1)
    return url


def _clean_gdal_uri(raw: str) -> str:
    """Strip ZARR: prefix, quotes, and /vsicurl/ from a GDAL URI."""
    if raw.upper().startswith("ZARR:"):
        raw = raw[5:]
    if raw.startswith('"') and raw.endswith('"'):
        raw = raw[1:-1]
    if raw.startswith("/vsicurl/"):
        raw = raw[9:]
    return raw


def _fetch_stac_item_json(stac_item_url: str) -> dict:
    """Fetch a STAC item JSON. Returns empty dict on failure."""
    try:
        data = geozarr_metadata._vsi_read(stac_item_url)
        if data:
            return json.loads(data)
    except (json.JSONDecodeError, OSError) as e:
        log.debug("STAC item fetch failed: %s", stac_item_url, exc_info=True)
        QgsMessageLog.logMessage(
            f"STAC item fetch failed for {stac_item_url}: {e}", TAG, Qgis.Warning,
        )
    return {}


def _extract_zarr_href(assets: dict) -> tuple:
    """Find Zarr asset href from STAC item assets. Returns (url, key)."""
    for key, asset in assets.items():
        if not isinstance(asset, dict):
            continue
        href = asset.get("href", "")
        media = asset.get("type", "")
        if "zarr" in media.lower() or ".zarr" in href.lower():
            return (_find_zarr_root(href), key)
    return ("", "")


def _fetch_zarr_href(stac_item_url: str) -> tuple:
    """Fetch a STAC item and find its Zarr asset href. Thread-safe."""
    item = _fetch_stac_item_json(stac_item_url)
    if not item:
        return ("", "")
    return _extract_zarr_href(item.get("assets", {}))


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


_TILE_RE = re.compile(r"_T(\d{2}[A-Z]{3})_")


def _extract_grid_code(feat: dict) -> str:
    """Extract grid/tile identifier from a STAC feature.

    Checks (in order):
    1. grid:code property (STAC Grid Extension, e.g. "MGRS-25WFU")
    2. s2:mgrs_tile property (Sentinel-2 extension)
    3. Sentinel-2 MGRS tile from item ID (regex fallback)
    """
    props = feat.get("properties", {})
    gc = props.get("grid:code")
    if gc:
        return str(gc)
    mgrs = props.get("s2:mgrs_tile")
    if mgrs:
        return str(mgrs)
    m = _TILE_RE.search(feat.get("id", ""))
    return m.group(1) if m else ""


def _query_stac_items(
    base_url: str, collection_id: str,
    bbox: tuple = None, datetime_range: str = None, limit: int = 24,
    grid_code: str = "",
) -> list:
    """Query STAC items endpoint. Returns sorted list of dicts.

    If grid_code is set, filters to items matching that grid/tile code
    and requests extra items to compensate for filtering.
    """
    # Request more if filtering by grid (multiple tiles per bbox)
    request_limit = limit * 6 if grid_code else limit
    url = f"{base_url}/collections/{collection_id}/items?limit={request_limit}"
    if bbox:
        url += f"&bbox={','.join(f'{v:.6f}' for v in bbox)}"
    if datetime_range:
        url += f"&datetime={datetime_range}"

    data = geozarr_metadata._vsi_read(url)
    if not data:
        return []

    try:
        result = json.loads(data)
    except (json.JSONDecodeError, ValueError):
        return []

    items = []
    for feat in result.get("features", []):
        if not isinstance(feat, dict):
            continue
        if grid_code and _extract_grid_code(feat) != grid_code:
            continue
        dt = feat.get("properties", {}).get("datetime")
        href, _ = _extract_zarr_href(feat.get("assets", {}))
        if dt and href:
            items.append({
                "datetime": dt, "zarr_url": href, "id": feat.get("id", ""),
            })
    items.sort(key=lambda x: x["datetime"])
    return items[:limit]


class _TimeSeriesSearchThread(QThread):
    """Background thread for STAC collection query + first item metadata."""

    finished = pyqtSignal(object, list, str)  # (ZarrRootInfo | None, items, item_datetime)
    error = pyqtSignal(str)  # error message

    def __init__(
        self, base_url: str, collection_id: str,
        item_url: str = "",
        bbox: tuple = None, datetime_range: str = None, limit: int = 24,
        grid_code: str = "",
    ):
        super().__init__()
        self.base_url = base_url
        self.collection_id = collection_id
        self.item_url = item_url
        self.bbox = bbox
        self.datetime_range = datetime_range
        self.limit = limit
        self.grid_code = grid_code

    def run(self):
        try:
            self._do_search()
        except Exception as e:
            log.warning("Time series search failed: %s", e, exc_info=True)
            self.error.emit(str(e))

    def _do_search(self):
        # If we have an item URL, fetch its datetime for dialog defaults
        item_datetime = ""
        if self.item_url:
            item_json = _fetch_stac_item_json(self.item_url)
            item_datetime = item_json.get("properties", {}).get("datetime", "")

        # Query collection items (filtered to same grid cell if available)
        items = _query_stac_items(
            self.base_url, self.collection_id,
            self.bbox, self.datetime_range, self.limit,
            grid_code=self.grid_code,
        )
        if not items:
            self.finished.emit(None, [], item_datetime)
            return

        # fetch() gives sub_group; fetch_resolved() gives CRS/transform
        root_info = geozarr_metadata.fetch(items[0]["zarr_url"])
        sub_group = root_info.sub_group if root_info else ""
        info, _ = geozarr_metadata.fetch_resolved(items[0]["zarr_url"])
        if info:
            info.sub_group = sub_group
        self.finished.emit(info, items, item_datetime)


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

        # Time series action for STAC items
        if zarr_url.startswith("STAC:"):
            ts_action = QAction("Load time series...", menu)
            ts_action.triggered.connect(lambda: self._load_timeseries(item))
            menu.addAction(ts_action)

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
        except Exception as e:
            log.debug("Zarr detection via mimeUris failed: %s", e, exc_info=True)

        try:
            path = item.path() or ""
            name = item.name() or ""
            if ".zarr" in path.lower() or ".zarr" in name.lower():
                return self._zarr_from_parent(item)
        except Exception as e:
            log.debug("Zarr detection via path failed: %s", e, exc_info=True)

        # Strategy 4: any STAC item - resolve via API when clicked
        try:
            path = item.path() or ""
            if "/items/" in path and "stac" in path.lower():
                return f"STAC:{path}"
        except Exception as e:
            log.debug("STAC item detection failed: %s", e, exc_info=True)

        return ""

    def _zarr_root_from_gdal_uri(self, gdal_uri: str) -> str:
        return _find_zarr_root(_clean_gdal_uri(gdal_uri))

    def _zarr_from_parent(self, item: QgsDataItem) -> str:
        parent = item.parent()
        while parent:
            try:
                for uri in parent.mimeUris():
                    raw = uri.uri or ""
                    if ".zarr" in raw.lower():
                        return _find_zarr_root(_clean_gdal_uri(raw))
            except Exception as e:
                log.debug("Zarr parent walk failed: %s", e, exc_info=True)
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
        """Extract STAC item API URL from data item tree (main thread only)."""
        ctx = self._build_stac_context(item)
        return ctx.get("item_url", "")

    def _build_stac_context(self, item: QgsDataItem) -> dict:
        """Extract STAC context from data item tree. Returns dict with:
        item_url, base_url, collection_id, item_id, conn_name."""
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
            return {}

        try:
            conn_data = QgsStacConnection.connection(conn_name)
        except (AttributeError, RuntimeError) as e:
            log.debug("STAC connection lookup failed: %s: %s", conn_name, e, exc_info=True)
            return {}
        if not conn_data or not conn_data.url:
            return {}

        base_url = conn_data.url.rstrip("/")
        return {
            "item_url": f"{base_url}/collections/{collection_id}/items/{item_id}",
            "base_url": base_url,
            "collection_id": collection_id,
            "item_id": item_id,
            "conn_name": conn_name,
        }

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

        self._msg("Fetching metadata...")
        QApplication.setOverrideCursor(Qt.WaitCursor)

        # Disconnect old thread to prevent double-click race
        self._disconnect_thread(self._fetch_thread)
        self._fetch_thread = _ProviderFetchThread(zarr_url, stac_api_url)
        self._fetch_thread.finished.connect(self._on_fetch_done)
        self._fetch_thread.start()

    def _on_fetch_done(self, info, zarr_url) -> None:
        """Handle completed metadata fetch (main thread)."""
        QApplication.restoreOverrideCursor()
        self._msg_clear()

        if not self._validate_fetch_result(info, zarr_url):
            return

        item_name = self._pending.get("item_name", "")
        collection_id = self._pending.get("collection_id", "")

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
        if not bands:
            QMessageBox.warning(None, TAG, "No bands selected.")
            return

        self._msg("Building VRT and loading...")
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            _create_layer(
                zarr_url, bands, dlg.selected_resolution(),
                dlg.layer_name(), info,
            )
        finally:
            QApplication.restoreOverrideCursor()
            self._msg_clear()

    @staticmethod
    def _validate_fetch_result(info, zarr_url) -> bool:
        """Check fetch result and show error dialog if invalid. Returns True if OK."""
        if info is None:
            if not zarr_url:
                QMessageBox.warning(
                    None, TAG,
                    "No Zarr assets found in this STAC item.\n\n"
                    "The item may use a different format (COG, NetCDF).",
                )
            else:
                QMessageBox.warning(
                    None, TAG,
                    f"Could not read metadata from:\n{zarr_url}\n\n"
                    "Check that the URL points to a Zarr store root "
                    "(.zarr directory).",
                )
            return False

        if not info.resolutions or not any(info.bands_per_resolution.values()):
            QMessageBox.warning(
                None, TAG,
                "No bands found in metadata.\n\n"
                "Expected resolution groups (r10m, r20m) containing "
                "band arrays. The dataset may not follow GeoZarr conventions.",
            )
            return False
        return True

    def _load_timeseries(self, item: QgsDataItem) -> None:
        """Show time series search dialog for a STAC collection."""
        ctx = self._build_stac_context(item)
        if not ctx:
            QMessageBox.warning(
                None, TAG,
                "Could not resolve STAC connection for this item.",
            )
            return

        # Fetch item JSON for datetime + bbox defaults
        item_datetime = ""
        item_bbox = None
        item_json = _fetch_stac_item_json(ctx["item_url"])
        if item_json:
            item_datetime = item_json.get("properties", {}).get("datetime", "")
            bbox_raw = item_json.get("bbox")
            if isinstance(bbox_raw, list) and len(bbox_raw) >= 4:
                item_bbox = tuple(float(v) for v in bbox_raw[:4])

        # Prefer map extent if data is loaded, otherwise use item bbox
        extent = self._get_map_extent_4326() or item_bbox

        # Extract grid/tile code for filtering same-tile items
        grid_code = _extract_grid_code(item_json) if item_json else ""

        from .stac_search_dialog import StacSearchDialog

        dlg = StacSearchDialog(
            collection_id=ctx["collection_id"],
            parent=None,
            item_datetime=item_datetime,
            map_extent=extent,
        )

        def on_search(dialog, bbox, datetime_range, limit):
            # Store thread on dialog so it's cleaned up when dialog closes
            thread = _TimeSeriesSearchThread(
                base_url=ctx["base_url"],
                collection_id=ctx["collection_id"],
                bbox=bbox,
                datetime_range=datetime_range,
                limit=limit,
                grid_code=grid_code,
            )
            thread.finished.connect(
                lambda info_r, items, dt: dialog.on_search_results(info_r, items),
            )
            thread.error.connect(dialog.on_search_error)
            dialog._search_thread = thread
            thread.start()

        dlg.set_search_callback(on_search)

        if dlg.exec_() != dlg.Accepted:
            return

        # Build VRT and create layer
        items = dlg.search_items()
        info = dlg.search_info()
        band = dlg.selected_band()
        resolution = dlg.selected_resolution()
        layer_name = dlg.layer_name()

        if not items or not info or not band:
            return

        self._msg("Building temporal VRT...")
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            vrt_path = _build_temporal_vrt(items, band, resolution, info)
            if not vrt_path:
                QMessageBox.warning(None, TAG, "Failed to build temporal VRT.")
                return
            if band not in layer_name:
                layer_name = f"{layer_name} {band}"
            _create_temporal_layer(vrt_path, items, layer_name, self._iface)
        finally:
            QApplication.restoreOverrideCursor()
            self._msg_clear()

    def _get_map_extent_4326(self) -> tuple:
        """Get map canvas extent in EPSG:4326. Returns None if no data loaded."""
        if not self._iface:
            return None
        try:
            canvas = self._iface.mapCanvas()
            extent = canvas.extent()
            # No layers loaded -> extent is empty or default full world
            if extent.isEmpty() or not QgsProject.instance().mapLayers():
                return None
            crs = canvas.mapSettings().destinationCrs()
            if not crs.isValid():
                return None
            if crs.authid() != "EPSG:4326":
                transform = QgsCoordinateTransform(
                    crs,
                    QgsCoordinateReferenceSystem("EPSG:4326"),
                    QgsProject.instance(),
                )
                extent = transform.transformBoundingBox(extent)
            return (
                extent.xMinimum(), extent.yMinimum(),
                extent.xMaximum(), extent.yMaximum(),
            )
        except Exception as e:
            log.debug("Map extent transform failed: %s", e, exc_info=True)
            return None

    @staticmethod
    def _disconnect_thread(thread) -> None:
        """Safely disconnect a QThread's signals to prevent stale callbacks."""
        if thread is None:
            return
        try:
            thread.finished.disconnect()
        except (RuntimeError, TypeError):
            pass

    def stop_fetch(self) -> None:
        """Cancel any running fetch thread."""
        if self._fetch_thread and self._fetch_thread.isRunning():
            self._disconnect_thread(self._fetch_thread)
            self._fetch_thread.quit()
            self._fetch_thread.wait(3000)
        self._fetch_thread = None


def _vsi_prefix(url: str) -> str:
    if url.startswith("s3://"):
        return "/vsis3/"
    return "/vsicurl/"


def _band_uri(zarr_url: str, resolution: str, band: str, sub_group: str = "") -> str:
    base = f"{zarr_url}/{sub_group}" if sub_group else zarr_url
    path = f"{base}/{resolution}/{band}" if resolution else f"{base}/{band}"
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


def _untrack_and_remove(path: str) -> None:
    """Remove a temp file and untrack it. Ignores errors."""
    with _temp_lock:
        _temp_files.discard(path)
    try:
        if os.path.exists(path):
            os.unlink(path)
    except OSError:
        pass


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

    try:
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
    except Exception:
        _untrack_and_remove(vrt_path)
        raise


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
        _create_single_band_layer(zarr_url, bands[0], resolution, layer_name, info)
    else:
        _create_multiband_vrt_layer(zarr_url, bands, resolution, layer_name, info)


def _create_single_band_layer(zarr_url, band, resolution, layer_name, info) -> None:
    """Direct ZARR open for single band, no VRT needed."""
    uri = _band_uri(zarr_url, resolution, band)
    layer = QgsRasterLayer(uri, layer_name, "gdal")
    if not layer.isValid():
        QMessageBox.warning(
            None, TAG, f"Failed to load layer:\n{layer.error().message()}",
        )
        return
    if info.epsg and not layer.crs().isValid():
        layer.setCrs(QgsCoordinateReferenceSystem(f"EPSG:{info.epsg}"))
    QgsProject.instance().addMapLayer(layer)


def _create_multiband_vrt_layer(zarr_url, bands, resolution, layer_name, info) -> None:
    """Build multi-band VRT with optional multiscale overviews."""
    band_uris = [_band_uri(zarr_url, resolution, b) for b in bands]

    vrt_file = tempfile.NamedTemporaryFile(
        suffix=".vrt", prefix="geozarr_", delete=False,
    )
    vrt_path = _track_temp(vrt_file.name)
    vrt_file.close()

    try:
        vrt_opts = gdal.BuildVRTOptions(separate=True)
        vrt_ds = gdal.BuildVRT(vrt_path, band_uris, options=vrt_opts)

        if vrt_ds is None:
            QMessageBox.warning(
                None, TAG, f"Failed to build VRT:\n{gdal.GetLastErrorMsg()}",
            )
            _untrack_and_remove(vrt_path)
            return

        if info.epsg:
            srs = osr.SpatialReference()
            srs.ImportFromEPSG(info.epsg)
            vrt_ds.SetProjection(srs.ExportToWkt())
        if info.geotransform:
            vrt_ds.SetGeoTransform(info.geotransform)

        vrt_ds.FlushCache()
        vrt_ds = None
    except Exception:
        _untrack_and_remove(vrt_path)
        raise

    # Add multiscale overviews
    ovr_levels = _overview_resolutions(resolution, bands, info)
    if ovr_levels:
        ovr_paths = []
        ovr_band_lists = []
        for ovr_res, ovr_bands_at_level in ovr_levels:
            ovr_path = _build_overview_vrt(
                zarr_url, ovr_bands_at_level, ovr_res, info,
            )
            ovr_paths.append(ovr_path)
            ovr_band_lists.append(ovr_bands_at_level)
        _inject_overviews(vrt_path, ovr_paths, bands, ovr_band_lists)

    layer = QgsRasterLayer(vrt_path, layer_name, "gdal")
    if not layer.isValid():
        QMessageBox.warning(
            None, TAG, f"Failed to load VRT:\n{layer.error().message()}",
        )
        return

    QgsProject.instance().addMapLayer(layer)
    _auto_style(layer, len(bands))
    QgsMessageLog.logMessage(
        f"Loaded: {layer_name} ({len(bands)} bands, {resolution})",
        TAG, Qgis.Info,
    )


def _auto_style(layer: QgsRasterLayer, band_count: int) -> None:
    """Apply RGB rendering with cumulative cut stretch for 3-band composites."""
    if band_count != 3:
        return
    dp = layer.dataProvider()
    renderer = QgsMultiBandColorRenderer(dp, 1, 2, 3)
    origin = QgsRasterMinMaxOrigin()
    origin.setLimits(QgsRasterMinMaxOrigin.CumulativeCut)
    origin.setCumulativeCutLower(0.02)
    origin.setCumulativeCutUpper(0.98)
    origin.setStatAccuracy(QgsRasterMinMaxOrigin.Estimated)
    renderer.setMinMaxOrigin(origin)

    for band_idx, setter in (
        (1, renderer.setRedContrastEnhancement),
        (2, renderer.setGreenContrastEnhancement),
        (3, renderer.setBlueContrastEnhancement),
    ):
        ce = QgsContrastEnhancement(dp.dataType(band_idx))
        ce.setContrastEnhancementAlgorithm(
            QgsContrastEnhancement.StretchToMinimumMaximum,
        )
        setter(ce)

    layer.setRenderer(renderer)
    layer.triggerRepaint()


def _build_temporal_vrt(
    items: list, band: str, resolution: str, info,
) -> str:
    """Build VRT XML with one band per timestep. Zero HTTP - uses metadata template."""
    shape = info.shape_per_resolution.get(resolution, (0, 0))
    ny, nx = shape
    if ny == 0 or nx == 0:
        return ""

    gt = info.transform_per_resolution.get(resolution) or info.geotransform
    dtype = info.dtype_per_resolution.get(resolution, "Float32")

    root = ET.Element("VRTDataset", rasterXSize=str(nx), rasterYSize=str(ny))

    if info.epsg:
        srs_el = ET.SubElement(root, "SRS")
        srs_obj = osr.SpatialReference()
        srs_obj.ImportFromEPSG(info.epsg)
        srs_el.text = srs_obj.ExportToWkt()

    if gt:
        gt_el = ET.SubElement(root, "GeoTransform")
        gt_el.text = ", ".join(str(v) for v in gt)

    for i, item in enumerate(items, 1):
        uri = _band_uri(item["zarr_url"], resolution, band, info.sub_group)
        band_el = ET.SubElement(
            root, "VRTRasterBand", dataType=dtype, band=str(i),
        )
        # Band name: date + item ID for identify tool / temporal controller
        dt_label = item["datetime"][:10]
        item_id = item.get("id", "")
        desc = f"{dt_label} {item_id}" if item_id else dt_label
        ET.SubElement(band_el, "Description").text = desc
        src = ET.SubElement(band_el, "SimpleSource")
        ET.SubElement(src, "SourceFilename", relativeToVRT="0").text = uri
        ET.SubElement(src, "SourceBand").text = "1"
        ET.SubElement(
            src, "SrcRect",
            xOff="0", yOff="0", xSize=str(nx), ySize=str(ny),
        )
        ET.SubElement(
            src, "DstRect",
            xOff="0", yOff="0", xSize=str(nx), ySize=str(ny),
        )

    vrt_file = tempfile.NamedTemporaryFile(
        suffix=".vrt", prefix="geozarr_ts_", delete=False,
    )
    path = _track_temp(vrt_file.name)
    vrt_file.close()
    try:
        ET.ElementTree(root).write(path, xml_declaration=True, encoding="utf-8")
    except Exception:
        _untrack_and_remove(path)
        raise
    return path


def _create_temporal_layer(
    vrt_path: str, items: list, layer_name: str, iface=None,
) -> None:
    """Create a raster layer with FixedRangePerBand temporal properties."""
    from qgis.PyQt.QtCore import QDateTime

    layer = QgsRasterLayer(vrt_path, layer_name, "gdal")
    if not layer.isValid():
        QMessageBox.warning(
            None, TAG,
            f"Failed to load temporal VRT:\n{layer.error().message()}",
        )
        return

    # Single-band gray renderer (temporal mode switches which band is shown)
    renderer = QgsSingleBandGrayRenderer(layer.dataProvider(), 1)
    ce = QgsContrastEnhancement(layer.dataProvider().dataType(1))
    ce.setContrastEnhancementAlgorithm(
        QgsContrastEnhancement.StretchToMinimumMaximum,
    )
    renderer.setContrastEnhancement(ce)
    layer.setRenderer(renderer)

    # Map each band to its acquisition datetime
    dts = _setup_temporal_properties(layer, items, QDateTime)
    QgsProject.instance().addMapLayer(layer)

    if iface and len(dts) >= 2:
        _activate_temporal_controller(iface, dts)

    QgsMessageLog.logMessage(
        f"Loaded time series: {layer_name} ({len(items)} dates)",
        TAG, Qgis.Info,
    )


def _setup_temporal_properties(layer, items, QDateTime) -> list:
    """Configure FixedRangePerBand temporal properties. Returns list of QDateTimes."""
    props = layer.temporalProperties()
    props.setIsActive(True)
    props.setMode(Qgis.RasterTemporalMode.FixedRangePerBand)

    ranges = {}
    dts = []
    for i, item in enumerate(items, 1):
        dt_str = item["datetime"][:19]
        dt = QDateTime.fromString(dt_str, "yyyy-MM-ddTHH:mm:ss")
        if not dt.isValid():
            dt = QDateTime.fromString(dt_str[:10], "yyyy-MM-dd")
        dts.append(dt)
        ranges[i] = QgsDateTimeRange(dt, dt)
    props.setFixedRangePerBand(ranges)
    return dts


def _activate_temporal_controller(iface, dts) -> None:
    """Set up temporal slider with auto-detected frame duration."""
    try:
        canvas = iface.mapCanvas()
        ctrl = canvas.temporalController()
        sorted_dts = sorted(dts, key=lambda d: d.toMSecsSinceEpoch())
        ctrl.setTemporalExtents(
            QgsDateTimeRange(sorted_dts[0], sorted_dts[-1]),
        )
        intervals = [
            sorted_dts[j].daysTo(sorted_dts[j + 1])
            for j in range(len(sorted_dts) - 1)
        ]
        median_days = sorted(intervals)[len(intervals) // 2]
        ctrl.setFrameDuration(
            QgsInterval(max(median_days, 1), Qgis.TemporalUnit.Days),
        )
        ctrl.setNavigationMode(
            QgsTemporalNavigationObject.NavigationMode.Animated,
        )
    except Exception as e:
        log.debug("Temporal controller setup failed: %s", e, exc_info=True)
