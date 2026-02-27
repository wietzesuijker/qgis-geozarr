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
from collections import OrderedDict

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
    QgsPalettedRasterRenderer,
    QgsProject,
    QgsRasterLayer,
    QgsRectangle,
    QgsSingleBandGrayRenderer,
    QgsStacConnection,
    QgsTemporalNavigationObject,
)
from qgis.gui import QgsDataItemGuiProvider
from qgis.PyQt.QtCore import Qt, QThread, pyqtSignal
from qgis.PyQt.QtWidgets import QAction, QApplication, QDialog, QMessageBox

from . import geozarr_metadata
from .geozarr_dialog import GeoZarrLoadDialog

TAG = "GeoZarr"
log = logging.getLogger(__name__)


def _error_dialog(msg: str, detail: str = "") -> None:
    """Show a warning dialog with optional expandable detail."""
    box = QMessageBox(QMessageBox.Icon.Warning, TAG, msg)
    if detail:
        box.setDetailedText(detail)
    box.exec()

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


_stac_cache: OrderedDict = OrderedDict()  # URL -> parsed JSON, LRU eviction
_stac_cache_lock = threading.Lock()
_STAC_CACHE_MAX = 50


def _fetch_stac_item_json(stac_item_url: str) -> dict:
    """Fetch a STAC item JSON with caching. Returns empty dict on failure."""
    with _stac_cache_lock:
        if stac_item_url in _stac_cache:
            _stac_cache.move_to_end(stac_item_url)
            return _stac_cache[stac_item_url]
    try:
        data = geozarr_metadata._vsi_read(stac_item_url)
        if data:
            result = json.loads(data)
            with _stac_cache_lock:
                if len(_stac_cache) >= _STAC_CACHE_MAX:
                    _stac_cache.popitem(last=False)
                _stac_cache[stac_item_url] = result
            return result
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


def _extract_thumbnail_url(item_json: dict) -> str:
    """Find thumbnail or overview asset href from a STAC item."""
    assets = item_json.get("assets", {})
    for key in ("thumbnail", "overview", "rendered_preview"):
        asset = assets.get(key, {})
        href = asset.get("href", "")
        if href:
            return href
    return ""


class _ProviderFetchThread(QThread):
    """Background thread for STAC resolve + zarr.json fetch.

    After emitting ``finished``, pre-warms default-resolution band sources
    and coarser overview levels so the vsicurl cache has each array's
    zarr.json ready when the VRT opens later.
    """

    finished = pyqtSignal(object, str)  # (ZarrRootInfo | None, zarr_url)
    thumbnail_ready = pyqtSignal(bytes)  # emitted after finished, non-blocking

    def __init__(self, zarr_url: str, stac_api_url: str = ""):
        super().__init__()
        self.zarr_url = zarr_url
        self.stac_api_url = stac_api_url

    def run(self):
        import time as _time

        t0 = _time.monotonic()
        zarr_url = self.zarr_url
        thumb_url = ""
        if self.stac_api_url:
            item = _fetch_stac_item_json(self.stac_api_url)
            t_stac = _time.monotonic()
            log.debug("STAC item fetch: %.2fs", t_stac - t0)
            if not item:
                self.finished.emit(None, "")
                return
            zarr_url, _ = _extract_zarr_href(item.get("assets", {}))
            if not zarr_url:
                self.finished.emit(None, "")
                return
            thumb_url = _extract_thumbnail_url(item)

        t_pre_meta = _time.monotonic()
        info, url = geozarr_metadata.fetch_resolved(zarr_url)
        t_meta = _time.monotonic()
        log.debug("Metadata fetch: %.2fs", t_meta - t_pre_meta)

        # Emit metadata immediately - dialog shows while we pre-warm
        self.finished.emit(info, url)

        # Thumbnail first (fast single HTTP GET, ~0.5s) - arrives while
        # dialog is still open. Pre-warm runs after.
        if thumb_url:
            data = geozarr_metadata._http_read(thumb_url)
            if data:
                self.thumbnail_ready.emit(data)

        # Pre-warm default resolution band sources in parallel while dialog
        # is shown. gdal.Open() caches zarr.json in the global vsicurl LRU.
        if info:
            self._prewarm_sources(info, url)
            log.debug("Pre-warm done: %.2fs", _time.monotonic() - t_meta)

        log.debug("Total fetch thread: %.2fs", _time.monotonic() - t0)

    @staticmethod
    def _prewarm_sources(info, url):
        """Pre-open default resolution band sources and their overview counterparts.

        gdal.Open() populates the vsicurl cache so subsequent VRT opens are
        near-instant. Only warms the default resolution and coarser levels
        where the same bands exist.
        """
        from concurrent.futures import ThreadPoolExecutor

        default_res = info.resolutions[0] if info.resolutions else ""
        if not default_res:
            return
        bands = info.bands_per_resolution.get(default_res, ())
        if not bands:
            return

        # Default resolution bands first
        uris = [_band_uri(url, default_res, b) for b in bands]
        # Overview sources for those bands (coarser resolutions)
        for res in info.resolutions[1:]:
            avail = {b.upper() for b in info.bands_per_resolution.get(res, ())}
            for b in bands:
                if b.upper() in avail:
                    uris.append(_band_uri(url, res, b))

        def _warm(uri):
            try:
                ds = gdal.Open(uri)
                del ds
            except Exception as e:
                log.warning("Pre-warm failed for %s: %s", uri, e)

        workers = min(len(uris), 8)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(_warm, uris))


_TILE_RE = re.compile(r"_T(\d{2}[A-Z]{3})_")


def _extract_grid_code(feat: dict) -> str:
    """Extract grid/tile identifier from a STAC feature.

    Checks (in order):
    1. grid:code property (STAC Grid Extension, e.g. "MGRS-25WFU")
    2. s2:mgrs_tile property (Sentinel-2 extension)
    3. landsat:wrs_path + landsat:wrs_row (Landsat WRS-2, e.g. "042/034")
    4. Sentinel-2 MGRS tile from item ID (regex fallback)
    """
    props = feat.get("properties", {})
    gc = props.get("grid:code")
    if gc:
        return str(gc)
    mgrs = props.get("s2:mgrs_tile")
    if mgrs:
        return str(mgrs)
    wrs_path = props.get("landsat:wrs_path")
    wrs_row = props.get("landsat:wrs_row")
    if wrs_path is not None and wrs_row is not None:
        return f"{int(wrs_path):03d}/{int(wrs_row):03d}"
    m = _TILE_RE.search(feat.get("id", ""))
    return m.group(1) if m else ""


_MAX_PAGES = 10  # safety cap on pagination


def _query_stac_items(
    base_url: str, collection_id: str,
    bbox: tuple = None, datetime_range: str = None, limit: int = 24,
    grid_code: str = "",
) -> list:
    """Query STAC items endpoint with pagination. Returns sorted list of dicts.

    Follows STAC ``rel=next`` links until *limit* matching items are collected
    or no more pages exist (capped at _MAX_PAGES requests).
    """
    page_size = min(limit * 6, 250) if grid_code else min(limit, 250)
    url = f"{base_url}/collections/{collection_id}/items?limit={page_size}"
    if bbox:
        url += f"&bbox={','.join(f'{v:.6f}' for v in bbox)}"
    if datetime_range:
        url += f"&datetime={datetime_range}"

    items = []
    for _page in range(_MAX_PAGES):
        data = geozarr_metadata._vsi_read(url)
        if not data:
            if not items:
                QgsMessageLog.logMessage(
                    f"STAC query failed: no response from {url}", TAG, Qgis.Warning,
                )
            break

        try:
            result = json.loads(data)
        except (json.JSONDecodeError, ValueError):
            QgsMessageLog.logMessage(
                f"STAC query: invalid JSON from {url}", TAG, Qgis.Warning,
            )
            break

        for feat in result.get("features", []):
            if not isinstance(feat, dict):
                continue
            if grid_code and _extract_grid_code(feat) != grid_code:
                continue
            props = feat.get("properties", {})
            dt = props.get("datetime")
            href, _ = _extract_zarr_href(feat.get("assets", {}))
            if dt and href:
                item = {
                    "datetime": dt, "zarr_url": href, "id": feat.get("id", ""),
                }
                cc = props.get("eo:cloud_cover")
                if cc is not None:
                    item["cloud_cover"] = cc
                items.append(item)

        if len(items) >= limit:
            break

        # Follow rel=next link for pagination
        next_url = ""
        for link in result.get("links", []):
            if isinstance(link, dict) and link.get("rel") == "next":
                next_url = link.get("href", "")
                break
        if not next_url:
            break
        url = next_url

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
        self._pending_thumbnail = None
        self._pending_dialog = None

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
            # Speculative prefetch: start STAC + zarr.json fetch NOW while
            # user reads the context menu. Caches populate before click.
            stac_api_url = self._build_stac_api_url(item)
            if stac_api_url:
                self._speculative_prefetch(stac_api_url)

    @staticmethod
    def _speculative_prefetch(stac_api_url: str) -> None:
        """Fire-and-forget: fetch STAC item + zarr.json into cache.

        Runs during context menu display so data is cached before the user
        clicks 'Load GeoZarr...'. Both _stac_cache and geozarr_metadata._cache
        are thread-safe.
        """
        with _stac_cache_lock:
            if stac_api_url in _stac_cache:
                return

        def _fetch():
            item = _fetch_stac_item_json(stac_api_url)
            if not item:
                return
            href, _ = _extract_zarr_href(item.get("assets", {}))
            if href:
                geozarr_metadata.fetch_resolved(href)

        t = threading.Thread(target=_fetch, daemon=True)
        t.start()

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
                _error_dialog(
                    "No Zarr assets found in this STAC item.\n\n"
                    "The item may use a different format (COG, NetCDF).",
                )
                return
            zarr_url = ""

        self._pending = {
            "item_name": item_name,
            "collection_id": self._extract_collection_id(item),
            "stac_api_url": stac_api_url,
        }

        self._msg("Fetching metadata...")
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)

        # Disconnect old thread to prevent double-click race
        self._disconnect_thread(self._fetch_thread)
        self._pending_thumbnail = None
        self._pending_dialog = None
        self._fetch_thread = _ProviderFetchThread(zarr_url, stac_api_url)
        self._fetch_thread.finished.connect(self._on_fetch_done)
        self._fetch_thread.thumbnail_ready.connect(self._on_thumbnail_ready)
        self._fetch_thread.start()

    def _on_thumbnail_ready(self, data: bytes) -> None:
        """Cache thumbnail; forward to dialog if already open."""
        self._pending_thumbnail = data
        if self._pending_dialog is not None:
            self._pending_dialog.set_thumbnail(data)

    def _on_fetch_done(self, info, zarr_url) -> None:
        """Handle completed metadata fetch (main thread)."""
        QApplication.restoreOverrideCursor()
        self._msg_clear()

        if not self._validate_fetch_result(info, zarr_url):
            return

        item_name = self._pending.get("item_name", "")
        collection_id = self._pending.get("collection_id", "")

        # Extract STAC properties for QA display
        stac_props = {}
        stac_url = self._pending.get("stac_api_url", "")
        if stac_url:
            item_json = _stac_cache.get(stac_url, {})
            stac_props = item_json.get("properties", {})

        dlg = GeoZarrLoadDialog(
            info,
            parent=None,
            collection_id=collection_id,
            zarr_url=zarr_url,
            item_name=item_name,
            stac_properties=stac_props,
        )
        self._pending_dialog = dlg
        # Apply cached thumbnail if it arrived before dialog was created
        if self._pending_thumbnail:
            dlg.set_thumbnail(self._pending_thumbnail)

        if dlg.exec() != QDialog.DialogCode.Accepted:
            self._cleanup_fetch_thread()
            return

        # Wait for pre-warm to finish (runs after finished.emit in thread).
        if self._fetch_thread and self._fetch_thread.isRunning():
            self._fetch_thread.wait(8000)

        self._pending_dialog = None
        self._pending_thumbnail = None

        bands = dlg.selected_bands()
        if not bands:
            _error_dialog("No bands selected.")
            return

        from . import band_presets
        satellite = band_presets.detect_satellite(collection_id) if collection_id else ""

        stretch = dlg.stretch_range()

        self._msg("Loading...")
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        QApplication.processEvents()  # flush UI before blocking GDALOpenEx
        try:
            _create_layer(
                zarr_url, bands, dlg.selected_resolution(),
                dlg.layer_name(), info, satellite=satellite or "",
                stretch_range=stretch,
            )
        finally:
            QApplication.restoreOverrideCursor()
            self._msg_clear()

    @staticmethod
    def _validate_fetch_result(info, zarr_url) -> bool:
        """Check fetch result and show error dialog if invalid. Returns True if OK."""
        if info is None:
            if not zarr_url:
                _error_dialog(
                    "No Zarr assets found in this STAC item.\n\n"
                    "The item may use a different format (COG, NetCDF).",
                )
            else:
                _error_dialog(
                    "Could not read metadata.",
                    detail=f"URL: {zarr_url}\n\n"
                    "Check that the URL points to a Zarr store root "
                    "(.zarr directory).",
                )
            return False

        if not info.resolutions or not any(info.bands_per_resolution.values()):
            _error_dialog(
                "No bands found in metadata.\n\n"
                "Expected resolution groups (r10m, r20m) or flat band arrays. "
                "The dataset may not follow GeoZarr conventions.",
            )
            return False

        if not info.epsg:
            QgsMessageLog.logMessage(
                "No CRS metadata found (proj:code, proj:projjson, or "
                "other_metadata) - layer may lack georeferencing",
                TAG, Qgis.Warning,
            )
        if not info.geotransform:
            QgsMessageLog.logMessage(
                "No geotransform found (spatial:transform) - layer may "
                "lack spatial positioning",
                TAG, Qgis.Warning,
            )
        return True

    def _load_timeseries(self, item: QgsDataItem) -> None:
        """Show time series search dialog for a STAC collection."""
        ctx = self._build_stac_context(item)
        if not ctx:
            _error_dialog("Could not resolve STAC connection for this item.")
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

        if dlg.exec() != QDialog.DialogCode.Accepted:
            self._cleanup_search_thread(dlg)
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
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        try:
            vrt_path = _build_temporal_vrt(items, band, resolution, info)
            if not vrt_path:
                _error_dialog("Failed to build temporal VRT.")
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

    def _cleanup_fetch_thread(self) -> None:
        """Stop fetch/pre-warm thread and clear pending state."""
        if self._fetch_thread and self._fetch_thread.isRunning():
            self._disconnect_thread(self._fetch_thread)
            self._fetch_thread.quit()
            self._fetch_thread.wait(3000)
        self._pending_dialog = None
        self._pending_thumbnail = None

    @staticmethod
    def _disconnect_thread(thread) -> None:
        """Safely disconnect a QThread's signals to prevent stale callbacks."""
        if thread is None:
            return
        try:
            thread.finished.disconnect()
        except (RuntimeError, TypeError):
            pass

    @staticmethod
    def _cleanup_search_thread(dlg) -> None:
        """Stop search thread stored on a dialog."""
        thread = getattr(dlg, "_search_thread", None)
        if thread and thread.isRunning():
            try:
                thread.finished.disconnect()
                thread.error.disconnect()
            except (RuntimeError, TypeError):
                pass
            thread.quit()
            thread.wait(3000)

    def stop_fetch(self) -> None:
        """Cancel any running fetch thread."""
        self._cleanup_fetch_thread()
        self._fetch_thread = None


def _vsi_prefix(url: str) -> str:
    if url.startswith("s3://"):
        return "/vsis3/"
    return "/vsicurl/"


def _band_uri(zarr_url: str, resolution: str, band: str, sub_group: str = "") -> str:
    base = f"{zarr_url}/{sub_group}" if sub_group else zarr_url
    # "default" = flat store with no resolution segment in the path
    if resolution and resolution != "default":
        path = f"{base}/{resolution}/{band}"
    else:
        path = f"{base}/{band}"
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


def _create_layer(
    zarr_url, bands, resolution, layer_name, info, satellite: str = "",
    stretch_range=None,
) -> None:
    """Create a QgsRasterLayer from selected bands."""
    if len(bands) == 1 and not _overview_resolutions(resolution, bands, info):
        _create_single_band_layer(zarr_url, bands[0], resolution, layer_name, info)
    else:
        _create_multiband_vrt_layer(
            zarr_url, bands, resolution, layer_name, info, satellite,
            stretch_range=stretch_range,
        )


def _create_single_band_layer(zarr_url, band, resolution, layer_name, info) -> None:
    """Direct ZARR open for single band, no VRT needed."""
    uri = _band_uri(zarr_url, resolution, band)
    layer = QgsRasterLayer(uri, layer_name, "gdal")
    if not layer.isValid():
        _error_dialog("Failed to load layer.", detail=layer.error().message())
        return
    if info.epsg and not layer.crs().isValid():
        layer.setCrs(QgsCoordinateReferenceSystem(f"EPSG:{info.epsg}"))
    _auto_style_single(layer)
    QgsProject.instance().addMapLayer(layer)


def _auto_style_single(layer: QgsRasterLayer) -> None:
    """Auto-style single-band: classified colormap for discrete data, gray for continuous."""
    dp = layer.dataProvider()
    dt = dp.dataType(1)
    # Only attempt classified styling for integer types
    int_types = {Qgis.DataType.Byte, Qgis.DataType.Int16, Qgis.DataType.UInt16,
                 Qgis.DataType.Int32, Qgis.DataType.UInt32}
    if dt not in int_types:
        return
    classes = QgsPalettedRasterRenderer.classDataFromRaster(
        dp, 1, ramp=None, feedback=None,
    )
    if not classes or len(classes) > 20:
        return  # too many values = continuous data, use default gray
    renderer = QgsPalettedRasterRenderer(dp, 1, classes)
    layer.setRenderer(renderer)


def _build_multiband_vrt_xml(zarr_url, bands, resolution, info, satellite="") -> str:
    """Build multi-band VRT XML with overview references. Zero HTTP calls.

    Uses metadata from ZarrRootInfo instead of gdal.BuildVRT() which would
    open every remote source to read dimensions. Returns temp file path.

    Overview references point to coarser-resolution bands, pre-warmed in
    _ProviderFetchThread so they open from the vsicurl cache.
    """
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

    from . import band_presets as _bp

    for i, band_name in enumerate(bands, 1):
        band_el = ET.SubElement(
            root, "VRTRasterBand", dataType=dtype, band=str(i),
        )
        desc = _bp.get_band_label(satellite, band_name) if satellite else band_name
        ET.SubElement(band_el, "Description").text = desc
        uri = _band_uri(zarr_url, resolution, band_name)
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

    # Add overview references to coarser resolutions (pre-warmed in cache)
    ovr_levels = _overview_resolutions(resolution, bands, info)
    for ovr_res, ovr_bands in ovr_levels:
        ovr_shape = info.shape_per_resolution.get(ovr_res, (0, 0))
        ony, onx = ovr_shape
        if ony == 0:
            continue
        ovr_set = {b.upper() for b in ovr_bands}
        for i, band_name in enumerate(bands):
            if band_name.upper() not in ovr_set:
                continue
            band_el = root.findall("VRTRasterBand")[i]
            ovr = ET.SubElement(band_el, "Overview")
            ovr_uri = _band_uri(zarr_url, ovr_res, band_name)
            ET.SubElement(ovr, "SourceFilename", relativeToVRT="0").text = ovr_uri
            ET.SubElement(ovr, "SourceBand").text = "1"

    vrt_file = tempfile.NamedTemporaryFile(
        suffix=".vrt", prefix="geozarr_", delete=False,
    )
    path = _track_temp(vrt_file.name)
    vrt_file.close()
    try:
        ET.ElementTree(root).write(path, xml_declaration=True, encoding="utf-8")
    except Exception:
        _untrack_and_remove(path)
        raise
    return path


def _create_multiband_vrt_layer(
    zarr_url, bands, resolution, layer_name, info, satellite: str = "",
    stretch_range=None,
) -> None:
    """Build multi-band VRT with optional multiscale overviews."""
    import time as _time

    t0 = _time.monotonic()
    vrt_path = _build_multiband_vrt_xml(zarr_url, bands, resolution, info, satellite)
    t_vrt = _time.monotonic()
    log.debug("VRT build: %.3fs", t_vrt - t0)

    if not vrt_path:
        _error_dialog(
            f"Failed to build VRT: missing shape metadata for "
            f"resolution {resolution}.",
        )
        return

    layer = QgsRasterLayer(vrt_path, layer_name, "gdal")
    t_open = _time.monotonic()
    log.debug("Layer open: %.3fs", t_open - t_vrt)

    if not layer.isValid():
        _error_dialog("Failed to load VRT.", detail=layer.error().message())
        return

    dtype = info.dtype_per_resolution.get(resolution, "")
    _auto_style(layer, len(bands), dtype=dtype, satellite=satellite,
                stretch_range=stretch_range)
    t_style = _time.monotonic()
    log.debug("Auto-style: %.3fs", t_style - t_open)

    QgsProject.instance().addMapLayer(layer)
    t_add = _time.monotonic()
    log.debug("Add to project: %.3fs (total post-dialog: %.2fs)",
              t_add - t_style, t_add - t0)
    QgsMessageLog.logMessage(
        f"Loaded: {layer_name} ({len(bands)} bands, {resolution}) "
        f"in {t_add - t0:.1f}s",
        TAG, Qgis.Info,
    )


_STRETCH_DEFAULTS = {
    ("sentinel-2", "UInt16"): (0, 3000),
    ("sentinel-2", "Float32"): (0.0, 0.4),  # BOA reflectance 0-1
    ("sentinel-2", "Float64"): (0.0, 0.4),
    ("sentinel-3", "UInt16"): (0, 3000),
    ("sentinel-3", "Float32"): (0.0, 0.4),
    ("landsat-8", "UInt16"): (0, 12000),
    ("landsat-9", "UInt16"): (0, 12000),
}
_DTYPE_DEFAULTS = {
    "UInt16": (0, 4000),
    "Int16": (-1000, 4000),
    "Float32": (0.0, 1.0),
    "Float64": (0.0, 1.0),
    "Byte": (0, 255),
}


def _auto_style(
    layer: QgsRasterLayer, band_count: int,
    dtype: str = "", satellite: str = "",
    stretch_range=None,
) -> None:
    """Apply RGB stretch.

    Priority: user override > satellite+dtype > dtype > cumulativeCut.
    """
    if band_count != 3:
        return
    dp = layer.dataProvider()
    renderer = QgsMultiBandColorRenderer(dp, 1, 2, 3)

    # Priority 1: user override from dialog
    lo, hi = None, None
    if stretch_range:
        lo, hi = stretch_range
    # Priority 2: satellite+dtype defaults
    if lo is None and satellite and dtype:
        lo, hi = _STRETCH_DEFAULTS.get((satellite, dtype), (None, None))
    # Priority 3: dtype defaults
    if lo is None and dtype:
        lo, hi = _DTYPE_DEFAULTS.get(dtype, (None, None))

    # Fallback: cumulativeCut with small center extent
    sample_extent = None
    if lo is None:
        ext = layer.extent()
        cx, cy = ext.center().x(), ext.center().y()
        w, h = ext.width() * 0.05, ext.height() * 0.05
        sample_extent = QgsRectangle(cx - w / 2, cy - h / 2, cx + w / 2, cy + h / 2)

    for band_idx, setter in (
        (1, renderer.setRedContrastEnhancement),
        (2, renderer.setGreenContrastEnhancement),
        (3, renderer.setBlueContrastEnhancement),
    ):
        if lo is not None:
            band_lo, band_hi = float(lo), float(hi)
        else:
            band_lo, band_hi = dp.cumulativeCut(
                band_idx, 0.02, 0.98, sample_extent, 250,
            )
        ce = QgsContrastEnhancement(dp.dataType(band_idx))
        ce.setMinimumValue(band_lo)
        ce.setMaximumValue(band_hi)
        ce.setContrastEnhancementAlgorithm(
            QgsContrastEnhancement.ContrastEnhancementAlgorithm.StretchToMinimumMaximum,
        )
        setter(ce)
    layer.setRenderer(renderer)


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
        # Band name: date + cloud% + item ID for identify tool
        dt_label = item["datetime"][:10]
        item_id = item.get("id", "")
        cc = item.get("cloud_cover")
        cc_str = f" ({cc:.0f}%)" if cc is not None else ""
        desc = f"{dt_label}{cc_str} {item_id}" if item_id else f"{dt_label}{cc_str}"
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
        _error_dialog("Failed to load temporal VRT.", detail=layer.error().message())
        return

    # Single-band gray renderer (temporal mode switches which band is shown)
    renderer = QgsSingleBandGrayRenderer(layer.dataProvider(), 1)
    ce = QgsContrastEnhancement(layer.dataProvider().dataType(1))
    ce.setContrastEnhancementAlgorithm(
        QgsContrastEnhancement.ContrastEnhancementAlgorithm.StretchToMinimumMaximum,
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
        # Enum path varies across QGIS versions
        try:
            animated = QgsTemporalNavigationObject.NavigationMode.Animated
        except AttributeError:
            animated = QgsTemporalNavigationObject.Animated
        ctrl.setNavigationMode(animated)
        ctrl.setCurrentFrameNumber(0)
    except Exception as e:
        QgsMessageLog.logMessage(
            f"Temporal controller setup failed: {e}", TAG, Qgis.Warning,
        )
