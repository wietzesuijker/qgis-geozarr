"""QgsDataItemGuiProvider: adds 'Load GeoZarr...' to STAC Zarr assets."""

from __future__ import annotations

import json
import os
import re
import tempfile
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
from qgis.PyQt.QtWidgets import QAction, QMessageBox

from . import geozarr_metadata
from .geozarr_dialog import GeoZarrLoadDialog

TAG = "GeoZarr"


def _find_zarr_root(url: str) -> str:
    """Find the Zarr store root from a deep asset URL.

    EOPF hrefs point deep into the store:
        https://.../S2A_...zarr/quality/atmosphere/r10m/aot
        https://.../S2A_...zarr/measurements/reflectance
    We truncate at the .zarr segment to get the store root.
    """
    # Find .zarr in path and truncate there
    m = re.search(r"(https?://[^?#]*\.zarr)", url)
    if m:
        return m.group(1)
    return url


class GeoZarrDataItemGuiProvider(QgsDataItemGuiProvider):
    """Injects 'Load GeoZarr...' context menu on STAC Zarr assets."""

    def name(self) -> str:
        return "GeoZarr"

    def populateContextMenu(
        self,
        item: QgsDataItem,
        menu,
        selectedItems,
        context,
    ):
        zarr_url = self._detect_zarr(item)
        if not zarr_url:
            return

        action = QAction("Load GeoZarr...", menu)
        action.triggered.connect(lambda: self._load_geozarr(item, zarr_url))
        menu.addAction(action)

    def _detect_zarr(self, item: QgsDataItem) -> str:
        """Detect if item is Zarr and return store root URL.

        Four detection strategies (in order):
        1. mimeUris with ZARR: prefix (QGIS-recognized Zarr assets)
        2. mimeUris with .zarr in href
        3. Item path/name containing .zarr
        4. STAC item - resolve Zarr URL on click via STAC API
        """
        try:
            for uri in item.mimeUris():
                raw = uri.uri or ""
                if "ZARR:" in raw.upper():
                    return self._zarr_root_from_gdal_uri(raw)
                if ".zarr" in raw.lower():
                    return _find_zarr_root(raw)
        except Exception:
            pass

        try:
            path = item.path() or ""
            name = item.name() or ""
            if ".zarr" in path.lower() or ".zarr" in name.lower():
                return self._zarr_from_parent(item)
        except Exception:
            pass

        # Strategy 4: any STAC item - resolve via API when clicked
        try:
            path = item.path() or ""
            if "/items/" in path and "stac" in path.lower():
                return f"STAC:{path}"
        except Exception:
            pass

        return ""

    def _zarr_root_from_gdal_uri(self, gdal_uri: str) -> str:
        """Extract store root from ZARR:"/vsicurl/https://.../.zarr/..." """
        raw = gdal_uri
        if raw.upper().startswith("ZARR:"):
            raw = raw[5:]
        if raw.startswith('"') and raw.endswith('"'):
            raw = raw[1:-1]
        if raw.startswith("/vsicurl/"):
            raw = raw[9:]
        return _find_zarr_root(raw)

    def _zarr_from_parent(self, item: QgsDataItem) -> str:
        """Walk up the tree looking for a parent with Zarr URIs."""
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
                pass
            parent = parent.parent()
        return ""

    def _extract_collection_id(self, item: QgsDataItem) -> str:
        """Try to extract STAC collection ID from item hierarchy."""
        parent = item.parent()
        while parent:
            name = parent.name() if hasattr(parent, "name") else ""
            path = parent.path() if hasattr(parent, "path") else ""
            if "collection" in path.lower():
                return name
            parent = parent.parent()
        return ""

    def _resolve_stac_zarr_url(self, item: QgsDataItem) -> str:
        """Resolve Zarr store URL from a STAC item via the STAC API.

        Extracts connection/collection/item IDs from the browser path
        (not display names, which may differ from API IDs).
        """
        # Parse IDs from browser path - more reliable than display names.
        # Path format: stac:/.../collections/<id>/items/<id>
        path = item.path() or ""
        item_id, collection_id, conn_name = "", "", ""

        parts = path.split("/")
        for i, part in enumerate(parts):
            if part == "items" and i + 1 < len(parts):
                item_id = parts[i + 1]
            elif part == "collections" and i + 1 < len(parts):
                collection_id = parts[i + 1]

        # Fall back to display names if path parsing fails
        if not item_id:
            item_id = item.name()
        if not collection_id:
            p = item.parent()
            collection_id = p.name() if p else ""

        # Connection name from tree (display name = connection name in settings)
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
                TAG, Qgis.Warning,
            )
            return ""

        try:
            conn_data = QgsStacConnection.connection(conn_name)
        except Exception:
            return ""
        if not conn_data or not conn_data.url:
            return ""

        api_url = (
            f"{conn_data.url.rstrip('/')}"
            f"/collections/{collection_id}/items/{item_id}"
        )

        url, _key = self._fetch_zarr_href(api_url)
        return url

    def _fetch_zarr_href(self, stac_item_url: str) -> tuple:
        """Fetch a STAC item JSON and return (zarr_root_url, asset_key)."""
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

    def _stac_item_name(self, item: QgsDataItem) -> str:
        """Extract STAC item ID from browser path or display name."""
        path = item.path() or ""
        parts = path.split("/")
        for i, part in enumerate(parts):
            if part == "items" and i + 1 < len(parts):
                return parts[i + 1]
        return item.name() or ""

    def _load_geozarr(self, item: QgsDataItem, zarr_url: str) -> None:
        """Fetch metadata, show dialog, create layer."""
        item_name = ""

        # Resolve STAC sentinel value to actual URL
        if zarr_url.startswith("STAC:"):
            item_name = self._stac_item_name(item)
            zarr_url = self._resolve_stac_zarr_url(item)
            if not zarr_url:
                QMessageBox.warning(
                    None, "GeoZarr",
                    "No Zarr assets found in this STAC item.",
                )
                return

        info = geozarr_metadata.fetch(zarr_url)

        # If parser found bands under a sub-group, re-fetch from there
        # for full CRS/multiscales metadata
        if info and info.sub_group and not info.epsg:
            sub_url = f"{zarr_url}/{info.sub_group}"
            sub_info = geozarr_metadata.fetch(sub_url)
            if sub_info and sub_info.resolutions:
                zarr_url = sub_url
                info = sub_info

        if info is None:
            QMessageBox.warning(
                None, "GeoZarr",
                f"Could not read zarr.json from:\n{zarr_url}",
            )
            return

        if not info.resolutions or not any(info.bands_per_resolution.values()):
            QMessageBox.warning(
                None, "GeoZarr",
                "No bands found in zarr.json. The dataset may not follow "
                "GeoZarr conventions.",
            )
            return

        collection_id = self._extract_collection_id(item)

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
            QMessageBox.warning(None, "GeoZarr", "No bands selected.")
            return

        _create_layer(zarr_url, bands, resolution, layer_name, info)


def _vsi_prefix(url: str) -> str:
    """Return VSI prefix for URL scheme."""
    if url.startswith("s3://"):
        return "/vsis3/"
    return "/vsicurl/"


def _band_uri(zarr_url: str, resolution: str, band: str) -> str:
    """Build ZARR: GDAL URI for a single band array."""
    path = f"{zarr_url}/{resolution}/{band}" if resolution else f"{zarr_url}/{band}"
    prefix = _vsi_prefix(path)
    if prefix == "/vsis3/":
        path = path[5:]  # strip s3://
    return f'ZARR:"{prefix}{path}"'


def _res_pixel_size(name: str) -> int:
    """Extract numeric pixel size from resolution name (r10m -> 10)."""
    m = re.search(r"(\d+)", name)
    return int(m.group(1)) if m else 0


def _overview_resolutions(
    base_res: str,
    bands: list,
    info,
) -> list:
    """Find coarser resolutions where all requested bands exist.

    Returns [(res_name, [bands_at_this_level]), ...] coarsest last.
    Only includes levels coarser than base_res.
    """
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


def _build_overview_vrt(
    zarr_url: str,
    bands: list,
    resolution: str,
    info,
) -> str:
    """Build a VRT for one overview level. Returns temp file path."""
    band_uris = [_band_uri(zarr_url, resolution, b) for b in bands]

    vrt_file = tempfile.NamedTemporaryFile(
        suffix=".vrt", prefix=f"geozarr_ovr_{resolution}_", delete=False,
    )
    vrt_path = vrt_file.name
    vrt_file.close()

    vrt_opts = gdal.BuildVRTOptions(separate=True)
    vrt_ds = gdal.BuildVRT(vrt_path, band_uris, options=vrt_opts)
    if vrt_ds is None:
        return ""

    # Set geotransform from multiscales metadata
    gt = info.transform_per_resolution.get(resolution)
    if gt:
        vrt_ds.SetGeoTransform(gt)
    elif info.geotransform and info.shape_per_resolution.get(resolution):
        # Derive from base geotransform + shape ratio
        base_gt = info.geotransform
        ovr_shape = info.shape_per_resolution[resolution]
        base_shape = max(info.shape_per_resolution.values(), key=lambda s: s[0] * s[1])
        if base_shape[0] > 0 and base_shape[1] > 0:
            sx = (base_gt[1] * base_shape[1]) / ovr_shape[1]
            sy = (base_gt[5] * base_shape[0]) / ovr_shape[0]
            vrt_ds.SetGeoTransform((base_gt[0], sx, base_gt[2],
                                    base_gt[3], base_gt[4], sy))

    if info.epsg:
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(info.epsg)
        vrt_ds.SetProjection(srs.ExportToWkt())

    vrt_ds.FlushCache()
    vrt_ds = None
    return vrt_path


def _inject_overviews(
    base_vrt_path: str,
    ovr_vrt_paths: list,
    bands: list,
    ovr_bands: list,
) -> None:
    """Insert <Overview> elements into base VRT XML.

    ovr_bands[i] = list of band names available at overview level i.
    Band index in overview VRT matches position in that list.
    """
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
            # Find this band's index in the overview VRT
            try:
                ovr_idx = [b.upper() for b in ovr_band_list].index(band_name.upper())
            except ValueError:
                continue
            ovr = ET.SubElement(band_elem, "Overview")
            sf = ET.SubElement(ovr, "SourceFilename")
            sf.set("relativeToVRT", "1")
            sf.text = os.path.basename(ovr_path)
            sb = ET.SubElement(ovr, "SourceBand")
            sb.text = str(ovr_idx + 1)

    tree.write(base_vrt_path, xml_declaration=True, encoding="utf-8")


def _create_layer(
    zarr_url: str,
    bands: list,
    resolution: str,
    layer_name: str,
    info,
) -> None:
    """Create a QgsRasterLayer from selected bands.

    Single band: direct ZARR: URI.
    Multiple bands: VRT composite (RGB).
    CRS/geotransform injected from zarr.json metadata.
    """
    if len(bands) == 1 and not _overview_resolutions(resolution, bands, info):
        uri = _band_uri(zarr_url, resolution, bands[0])
        layer = QgsRasterLayer(uri, layer_name, "gdal")
        if not layer.isValid():
            QMessageBox.warning(
                None, "GeoZarr",
                f"Failed to load layer:\n{layer.error().message()}",
            )
            return
        if info.epsg and not layer.crs().isValid():
            layer.setCrs(QgsCoordinateReferenceSystem(f"EPSG:{info.epsg}"))
        QgsProject.instance().addMapLayer(layer)
        return

    # Multi-band: build VRT composite
    band_uris = [_band_uri(zarr_url, resolution, b) for b in bands]

    vrt_file = tempfile.NamedTemporaryFile(
        suffix=".vrt", prefix="geozarr_", delete=False,
    )
    vrt_path = vrt_file.name
    vrt_file.close()

    vrt_opts = gdal.BuildVRTOptions(separate=True)
    vrt_ds = gdal.BuildVRT(vrt_path, band_uris, options=vrt_opts)

    if vrt_ds is None:
        QMessageBox.warning(
            None, "GeoZarr",
            f"Failed to build VRT:\n{gdal.GetLastErrorMsg()}",
        )
        return

    # Inject CRS from zarr.json
    if info.epsg:
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(info.epsg)
        vrt_ds.SetProjection(srs.ExportToWkt())

    # Inject geotransform from zarr.json
    if info.geotransform:
        vrt_ds.SetGeoTransform(info.geotransform)

    vrt_ds.FlushCache()
    vrt_ds = None

    # Add multiscale overviews from coarser Zarr resolution levels
    ovr_levels = _overview_resolutions(resolution, bands, info)
    if ovr_levels:
        ovr_paths = []
        ovr_band_lists = []
        for ovr_res, ovr_bands_at_level in ovr_levels:
            ovr_path = _build_overview_vrt(zarr_url, ovr_bands_at_level, ovr_res, info)
            ovr_paths.append(ovr_path)
            ovr_band_lists.append(ovr_bands_at_level)
        _inject_overviews(vrt_path, ovr_paths, bands, ovr_band_lists)

    layer = QgsRasterLayer(vrt_path, layer_name, "gdal")
    if not layer.isValid():
        QMessageBox.warning(
            None, "GeoZarr",
            f"Failed to load VRT:\n{layer.error().message()}",
        )
        return

    QgsProject.instance().addMapLayer(layer)
    QgsMessageLog.logMessage(
        f"Loaded GeoZarr: {layer_name} ({len(bands)} bands)", TAG, Qgis.Info,
    )
