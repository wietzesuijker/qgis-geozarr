"""GeoZarr convention parsing from zarr.json (v3) and .zmetadata (v2).

Reads a single metadata file to discover resolutions, bands, CRS, and transform.
Zarr v3: user attributes under root["attributes"], consolidated_metadata.
Zarr v2: .zmetadata with paths like "measurements/reflectance/r10m/b02/.zarray".
"""

from __future__ import annotations

import json
import logging
import re
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from osgeo import gdal

log = logging.getLogger(__name__)

# Coordinate/auxiliary arrays - not spectral bands
_NON_BAND = frozenset({
    "spatial_ref", "x", "y", "crs", "time", "latitude", "longitude",
})


@dataclass
class ZarrRootInfo:
    """Parsed zarr.json: resolutions, bands, and GeoZarr conventions."""

    resolutions: Tuple[str, ...]
    bands_per_resolution: Dict[str, Tuple[str, ...]]
    shape_per_resolution: Dict[str, Tuple[int, int]] = field(default_factory=dict)
    transform_per_resolution: Dict[str, Tuple[float, ...]] = field(default_factory=dict)
    epsg: Optional[int] = None
    geotransform: Optional[Tuple[float, ...]] = None  # GDAL order
    conventions: Tuple[str, ...] = ()
    sub_group: str = ""  # prefix path for bands (e.g. "measurements/reflectance")


_cache: Dict[str, ZarrRootInfo] = {}
_lock = threading.Lock()


def _vsi_read(url: str) -> Optional[bytes]:
    """Read a remote file via GDAL's /vsicurl/. Returns None on failure."""
    vsi_path = f"/vsicurl/{url}"
    fp = gdal.VSIFOpenL(vsi_path, "rb")
    if fp is None:
        return None
    data = b""
    while True:
        chunk = gdal.VSIFReadL(1, 65536, fp)
        if not chunk:
            break
        data += chunk
    gdal.VSIFCloseL(fp)
    return data if data else None


def fetch(zarr_url: str) -> Optional[ZarrRootInfo]:
    """Fetch and cache metadata (v3 zarr.json or v2 .zmetadata)."""
    url = zarr_url.rstrip("/")

    with _lock:
        if url in _cache:
            return _cache[url]

    info = None

    # Try Zarr v3 first
    try:
        data = _vsi_read(f"{url}/zarr.json")
        if data:
            info = _parse(json.loads(data))
    except Exception:
        log.debug("v3 zarr.json parse failed for %s", url, exc_info=True)

    # Fall back to Zarr v2
    if info is None:
        try:
            data = _vsi_read(f"{url}/.zmetadata")
            if data:
                info = _parse_v2(json.loads(data))
        except Exception:
            log.debug("v2 .zmetadata parse failed for %s", url, exc_info=True)

    if info is None:
        log.debug("No zarr.json or .zmetadata found at %s", url)
        return None

    with _lock:
        _cache[url] = info
    return info


def clear_cache() -> None:
    with _lock:
        _cache.clear()


def fetch_resolved(zarr_url: str) -> Tuple[Optional[ZarrRootInfo], str]:
    """Fetch metadata and resolve sub-groups. Returns (info, final_url).

    When bands live under a sub-group (e.g. measurements/reflectance),
    the returned URL includes the sub-group prefix so _band_uri()
    constructs correct paths like {url}/r10m/b02.
    """
    info = fetch(zarr_url)
    url = zarr_url.rstrip("/")
    if info and info.sub_group:
        sub_url = f"{url}/{info.sub_group}"
        # v3: each group has its own zarr.json
        sub_info = fetch(sub_url)
        if sub_info and sub_info.resolutions:
            return sub_info, sub_url
        # v2: sub-group has no separate metadata, use root info with sub URL
        if info.resolutions:
            return info, sub_url
    return info, url


def _parse(root: Dict[str, Any]) -> ZarrRootInfo:
    """Extract resolutions, bands, CRS, and transform from zarr.json."""
    bands_per_res: Dict[str, List[str]] = {}
    shape_per_res: Dict[str, Tuple[int, int]] = {}
    transform_per_res: Dict[str, Tuple[float, ...]] = {}

    # Zarr v3: user attributes under "attributes" key
    attrs = root.get("attributes", {})
    if not isinstance(attrs, dict):
        attrs = {}
    src = {**root, **attrs}

    # Parse GeoZarr conventions
    conventions = _parse_conventions(src)
    epsg = _parse_crs(src)
    geotransform = _parse_transform(src)

    # Multiscales: extract resolution levels
    ms = src.get("multiscales")
    if isinstance(ms, dict):
        for entry in ms.get("layout", []):
            if not isinstance(entry, dict):
                continue
            asset = entry.get("asset")
            if not asset:
                continue
            shape = entry.get("spatial:shape")
            if isinstance(shape, (list, tuple)) and len(shape) >= 2:
                shape_per_res[asset] = (int(shape[-2]), int(shape[-1]))
            st = entry.get("spatial:transform")
            if isinstance(st, (list, tuple)) and len(st) == 6:
                vals = [float(v) for v in st]
                # spatial:transform [a,b,c,d,e,f] -> GDAL [c,a,b,f,d,e]
                gdal_gt = (vals[2], vals[0], vals[1],
                           vals[5], vals[3], vals[4])
                transform_per_res[asset] = gdal_gt
                if geotransform is None:
                    geotransform = gdal_gt

    # Consolidated metadata: discover resolution/band structure at any depth.
    # Paths like "r10m/b02" (depth 2) or "measurements/reflectance/r10m/b02"
    # (depth 4) are both handled. We find the sub-group with the most bands.
    consol = root.get("consolidated_metadata", {}).get("metadata", {})
    sub_group = ""

    if consol:
        bands_per_res, sub_group, shape_per_res = _parse_consolidated(
            consol, shape_per_res,
        )

    # Members fallback (inline zarr.json without consolidated_metadata)
    if not bands_per_res:
        for key, value in root.get("members", {}).items():
            if isinstance(value, dict) and value.get("node_type") == "group":
                bands = [
                    k for k, v in value.get("members", {}).items()
                    if isinstance(v, dict) and v.get("node_type") == "array"
                    and k not in _NON_BAND
                ]
                if bands:
                    bands_per_res[key] = bands

    def _sort_key(name: str) -> int:
        m = re.search(r"(\d+)", name)
        return int(m.group(1)) if m else 0

    resolutions = sorted(bands_per_res.keys(), key=_sort_key)

    return ZarrRootInfo(
        resolutions=tuple(resolutions),
        bands_per_resolution={k: tuple(sorted(v)) for k, v in bands_per_res.items()},
        shape_per_resolution=shape_per_res,
        transform_per_resolution=transform_per_res,
        epsg=epsg,
        geotransform=geotransform,
        conventions=tuple(conventions),
        sub_group=sub_group,
    )


def _parse_v2(zmetadata: Dict[str, Any]) -> ZarrRootInfo:
    """Parse Zarr v2 .zmetadata into ZarrRootInfo.

    Transforms v2 consolidated paths (ending in /.zarray, /.zattrs, /.zgroup)
    into the same structure _parse_consolidated() expects.
    """
    meta = zmetadata.get("metadata", {})

    # Root attributes for CRS
    root_attrs = meta.get(".zattrs", {})
    if not isinstance(root_attrs, dict):
        root_attrs = {}
    epsg = _parse_crs(root_attrs)

    # Build array-path -> metadata dict from /.zarray entries
    consol: Dict[str, Any] = {}
    for path, value in meta.items():
        if not path.endswith("/.zarray"):
            continue
        array_path = path[: -len("/.zarray")]
        shape = value.get("shape") if isinstance(value, dict) else None
        consol[array_path] = {"node_type": "array", "shape": shape}

    shape_per_res: Dict[str, Tuple[int, int]] = {}
    bands_per_res, sub_group, shape_per_res = _parse_consolidated(
        consol, shape_per_res,
    )

    def _sort_key(name: str) -> int:
        m = re.search(r"(\d+)", name)
        return int(m.group(1)) if m else 0

    resolutions = sorted(bands_per_res.keys(), key=_sort_key)

    return ZarrRootInfo(
        resolutions=tuple(resolutions),
        bands_per_resolution={
            k: tuple(sorted(v)) for k, v in bands_per_res.items()
        },
        shape_per_resolution=shape_per_res,
        epsg=epsg,
        conventions=(),
        sub_group=sub_group,
    )


_RES_RE = re.compile(r"r\d+m$")


def _parse_consolidated(
    consol: Dict[str, Any],
    shape_per_res: Dict[str, Tuple[int, int]],
) -> Tuple[Dict[str, List[str]], str, Dict[str, Tuple[int, int]]]:
    """Parse consolidated_metadata paths at any depth.

    Returns (bands_per_res, sub_group_prefix, shape_per_res).

    Scans for leaf arrays under resolution segments (r10m, r20m, ...).
    Groups by prefix path. If multiple prefixes exist (e.g.
    measurements/reflectance vs conditions/mask/detector_footprint),
    picks the one with the most bands.
    """
    # prefix -> {resolution -> [band_names]}
    groups: Dict[str, Dict[str, List[str]]] = {}

    for path, meta in consol.items():
        parts = path.strip("/").split("/")
        if len(parts) < 2:
            continue

        # Find resolution segment: second-to-last must match r\d+m,
        # last segment is the band/array name
        res_seg = parts[-2]
        leaf = parts[-1]
        if not _RES_RE.match(res_seg):
            continue
        if leaf in _NON_BAND:
            continue
        if not isinstance(meta, dict) or meta.get("node_type") != "array":
            continue

        prefix = "/".join(parts[:-2])
        groups.setdefault(prefix, {}).setdefault(res_seg, []).append(leaf)

        # Extract shape
        if res_seg not in shape_per_res:
            shape = meta.get("shape")
            if isinstance(shape, (list, tuple)) and len(shape) >= 2:
                shape_per_res[res_seg] = (int(shape[-2]), int(shape[-1]))

    if not groups:
        return {}, "", shape_per_res

    # Pick prefix with the most total bands
    best_prefix = max(
        groups, key=lambda p: sum(len(v) for v in groups[p].values()),
    )
    return groups[best_prefix], best_prefix, shape_per_res


def _parse_conventions(src: Dict[str, Any]) -> List[str]:
    """Parse zarr_conventions attribute."""
    conventions: List[str] = []
    zc = src.get("zarr_conventions")
    if isinstance(zc, list):
        for entry in zc:
            if isinstance(entry, dict):
                name = entry.get("name", "")
                if name:
                    conventions.append(name.rstrip(":"))
    elif isinstance(zc, dict):
        for _uuid, entry in zc.items():
            if isinstance(entry, dict):
                name = entry.get("name", "")
                if name:
                    conventions.append(name.rstrip(":"))
    return conventions


def _parse_crs(src: Dict[str, Any]) -> Optional[int]:
    """Extract EPSG code from proj:code, proj:projjson, or v2 other_metadata."""
    code = src.get("proj:code")
    if isinstance(code, str) and code.upper().startswith("EPSG:"):
        try:
            return int(code.split(":", 1)[1])
        except (ValueError, IndexError):
            pass

    projjson = src.get("proj:projjson")
    if isinstance(projjson, dict):
        pid = projjson.get("id", {})
        if isinstance(pid, dict) and pid.get("authority") == "EPSG":
            try:
                return int(pid["code"])
            except (KeyError, ValueError):
                pass

    # Zarr v2 EOPF: other_metadata.horizontal_CRS_code = "EPSG:XXXXX"
    om = src.get("other_metadata")
    if isinstance(om, dict):
        hcrs = om.get("horizontal_CRS_code", "")
        if isinstance(hcrs, str) and hcrs.upper().startswith("EPSG:"):
            try:
                return int(hcrs.split(":", 1)[1])
            except (ValueError, IndexError):
                pass

    return None


def _parse_transform(src: Dict[str, Any]) -> Optional[Tuple[float, ...]]:
    """Parse spatial:transform -> GDAL geotransform order."""
    st = src.get("spatial:transform")
    if isinstance(st, (list, tuple)) and len(st) == 6:
        try:
            vals = [float(v) for v in st]
            # spatial:transform [a,b,c,d,e,f] -> GDAL [c,a,b,f,d,e]
            return (vals[2], vals[0], vals[1], vals[5], vals[3], vals[4])
        except (ValueError, TypeError):
            pass
    return None
