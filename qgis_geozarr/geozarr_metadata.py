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


# Zarr data_type -> GDAL VRT dataType
_ZARR_TO_GDAL_DTYPE: Dict[str, str] = {
    "bool": "Byte",
    "uint8": "Byte",
    "uint16": "UInt16",
    "int16": "Int16",
    "uint32": "UInt32",
    "int32": "Int32",
    "uint64": "UInt64",
    "int64": "Int64",
    "float32": "Float32",
    "float64": "Float64",
}

# Zarr v2 numpy dtype string -> canonical name (for _ZARR_TO_GDAL_DTYPE)
_V2_DTYPE_MAP: Dict[str, str] = {
    "|u1": "uint8", "<u2": "uint16", ">u2": "uint16",
    "<i2": "int16", ">i2": "int16",
    "<u4": "uint32", ">u4": "uint32",
    "<i4": "int32", ">i4": "int32",
    "<f4": "float32", ">f4": "float32",
    "<f8": "float64", ">f8": "float64",
}


@dataclass
class ZarrRootInfo:
    """Parsed zarr.json: resolutions, bands, and GeoZarr conventions."""

    resolutions: Tuple[str, ...]
    bands_per_resolution: Dict[str, Tuple[str, ...]]
    shape_per_resolution: Dict[str, Tuple[int, int]] = field(default_factory=dict)
    transform_per_resolution: Dict[str, Tuple[float, ...]] = field(default_factory=dict)
    dtype_per_resolution: Dict[str, str] = field(default_factory=dict)  # GDAL dtype
    epsg: Optional[int] = None
    geotransform: Optional[Tuple[float, ...]] = None  # GDAL order
    conventions: Tuple[str, ...] = ()
    sub_group: str = ""  # prefix path for bands (e.g. "measurements/reflectance")
    band_descriptions: Dict[str, str] = field(default_factory=dict)  # band_id -> description


def _res_sort_key(name: str) -> int:
    """Sort resolution names by numeric value (r10m -> 10, r20m -> 20)."""
    m = re.search(r"(\d+)", name)
    return int(m.group(1)) if m else 0


_cache: Dict[str, ZarrRootInfo] = {}
_lock = threading.Lock()


def _vsi_read(url: str) -> Optional[bytes]:
    """Read a remote file via GDAL's /vsicurl/. Returns None on failure."""
    vsi_path = f"/vsicurl/{url}"
    fp = gdal.VSIFOpenL(vsi_path, "rb")
    if fp is None:
        return None
    try:
        buf = bytearray()
        while True:
            chunk = gdal.VSIFReadL(1, 65536, fp)
            if not chunk:
                break
            buf.extend(chunk)
    finally:
        gdal.VSIFCloseL(fp)
    return bytes(buf) if buf else None


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
    except (json.JSONDecodeError, OSError, KeyError) as e:
        log.warning("Zarr v3 parse failed for %s: %s", url, e)

    # Fall back to Zarr v2
    if info is None:
        try:
            data = _vsi_read(f"{url}/.zmetadata")
            if data:
                info = _parse_v2(json.loads(data))
        except (json.JSONDecodeError, OSError, KeyError) as e:
            log.warning("Zarr v2 parse failed for %s: %s", url, e)

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
    dtype_per_res: Dict[str, str] = {}

    if consol:
        bands_per_res, sub_group, shape_per_res = _parse_consolidated(
            consol, shape_per_res, dtype_per_res,
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

    # Extract band descriptions from consolidated metadata attributes
    band_descriptions: Dict[str, str] = {}
    for path, meta in consol.items():
        if not isinstance(meta, dict):
            continue
        member_attrs = meta.get("attributes", {})
        if not isinstance(member_attrs, dict):
            continue
        band_id = path.rsplit("/", 1)[-1]
        desc = member_attrs.get("long_name") or member_attrs.get(
            "standard_name", "",
        )
        if desc and isinstance(desc, str):
            band_descriptions[band_id] = desc

    resolutions = sorted(bands_per_res.keys(), key=_res_sort_key)

    return ZarrRootInfo(
        resolutions=tuple(resolutions),
        bands_per_resolution={k: tuple(sorted(v)) for k, v in bands_per_res.items()},
        shape_per_resolution=shape_per_res,
        transform_per_resolution=transform_per_res,
        dtype_per_resolution=dtype_per_res,
        epsg=epsg,
        geotransform=geotransform,
        conventions=tuple(conventions),
        sub_group=sub_group,
        band_descriptions=band_descriptions,
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
        entry: Dict[str, Any] = {"node_type": "array"}
        if isinstance(value, dict):
            entry["shape"] = value.get("shape")
            # v2 dtype: numpy-style string like "<u2", "|u1", "<f4"
            v2_dtype = value.get("dtype", "")
            if isinstance(v2_dtype, str):
                mapped = _V2_DTYPE_MAP.get(v2_dtype)
                if mapped:
                    entry["data_type"] = mapped
        consol[array_path] = entry

    shape_per_res: Dict[str, Tuple[int, int]] = {}
    dtype_per_res: Dict[str, str] = {}
    bands_per_res, sub_group, shape_per_res = _parse_consolidated(
        consol, shape_per_res, dtype_per_res,
    )

    # Extract band descriptions from per-array .zattrs
    band_descriptions: Dict[str, str] = {}
    for path, value in meta.items():
        if not path.endswith("/.zattrs") or not isinstance(value, dict):
            continue
        array_path = path[: -len("/.zattrs")]
        band_id = array_path.rsplit("/", 1)[-1]
        desc = value.get("long_name") or value.get("standard_name", "")
        if desc and isinstance(desc, str):
            band_descriptions[band_id] = desc

    resolutions = sorted(bands_per_res.keys(), key=_res_sort_key)

    return ZarrRootInfo(
        resolutions=tuple(resolutions),
        bands_per_resolution={
            k: tuple(sorted(v)) for k, v in bands_per_res.items()
        },
        shape_per_resolution=shape_per_res,
        dtype_per_resolution=dtype_per_res,
        epsg=epsg,
        conventions=(),
        sub_group=sub_group,
        band_descriptions=band_descriptions,
    )


_RES_RE = re.compile(r"r\d+m$")


def _parse_consolidated(
    consol: Dict[str, Any],
    shape_per_res: Dict[str, Tuple[int, int]],
    dtype_per_res: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, List[str]], str, Dict[str, Tuple[int, int]]]:
    """Parse consolidated_metadata paths at any depth.

    Returns (bands_per_res, sub_group_prefix, shape_per_res).

    Scans for leaf arrays under resolution segments (r10m, r20m, ...).
    Groups by prefix path. If multiple prefixes exist (e.g.
    measurements/reflectance vs conditions/mask/detector_footprint),
    picks the one with the most bands.
    """
    if dtype_per_res is None:
        dtype_per_res = {}

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
                try:
                    shape_per_res[res_seg] = (int(shape[-2]), int(shape[-1]))
                except (ValueError, TypeError):
                    log.warning("Invalid shape %s for %s", shape, path)

        # Extract data type (first band per resolution wins)
        if res_seg not in dtype_per_res:
            zarr_dtype = meta.get("data_type")
            if isinstance(zarr_dtype, str):
                gdal_dtype = _ZARR_TO_GDAL_DTYPE.get(zarr_dtype.lower())
                if gdal_dtype:
                    dtype_per_res[res_seg] = gdal_dtype

    if not groups:
        return {}, "", shape_per_res

    # Pick prefix with the most total bands
    best_prefix = max(
        groups, key=lambda p: sum(len(v) for v in groups[p].values()),
    )
    total = sum(len(v) for v in groups[best_prefix].values())
    if len(groups) > 1:
        log.debug(
            "Consolidated: %d prefixes, selected '%s' (%d bands)",
            len(groups), best_prefix, total,
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
