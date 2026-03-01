"""GeoZarr convention parsing from zarr.json (v3) and .zmetadata (v2).

Reads a single metadata file to discover resolutions, bands, CRS, and transform.
Zarr v3: user attributes under root["attributes"], consolidated_metadata.
Zarr v2: .zmetadata with paths like "measurements/reflectance/r10m/b02/.zarray".
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
import time
import urllib.request
from dataclasses import asdict, dataclass, field
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
    scale_per_band: Dict[str, float] = field(default_factory=dict)  # band_id -> scale_factor
    valid_range_per_band: Dict[str, Tuple[float, float]] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # Tuples become lists in asdict(); keep JSON-safe
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ZarrRootInfo":
        # Restore tuples from lists
        d["resolutions"] = tuple(d.get("resolutions", ()))
        d["conventions"] = tuple(d.get("conventions", ()))
        bpr = d.get("bands_per_resolution", {})
        d["bands_per_resolution"] = {k: tuple(v) for k, v in bpr.items()}
        spr = d.get("shape_per_resolution", {})
        d["shape_per_resolution"] = {k: tuple(v) for k, v in spr.items()}
        tpr = d.get("transform_per_resolution", {})
        d["transform_per_resolution"] = {k: tuple(v) for k, v in tpr.items()}
        vrpb = d.get("valid_range_per_band", {})
        d["valid_range_per_band"] = {k: tuple(v) for k, v in vrpb.items()}
        gt = d.get("geotransform")
        if gt is not None:
            d["geotransform"] = tuple(gt)
        return cls(**d)


# ---------------------------------------------------------------------------
# Disk cache for persistent metadata across QGIS sessions
# ---------------------------------------------------------------------------

_DISK_CACHE_FRESH_AGE = 3600  # 1 hour - serve from disk without HTTP
_DISK_CACHE_MAX_AGE = 7 * 24 * 3600  # 7 days - hard eviction


def _disk_cache_dir() -> str:
    """Platform-appropriate cache directory for GeoZarr metadata."""
    # ~/Library/Caches/geozarr (macOS), ~/.cache/geozarr (Linux)
    base = os.environ.get("XDG_CACHE_HOME") or os.path.join(
        os.path.expanduser("~"), ".cache",
    )
    d = os.path.join(base, "geozarr")
    os.makedirs(d, exist_ok=True)
    return d


def _disk_cache_path(url: str) -> str:
    return os.path.join(
        _disk_cache_dir(),
        hashlib.sha256(url.encode()).hexdigest()[:16] + ".json",
    )


def _disk_cache_read(url: str) -> Optional[Tuple[ZarrRootInfo, float]]:
    """Read cached (info, age_seconds) from disk. None on miss."""
    path = _disk_cache_path(url)
    try:
        age = time.time() - os.path.getmtime(path)
        with open(path) as f:
            obj = json.load(f)
        info = ZarrRootInfo.from_dict(obj["info"])
        return info, age
    except (OSError, KeyError, json.JSONDecodeError, TypeError):
        return None


def _disk_cache_write(url: str, info: ZarrRootInfo) -> None:
    """Write metadata to disk cache (always, regardless of server headers)."""
    path = _disk_cache_path(url)
    try:
        with open(path, "w") as f:
            json.dump({"info": info.to_dict()}, f)
    except OSError as e:
        log.debug("Disk cache write failed for %s: %s", url, e)


def _disk_cache_evict() -> None:
    """Remove cache entries older than _DISK_CACHE_MAX_AGE."""
    try:
        cache_dir = _disk_cache_dir()
        now = time.time()
        for name in os.listdir(cache_dir):
            if not name.endswith(".json"):
                continue
            path = os.path.join(cache_dir, name)
            try:
                if now - os.path.getmtime(path) > _DISK_CACHE_MAX_AGE:
                    os.unlink(path)
            except OSError:
                pass
    except OSError:
        pass


def _res_sort_key(name: str) -> int:
    """Sort resolution names by numeric value (r10m -> 10, r20m -> 20)."""
    m = re.search(r"(\d+)", name)
    return int(m.group(1)) if m else 0


_cache: Dict[str, ZarrRootInfo] = {}
_lock = threading.Lock()

# Evict stale disk cache entries on module load
_disk_cache_evict()



def _http_read(url: str, timeout: float = 10) -> Optional[bytes]:
    """Single GET via urllib. Used for metadata JSON and thumbnails."""
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except (urllib.error.HTTPError, urllib.error.URLError, OSError, TimeoutError):
        return None


def fetch(zarr_url: str) -> Optional[ZarrRootInfo]:
    """Fetch and cache metadata (v3 zarr.json or v2 .zmetadata).

    Three-tier cache: in-memory -> disk (fresh < 1h) -> network.
    v3 and v2 probed in parallel on cache miss. Always writes to disk.
    """
    url = zarr_url.rstrip("/")

    # Tier 1: in-memory
    with _lock:
        if url in _cache:
            return _cache[url]

    # Tier 2: disk cache - serve fresh entries without any HTTP
    disk_hit = _disk_cache_read(url)
    if disk_hit:
        info, age = disk_hit
        if age < _DISK_CACHE_FRESH_AGE:
            log.debug("Disk cache fresh (%.0fs) for %s", age, url)
            with _lock:
                _cache[url] = info
            return info

    # Tier 3: network fetch (v3 + v2 probed in parallel)
    info = _probe_metadata(url)

    if info is None and disk_hit:
        # Network failed - serve stale cache rather than error
        info = disk_hit[0]
        log.debug("Serving stale cache for %s (network failed)", url)

    if info is None:
        log.debug("No zarr.json or .zmetadata found at %s", url)
        return None

    with _lock:
        _cache[url] = info
    _disk_cache_write(url, info)
    return info


def _probe_metadata(url: str) -> Optional[ZarrRootInfo]:
    """Probe v3 zarr.json and v2 .zmetadata in parallel. First success wins."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _try_v3():
        data = _http_read(f"{url}/zarr.json")
        if data:
            return _parse(json.loads(data))
        return None

    def _try_v2():
        data = _http_read(f"{url}/.zmetadata")
        if data:
            return _parse_v2(json.loads(data))
        return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        futs = {pool.submit(_try_v3): "v3", pool.submit(_try_v2): "v2"}
        for fut in as_completed(futs, timeout=25):
            try:
                info = fut.result()
                if info is not None:
                    log.debug("Loaded %s metadata for %s", futs[fut], url)
                    return info
            except Exception as e:
                log.debug("%s probe failed for %s: %s", futs[fut], url, e)
    return None


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
        # Skip sub-group fetch if root consolidated already gave us everything
        if info.resolutions and info.shape_per_resolution:
            return info, sub_url
        # v3: each group has its own zarr.json (non-consolidated case)
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

        # CRS/transform/multiscales may live in sub-group entries rather than
        # root attributes (e.g. EOPF Explorer v3: measurements/reflectance).
        if sub_group:
            sg_meta = consol.get(sub_group, {})
            sg_attrs = sg_meta.get("attributes", {}) if isinstance(sg_meta, dict) else {}
            if isinstance(sg_attrs, dict):
                if epsg is None:
                    epsg = _parse_crs(sg_attrs)
                if geotransform is None:
                    geotransform = _parse_transform(sg_attrs)
                ms = sg_attrs.get("multiscales")
                if isinstance(ms, dict):
                    for entry in ms.get("layout", []):
                        if not isinstance(entry, dict):
                            continue
                        asset = entry.get("asset")
                        if not asset:
                            continue
                        shape = entry.get("spatial:shape")
                        if isinstance(shape, (list, tuple)) and len(shape) >= 2:
                            shape_per_res.setdefault(asset, (int(shape[-2]), int(shape[-1])))
                        st = entry.get("spatial:transform")
                        if isinstance(st, (list, tuple)) and len(st) == 6:
                            vals = [float(v) for v in st]
                            gdal_gt = (vals[2], vals[0], vals[1],
                                       vals[5], vals[3], vals[4])
                            transform_per_res.setdefault(asset, gdal_gt)
                            if geotransform is None:
                                geotransform = gdal_gt

            # Per-resolution CRS/transform from consolidated entries
            for res in list(bands_per_res.keys()):
                res_path = f"{sub_group}/{res}"
                res_meta = consol.get(res_path, {})
                res_attrs = res_meta.get("attributes", {}) if isinstance(res_meta, dict) else {}
                if isinstance(res_attrs, dict):
                    if epsg is None:
                        epsg = _parse_crs(res_attrs)
                    if res not in transform_per_res:
                        gt = _parse_transform(res_attrs)
                        if gt:
                            transform_per_res[res] = gt
                            if geotransform is None:
                                geotransform = gt

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

    # Flat members fallback: arrays directly under root (no resolution groups)
    if not bands_per_res:
        flat_bands = [
            k for k, v in root.get("members", {}).items()
            if isinstance(v, dict) and v.get("node_type") == "array"
            and k not in _NON_BAND
        ]
        if flat_bands:
            bands_per_res["default"] = flat_bands

    # Extract band descriptions, scale_factor, valid_range from attributes
    band_descriptions: Dict[str, str] = {}
    scale_per_band: Dict[str, float] = {}
    valid_range_per_band: Dict[str, Tuple[float, float]] = {}
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
        # Scale factor (e.g. 10000 for S2 UInt16 reflectance)
        sf = member_attrs.get("scale_factor")
        if sf is not None:
            try:
                scale_per_band[band_id] = float(sf)
            except (ValueError, TypeError):
                pass
        # Valid range from valid_min/valid_max or valid_range
        vmin = member_attrs.get("valid_min")
        vmax = member_attrs.get("valid_max")
        vrange = member_attrs.get("valid_range")
        if vrange is not None and isinstance(vrange, (list, tuple)) and len(vrange) == 2:
            try:
                valid_range_per_band[band_id] = (float(vrange[0]), float(vrange[1]))
            except (ValueError, TypeError):
                pass
        elif vmin is not None and vmax is not None:
            try:
                valid_range_per_band[band_id] = (float(vmin), float(vmax))
            except (ValueError, TypeError):
                pass

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
        scale_per_band=scale_per_band,
        valid_range_per_band=valid_range_per_band,
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

    # Extract band descriptions, scale_factor, valid_range from .zattrs
    band_descriptions: Dict[str, str] = {}
    scale_per_band: Dict[str, float] = {}
    valid_range_per_band: Dict[str, Tuple[float, float]] = {}
    for path, value in meta.items():
        if not path.endswith("/.zattrs") or not isinstance(value, dict):
            continue
        array_path = path[: -len("/.zattrs")]
        band_id = array_path.rsplit("/", 1)[-1]
        desc = value.get("long_name") or value.get("standard_name", "")
        if desc and isinstance(desc, str):
            band_descriptions[band_id] = desc
        sf = value.get("scale_factor")
        if sf is not None:
            try:
                scale_per_band[band_id] = float(sf)
            except (ValueError, TypeError):
                pass
        vmin = value.get("valid_min")
        vmax = value.get("valid_max")
        vrange = value.get("valid_range")
        if vrange is not None and isinstance(vrange, (list, tuple)) and len(vrange) == 2:
            try:
                valid_range_per_band[band_id] = (float(vrange[0]), float(vrange[1]))
            except (ValueError, TypeError):
                pass
        elif vmin is not None and vmax is not None:
            try:
                valid_range_per_band[band_id] = (float(vmin), float(vmax))
            except (ValueError, TypeError):
                pass

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
        scale_per_band=scale_per_band,
        valid_range_per_band=valid_range_per_band,
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
        # Fallback: flat Zarr store with no resolution groups.
        # Treat all 2D leaf arrays as a single "default" resolution.
        flat_bands: List[str] = []
        for path, meta in consol.items():
            if not isinstance(meta, dict) or meta.get("node_type") != "array":
                continue
            leaf = path.strip("/").rsplit("/", 1)[-1]
            if leaf in _NON_BAND:
                continue
            shape = meta.get("shape")
            if not isinstance(shape, (list, tuple)) or len(shape) < 2:
                continue
            flat_bands.append(leaf)
            if "default" not in shape_per_res:
                try:
                    shape_per_res["default"] = (int(shape[-2]), int(shape[-1]))
                except (ValueError, TypeError):
                    pass
            if dtype_per_res is not None and "default" not in dtype_per_res:
                zarr_dtype = meta.get("data_type")
                if isinstance(zarr_dtype, str):
                    gdal_dtype = _ZARR_TO_GDAL_DTYPE.get(zarr_dtype.lower())
                    if gdal_dtype:
                        dtype_per_res["default"] = gdal_dtype
        if flat_bands:
            log.debug("Flat Zarr fallback: %d bands (no resolution groups)", len(flat_bands))
            return {"default": flat_bands}, "", shape_per_res
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
