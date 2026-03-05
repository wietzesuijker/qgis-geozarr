"""STAC item search for time series: bbox + datetime + grid code filtering."""

from __future__ import annotations

import json
import logging
import re
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime

log = logging.getLogger(__name__)

# Grid code patterns per satellite family
_MGRS_RE = re.compile(r"(?:^|[_\-])(\d{2}[A-Z]{3})(?:[_\-]|$)")  # e.g. _27XVB_
_WRS2_RE = re.compile(r"(\d{3}/\d{3})")  # e.g. 044/034


def _find_zarr_root(url: str) -> str:
    """Find the Zarr store root from a deep asset URL."""
    m = re.search(r"(https?://[^?#]*\.zarr)", url)
    return m.group(1) if m else url


def _extract_zarr_href(assets: dict) -> tuple[str, str]:
    """Find Zarr asset href from STAC item assets. Returns (url, key)."""
    for key, asset in assets.items():
        if not isinstance(asset, dict):
            continue
        href = asset.get("href", "")
        media = asset.get("type", "")
        if "zarr" in media.lower() or ".zarr" in href.lower():
            return (_find_zarr_root(href), key)
    return ("", "")


@dataclass
class TimeSeriesItem:
    """One STAC item in a time series."""

    datetime_str: str  # ISO 8601
    zarr_url: str  # resolved Zarr store root
    item_id: str
    cloud_cover: float | None = None


def extract_grid_code(feature: dict) -> str:
    """Extract grid tile identifier from STAC feature properties.

    Tries (in order): grid:code, s2:mgrs_tile, mgrs:grid_square,
    then regex on item ID for MGRS or WRS-2 patterns.
    """
    props = feature.get("properties", {})
    for key in ("grid:code", "s2:mgrs_tile", "mgrs:grid_square"):
        val = props.get(key)
        if val:
            return str(val)
    item_id = feature.get("id", "")
    m = _MGRS_RE.search(item_id)
    if m:
        return m.group(1)
    m = _WRS2_RE.search(item_id)
    if m:
        return m.group(1)
    return ""


def _http_get_json(url: str, timeout: int = 15) -> dict | list | None:
    """Simple HTTP GET returning parsed JSON, or None on failure."""
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/geo+json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except (OSError, json.JSONDecodeError, ValueError) as e:
        log.debug("HTTP GET failed: %s: %s", url, e)
        return None


def query_stac_items(
    base_url: str,
    collection_id: str,
    *,
    bbox: tuple[float, ...] | None = None,
    datetime_range: str | None = None,
    grid_code: str = "",
    limit: int = 50,
    max_pages: int = 10,
) -> list[TimeSeriesItem]:
    """Query STAC /items endpoint with filtering. Returns sorted by datetime.

    Parameters
    ----------
    base_url : str
        STAC API root (e.g. "https://api.example.com/stac").
    collection_id : str
        STAC collection ID.
    bbox : tuple, optional
        Bounding box (west, south, east, north).
    datetime_range : str, optional
        ISO 8601 interval (e.g. "2025-01-01T00:00:00Z/2026-01-01T00:00:00Z").
    grid_code : str, optional
        Filter results to this grid tile (MGRS/WRS-2).
    limit : int
        Max items to return (across all pages).
    max_pages : int
        Max pagination requests.
    """
    params: dict[str, str] = {
        "limit": str(min(limit, 100)),
        "sortby": "+datetime",  # oldest first so limit doesn't clip early dates
    }
    if bbox:
        params["bbox"] = ",".join(str(v) for v in bbox)
    if datetime_range:
        params["datetime"] = datetime_range

    url = (
        f"{base_url.rstrip('/')}/collections/{collection_id}/items"
        f"?{urllib.parse.urlencode(params)}"
    )

    items: list[TimeSeriesItem] = []
    seen_ids: set[str] = set()

    for _ in range(max_pages):
        if not url:
            break
        data = _http_get_json(url)
        if not data or not isinstance(data, dict):
            break

        for feat in data.get("features", []):
            if len(items) >= limit:
                break
            item_id = feat.get("id", "")
            if item_id in seen_ids:
                continue
            seen_ids.add(item_id)

            # Grid code filter
            if grid_code and extract_grid_code(feat) != grid_code:
                continue

            props = feat.get("properties", {})
            dt = props.get("datetime", "")
            if not dt:
                continue

            zarr_url, _ = _extract_zarr_href(feat.get("assets", {}))
            if not zarr_url:
                continue

            cc = props.get("eo:cloud_cover")
            items.append(TimeSeriesItem(
                datetime_str=dt,
                zarr_url=zarr_url,
                item_id=item_id,
                cloud_cover=float(cc) if cc is not None else None,
            ))

        if len(items) >= limit:
            break

        # Follow pagination
        url = ""
        for link in data.get("links", []):
            if link.get("rel") == "next":
                url = link.get("href", "")
                break

    items.sort(key=lambda x: x.datetime_str)
    return items


def parse_datetime(dt_str: str) -> datetime | None:
    """Parse ISO 8601 datetime string (handles Z, +00:00, microseconds)."""
    if not dt_str:
        return None
    # fromisoformat needs +00:00 not Z (Python < 3.11)
    normalized = dt_str.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
        # Strip timezone to keep naive (consistent with rest of codebase)
        return dt.replace(tzinfo=None)
    except (ValueError, TypeError):
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    return None
