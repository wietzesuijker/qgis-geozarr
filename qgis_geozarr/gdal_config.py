"""GDAL configuration for cloud Zarr v3 access.

Applied on plugin load, restored on unload. Zarr v3 sharded reads benefit
from HTTP/2 multiplexing, tuned vsicurl cache, and parallel decode.
"""

from __future__ import annotations

import os
from typing import Dict, Optional

from osgeo import gdal

# Zarr v3 optimized settings (3-10x over GDAL defaults)
_ZARR_CONFIG: Dict[str, str] = {
    # HTTP/2 + batching
    "GDAL_HTTP_MULTIPLEX": "YES",
    "GDAL_HTTP_VERSION": "2",
    "GDAL_HTTP_MERGE_CONSECUTIVE_RANGES": "YES",
    # Retry transient errors (429/5xx)
    "GDAL_HTTP_MAX_RETRY": "5",
    "GDAL_HTTP_RETRY_DELAY": "1",
    "GDAL_HTTP_TIMEOUT": "30",
    "GDAL_HTTP_CONNECTTIMEOUT": "10",
    # vsicurl cache: 256 KB chunks, 256 MB per-handle, 128 MB global LRU
    "VSI_CACHE": "TRUE",
    "VSI_CACHE_SIZE": "268435456",
    "CPL_VSIL_CURL_CHUNK_SIZE": "262144",
    "CPL_VSIL_CURL_CACHE_SIZE": "134217728",
    # Parallel decode + block cache
    "GDAL_NUM_THREADS": "ALL_CPUS",
    "GDAL_CACHEMAX": "512",
    # Zarr v3: skip dir listing, small initial read
    "GDAL_DISABLE_READDIR_ON_OPEN": "YES",
    "GDAL_INGESTED_BYTES_AT_OPEN": "32768",
}

_saved: Dict[str, Optional[str]] = {}
_active = False


def _system_ram_mb() -> int:
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") // (1024 * 1024)
    except (ValueError, OSError, AttributeError):
        pass
    try:
        import subprocess
        out = subprocess.check_output(["sysctl", "-n", "hw.memsize"], timeout=2)
        return int(out.strip()) // (1024 * 1024)
    except Exception:
        return 8192


def apply() -> None:
    """Apply Zarr v3 cloud config. Saves originals for restore."""
    global _active
    if _active:
        return

    config = dict(_ZARR_CONFIG)
    # Auto-tune block cache: 25% of RAM, clamped 512-2048 MB
    ram = _system_ram_mb()
    config["GDAL_CACHEMAX"] = str(max(512, min(ram // 4, 2048)))

    for key in config:
        _saved[key] = gdal.GetConfigOption(key)
    for key, value in config.items():
        gdal.SetConfigOption(key, value)
    _active = True


def restore() -> None:
    """Restore original GDAL config."""
    global _active
    for key, value in _saved.items():
        gdal.SetConfigOption(key, value)
    _saved.clear()
    _active = False
