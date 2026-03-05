#!/usr/bin/env python3
"""COG vs GeoZarr benchmark.

GDAL-native benchmark comparing Cloud-Optimized GeoTIFF and GeoZarr (Zarr v2)
for Sentinel-2 data. Measures dataset open time and raster read time across
scenarios (single band, RGB composite, zoom levels).

Uses GDAL directly - no QGIS dependency. This isolates format performance
since the QGIS rendering pipeline is identical for both formats.

Usage:
    python bench_cog_vs_zarr.py
    python bench_cog_vs_zarr.py --gdal-tuned --runs 5

Requires: GDAL 3.10+ with Zarr driver, prep_data.sh output in ./data/
"""
import argparse
import csv
import json
import os
import sys
import tempfile
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from osgeo import gdal, osr

gdal.UseExceptions()


# ---------------------------------------------------------------------------
# GDAL config profiles
# ---------------------------------------------------------------------------

# Tuned config from qgis-geozarr plugin (gdal_config.py)
_TUNED_CONFIG = {
    "GDAL_HTTP_MULTIPLEX": "YES",
    "GDAL_HTTP_VERSION": "2",
    "GDAL_HTTP_MERGE_CONSECUTIVE_RANGES": "YES",
    "GDAL_HTTP_MAX_RETRY": "5",
    "GDAL_HTTP_RETRY_DELAY": "1",
    "GDAL_HTTP_TIMEOUT": "30",
    "GDAL_HTTP_CONNECTTIMEOUT": "10",
    "VSI_CACHE": "TRUE",
    "VSI_CACHE_SIZE": "268435456",
    "CPL_VSIL_CURL_CHUNK_SIZE": "262144",
    "CPL_VSIL_CURL_CACHE_SIZE": "134217728",
    "GDAL_NUM_THREADS": "ALL_CPUS",
    "GDAL_CACHEMAX": "512",
    "GDAL_DISABLE_READDIR_ON_OPEN": "YES",
    "GDAL_INGESTED_BYTES_AT_OPEN": "32768",
}

# Parallel config: only GDAL_NUM_THREADS for local benchmarks
_PARALLEL_CONFIG = {
    "GDAL_NUM_THREADS": "ALL_CPUS",
}


def apply_gdal_config(config: Dict[str, str]) -> Dict[str, Optional[str]]:
    saved = {}
    for key in config:
        saved[key] = gdal.GetConfigOption(key)
    for key, value in config.items():
        gdal.SetConfigOption(key, value)
    return saved


def restore_gdal_config(saved: Dict[str, Optional[str]]) -> None:
    for key, value in saved.items():
        gdal.SetConfigOption(key, value)


def flush_vsicurl_cache() -> None:
    """Clear GDAL's /vsicurl/ cache between runs for cold-start tests."""
    gdal.VSICurlClearCache()


# ---------------------------------------------------------------------------
# URI construction
# ---------------------------------------------------------------------------

def build_zarr_uri(zarr_url: str, sub_group: str, resolution: str, band: str) -> str:
    parts = [zarr_url.rstrip("/")]
    if sub_group:
        parts.append(sub_group)
    parts.extend([resolution, band])
    path = "/".join(parts)
    return f'ZARR:"/vsicurl/{path}"'


def build_zarr_v3_uri(store_url: str, group: str, resolution: str, band: str) -> str:
    """Build ZARR URI for v3 via group (enables multiscale overview discovery)."""
    group_url = f"{store_url.rstrip('/')}/{group}"
    return f'ZARR:"/vsicurl/{group_url}":/{resolution}/{band}'


def build_cog_cloud_uri(url: str) -> str:
    return f"/vsicurl/{url}"


def build_vrt(band_uris: List[str], epsg: Optional[int] = None) -> str:
    """Stack bands into a VRT. Returns temp file path."""
    fd, vrt_path = tempfile.mkstemp(suffix=".vrt", prefix="bench_")
    os.close(fd)

    opts = gdal.BuildVRTOptions(separate=True)
    vrt_ds = gdal.BuildVRT(vrt_path, band_uris, options=opts)
    if vrt_ds is None:
        raise RuntimeError(f"BuildVRT failed: {gdal.GetLastErrorMsg()}")

    if epsg:
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(epsg)
        vrt_ds.SetProjection(srs.ExportToWkt())

    vrt_ds.FlushCache()
    vrt_ds = None
    return vrt_path


# ---------------------------------------------------------------------------
# Read operations (replaces QGIS rendering - measures GDAL I/O directly)
# ---------------------------------------------------------------------------

def open_dataset(uri: str) -> gdal.Dataset:
    ds = gdal.Open(uri)
    if ds is None:
        raise RuntimeError(f"Cannot open: {uri}")
    return ds


def read_raster(ds: gdal.Dataset, width: int, height: int,
                window: Optional[Tuple[int, int, int, int]] = None) -> float:
    """Read raster data at target resolution. Returns elapsed ms.

    window: (xoff, yoff, xsize, ysize) in pixel coords, or None for full extent.
    Output is always resampled to width x height.
    """
    if window:
        xoff, yoff, xsize, ysize = window
    else:
        xoff, yoff = 0, 0
        xsize, ysize = ds.RasterXSize, ds.RasterYSize

    start = time.perf_counter()
    # ReadRaster with buf_xsize/buf_ysize triggers GDAL overview selection
    _ = ds.ReadRaster(
        xoff, yoff, xsize, ysize,
        buf_xsize=width, buf_ysize=height,
    )
    elapsed = (time.perf_counter() - start) * 1000.0
    return elapsed


def zoom_window(ds: gdal.Dataset, fraction: float) -> Tuple[int, int, int, int]:
    """Centered window covering `fraction` of the full extent area."""
    f = fraction ** 0.5
    w = int(ds.RasterXSize * f)
    h = int(ds.RasterYSize * f)
    xoff = (ds.RasterXSize - w) // 2
    yoff = (ds.RasterYSize - h) // 2
    return (xoff, yoff, w, h)


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------

@dataclass
class BenchResult:
    fmt: str           # cog, zarr_v2
    source: str        # local, cloud
    scenario: str      # single_band, rgb_vrt, zoom_25, zoom_5
    gdal_config: str   # default, tuned
    run: int
    open_ms: float
    read_ms: float
    width: int
    height: int


@dataclass
class BenchConfig:
    data_dir: str
    urls: Dict
    width: int = 2048
    height: int = 2048
    runs: int = 5
    warmup: int = 2


def run_scenario(
    uri_or_uris,
    label: str,
    fmt: str,
    source: str,
    scenario: str,
    gdal_cfg_name: str,
    cfg: BenchConfig,
    extent_fraction: Optional[float] = None,
    buf_size: Optional[Tuple[int, int]] = None,
) -> List[BenchResult]:
    """Run warmup + measured iterations for one scenario.

    buf_size: (width, height) override for output buffer. If None, uses cfg defaults.
    """
    results = []
    epsg = cfg.urls.get("epsg")
    temp_vrts = []
    out_w = buf_size[0] if buf_size else cfg.width
    out_h = buf_size[1] if buf_size else cfg.height

    total = cfg.warmup + cfg.runs
    for i in range(total):
        flush_vsicurl_cache()

        # Open
        t0 = time.perf_counter()
        if isinstance(uri_or_uris, list):
            vrt_path = build_vrt(uri_or_uris, epsg=epsg)
            temp_vrts.append(vrt_path)
            ds = open_dataset(vrt_path)
        else:
            ds = open_dataset(uri_or_uris)
        open_ms = (time.perf_counter() - t0) * 1000.0

        # Read
        window = zoom_window(ds, extent_fraction) if extent_fraction else None
        read_ms = read_raster(ds, out_w, out_h, window)
        ds = None  # close

        if i >= cfg.warmup:
            results.append(BenchResult(
                fmt=fmt, source=source, scenario=scenario,
                gdal_config=gdal_cfg_name, run=i - cfg.warmup + 1,
                open_ms=open_ms, read_ms=read_ms,
                width=out_w, height=out_h,
            ))
        tag = "warmup" if i < cfg.warmup else f"run {i - cfg.warmup + 1}"
        total_ms = open_ms + read_ms
        print(f"  {tag}: open={open_ms:.0f}ms read={read_ms:.0f}ms total={total_ms:.0f}ms")

    for vrt in temp_vrts:
        try:
            os.unlink(vrt)
        except OSError:
            pass

    return results


def run_all(cfg: BenchConfig, gdal_tuned: bool,
            gdal_parallel: bool = False,
            incremental_csv: Optional[str] = None) -> List[BenchResult]:
    results = []
    urls = cfg.urls

    zarr_url = urls["zarr_store"]
    zarr_sub = urls["zarr_sub_group"]
    zarr_res = urls["zarr_resolution"]
    zarr_bands = urls["zarr_bands"]
    cog_cloud = urls["cog_cloud"]

    # --- Define test cases ---
    # (fmt, source, scenario, uri_or_uris, extent_frac, buf_size)
    # buf_size: None = use cfg defaults, (w,h) = override output buffer size
    cases = []

    # Local COG (with overviews)
    cog_local_b04 = os.path.join(cfg.data_dir, "b04_cog.tif")
    rgb_cog = os.path.join(cfg.data_dir, "rgb_cog.tif")
    if os.path.isfile(cog_local_b04):
        cases.append(("cog", "local", "single_band", cog_local_b04, None))
    if os.path.isfile(rgb_cog):
        cases.append(("cog", "local", "rgb_stacked", rgb_cog, None))
        cases.append(("cog", "local", "zoom_25", rgb_cog, 0.25))
        cases.append(("cog", "local", "zoom_5", rgb_cog, 0.05))

    # Local COG with ZSTD (fair codec comparison with Zarr ZSTD)
    cog_zstd_b04 = os.path.join(cfg.data_dir, "b04_cog_zstd.tif")
    rgb_cog_zstd = os.path.join(cfg.data_dir, "rgb_cog_zstd.tif")
    if os.path.isfile(cog_zstd_b04):
        cases.append(("cog_zstd", "local", "single_band", cog_zstd_b04, None))
    if os.path.isfile(rgb_cog_zstd):
        cases.append(("cog_zstd", "local", "rgb_stacked", rgb_cog_zstd, None))
        cases.append(("cog_zstd", "local", "zoom_25", rgb_cog_zstd, 0.25))
        cases.append(("cog_zstd", "local", "zoom_5", rgb_cog_zstd, 0.05))

    # Local COG WITHOUT overviews (fair comparison with Zarr)
    cog_noovr_b04 = os.path.join(cfg.data_dir, "b04_cog_noovr.tif")
    rgb_cog_noovr = os.path.join(cfg.data_dir, "rgb_cog_noovr.tif")
    if os.path.isfile(cog_noovr_b04):
        cases.append(("cog_noovr", "local", "single_band", cog_noovr_b04, None))
    if os.path.isfile(rgb_cog_noovr):
        cases.append(("cog_noovr", "local", "rgb_stacked", rgb_cog_noovr, None))
        cases.append(("cog_noovr", "local", "zoom_25", rgb_cog_noovr, 0.25))
        cases.append(("cog_noovr", "local", "zoom_5", rgb_cog_noovr, 0.05))

    # Local Zarr (same data, 1830x1830 chunks, DEFLATE - no overviews)
    zarr_local_b04 = os.path.join(cfg.data_dir, "b04_zarr")
    if os.path.isdir(zarr_local_b04):
        cases.append(("zarr_v2", "local", "single_band", zarr_local_b04, None))
    zarr_local_bands = [os.path.join(cfg.data_dir, f"{b}_zarr") for b in ["b04", "b03", "b02"]]
    if all(os.path.isdir(d) for d in zarr_local_bands):
        cases.append(("zarr_v2", "local", "rgb_vrt", zarr_local_bands, None))
        cases.append(("zarr_v2", "local", "zoom_25", zarr_local_bands, 0.25))
        cases.append(("zarr_v2", "local", "zoom_5", zarr_local_bands, 0.05))

    # Local Zarr v3 + zstd (sharded, no overviews)
    def v3_local_uri(band_dir, arr_name=None):
        """Build ZARR: URI for a v3 dataset, targeting the 2D data array."""
        if arr_name is None:
            arr_name = os.path.basename(band_dir)
        return f'ZARR:"{band_dir}":/{arr_name}'

    for suffix, fmt_label in [("zarr_v3_zstd", "zarr_v3_zstd"),
                               ("zarr_v3_blosc", "zarr_v3_blosc")]:
        v3_b04 = os.path.join(cfg.data_dir, f"b04_{suffix}")
        if os.path.isdir(v3_b04):
            cases.append((fmt_label, "local", "single_band",
                          v3_local_uri(v3_b04), None))
        v3_bands = [os.path.join(cfg.data_dir, f"{b}_{suffix}")
                    for b in ["b04", "b03", "b02"]]
        if all(os.path.isdir(d) for d in v3_bands):
            v3_uris = [v3_local_uri(d) for d in v3_bands]
            cases.append((fmt_label, "local", "rgb_vrt", v3_uris, None))
            cases.append((fmt_label, "local", "zoom_25", v3_uris, 0.25))
            cases.append((fmt_label, "local", "zoom_5", v3_uris, 0.05))

    # Local Zarr v3 + zstd + overviews (sharded, with multiscales)
    # Array name comes from the source dataset (e.g. b04_zarr_v3_zstd),
    # not the _ovr directory name.
    v3_ovr_b04 = os.path.join(cfg.data_dir, "b04_zarr_v3_zstd_ovr")
    if os.path.isdir(v3_ovr_b04):
        cases.append(("zarr_v3_zstd_ovr", "local", "single_band",
                      v3_local_uri(v3_ovr_b04, "b04_zarr_v3_zstd"), None))
    v3_ovr_bands = [os.path.join(cfg.data_dir, f"{b}_zarr_v3_zstd_ovr")
                    for b in ["b04", "b03", "b02"]]
    v3_ovr_arr_names = [f"{b}_zarr_v3_zstd" for b in ["b04", "b03", "b02"]]
    if all(os.path.isdir(d) for d in v3_ovr_bands):
        v3_ovr_uris = [v3_local_uri(d, n) for d, n in zip(v3_ovr_bands, v3_ovr_arr_names)]
        cases.append(("zarr_v3_zstd_ovr", "local", "rgb_vrt", v3_ovr_uris, None))
        cases.append(("zarr_v3_zstd_ovr", "local", "zoom_25", v3_ovr_uris, 0.25))
        cases.append(("zarr_v3_zstd_ovr", "local", "zoom_5", v3_ovr_uris, 0.05))

    # Cloud COG (Element84)
    if cog_cloud.get("B04"):
        cases.append(("cog", "cloud", "single_band",
                       build_cog_cloud_uri(cog_cloud["B04"]), None))
    if all(cog_cloud.get(b) for b in ["B04", "B03", "B02"]):
        cog_rgb = [build_cog_cloud_uri(cog_cloud[b]) for b in ["B04", "B03", "B02"]]
        cases.append(("cog", "cloud", "rgb_vrt", cog_rgb, None))
        cases.append(("cog", "cloud", "zoom_25", cog_rgb, 0.25))
        cases.append(("cog", "cloud", "zoom_5", cog_rgb, 0.05))

    # Cloud Zarr v2 (EOPF Production)
    if zarr_url and zarr_bands:
        zarr_b04 = build_zarr_uri(zarr_url, zarr_sub, zarr_res, zarr_bands[0])
        cases.append(("zarr_v2", "cloud", "single_band", zarr_b04, None))
        zarr_rgb = [build_zarr_uri(zarr_url, zarr_sub, zarr_res, b) for b in zarr_bands]
        cases.append(("zarr_v2", "cloud", "rgb_vrt", zarr_rgb, None))
        cases.append(("zarr_v2", "cloud", "zoom_25", zarr_rgb, 0.25))
        cases.append(("zarr_v2", "cloud", "zoom_5", zarr_rgb, 0.05))

    # Cloud Zarr v3 (EOPF Explorer - sharded + multiscales)
    v3_cloud = urls.get("zarr_v3_cloud", {})
    v3_store = v3_cloud.get("store", "")
    v3_group = v3_cloud.get("group", "")
    v3_res = v3_cloud.get("resolution", "")
    v3_bands = v3_cloud.get("bands", [])
    if v3_store and v3_bands:
        v3_b04 = build_zarr_v3_uri(v3_store, v3_group, v3_res, v3_bands[0])
        cases.append(("zarr_v3", "cloud", "single_band", v3_b04, None))
        v3_rgb = [build_zarr_v3_uri(v3_store, v3_group, v3_res, b) for b in v3_bands]
        cases.append(("zarr_v3", "cloud", "rgb_vrt", v3_rgb, None))
        cases.append(("zarr_v3", "cloud", "zoom_25", v3_rgb, 0.25))
        cases.append(("zarr_v3", "cloud", "zoom_5", v3_rgb, 0.05))

    # Cloud full-extent reads at canvas resolution (triggers overview selection)
    # Reads full 10980x10980 extent into 1024x1024 output buffer (~11:1 downsample)
    fe_buf = (1024, 1024)
    if cog_cloud.get("B04"):
        cases.append(("cog", "cloud", "full_extent",
                       build_cog_cloud_uri(cog_cloud["B04"]), None, fe_buf))
    if zarr_url and zarr_bands:
        zarr_b04 = build_zarr_uri(zarr_url, zarr_sub, zarr_res, zarr_bands[0])
        cases.append(("zarr_v2", "cloud", "full_extent", zarr_b04, None, fe_buf))
    if v3_store and v3_bands:
        v3_b04 = build_zarr_v3_uri(v3_store, v3_group, v3_res, v3_bands[0])
        cases.append(("zarr_v3", "cloud", "full_extent", v3_b04, None, fe_buf))

    # --- Run with each config ---
    configs = [("default", {})]
    if gdal_tuned:
        configs.append(("tuned", _TUNED_CONFIG))
    if gdal_parallel:
        configs.append(("parallel", _PARALLEL_CONFIG))

    for cfg_name, cfg_dict in configs:
        saved = apply_gdal_config(cfg_dict) if cfg_dict else {}

        for case in cases:
            fmt, source, scenario, uri_or_uris, extent_frac = case[:5]
            buf_size = case[5] if len(case) > 5 else None
            if cfg_name == "tuned" and source == "local":
                continue
            if cfg_name == "parallel" and source != "local":
                continue

            label = f"{fmt}_{source}_{scenario}"
            print(f"\n--- {label} [gdal={cfg_name}] ---")

            try:
                batch = run_scenario(
                    uri_or_uris, label, fmt, source, scenario,
                    cfg_name, cfg, extent_frac, buf_size,
                )
                results.extend(batch)
                if incremental_csv and batch:
                    append_csv(batch, incremental_csv)
            except Exception as e:
                print(f"  FAILED: {e}")

        if saved:
            restore_gdal_config(saved)

    return results


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

_CSV_HEADER = [
    "format", "source", "scenario", "gdal_config",
    "run", "open_ms", "read_ms", "width", "height",
]


def write_csv(results: List[BenchResult], out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(_CSV_HEADER)
        for r in results:
            writer.writerow([
                r.fmt, r.source, r.scenario, r.gdal_config,
                r.run, f"{r.open_ms:.2f}", f"{r.read_ms:.2f}",
                r.width, r.height,
            ])


def append_csv(results: List[BenchResult], out_path: str) -> None:
    """Append results incrementally. Creates file with header if needed."""
    needs_header = not os.path.isfile(out_path)
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "a", newline="") as f:
        writer = csv.writer(f)
        if needs_header:
            writer.writerow(_CSV_HEADER)
        for r in results:
            writer.writerow([
                r.fmt, r.source, r.scenario, r.gdal_config,
                r.run, f"{r.open_ms:.2f}", f"{r.read_ms:.2f}",
                r.width, r.height,
            ])


def print_summary(results: List[BenchResult]) -> None:
    groups = defaultdict(list)
    for r in results:
        key = (r.fmt, r.source, r.scenario, r.gdal_config)
        groups[key].append(r)

    print("\n" + "=" * 82)
    print(f"{'Format':<10} {'Source':<8} {'Scenario':<14} {'Config':<8} "
          f"{'Runs':>4} {'Open(ms)':>10} {'Read(ms)':>10} {'Total(ms)':>10}")
    print("-" * 82)

    for key in sorted(groups.keys()):
        batch = groups[key]
        n = len(batch)
        avg_open = sum(r.open_ms for r in batch) / n
        avg_read = sum(r.read_ms for r in batch) / n
        avg_total = avg_open + avg_read
        fmt, source, scenario, gdal_cfg = key
        print(f"{fmt:<10} {source:<8} {scenario:<14} {gdal_cfg:<8} "
              f"{n:>4} {avg_open:>10.1f} {avg_read:>10.1f} {avg_total:>10.1f}")

    print("=" * 82)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Benchmark COG vs GeoZarr (GDAL).")
    p.add_argument("--data-dir",
                    default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"),
                    help="Data directory from prep_data.sh (default: ./data)")
    p.add_argument("--runs", type=int, default=5)
    p.add_argument("--warmup", type=int, default=2)
    p.add_argument("--width", type=int, default=2048)
    p.add_argument("--height", type=int, default=2048)
    p.add_argument("--gdal-tuned", action="store_true",
                    help="Also run with tuned GDAL cloud config (HTTP/2, vsicurl cache)")
    p.add_argument("--gdal-parallel", action="store_true",
                    help="Also run local scenarios with GDAL_NUM_THREADS=ALL_CPUS")
    p.add_argument("--local-only", action="store_true",
                    help="Skip cloud scenarios (faster, no network)")
    p.add_argument("--out", default=None,
                    help="Output CSV (default: results/bench_cog_vs_zarr.csv)")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    urls_path = os.path.join(args.data_dir, "urls.json")
    if not os.path.isfile(urls_path):
        print(f"ERROR: {urls_path} not found. Run prep_data.sh first.", file=sys.stderr)
        return 1
    with open(urls_path) as f:
        urls = json.load(f)

    if args.local_only:
        urls["cog_cloud"] = {}
        urls["zarr_store"] = ""
        urls["zarr_bands"] = []

    print(f"GDAL:       {gdal.__version__}")
    print(f"Data dir:   {args.data_dir}")
    print(f"Runs: {args.runs}, Warmup: {args.warmup}, Size: {args.width}x{args.height}")
    print(f"Tuned:      {args.gdal_tuned}")
    print(f"Parallel:   {args.gdal_parallel}")
    print(f"Local only: {args.local_only}")

    out_path = args.out or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "results", "bench_cog_vs_zarr.csv",
    )

    cfg = BenchConfig(
        data_dir=args.data_dir, urls=urls,
        width=args.width, height=args.height,
        runs=args.runs, warmup=args.warmup,
    )
    results = run_all(cfg, args.gdal_tuned, args.gdal_parallel,
                      incremental_csv=out_path)

    # Final write ensures complete file (overwrites incremental)
    write_csv(results, out_path)
    print_summary(results)
    print(f"\nResults: {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
