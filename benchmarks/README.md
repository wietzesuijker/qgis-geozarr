# COG vs GeoZarr Benchmark

GDAL-native benchmark comparing Cloud-Optimized GeoTIFF and GeoZarr (Zarr v2)
for Sentinel-2 L2A data. Measures dataset open time and raster read time.

Uses GDAL directly (no QGIS dependency) to isolate format performance - the
QGIS rendering pipeline is identical for both formats.

## Prerequisites

- GDAL 3.10+ with Zarr driver
- Python 3.10+ with GDAL bindings

## Quick start

```bash
# 1. Download test data (~700 MB)
./prep_data.sh

# 2. Run benchmark (local only - fast)
python bench_cog_vs_zarr.py --local-only --runs 3

# 3. Full benchmark (local + cloud endpoints)
python bench_cog_vs_zarr.py --runs 5 --warmup 2

# 4. With tuned GDAL config (HTTP/2, vsicurl cache)
python bench_cog_vs_zarr.py --gdal-tuned --runs 5
```

## Data sources

| Format | Source | Endpoint | Tile |
|--------|--------|----------|------|
| Zarr v2 | EOPF S2 L2A | `objects.eodc.eu` (EU) | T25WES |
| COG | Element84 S2 L2A | `sentinel-cogs.s3.us-west-2.amazonaws.com` (US) | T25WES |
| Local COG | Converted from EOPF Zarr | Local disk | T25WES |

Bands: B04 (Red), B03 (Green), B02 (Blue) at r10m (10980x10980 px).

## Scenarios

| Scenario | Description |
|----------|-------------|
| `single_band` | 1 band, full extent |
| `rgb_stacked` | 3-band stacked COG, full extent (local only) |
| `rgb_vrt` | 3-band VRT composite, full extent |
| `zoom_25` | RGB, 25% of extent area (4x zoom) |
| `zoom_5` | RGB, 5% of extent area (20x zoom) |

## Output

CSV in `results/bench_cog_vs_zarr.csv`:
```
format,source,scenario,gdal_config,run,open_ms,read_ms,width,height
```

Console prints summary table with averages per group.

## Notes

- Cloud COG server (US-West) vs cloud Zarr server (EU) - latency differs.
  Local COG comparison isolates format overhead.
- `--gdal-tuned` applies HTTP/2 multiplexing, vsicurl caching, parallel decode
  (same config as qgis-geozarr plugin).
- Each iteration clears vsicurl cache for cold-start measurement.
- Warmup runs are not recorded.
