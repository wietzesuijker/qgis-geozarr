# GeoZarr for QGIS

GeoZarr-aware loading for the QGIS STAC browser: band selection, multiscale overviews, satellite presets, and optimized GDAL configuration for cloud-native Zarr access.

**QGIS 3.44+** | **GDAL 3.13+** | **Zarr v2 + v3**

## Features

- **STAC browser integration** - right-click any STAC item to load with band/resolution selection
- **Satellite band presets** - Sentinel-2, Landsat 8/9, MODIS, Sentinel-3 (True Color, False Color, Agriculture, etc.)
- **Time series** - search STAC collections by date range, load as temporal layer with animation slider
- **Multiscale overviews** - VRT overviews from coarser Zarr resolutions for smooth zoom
- **Auto RGB styling** - stretch defaults per satellite/dtype, persisted across sessions
- **Cloud cover badges** - color-coded quality indicators in the load dialog
- **Band descriptions** - metadata names shown in Layers panel, band count and file size estimates in UI
- **EOPF support** - Sentinel-2 L2A via both EOPF Production (v2) and Explorer (v3, sharded)
- **URL loader** - standalone dialog with recent URL history and clipboard paste
- **GDAL tuning** - HTTP/2, vsicurl cache, parallel decode, shard index cache - applied automatically

## Requirements

- QGIS 3.44 or later
- GDAL 3.13 or later (ships with QGIS 3.44+)

## Install

**From QGIS Plugin Manager** (recommended):
Plugins > Manage and Install Plugins > Search "GeoZarr" > Install

**Manual install**:
Download `qgis_geozarr.zip` from [Releases](https://github.com/wietzesuijker/qgis-geozarr/releases), then Plugins > Manage and Install Plugins > Install from ZIP.

## Usage

**Via STAC browser**: Browse a STAC catalog, right-click any item, select "Load GeoZarr..." to open the band picker. For time series, select "Load time series..." to search by date range.

**Via URL loader**: Click the GeoZarr toolbar icon or Plugins > GeoZarr > Load from URL. Paste a Zarr store URL.

### Tested with

- [EOPF Explorer](https://explorer.eopf.copernicus.eu/) - Sentinel-2 L2A (Zarr v3, sharded)
- [EOPF Production](https://stac.core.eopf.eodc.eu/) - Sentinel-2 L2A (Zarr v2)

### Known limitations

- Requires GDAL 3.13+ for Zarr v3 sharding. QGIS 3.44 ships this by default.
- CRS metadata (`proj:code`, `proj:projjson`, or EOPF `other_metadata`) recommended; warns if absent.

## Development

```bash
make test    # 96 tests
make lint    # ruff check
make zip     # build plugin zip
```

## License

GPL-2.0 - see [LICENSE](LICENSE)
