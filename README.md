# GeoZarr for QGIS

GeoZarr-aware loading for the QGIS STAC browser: band selection, multiscale overviews, satellite presets, and optimized GDAL configuration for cloud-native Zarr access.

**QGIS 3.44+** | **GDAL 3.13+** | **Zarr v2 + v3**

## Features

- **STAC browser integration** - right-click Zarr assets to load with band/resolution selection
- **Satellite band presets** - Sentinel-2, Landsat 8/9, MODIS, Sentinel-3 (True Color, False Color, etc.)
- **Time series** - search STAC collections by date range, load as temporal layer with slider
- **Multiscale overviews** - VRT overviews from coarser Zarr resolutions for smooth zoom
- **Auto RGB styling** - cumulative-cut stretch for 3-band composites
- **Band descriptions** - metadata names (long_name/standard_name) shown in Layers panel
- **EOPF support** - Sentinel-2 L2A via both EOPF Production (v2) and Explorer (v3)
- **URL loader** - standalone dialog with recent URL history
- **GDAL tuning** - HTTP/2, connection pooling, vsicurl cache, parallel decode, shard index cache

## Requirements

- QGIS 3.44 or later
- GDAL 3.13 or later (ships with QGIS 3.44+)

## Install

**From QGIS Plugin Manager** (recommended):
Plugins > Manage and Install Plugins > Search "GeoZarr" > Install

**Manual install**:
Download `qgis_geozarr.zip` from [Releases](https://github.com/wietzesuijker/qgis-geozarr/releases), then Plugins > Manage and Install Plugins > Install from ZIP.

## Usage

**Via STAC browser**: Browse a STAC catalog, right-click a Zarr asset, select "Load GeoZarr..." to open the band picker dialog.

**Via URL loader**: Click the GeoZarr toolbar icon or go to Plugins > GeoZarr > Load from URL. Paste a Zarr store URL.

### Tested with

- [EOPF Explorer](https://explorer.eopf.copernicus.eu/) - Sentinel-2 L2A (Zarr v3, sharded)
- [EOPF Production](https://stac.core.eopf.eodc.eu/) - Sentinel-2 L2A (Zarr v2)

### Known limitations

- Requires GDAL 3.13+ (Zarr v3 sharding support). QGIS 3.44 ships this by default.
- CRS metadata (`proj:code`, `proj:projjson`, or EOPF `other_metadata`) recommended; warns if absent.

## License

GPL-2.0 - see [LICENSE](LICENSE)
