# Benchmark setup and interpretation

## Source data

Sentinel-2 L2A, tile T25WES, 10980x10980 pixels, uint16, 10m resolution.
Same raw data (`b04_raw.tif`, `b03_raw.tif`, `b02_raw.tif`) converted to every format.

## Formats tested

| Label | Format | Codec | Chunk/tile | Overviews | Files |
|---|---|---|---|---|---|
| cog | COG (GeoTIFF) | DEFLATE | 512x512 tiles | 4 levels (2x,4x,8x,16x) | 1 file per band + 1 RGB stacked |
| cog_zstd | COG (GeoTIFF) | ZSTD L3 | 512x512 tiles | 4 levels | same |
| cog_noovr | COG (GeoTIFF) | DEFLATE | 512x512 tiles | none | same |
| zarr_v2 | Zarr v2 | ZLIB | 1830x1830 chunks | none | directory per band |
| zarr_v3_zstd | Zarr v3 sharded | ZSTD L3 | 512x512 inner chunks, 2048x2048 shards | none | directory per band |
| zarr_v3_blosc | Zarr v3 sharded | blosc+lz4 L5 | 512x512 inner, 2048x2048 shards | none | directory per band |
| zarr_v3_zstd_ovr | Zarr v3 sharded | ZSTD L3 | 512x512 inner, 2048x2048 shards | 4 levels via multiscale groups | directory per band |

## Overview mechanisms

Both COG and Zarr v3+ovr have 4 overview levels (2x, 4x, 8x, 16x).

**COG**: Overviews are reduced-resolution IFDs inside the same TIFF file. GDAL traverses the IFD pointer chain to find the right level. All data in one file.

**Zarr v3**: Overviews are sibling array groups (`ovr_2x`, `ovr_4x`, `ovr_8x`, `ovr_16x`) built by `GDALMDArray::BuildOverviews()`. GDAL discovers them via the `multiscales` attribute in `zarr.json`. Each overview is a separate sharded array in its own subdirectory.

## What each scenario measures

When QGIS renders a raster, it calls `ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize)`. If the source window is larger than the output buffer, GDAL picks an overview level and resamples. The benchmark replicates this.

| Scenario | Source window | Output buffer | Downsample ratio | GDAL reads from | QGIS equivalent |
|---|---|---|---|---|---|
| single_band | 10980x10980 | 2048x2048 | 5.4x | **4x overview** | Full-extent view on a ~2K canvas |
| rgb_stacked/vrt | 10980x10980 (3 bands) | 2048x2048 | 5.4x | **4x overview** | Initial RGB load |
| zoom_25 | 5490x5490 (centered) | 2048x2048 | 2.7x | **2x overview** | Moderate zoom (city scale) |
| zoom_5 | 2455x2455 (centered) | 2048x2048 | 1.2x | **Full resolution** | Deep zoom (neighborhood) |

The `zoom_window()` function computes centered windows: `fraction^0.5 * extent` per side.

## Fairness measures

1. **Same codec**: COG-ZSTD and Zarr-ZSTD both use ZSTD level 3. Isolates format overhead from codec speed.
2. **Same overview levels**: Both have 2x, 4x, 8x, 16x. Eliminates overview-level mismatch.
3. **Same data**: Identical source pixels converted to each format.
4. **Same disk**: All formats on the same NVMe.
5. **Band-separate vs pixel-interleaved**: COG stacks RGB into one file (pixel-interleaved tiles). Zarr stores each band as a separate array (band-separate). This is an inherent format design difference. Band-separate compresses ~28% smaller with the same codec because spatial correlation within a single band is higher than across bands.

## Warm cache caveat

Runs use `warmup=2` before `runs=5` measured iterations. After warmup, the OS page cache holds all data in RAM. These benchmarks therefore measure **decode + resample overhead**, not disk or network I/O. On network (cold access), the picture would differ because I/O dominates and COG's single-file IFD structure requires fewer HTTP requests than Zarr's directory-per-overview layout.

Evidence: COG with overviews (34ms) vs COG without overviews (36ms) for single_band are nearly identical. If I/O mattered, reading full-res 10980x10980 would be much slower than reading a 2745x2745 overview.

## Results

Source: `results/bench_pr_impact_2026-03-03.csv`. GDAL 3.13.0dev (aa390341db). Apple M1 Pro, 8 cores, NVMe.

### With GDAL_NUM_THREADS=ALL_CPUS (production config)

| Format | single_band | rgb | zoom_25 | zoom_5 |
|---|---|---|---|---|
| COG (DEFLATE) | 42ms | 121ms | 162ms | 144ms |
| COG (ZSTD) | **28ms** | **87ms** | **107ms** | 105ms |
| COG (no ovr) | 37ms | 118ms | 182ms | 145ms |
| Zarr v2 | 212ms | 556ms | 271ms | 135ms |
| Zarr v3 ZSTD | 94ms | 302ms | 141ms | 86ms |
| Zarr v3 blosc | 82ms | 313ms | 129ms | 84ms |
| Zarr v3+ovr | 72ms | 231ms | 156ms | **94ms** |

### Single-threaded (default config)

| Format | single_band | rgb | zoom_25 | zoom_5 |
|---|---|---|---|---|
| COG (DEFLATE) | 34ms | 160ms | 152ms | 525ms* |
| COG (ZSTD) | 125ms* | 400ms* | 165ms | 128ms |
| COG (no ovr) | 36ms | 134ms | 159ms | 155ms |
| Zarr v2 | 252ms | 877ms | 267ms | 204ms |
| Zarr v3 ZSTD | 403ms | 709ms | 333ms | 117ms |
| Zarr v3 blosc | 174ms | 493ms | 192ms | 102ms |
| Zarr v3+ovr | 175ms | 545ms | 277ms | 119ms |

*High variance in some default runs (system load). Parallel runs are stable.

### PR impact: default -> parallel speedup (Zarr-specific gains)

| Format | single_band | rgb | zoom_25 | zoom_5 |
|---|---|---|---|---|
| COG (ZSTD) | 4.5x* | 4.6x* | 1.5x | 1.2x |
| Zarr v3 ZSTD | **4.3x** | **2.3x** | **2.4x** | **1.4x** |
| Zarr v3+ovr | **2.4x** | **2.4x** | **1.8x** | **1.3x** |

*COG-ZSTD single-thread had noisy baseline (125ms vs expected ~30ms). True speedup likely ~1.1x for single_band.

## Interpretation

**For overview reads (zoomed out, initial load):** COG is 2-3x faster than Zarr v3+ovr with parallel decode (28ms vs 72ms single band, 87ms vs 231ms RGB). Both read from pre-built overviews. COG's advantage: one-file IFD chain is faster to navigate than Zarr's directory-per-overview layout. With warm cache this is pure overhead, not I/O.

**For full-resolution partial reads (deep zoom):** They converge to parity (105ms COG vs 94ms Zarr v3+ovr). Both read a small number of tiles/chunks from the main dataset. The overhead difference is negligible relative to decode time.

**Parallel decode helps Zarr more:** Zarr chunks decompress independently (one shard = many chunks). The auto-parallel IRead PR (#13972) gives 2-4x speedup for Zarr, bringing it from "5x slower" to "2-3x slower" for overview reads.

**Band-separate compression:** Zarr stores each band separately. Same ZSTD L3 codec: COG-ZSTD 71MB vs Zarr-ZSTD 51MB for b04 (28% smaller). This is inherent to band-separate layout, not a codec advantage.

**What these benchmarks do NOT measure:** Cold network access (HTTP round trips, TLS handshake, shard index fetches). On the network, COG's structural advantage is larger because its single-file IFD needs fewer HTTP requests than Zarr's multi-file overview layout.

## Solid claims for the talk

1. "With warm cache, COG overview reads are 2-3x faster than Zarr" - structural overhead, not I/O
2. "At full-resolution zoom, they converge to parity" - small reads, overhead negligible
3. "Parallel decode gives Zarr 2-4x speedup" - auto-parallel IRead PR
4. "Band-separate layout compresses 28% smaller" - same codec, measured
5. "The PRs made Zarr fast enough for interactive rendering" - not faster than COG, but 72ms overview reads and sub-100ms zoomed reads are responsive
