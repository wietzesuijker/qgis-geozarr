# Benchmark claims and sources

Claims made in [Pangeo Showcase talk](https://discourse.pangeo.io/t/pangeo-showcase-no-shard-feelings-geozarr-rendering-in-qgis-powered-by-gdal-march-4-2026-at-12-pm-et/5526) (March 4, 2026) and the [post-talk follow-up](https://github.com/wietzesuijker/qgis-geozarr/blob/main/benchmarks/BENCHMARK-SETUP.md).

## Platform

- Apple M1 Pro, 8 cores, NVMe SSD
- macOS 14.7
- GDAL 3.13.0dev (commit aa390341db, includes PR #14059)
- Sentinel-2 L2A, 3 bands (B04/B03/B02), 10980x10980, uint16
- Same ZSTD Level 3 for COG and Zarr unless noted
- 2 warmup + 7 measured runs, median (not mean)
- Warm cache = kernel page cache hot, not GDAL block cache

## Claims

### "Overview read: COG 28ms, Zarr 72ms - COG 2.6x faster"

- **Source**: `results/bench_pr_impact_2026-03-03.csv`
- **Config**: `gdal_config=parallel` (GDAL_NUM_THREADS=ALL_CPUS)
- **COG**: `cog_zstd, local, single_band, parallel` - median read_ms across 5 runs: 26-30ms range, median 28ms (lines 162-166)
- **Zarr**: `zarr_v3_zstd_ovr, local, single_band, parallel` - median read_ms: 69-76ms range, median 72ms (lines 262-266)
- **Ratio**: 72/28 = 2.6x
- **Script**: `bench_cog_vs_zarr.py --gdal-parallel --local-only`

### "Full-res zoom: COG 105ms, Zarr 94ms - parity"

- **Source**: `results/bench_pr_impact_2026-03-03.csv`
- **Config**: `gdal_config=parallel`
- **COG**: `cog_zstd, local, zoom_5, parallel` - median read_ms: 102-107ms range, median 105ms (lines 177-181)
- **Zarr**: `zarr_v3_zstd_ovr, local, zoom_5, parallel` - median read_ms: 92-96ms range, median 94ms (lines 277-281)
- **Script**: `bench_cog_vs_zarr.py --gdal-parallel --local-only`

### "Random tiles (50 x 512x512): COG 288ms, Zarr 136ms - Zarr 2.1x faster"

- **Source**: separate random-tile benchmark script, results in `gdal-docs/pr-drafts/perf/benchmark-results-v2.md`
- **Config**: 8 threads
- **COG**: COG-ZSTD 8t = 0.288s
- **Zarr**: Zarr-v3-ZSTD 8t = 0.136s
- **Ratio**: 288/136 = 2.1x
- **Why**: Zarr chunks are direct-addressable by index; COG requires scanning IFD chain + tile byte offset table

### "Compression: COG 71 MB, Zarr 51 MB - Zarr 28% smaller"

- **Source**: on-disk sizes from `prep_data.sh` output
- **Codec**: ZSTD Level 3 for both
- **COG**: pixel-interleaved tiles (512x512, 3 bands per tile)
- **Zarr**: band-separate chunks (1x512x512, 1 band per chunk)
- **Why**: band-separate layout has higher spatial correlation within each chunk, compresses better with the same codec
- **Cross-check**: Element 84 showed byte-identical compressed data with same codec + same chunk layout

### "Parallel scaling: COG 1.1x, Zarr 4.3x (overview, 1 to 8 threads)"

- **Source**: `results/bench_pr_impact_2026-03-03.csv`, comparing `default` vs `parallel` configs for single_band scenario
- **COG (DEFLATE)**: default median ~34ms, parallel median ~36ms = 0.9x (no benefit, as expected for single-IFD overview read)
- **Zarr v3 ZSTD (no ovr)**: default median ~403ms*, parallel median ~94ms = 4.3x
- **Note**: COG-ZSTD default had noisy baseline (125ms median vs expected ~30ms). The 1.1x figure uses COG-DEFLATE which had stable baselines. See BENCHMARK-SETUP.md "Interpretation" section.
- **Why**: at overview scale, COG reads from one pre-built reduced IFD (few tiles to decompress). Zarr reads many small chunks that decompress independently across cores.

### "Both feel instant - under 100ms"

- 100ms is the classic usability threshold (Jakob Nielsen, 1993). Both 28ms and 72ms are well under this. The user perceives no delay for either format at overview zoom.

## Reproduce

```bash
# 1. Prepare test data
./prep_data.sh

# 2. Run benchmarks
python bench_cog_vs_zarr.py --gdal-parallel --local-only --runs 7 --warmup 2

# 3. Plot
python plot_results.py
```

Requires GDAL 3.13+ with Zarr v3 driver. See [BENCHMARK-SETUP.md](BENCHMARK-SETUP.md) for full details.
