#!/usr/bin/env bash
# Prepare benchmark data: download EOPF S2 Zarr bands, convert to COG.
#
# Usage: ./prep_data.sh [DATA_DIR]
#   DATA_DIR defaults to ./data
#
# Requires: GDAL 3.13+ (gdal_translate with Zarr v2 support)
set -euo pipefail

DATA_DIR="${1:-$(dirname "$0")/data}"
mkdir -p "$DATA_DIR"

# --- EOPF Sentinel-2 L2A (Zarr v2) ---
ZARR_BASE="https://objects.eodc.eu/e05ab01a9d56408d82ac32d69a5aae2a:202602-s02msil2a-eu/16/products/cpm_v262/S2A_MSIL2A_20260216T142251_N0512_R096_T25WES_20260216T202508.zarr"
ZARR_SUB="measurements/reflectance"
BANDS=(b04 b03 b02)
RES="r10m"

# --- Element84 S2 L2A COGs (same tile, 2 days later) ---
COG_BASE="https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/25/W/ES/2026/2/S2C_25WES_20260218_0_L2A"
COG_BANDS=(B04 B03 B02)

echo "=== Step 1: Download EOPF Zarr bands as local GeoTIFF ==="
for band in "${BANDS[@]}"; do
    out="$DATA_DIR/${band}_raw.tif"
    if [ -f "$out" ]; then
        echo "  $out exists, skipping"
        continue
    fi
    uri="ZARR:\"/vsicurl/${ZARR_BASE}/${ZARR_SUB}/${RES}/${band}\""
    echo "  Downloading $band -> $out"
    gdal_translate "$uri" "$out" -q
done

echo "=== Step 2: Convert to per-band COG ==="
for band in "${BANDS[@]}"; do
    raw="$DATA_DIR/${band}_raw.tif"
    cog="$DATA_DIR/${band}_cog.tif"
    if [ -f "$cog" ]; then
        echo "  $cog exists, skipping"
        continue
    fi
    echo "  Converting $band -> COG"
    gdal_translate "$raw" "$cog" -of COG \
        -co COMPRESS=DEFLATE -co BLOCKSIZE=512 -co OVERVIEW_COUNT=4 -q
done

echo "=== Step 2b: Convert to per-band COG with ZSTD (fair codec comparison) ==="
for band in "${BANDS[@]}"; do
    raw="$DATA_DIR/${band}_raw.tif"
    cog="$DATA_DIR/${band}_cog_zstd.tif"
    if [ -f "$cog" ]; then
        echo "  $cog exists, skipping"
        continue
    fi
    echo "  Converting $band -> COG (ZSTD level 3)"
    gdal_translate "$raw" "$cog" -of COG \
        -co COMPRESS=ZSTD -co LEVEL=3 -co BLOCKSIZE=512 -co OVERVIEW_COUNT=4 -q
done

echo "=== Step 3: Create per-band COG WITHOUT overviews (fair comparison) ==="
for band in "${BANDS[@]}"; do
    raw="$DATA_DIR/${band}_raw.tif"
    cog="$DATA_DIR/${band}_cog_noovr.tif"
    if [ -f "$cog" ]; then
        echo "  $cog exists, skipping"
        continue
    fi
    echo "  Converting $band -> COG (no overviews)"
    gdal_translate "$raw" "$cog" -of COG \
        -co COMPRESS=DEFLATE -co BLOCKSIZE=512 -ovr NONE -q
done

echo "=== Step 4: Create local Zarr from same data ==="
for band in "${BANDS[@]}"; do
    raw="$DATA_DIR/${band}_raw.tif"
    zarr_dir="$DATA_DIR/${band}_zarr"
    if [ -d "$zarr_dir" ]; then
        echo "  $zarr_dir exists, skipping"
        continue
    fi
    echo "  Converting $band -> Zarr (1830x1830 chunks, ZLIB)"
    gdal_translate "$raw" "$zarr_dir" -of ZARR \
        -co COMPRESS=ZLIB -co "BLOCKSIZE=1830,1830" -q
done

echo "=== Step 5: Create local Zarr v3 (zstd, sharded) ==="
for band in "${BANDS[@]}"; do
    raw="$DATA_DIR/${band}_raw.tif"
    zarr_dir="$DATA_DIR/${band}_zarr_v3_zstd"
    if [ -d "$zarr_dir" ]; then
        echo "  $zarr_dir exists, skipping"
        continue
    fi
    echo "  Converting $band -> Zarr v3 (512x512 chunks, 2048x2048 shards, zstd)"
    gdal_translate "$raw" "$zarr_dir" -of ZARR \
        -co FORMAT=ZARR_V3 -co COMPRESS=ZSTD -co ZSTD_LEVEL=3 \
        -co "BLOCKSIZE=2048,2048" -co "SHARD_CHUNK_SHAPE=512,512" -q
done

echo "=== Step 6: Create local Zarr v3 (blosc+lz4, sharded) ==="
for band in "${BANDS[@]}"; do
    raw="$DATA_DIR/${band}_raw.tif"
    zarr_dir="$DATA_DIR/${band}_zarr_v3_blosc"
    if [ -d "$zarr_dir" ]; then
        echo "  $zarr_dir exists, skipping"
        continue
    fi
    echo "  Converting $band -> Zarr v3 (512x512 chunks, 2048x2048 shards, blosc+lz4)"
    gdal_translate "$raw" "$zarr_dir" -of ZARR \
        -co FORMAT=ZARR_V3 -co COMPRESS=BLOSC -co BLOSC_CNAME=lz4 \
        -co BLOSC_CLEVEL=5 -co BLOSC_SHUFFLE=BYTE \
        -co "BLOCKSIZE=2048,2048" -co "SHARD_CHUNK_SHAPE=512,512" -q
done

echo "=== Step 7: Create 3-band stacked COGs (RGB) ==="
RGB_COG="$DATA_DIR/rgb_cog.tif"
if [ ! -f "$RGB_COG" ]; then
    VRT_TMP="$DATA_DIR/_rgb_tmp.vrt"
    gdalbuildvrt -separate "$VRT_TMP" \
        "$DATA_DIR/b04_cog.tif" "$DATA_DIR/b03_cog.tif" "$DATA_DIR/b02_cog.tif" -q
    gdal_translate "$VRT_TMP" "$RGB_COG" -of COG \
        -co COMPRESS=DEFLATE -co BLOCKSIZE=512 -co OVERVIEW_COUNT=4 -q
    rm -f "$VRT_TMP"
    echo "  Created $RGB_COG (with overviews)"
else
    echo "  $RGB_COG exists, skipping"
fi

RGB_COG_ZSTD="$DATA_DIR/rgb_cog_zstd.tif"
if [ ! -f "$RGB_COG_ZSTD" ]; then
    VRT_TMP="$DATA_DIR/_rgb_tmp.vrt"
    gdalbuildvrt -separate "$VRT_TMP" \
        "$DATA_DIR/b04_cog_zstd.tif" "$DATA_DIR/b03_cog_zstd.tif" "$DATA_DIR/b02_cog_zstd.tif" -q
    gdal_translate "$VRT_TMP" "$RGB_COG_ZSTD" -of COG \
        -co COMPRESS=ZSTD -co LEVEL=3 -co BLOCKSIZE=512 -co OVERVIEW_COUNT=4 -q
    rm -f "$VRT_TMP"
    echo "  Created $RGB_COG_ZSTD (ZSTD, with overviews)"
else
    echo "  $RGB_COG_ZSTD exists, skipping"
fi

RGB_COG_NOOVR="$DATA_DIR/rgb_cog_noovr.tif"
if [ ! -f "$RGB_COG_NOOVR" ]; then
    VRT_TMP="$DATA_DIR/_rgb_tmp.vrt"
    gdalbuildvrt -separate "$VRT_TMP" \
        "$DATA_DIR/b04_cog_noovr.tif" "$DATA_DIR/b03_cog_noovr.tif" "$DATA_DIR/b02_cog_noovr.tif" -q
    gdal_translate "$VRT_TMP" "$RGB_COG_NOOVR" -of COG \
        -co COMPRESS=DEFLATE -co BLOCKSIZE=512 -ovr NONE -q
    rm -f "$VRT_TMP"
    echo "  Created $RGB_COG_NOOVR (no overviews)"
else
    echo "  $RGB_COG_NOOVR exists, skipping"
fi

echo "=== Step 8: Write URLs config ==="
cat > "$DATA_DIR/urls.json" <<EOF
{
  "zarr_store": "${ZARR_BASE}",
  "zarr_sub_group": "${ZARR_SUB}",
  "zarr_resolution": "${RES}",
  "zarr_bands": ["b04", "b03", "b02"],
  "cog_cloud": {
    "B04": "${COG_BASE}/B04.tif",
    "B03": "${COG_BASE}/B03.tif",
    "B02": "${COG_BASE}/B02.tif"
  },
  "epsg": 32625,
  "tile": "T25WES"
}
EOF
echo "  Wrote $DATA_DIR/urls.json"

echo "=== Step 9: Verify ==="
echo "  Local COGs:"
for band in "${BANDS[@]}"; do
    cog="$DATA_DIR/${band}_cog.tif"
    size=$(du -h "$cog" | cut -f1)
    dims=$(gdalinfo "$cog" -json 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin)['size']; print(f'{d[0]}x{d[1]}')" 2>/dev/null || echo "?")
    echo "    $band: $size ($dims)"
done
rgb_size=$(du -h "$RGB_COG" | cut -f1)
echo "  RGB COG: $rgb_size"

echo ""
echo "Data ready in $DATA_DIR"
echo "Cloud COG URLs in $DATA_DIR/urls.json"
