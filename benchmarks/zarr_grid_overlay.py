#!/usr/bin/env python3
"""
zarr_grid_overlay.py - Generate a GeoJSON grid showing Zarr shard and inner chunk boundaries.

Usage:
  python zarr_grid_overlay.py <zarr_json_url_or_path> [--epsg N] [--out FILE] [--band-dim 0]

Output: GeoJSON FeatureCollection with two feature types:
  "shard" - shard boundaries (the large files on S3)
  "chunk" - chunk boundaries (the unit of compression inside each shard)

Load the GeoJSON as a vector layer in QGIS on top of your GeoZarr raster to visualize
which shard and inner chunk a pixel belongs to.

Requires: Python 3.8+, no external dependencies (pure stdlib).

Examples:
  # Local zarr store
  python zarr_grid_overlay.py /path/to/store/B04/zarr.json --out b04_grid.geojson

  # Remote store (fetches zarr.json over HTTP)
  python zarr_grid_overlay.py https://s3.example.com/store/B04/zarr.json --epsg 32632
"""

import argparse
import json
import sys
import urllib.request
from math import ceil
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def load_zarr_json(source: str) -> Dict[str, Any]:
    if source.startswith("http://") or source.startswith("https://"):
        with urllib.request.urlopen(source, timeout=30) as resp:
            return json.loads(resp.read())
    else:
        return json.loads(Path(source).read_text())


def parse_geotransform(attrs: Dict[str, Any]) -> Optional[Tuple[float, ...]]:
    """Extract GDAL geotransform from GeoZarr spatial:transform attribute.

    spatial:transform format: [a, b, c, d, e, f]
    GDAL geotransform:        (c, a, b, f, d, e)
      where:
        c = x_origin (west edge of top-left pixel)
        a = pixel width
        b = x rotation (0 for north-up)
        f = y_origin (north edge of top-left pixel)
        d = y rotation (0 for north-up)
        e = pixel height (negative for north-up images)
    """
    st = attrs.get("spatial:transform")
    if not st or len(st) < 6:
        return None
    a, b, c, d, e, f = st[:6]
    return (c, a, b, f, d, e)


def parse_epsg(attrs: Dict[str, Any]) -> Optional[int]:
    code = attrs.get("proj:code") or attrs.get("crs")
    if not code:
        return None
    if isinstance(code, str) and ":" in code:
        try:
            return int(code.split(":")[-1])
        except ValueError:
            return None
    try:
        return int(code)
    except (TypeError, ValueError):
        return None


def parse_chunk_shapes(
    zarr_meta: Dict[str, Any], band_dim: int
) -> Tuple[Optional[List[int]], Optional[List[int]]]:
    """Return (outer_chunk_shape, inner_chunk_shape) in spatial dims [height, width].

    outer_chunk_shape: the regular chunk grid size (= shard file dimensions)
    inner_chunk_shape: the sub-grid inside each shard (from sharding_indexed codec)
    """
    chunk_grid = zarr_meta.get("chunk_grid", {})
    outer = chunk_grid.get("configuration", {}).get("chunk_shape")
    if not outer:
        return None, None

    # Drop the band/time dimension to get spatial dims
    spatial = _drop_band_dim(outer, band_dim)

    inner = None
    for codec in zarr_meta.get("codecs", []):
        if codec.get("name") == "sharding_indexed":
            inner_raw = codec.get("configuration", {}).get("chunk_shape")
            if inner_raw:
                inner = _drop_band_dim(inner_raw, band_dim)
            break

    return spatial, inner


def _drop_band_dim(shape: List[int], band_dim: int) -> List[int]:
    """Remove a single non-spatial dimension from a shape list."""
    if len(shape) <= 2:
        return list(shape)
    result = list(shape)
    if 0 <= band_dim < len(result):
        result.pop(band_dim)
    return result[-2:]  # last two are always spatial [height, width]


def pixel_to_geo(
    row: float, col: float, gt: Tuple[float, ...]
) -> Tuple[float, float]:
    """Convert pixel (row, col) to geographic (x, y) using GDAL geotransform."""
    x = gt[0] + col * gt[1] + row * gt[2]
    y = gt[3] + col * gt[4] + row * gt[5]
    return x, y


def make_polygon(
    row_start: int, col_start: int, row_end: int, col_end: int,
    gt: Tuple[float, ...]
) -> List[List[List[float]]]:
    """Build a closed GeoJSON polygon ring for a pixel-space rectangle."""
    corners = [
        pixel_to_geo(row_start, col_start, gt),
        pixel_to_geo(row_start, col_end, gt),
        pixel_to_geo(row_end, col_end, gt),
        pixel_to_geo(row_end, col_start, gt),
    ]
    ring = [[x, y] for x, y in corners]
    ring.append(ring[0])  # close
    return [ring]


def make_grid_features(
    total_rows: int,
    total_cols: int,
    chunk_h: int,
    chunk_w: int,
    feature_type: str,
    gt: Tuple[float, ...],
) -> List[Dict[str, Any]]:
    features = []
    shard_row = 0
    for row_start in range(0, total_rows, chunk_h):
        shard_col = 0
        row_end = min(row_start + chunk_h, total_rows)
        for col_start in range(0, total_cols, chunk_w):
            col_end = min(col_start + chunk_w, total_cols)
            features.append({
                "type": "Feature",
                "properties": {
                    "type": feature_type,
                    "grid_row": shard_row,
                    "grid_col": shard_col,
                    "pixel_row_start": row_start,
                    "pixel_col_start": col_start,
                    "pixel_height": row_end - row_start,
                    "pixel_width": col_end - col_start,
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": make_polygon(row_start, col_start, row_end, col_end, gt),
                },
            })
            shard_col += 1
        shard_row += 1
    return features


def build_geojson(
    zarr_meta: Dict[str, Any],
    attrs: Dict[str, Any],
    band_dim: int,
    epsg_override: Optional[int],
) -> Dict[str, Any]:
    shape = zarr_meta.get("shape", [])
    if len(shape) < 2:
        sys.exit(f"Error: 'shape' has fewer than 2 dimensions: {shape}")

    total_rows, total_cols = shape[-2], shape[-1]

    gt = parse_geotransform(attrs)
    if gt is None:
        sys.exit(
            "Error: no 'spatial:transform' found in attributes. "
            "Set --epsg and check the zarr.json attributes key."
        )

    epsg = epsg_override or parse_epsg(attrs)

    outer_shape, inner_shape = parse_chunk_shapes(zarr_meta, band_dim)
    if outer_shape is None:
        sys.exit("Error: could not parse chunk_grid.configuration.chunk_shape")

    chunk_h, chunk_w = outer_shape[-2], outer_shape[-1]

    features = make_grid_features(total_rows, total_cols, chunk_h, chunk_w, "shard", gt)

    n_shards = ceil(total_rows / chunk_h) * ceil(total_cols / chunk_w)
    print(f"Array:  {total_rows} x {total_cols} pixels")
    print(f"Shards: {chunk_h} x {chunk_w} px each -> {n_shards} shard polygons")

    if inner_shape is not None:
        inner_h, inner_w = inner_shape[-2], inner_shape[-1]
        chunk_features = make_grid_features(
            total_rows, total_cols, inner_h, inner_w, "chunk", gt
        )
        features.extend(chunk_features)
        n_chunks = ceil(total_rows / inner_h) * ceil(total_cols / inner_w)
        print(f"Chunks: {inner_h} x {inner_w} px each -> {n_chunks} chunk polygons")
        print(f"Chunks per shard: {ceil(chunk_h / inner_h)} x {ceil(chunk_w / inner_w)}")
    else:
        print("No sharding_indexed codec found - only shard polygons generated")

    collection: Dict[str, Any] = {
        "type": "FeatureCollection",
        "features": features,
    }
    if epsg:
        collection["crs"] = {
            "type": "name",
            "properties": {"name": f"urn:ogc:def:crs:EPSG::{epsg}"},
        }

    return collection


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a GeoJSON shard/chunk grid from a Zarr v3 zarr.json"
    )
    parser.add_argument("source", help="Path or URL to zarr.json")
    parser.add_argument("--epsg", type=int, default=None, help="Override EPSG code")
    parser.add_argument(
        "--out", default="zarr_grid.geojson", help="Output GeoJSON file (default: zarr_grid.geojson)"
    )
    parser.add_argument(
        "--band-dim",
        type=int,
        default=0,
        help="Index of the non-spatial band/time dimension to drop (default: 0)",
    )
    args = parser.parse_args()

    print(f"Loading {args.source} ...")
    zarr_meta = load_zarr_json(args.source)

    # GeoZarr attributes live under "attributes" key in zarr.json (v3)
    # or directly in .zattrs (v2). Try both.
    attrs = zarr_meta.get("attributes", zarr_meta)

    collection = build_geojson(zarr_meta, attrs, args.band_dim, args.epsg)

    out_path = Path(args.out)
    out_path.write_text(json.dumps(collection, indent=2))
    print(f"Written {len(collection['features'])} features to {out_path}")
    print(f"\nIn QGIS: Layer > Add Layer > Add Vector Layer, pick {out_path}")
    print("Style shards with thick outline, chunks with thin outline.")
    print('Use "type" attribute to filter: \"type\" = \'shard\' or \"type\" = \'chunk\'')


if __name__ == "__main__":
    main()
