#!/usr/bin/env python3
"""Build multiscale overviews on Zarr v3 datasets.

Requires GDAL 3.13+ with BuildOverviews support (feat/zarr-build-overviews).

Usage:
    python prep_overviews.py [DATA_DIR]
"""
import os
import shutil
import sys

from osgeo import gdal

gdal.UseExceptions()

DATA_DIR = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data"
)
BANDS = ["b04", "b03", "b02"]
FACTORS = [2, 4, 8, 16]


def build_overviews(src_dir: str, dst_dir: str) -> None:
    """Copy a Zarr v3 dataset and build overviews on the copy."""
    if os.path.isdir(dst_dir):
        print(f"  {dst_dir} exists, skipping")
        return

    if not os.path.isdir(src_dir):
        print(f"  {src_dir} not found, skipping")
        return

    print(f"  Copying {src_dir} -> {dst_dir}")
    shutil.copytree(src_dir, dst_dir)

    # Open root group, find the 2D data array (skip 1D coord arrays)
    ds = gdal.OpenEx(dst_dir, gdal.OF_MULTIDIM_RASTER | gdal.OF_UPDATE)
    rg = ds.GetRootGroup()
    ar = None
    for name in rg.GetMDArrayNames():
        candidate = rg.OpenMDArray(name)
        if candidate.GetDimensionCount() >= 2:
            ar = candidate
            break
    if ar is None:
        print(f"  WARNING: no >=2D arrays in {dst_dir}")
        return

    dims = [ar.GetDimensions()[i].GetSize() for i in range(ar.GetDimensionCount())]
    print(f"  Array '{ar.GetName()}' dims={dims}, building overviews {FACTORS}")
    ar.BuildOverviews("AVERAGE", FACTORS)

    ds = None  # flush
    print(f"  Done: {dst_dir}")


def main() -> int:
    print(f"Building overviews in {DATA_DIR}")
    print(f"Factors: {FACTORS}")

    for band in BANDS:
        src = os.path.join(DATA_DIR, f"{band}_zarr_v3_zstd")
        dst = os.path.join(DATA_DIR, f"{band}_zarr_v3_zstd_ovr")
        print(f"\n--- {band} (zstd + overviews) ---")
        build_overviews(src, dst)

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
