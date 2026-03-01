"""Shared fixtures for qgis-geozarr tests."""

import pytest

from qgis_geozarr.geozarr_metadata import ZarrRootInfo


@pytest.fixture
def sample_v3_zarr_json():
    """Minimal Zarr v3 zarr.json resembling EOPF Sentinel-2 structure."""
    return {
        "zarr_format": 3,
        "node_type": "group",
        "attributes": {
            "proj:code": "EPSG:32627",
            "spatial:transform": [10.0, 0.0, 399960.0, 0.0, -10.0, 4500000.0],
        },
        "consolidated_metadata": {
            "metadata": {
                "measurements/reflectance/r10m/b02": {
                    "node_type": "array",
                    "shape": [10980, 10980],
                    "data_type": "uint16",
                },
                "measurements/reflectance/r10m/b03": {
                    "node_type": "array",
                    "shape": [10980, 10980],
                    "data_type": "uint16",
                },
                "measurements/reflectance/r10m/b04": {
                    "node_type": "array",
                    "shape": [10980, 10980],
                    "data_type": "uint16",
                },
                "measurements/reflectance/r10m/b08": {
                    "node_type": "array",
                    "shape": [10980, 10980],
                    "data_type": "uint16",
                },
                "measurements/reflectance/r20m/b05": {
                    "node_type": "array",
                    "shape": [5490, 5490],
                    "data_type": "uint16",
                },
                "measurements/reflectance/r20m/b06": {
                    "node_type": "array",
                    "shape": [5490, 5490],
                    "data_type": "uint16",
                },
                "measurements/reflectance/r20m/b8a": {
                    "node_type": "array",
                    "shape": [5490, 5490],
                    "data_type": "uint16",
                },
                # Noise group with fewer bands - should NOT be selected
                "conditions/mask/detector_footprint/r10m/b02": {
                    "node_type": "array",
                    "shape": [10980, 10980],
                    "data_type": "uint8",
                },
            },
        },
    }


@pytest.fixture
def sample_v2_zmetadata():
    """Minimal Zarr v2 .zmetadata resembling EOPF production."""
    return {
        "metadata": {
            ".zattrs": {
                "other_metadata": {"horizontal_CRS_code": "EPSG:32632"},
            },
            ".zgroup": {"zarr_format": 2},
            "measurements/reflectance/r10m/b02/.zarray": {
                "shape": [10980, 10980],
                "dtype": "<u2",
                "chunks": [1024, 1024],
            },
            "measurements/reflectance/r10m/b02/.zattrs": {
                "long_name": "Blue (490 nm)",
            },
            "measurements/reflectance/r10m/b03/.zarray": {
                "shape": [10980, 10980],
                "dtype": "<u2",
            },
            "measurements/reflectance/r10m/b03/.zattrs": {
                "standard_name": "green_reflectance",
            },
            "measurements/reflectance/r20m/b05/.zarray": {
                "shape": [5490, 5490],
                "dtype": "<u2",
            },
            "measurements/reflectance/r20m/b05/.zattrs": {},
        },
    }


@pytest.fixture
def sample_v3_subgroup_crs():
    """Zarr v3 with empty root attributes; CRS/transforms in consolidated sub-group.

    Matches real EOPF Explorer v3 structure where proj:code, multiscales,
    and spatial:transform live in sub-group entries, not root attributes.
    """
    return {
        "zarr_format": 3,
        "node_type": "group",
        "attributes": {},
        "consolidated_metadata": {
            "metadata": {
                "measurements/reflectance": {
                    "node_type": "group",
                    "attributes": {
                        "proj:code": "EPSG:32626",
                        "multiscales": {
                            "layout": [
                                {
                                    "asset": "r10m",
                                    "spatial:shape": [10980, 10980],
                                    "spatial:transform": [10.0, 0.0, 499980.0, 0.0, -10.0, 8000040.0],
                                },
                                {
                                    "asset": "r20m",
                                    "derived_from": "r10m",
                                    "spatial:shape": [5490, 5490],
                                    "spatial:transform": [20.0, 0.0, 499980.0, 0.0, -20.0, 8000040.0],
                                },
                            ],
                        },
                    },
                },
                "measurements/reflectance/r10m": {
                    "node_type": "group",
                    "attributes": {
                        "proj:code": "EPSG:32626",
                        "spatial:transform": [10.0, 0.0, 499980.0, 0.0, -10.0, 8000040.0],
                    },
                },
                "measurements/reflectance/r10m/b02": {
                    "node_type": "array",
                    "shape": [10980, 10980],
                    "data_type": "float32",
                },
                "measurements/reflectance/r10m/b03": {
                    "node_type": "array",
                    "shape": [10980, 10980],
                    "data_type": "float32",
                },
                "measurements/reflectance/r10m/b04": {
                    "node_type": "array",
                    "shape": [10980, 10980],
                    "data_type": "float32",
                },
                "measurements/reflectance/r20m": {
                    "node_type": "group",
                    "attributes": {
                        "proj:code": "EPSG:32626",
                        "spatial:transform": [20.0, 0.0, 499980.0, 0.0, -20.0, 8000040.0],
                    },
                },
                "measurements/reflectance/r20m/b05": {
                    "node_type": "array",
                    "shape": [5490, 5490],
                    "data_type": "float32",
                },
            },
        },
    }


