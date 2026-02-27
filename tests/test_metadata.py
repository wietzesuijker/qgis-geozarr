"""Tests for geozarr_metadata parsing (v2 + v3)."""

import pytest

from qgis_geozarr.geozarr_metadata import (
    _ZARR_TO_GDAL_DTYPE,
    _V2_DTYPE_MAP,
    _parse,
    _parse_consolidated,
    _parse_crs,
    _parse_transform,
    _parse_v2,
)


class TestParseV3:
    def test_consolidated_sentinel2(self, sample_v3_zarr_json):
        info = _parse(sample_v3_zarr_json)
        assert info.resolutions == ("r10m", "r20m")
        assert info.bands_per_resolution["r10m"] == ("b02", "b03", "b04", "b08")
        assert info.bands_per_resolution["r20m"] == ("b05", "b06", "b8a")
        assert info.sub_group == "measurements/reflectance"

    def test_shape_extraction(self, sample_v3_zarr_json):
        info = _parse(sample_v3_zarr_json)
        assert info.shape_per_resolution["r10m"] == (10980, 10980)
        assert info.shape_per_resolution["r20m"] == (5490, 5490)

    def test_dtype_extraction(self, sample_v3_zarr_json):
        info = _parse(sample_v3_zarr_json)
        assert info.dtype_per_resolution["r10m"] == "UInt16"
        assert info.dtype_per_resolution["r20m"] == "UInt16"

    def test_crs_from_proj_code(self, sample_v3_zarr_json):
        info = _parse(sample_v3_zarr_json)
        assert info.epsg == 32627

    def test_transform(self, sample_v3_zarr_json):
        info = _parse(sample_v3_zarr_json)
        # spatial:transform [a,b,c,d,e,f] -> GDAL [c,a,b,f,d,e]
        # Input: [10.0, 0.0, 399960.0, 0.0, -10.0, 4500000.0]
        assert info.geotransform == (399960.0, 10.0, 0.0, 4500000.0, 0.0, -10.0)

    def test_multi_prefix_picks_most_bands(self, sample_v3_zarr_json):
        """measurements/reflectance has 7 bands, conditions/mask has 1."""
        info = _parse(sample_v3_zarr_json)
        assert info.sub_group == "measurements/reflectance"
        # Should NOT include detector_footprint bands
        assert "b02" in info.bands_per_resolution["r10m"]
        assert len(info.bands_per_resolution["r10m"]) == 4

    def test_members_fallback(self):
        """When no consolidated_metadata, fall back to members."""
        root = {
            "zarr_format": 3,
            "node_type": "group",
            "members": {
                "r10m": {
                    "node_type": "group",
                    "members": {
                        "b02": {"node_type": "array"},
                        "b03": {"node_type": "array"},
                        "spatial_ref": {"node_type": "array"},
                    },
                },
            },
        }
        info = _parse(root)
        assert "r10m" in info.bands_per_resolution
        assert "b02" in info.bands_per_resolution["r10m"]
        assert "spatial_ref" not in info.bands_per_resolution["r10m"]

    def test_empty_root(self):
        info = _parse({})
        assert info.resolutions == ()
        assert info.epsg is None

    def test_attributes_not_dict(self):
        """attributes key is not a dict - should not crash."""
        info = _parse({"attributes": "invalid"})
        assert info.resolutions == ()

    def test_band_descriptions(self):
        root = {
            "consolidated_metadata": {
                "metadata": {
                    "r10m/b02": {
                        "node_type": "array",
                        "shape": [100, 100],
                        "attributes": {"long_name": "Blue"},
                    },
                    "r10m/b03": {
                        "node_type": "array",
                        "shape": [100, 100],
                        "attributes": {"standard_name": "green_reflectance"},
                    },
                },
            },
        }
        info = _parse(root)
        assert info.band_descriptions["b02"] == "Blue"
        assert info.band_descriptions["b03"] == "green_reflectance"


class TestParseV2:
    def test_zmetadata(self, sample_v2_zmetadata):
        info = _parse_v2(sample_v2_zmetadata)
        assert info.resolutions == ("r10m", "r20m")
        assert "b02" in info.bands_per_resolution["r10m"]
        assert "b03" in info.bands_per_resolution["r10m"]
        assert "b05" in info.bands_per_resolution["r20m"]

    def test_crs_from_other_metadata(self, sample_v2_zmetadata):
        info = _parse_v2(sample_v2_zmetadata)
        assert info.epsg == 32632

    def test_dtype_from_numpy_string(self, sample_v2_zmetadata):
        info = _parse_v2(sample_v2_zmetadata)
        assert info.dtype_per_resolution["r10m"] == "UInt16"

    def test_band_descriptions_from_zattrs(self, sample_v2_zmetadata):
        info = _parse_v2(sample_v2_zmetadata)
        assert info.band_descriptions["b02"] == "Blue (490 nm)"
        assert info.band_descriptions["b03"] == "green_reflectance"

    def test_sub_group(self, sample_v2_zmetadata):
        info = _parse_v2(sample_v2_zmetadata)
        assert info.sub_group == "measurements/reflectance"

    def test_shape_extraction(self, sample_v2_zmetadata):
        info = _parse_v2(sample_v2_zmetadata)
        assert info.shape_per_resolution["r10m"] == (10980, 10980)
        assert info.shape_per_resolution["r20m"] == (5490, 5490)


class TestParseCrs:
    def test_proj_code(self):
        assert _parse_crs({"proj:code": "EPSG:32627"}) == 32627

    def test_proj_projjson(self):
        src = {"proj:projjson": {"id": {"authority": "EPSG", "code": 4326}}}
        assert _parse_crs(src) == 4326

    def test_other_metadata(self):
        src = {"other_metadata": {"horizontal_CRS_code": "EPSG:32632"}}
        assert _parse_crs(src) == 32632

    def test_invalid_epsg_string(self):
        assert _parse_crs({"proj:code": "not-epsg"}) is None

    def test_missing(self):
        assert _parse_crs({}) is None


class TestParseTransform:
    def test_spatial_transform(self):
        # spatial:transform [a,b,c,d,e,f] -> GDAL [c,a,b,f,d,e]
        result = _parse_transform(
            {"spatial:transform": [10.0, 0.0, 399960.0, 0.0, -10.0, 4500000.0]}
        )
        assert result == (399960.0, 10.0, 0.0, 4500000.0, 0.0, -10.0)

    def test_wrong_length(self):
        assert _parse_transform({"spatial:transform": [1, 2, 3]}) is None

    def test_missing(self):
        assert _parse_transform({}) is None

    def test_non_numeric(self):
        assert _parse_transform({"spatial:transform": ["a", "b", "c", "d", "e", "f"]}) is None


class TestDtypeMapping:
    def test_all_zarr_to_gdal(self):
        expected = {
            "bool": "Byte", "uint8": "Byte", "uint16": "UInt16",
            "int16": "Int16", "uint32": "UInt32", "int32": "Int32",
            "uint64": "UInt64", "int64": "Int64",
            "float32": "Float32", "float64": "Float64",
        }
        for zarr_type, gdal_type in expected.items():
            assert _ZARR_TO_GDAL_DTYPE[zarr_type] == gdal_type

    def test_v2_dtype_map(self):
        assert _V2_DTYPE_MAP["|u1"] == "uint8"
        assert _V2_DTYPE_MAP["<u2"] == "uint16"
        assert _V2_DTYPE_MAP["<f4"] == "float32"


class TestParseConsolidated:
    def test_non_band_filtering(self):
        consol = {
            "r10m/b02": {"node_type": "array", "shape": [100, 100]},
            "r10m/spatial_ref": {"node_type": "array", "shape": [1]},
            "r10m/x": {"node_type": "array", "shape": [100]},
            "r10m/y": {"node_type": "array", "shape": [100]},
        }
        shape_per_res = {}
        bands_per_res, sub_group, _ = _parse_consolidated(consol, shape_per_res)
        assert "b02" in bands_per_res.get("r10m", [])
        assert "spatial_ref" not in bands_per_res.get("r10m", [])
        assert "x" not in bands_per_res.get("r10m", [])

    def test_single_segment_path_flat_fallback(self):
        """Paths with < 2 segments: not a resolution group, but picked up by flat fallback."""
        consol = {"b02": {"node_type": "array", "shape": [100, 100]}}
        shape_per_res = {}
        bands, _, _ = _parse_consolidated(consol, shape_per_res)
        assert bands == {"default": ["b02"]}

    def test_non_resolution_segment_flat_fallback(self):
        """Paths where second-to-last is not r\\d+m: flat fallback discovers them."""
        consol = {"data/b02": {"node_type": "array", "shape": [100, 100]}}
        shape_per_res = {}
        bands, _, _ = _parse_consolidated(consol, shape_per_res)
        assert bands == {"default": ["b02"]}

    def test_group_nodes_skipped(self):
        consol = {"r10m/b02": {"node_type": "group"}}
        shape_per_res = {}
        bands, _, _ = _parse_consolidated(consol, shape_per_res)
        assert bands == {}


class TestSubgroupCrs:
    """CRS/transform/multiscales from consolidated sub-group entries.

    Real EOPF Explorer v3 data has empty root attributes; metadata lives
    in consolidated entries like measurements/reflectance and
    measurements/reflectance/r10m.
    """

    def test_crs_from_consolidated_subgroup(self, sample_v3_subgroup_crs):
        info = _parse(sample_v3_subgroup_crs)
        assert info.epsg == 32626

    def test_transform_from_consolidated_subgroup(self, sample_v3_subgroup_crs):
        info = _parse(sample_v3_subgroup_crs)
        # spatial:transform [10,0,499980,0,-10,8000040] -> GDAL [499980,10,0,8000040,0,-10]
        assert info.geotransform == (499980.0, 10.0, 0.0, 8000040.0, 0.0, -10.0)

    def test_per_resolution_transforms(self, sample_v3_subgroup_crs):
        info = _parse(sample_v3_subgroup_crs)
        assert "r10m" in info.transform_per_resolution
        assert "r20m" in info.transform_per_resolution
        assert info.transform_per_resolution["r20m"] == (499980.0, 20.0, 0.0, 8000040.0, 0.0, -20.0)

    def test_multiscales_shapes(self, sample_v3_subgroup_crs):
        info = _parse(sample_v3_subgroup_crs)
        assert info.shape_per_resolution["r10m"] == (10980, 10980)
        assert info.shape_per_resolution["r20m"] == (5490, 5490)

    def test_bands_still_discovered(self, sample_v3_subgroup_crs):
        info = _parse(sample_v3_subgroup_crs)
        assert info.sub_group == "measurements/reflectance"
        assert "b02" in info.bands_per_resolution["r10m"]
        assert "b05" in info.bands_per_resolution["r20m"]

    def test_root_attrs_take_precedence(self, sample_v3_zarr_json):
        """When root attributes have CRS, sub-group CRS is not needed."""
        info = _parse(sample_v3_zarr_json)
        assert info.epsg == 32627  # from root, not sub-group


class TestFlatZarrFallback:
    """Flat Zarr stores without resolution groups (no r10m/r20m pattern)."""

    def test_consolidated_flat_arrays(self):
        """Arrays directly at root level, no resolution groups."""
        root = {
            "consolidated_metadata": {
                "metadata": {
                    "temperature": {
                        "node_type": "array",
                        "shape": [720, 1440],
                        "data_type": "float32",
                    },
                    "precipitation": {
                        "node_type": "array",
                        "shape": [720, 1440],
                        "data_type": "float32",
                    },
                    "spatial_ref": {
                        "node_type": "array",
                        "shape": [1],
                    },
                },
            },
        }
        info = _parse(root)
        assert "default" in info.bands_per_resolution
        assert "temperature" in info.bands_per_resolution["default"]
        assert "precipitation" in info.bands_per_resolution["default"]
        assert "spatial_ref" not in info.bands_per_resolution["default"]
        assert info.shape_per_resolution["default"] == (720, 1440)
        assert info.dtype_per_resolution.get("default") == "Float32"

    def test_members_flat_arrays(self):
        """v3 members with arrays directly under root (no resolution groups)."""
        root = {
            "zarr_format": 3,
            "node_type": "group",
            "members": {
                "wind_speed": {"node_type": "array"},
                "wind_dir": {"node_type": "array"},
                "spatial_ref": {"node_type": "array"},
            },
        }
        info = _parse(root)
        assert "default" in info.bands_per_resolution
        assert "wind_speed" in info.bands_per_resolution["default"]
        assert "wind_dir" in info.bands_per_resolution["default"]
        assert "spatial_ref" not in info.bands_per_resolution["default"]

    def test_resolution_groups_take_precedence(self, sample_v3_zarr_json):
        """When resolution groups exist, flat fallback is not used."""
        info = _parse(sample_v3_zarr_json)
        assert "default" not in info.bands_per_resolution
        assert "r10m" in info.bands_per_resolution

    def test_flat_consolidated_with_prefix(self):
        """Flat arrays under a prefix but no resolution segment."""
        root = {
            "consolidated_metadata": {
                "metadata": {
                    "data/temperature": {
                        "node_type": "array",
                        "shape": [360, 720],
                        "data_type": "float64",
                    },
                    "data/humidity": {
                        "node_type": "array",
                        "shape": [360, 720],
                        "data_type": "float64",
                    },
                },
            },
        }
        info = _parse(root)
        assert "default" in info.bands_per_resolution
        assert "temperature" in info.bands_per_resolution["default"]
        assert "humidity" in info.bands_per_resolution["default"]

    def test_1d_arrays_excluded(self):
        """1D arrays (coordinate axes) should not be treated as bands."""
        root = {
            "consolidated_metadata": {
                "metadata": {
                    "temperature": {
                        "node_type": "array",
                        "shape": [720, 1440],
                        "data_type": "float32",
                    },
                    "lat": {
                        "node_type": "array",
                        "shape": [720],
                        "data_type": "float64",
                    },
                },
            },
        }
        info = _parse(root)
        assert "temperature" in info.bands_per_resolution["default"]
        # 1D array should not be included (shape check: need >= 2D)
        assert "lat" not in info.bands_per_resolution.get("default", [])
