"""Tests for geozarr_provider pure functions.

Mocks qgis.* modules since they're unavailable outside QGIS.
"""

import sys
import types
import xml.etree.ElementTree as ET
from unittest.mock import MagicMock

# Stub qgis modules before importing the provider.
# Only needed because geozarr_provider.py has top-level qgis imports.
_mock_qt_core = types.ModuleType("qgis.PyQt.QtCore")
_mock_qt_core.Qt = MagicMock()
_mock_qt_core.QThread = type("QThread", (), {
    "__init__": lambda self, *a, **kw: None,
})
_mock_qt_core.pyqtSignal = lambda *a, **kw: MagicMock()

for mod_name, mod in [
    ("qgis", types.ModuleType("qgis")),
    ("qgis.core", MagicMock()),
    ("qgis.gui", MagicMock()),
    ("qgis.PyQt", types.ModuleType("qgis.PyQt")),
    ("qgis.PyQt.QtCore", _mock_qt_core),
    ("qgis.PyQt.QtWidgets", MagicMock()),
]:
    sys.modules.setdefault(mod_name, mod)

import pytest  # noqa: E402

from qgis_geozarr.geozarr_provider import (  # noqa: E402
    _band_uri,
    _build_temporal_vrt,
    _extract_grid_code,
    _find_zarr_root,
    _vsi_prefix,
)


class TestVsiPrefix:
    def test_https(self):
        assert _vsi_prefix("https://example.com/data.zarr") == "/vsicurl/"

    def test_s3(self):
        assert _vsi_prefix("s3://bucket/data.zarr") == "/vsis3/"

    def test_http(self):
        assert _vsi_prefix("http://example.com/data.zarr") == "/vsicurl/"


class TestBandUri:
    def test_basic(self):
        result = _band_uri("https://example.com/data.zarr", "r10m", "b02")
        assert result == 'ZARR:"/vsicurl/https://example.com/data.zarr/r10m/b02"'

    def test_with_sub_group(self):
        result = _band_uri(
            "https://example.com/data.zarr", "r10m", "b02",
            sub_group="measurements/reflectance",
        )
        assert result == (
            'ZARR:"/vsicurl/https://example.com/data.zarr/'
            'measurements/reflectance/r10m/b02"'
        )

    def test_no_resolution(self):
        result = _band_uri("https://example.com/data.zarr", "", "b02")
        assert result == 'ZARR:"/vsicurl/https://example.com/data.zarr/b02"'

    def test_s3_prefix(self):
        result = _band_uri("s3://bucket/data.zarr", "r10m", "b02")
        assert result == 'ZARR:"/vsis3/bucket/data.zarr/r10m/b02"'

    def test_empty_sub_group(self):
        result = _band_uri("https://example.com/data.zarr", "r10m", "b02", sub_group="")
        assert result == 'ZARR:"/vsicurl/https://example.com/data.zarr/r10m/b02"'


class TestFindZarrRoot:
    def test_zarr_extension(self):
        url = "https://example.com/bucket/data.zarr/measurements/r10m/b02"
        assert _find_zarr_root(url) == "https://example.com/bucket/data.zarr"

    def test_no_zarr_extension(self):
        url = "https://example.com/bucket/data"
        assert _find_zarr_root(url) == url

    def test_query_string_stripped(self):
        url = "https://example.com/data.zarr?token=abc"
        assert _find_zarr_root(url) == "https://example.com/data.zarr"


class TestExtractGridCode:
    def test_grid_code_property(self):
        feat = {"properties": {"grid:code": "MGRS-25WFU"}}
        assert _extract_grid_code(feat) == "MGRS-25WFU"

    def test_s2_mgrs_tile(self):
        feat = {"properties": {"s2:mgrs_tile": "25WFU"}}
        assert _extract_grid_code(feat) == "25WFU"

    def test_regex_fallback(self):
        feat = {
            "id": "S2C_MSIL2A_20260213T132251_N0512_R124_T27WVR_20260213T152517",
            "properties": {},
        }
        assert _extract_grid_code(feat) == "27WVR"

    def test_priority_order(self):
        """grid:code takes precedence over s2:mgrs_tile."""
        feat = {
            "properties": {"grid:code": "MGRS-25WFU", "s2:mgrs_tile": "25WFU"},
            "id": "S2C_T27WVR_test",
        }
        assert _extract_grid_code(feat) == "MGRS-25WFU"

    def test_no_grid_info(self):
        feat = {"id": "some-other-satellite-item", "properties": {}}
        assert _extract_grid_code(feat) == ""

    def test_empty_feature(self):
        assert _extract_grid_code({}) == ""


class TestBuildTemporalVrt:
    def test_basic_structure(self, sample_zarr_root_info):
        items = [
            {"datetime": "2026-01-01T10:00:00Z", "zarr_url": "https://ex.com/a.zarr", "id": "item1"},
            {"datetime": "2026-01-15T10:00:00Z", "zarr_url": "https://ex.com/b.zarr", "id": "item2"},
        ]
        path = _build_temporal_vrt(items, "b02", "r10m", sample_zarr_root_info)
        assert path.endswith(".vrt")

        tree = ET.parse(path)
        root = tree.getroot()

        assert root.get("rasterXSize") == "10980"
        assert root.get("rasterYSize") == "10980"

        # SRS present
        srs = root.find("SRS")
        assert srs is not None

        # GeoTransform present
        gt = root.find("GeoTransform")
        assert gt is not None

        # 2 bands for 2 timesteps
        bands = root.findall("VRTRasterBand")
        assert len(bands) == 2

    def test_band_descriptions(self, sample_zarr_root_info):
        items = [
            {"datetime": "2026-01-01T10:00:00Z", "zarr_url": "https://ex.com/a.zarr", "id": "item1"},
        ]
        path = _build_temporal_vrt(items, "b02", "r10m", sample_zarr_root_info)
        tree = ET.parse(path)
        root = tree.getroot()

        band = root.find("VRTRasterBand")
        desc = band.find("Description")
        assert desc is not None
        assert "2026-01-01" in desc.text
        assert "item1" in desc.text

    def test_sub_group_in_uri(self, sample_zarr_root_info):
        items = [
            {"datetime": "2026-01-01T10:00:00Z", "zarr_url": "https://ex.com/a.zarr", "id": "item1"},
        ]
        path = _build_temporal_vrt(items, "b02", "r10m", sample_zarr_root_info)
        tree = ET.parse(path)
        root = tree.getroot()

        band = root.find("VRTRasterBand")
        src = band.find("SimpleSource/SourceFilename")
        assert "measurements/reflectance" in src.text

    def test_empty_shape_returns_empty(self, sample_zarr_root_info):
        sample_zarr_root_info.shape_per_resolution = {}
        result = _build_temporal_vrt([], "b02", "r10m", sample_zarr_root_info)
        assert result == ""

    def test_dtype_in_vrt(self, sample_zarr_root_info):
        items = [
            {"datetime": "2026-01-01T10:00:00Z", "zarr_url": "https://ex.com/a.zarr", "id": "item1"},
        ]
        path = _build_temporal_vrt(items, "b02", "r10m", sample_zarr_root_info)
        tree = ET.parse(path)
        root = tree.getroot()

        band = root.find("VRTRasterBand")
        assert band.get("dataType") == "UInt16"
