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
    ("qgis.PyQt.QtGui", MagicMock()),
    ("qgis.PyQt.QtWidgets", MagicMock()),
]:
    sys.modules.setdefault(mod_name, mod)

import pytest  # noqa: E402

from qgis_geozarr.geozarr_provider import (  # noqa: E402
    _band_uri,
    _build_temporal_vrt,
    _extract_grid_code,
    _find_zarr_root,
    _query_stac_items,
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

    def test_default_resolution(self):
        """'default' resolution = flat store, no resolution segment in path."""
        result = _band_uri("https://example.com/data.zarr", "default", "temperature")
        assert result == 'ZARR:"/vsicurl/https://example.com/data.zarr/temperature"'

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

    def test_landsat_wrs(self):
        """Landsat WRS path/row from properties."""
        feat = {"properties": {"landsat:wrs_path": 42, "landsat:wrs_row": 34}}
        assert _extract_grid_code(feat) == "042/034"

    def test_landsat_wrs_string_values(self):
        feat = {"properties": {"landsat:wrs_path": "42", "landsat:wrs_row": "34"}}
        assert _extract_grid_code(feat) == "042/034"

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


class TestQueryStacPagination:
    """Tests for _query_stac_items with STAC pagination."""

    @staticmethod
    def _make_page(features, next_url=None):
        """Build a STAC FeatureCollection response."""
        import json as _json
        page = {"type": "FeatureCollection", "features": features}
        if next_url:
            page["links"] = [{"rel": "next", "href": next_url}]
        return _json.dumps(page).encode()

    @staticmethod
    def _make_feature(item_id, dt, zarr_url):
        return {
            "type": "Feature",
            "id": item_id,
            "properties": {"datetime": dt},
            "assets": {
                "zarr": {"href": zarr_url, "type": "application/x-zarr"}
            },
        }

    def test_single_page(self, monkeypatch):
        """Single page with enough items - no pagination needed."""
        feats = [
            self._make_feature(f"item{i}", f"2026-01-{i+1:02d}T00:00:00Z",
                               f"https://ex.com/{i}.zarr")
            for i in range(3)
        ]
        page = self._make_page(feats)
        monkeypatch.setattr(
            "qgis_geozarr.geozarr_metadata._vsi_read", lambda url: page,
        )
        items = _query_stac_items("https://api.example.com", "test-collection", limit=10)
        assert len(items) == 3

    def test_multi_page(self, monkeypatch):
        """Pagination across 2 pages."""
        page1_feats = [
            self._make_feature("a", "2026-01-01T00:00:00Z", "https://ex.com/a.zarr"),
            self._make_feature("b", "2026-01-02T00:00:00Z", "https://ex.com/b.zarr"),
        ]
        page2_feats = [
            self._make_feature("c", "2026-01-03T00:00:00Z", "https://ex.com/c.zarr"),
        ]
        calls = []

        def mock_read(url):
            calls.append(url)
            if len(calls) == 1:
                return self._make_page(page1_feats, next_url="https://api.example.com/next")
            return self._make_page(page2_feats)

        monkeypatch.setattr("qgis_geozarr.geozarr_metadata._vsi_read", mock_read)
        items = _query_stac_items("https://api.example.com", "test-collection", limit=10)
        assert len(items) == 3
        assert len(calls) == 2

    def test_limit_stops_pagination(self, monkeypatch):
        """Stop after reaching the requested limit, even if more pages exist."""
        feats = [
            self._make_feature(f"item{i}", f"2026-01-{i+1:02d}T00:00:00Z",
                               f"https://ex.com/{i}.zarr")
            for i in range(5)
        ]
        page = self._make_page(feats, next_url="https://api.example.com/next")
        monkeypatch.setattr(
            "qgis_geozarr.geozarr_metadata._vsi_read", lambda url: page,
        )
        items = _query_stac_items("https://api.example.com", "test-collection", limit=3)
        assert len(items) == 3

    def test_no_response_returns_empty(self, monkeypatch):
        monkeypatch.setattr(
            "qgis_geozarr.geozarr_metadata._vsi_read", lambda url: None,
        )
        items = _query_stac_items("https://api.example.com", "test-collection")
        assert items == []

    def test_grid_filter_with_pagination(self, monkeypatch):
        """Grid filtering works across paginated results."""
        page1_feats = [
            {**self._make_feature("a", "2026-01-01T00:00:00Z", "https://ex.com/a.zarr"),
             "properties": {"datetime": "2026-01-01T00:00:00Z", "grid:code": "TILE-A"}},
            {**self._make_feature("b", "2026-01-02T00:00:00Z", "https://ex.com/b.zarr"),
             "properties": {"datetime": "2026-01-02T00:00:00Z", "grid:code": "TILE-B"}},
        ]
        page2_feats = [
            {**self._make_feature("c", "2026-01-03T00:00:00Z", "https://ex.com/c.zarr"),
             "properties": {"datetime": "2026-01-03T00:00:00Z", "grid:code": "TILE-A"}},
        ]
        calls = []

        def mock_read(url):
            calls.append(url)
            if len(calls) == 1:
                return self._make_page(page1_feats, next_url="https://api.example.com/next")
            return self._make_page(page2_feats)

        monkeypatch.setattr("qgis_geozarr.geozarr_metadata._vsi_read", mock_read)
        items = _query_stac_items(
            "https://api.example.com", "test-collection",
            limit=10, grid_code="TILE-A",
        )
        assert len(items) == 2
        assert all(it["id"] in ("a", "c") for it in items)
