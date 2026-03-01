"""Tests for geozarr_provider pure functions.

Mocks qgis.* modules since they're unavailable outside QGIS.
"""

import sys
import types
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
    _error_dialog,
    _find_zarr_root,
    _stac_cache,
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



class TestStacCacheLRU:
    """Tests for OrderedDict-based STAC cache."""

    def test_cache_is_ordered_dict(self):
        from collections import OrderedDict
        assert isinstance(_stac_cache, OrderedDict)

    def test_cache_hit_moves_to_end(self, monkeypatch):
        """Cache hit promotes entry to most-recently-used position."""
        from qgis_geozarr.geozarr_provider import (
            _fetch_stac_item_json, _stac_cache, _stac_cache_lock,
        )
        # Pre-populate cache
        with _stac_cache_lock:
            _stac_cache.clear()
            _stac_cache["url-a"] = {"a": 1}
            _stac_cache["url-b"] = {"b": 2}
        # Hit url-a - should move to end
        result = _fetch_stac_item_json("url-a")
        assert result == {"a": 1}
        with _stac_cache_lock:
            keys = list(_stac_cache.keys())
            assert keys[-1] == "url-a"
            _stac_cache.clear()


class TestErrorDialog:
    """Tests for _error_dialog helper."""

    def test_callable(self):
        assert callable(_error_dialog)


class TestPrewarmLogging:
    """Test that pre-warm logs warnings on failure."""

    def test_prewarm_logs_failure(self, monkeypatch):
        import logging
        from qgis_geozarr.geozarr_provider import _ProviderFetchThread
        from qgis_geozarr.geozarr_metadata import ZarrRootInfo

        # Create a minimal info with one band
        info = ZarrRootInfo(
            resolutions=["r10m"],
            bands_per_resolution={"r10m": ["b02"]},
            band_descriptions={},
            shape_per_resolution={"r10m": (100, 100)},
            dtype_per_resolution={"r10m": "UInt16"},
            epsg=32632,
            geotransform=(600000, 10, 0, 5000000, 0, -10),
            conventions=[],
            sub_group="",
            scale_per_band={},
            valid_range_per_band={},
            transform_per_resolution={},
        )
        # Mock gdal.Open to raise
        monkeypatch.setattr("qgis_geozarr.geozarr_provider.gdal.Open", lambda uri: (_ for _ in ()).throw(RuntimeError("test")))

        with monkeypatch.context() as m:
            warnings = []
            m.setattr("qgis_geozarr.geozarr_provider.log.warning", lambda *a, **kw: warnings.append(a))
            _ProviderFetchThread._prewarm_sources(info, "https://ex.com/data.zarr")
            assert len(warnings) >= 1
            assert "Pre-warm failed" in warnings[0][0]
