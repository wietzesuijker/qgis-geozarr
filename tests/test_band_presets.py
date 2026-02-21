"""Tests for band_presets satellite detection and label generation."""

from qgis_geozarr.band_presets import (
    default_preset,
    detect_satellite,
    get_band_label,
    get_band_tooltip,
    get_preset_tooltip,
    get_presets,
)


class TestDetectSatellite:
    def test_sentinel2(self):
        assert detect_satellite("sentinel-2-l2a") == "sentinel-2"

    def test_sentinel2_short(self):
        assert detect_satellite("S2_L2A") == "sentinel-2"

    def test_landsat8(self):
        assert detect_satellite("landsat-8-c2-l2") == "landsat-8"

    def test_landsat9(self):
        assert detect_satellite("landsat-9-c2-l2") == "landsat-9"

    def test_modis(self):
        assert detect_satellite("MODIS_MOD09") == "modis"

    def test_sentinel3(self):
        assert detect_satellite("sentinel-3-olci") == "sentinel-3"

    def test_unknown(self):
        assert detect_satellite("some-random-collection") is None

    def test_case_insensitive(self):
        assert detect_satellite("SENTINEL-2-L2A") == "sentinel-2"


class TestGetPresets:
    def test_sentinel2_has_true_color(self):
        presets = get_presets("sentinel-2")
        assert "true_color" in presets
        assert presets["true_color"] == ("B04", "B03", "B02")

    def test_unknown_returns_none(self):
        assert get_presets("unknown") is None


class TestDefaultPreset:
    def test_sentinel2(self):
        assert default_preset("sentinel-2") == ("B04", "B03", "B02")

    def test_unknown(self):
        assert default_preset("unknown") is None


class TestGetBandLabel:
    def test_known_satellite_band(self):
        label = get_band_label("sentinel-2", "B02")
        assert "Blue" in label
        assert "490" in label

    def test_unknown_satellite_with_description(self):
        label = get_band_label(None, "b02", "Blue band")
        assert "Blue band" in label
        assert "b02" in label

    def test_no_info_falls_back_to_id(self):
        assert get_band_label(None, "b02") == "b02"

    def test_case_insensitive_lookup(self):
        label = get_band_label("sentinel-2", "b02")
        assert "Blue" in label


class TestGetBandTooltip:
    def test_known_band(self):
        tooltip = get_band_tooltip("sentinel-2", "B02")
        assert "Blue" in tooltip
        assert "490" in tooltip

    def test_unknown_returns_id(self):
        assert get_band_tooltip(None, "b02") == "b02"


class TestGetPresetTooltip:
    def test_true_color(self):
        tooltip = get_preset_tooltip("sentinel-2", "true_color")
        assert "R:" in tooltip
        assert "G:" in tooltip
        assert "B:" in tooltip

    def test_no_satellite(self):
        assert get_preset_tooltip(None, "true_color") == ""
