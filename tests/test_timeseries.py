"""Tests for time series logic."""

from datetime import datetime

from qgis_geozarr.stac_search import TimeSeriesItem, parse_datetime


# -- parse_datetime ----------------------------------------------------------

class TestParseDatetime:
    def test_iso_with_z(self):
        dt = parse_datetime("2025-06-15T10:30:00Z")
        assert dt == datetime(2025, 6, 15, 10, 30, 0)

    def test_iso_with_fractional_z(self):
        dt = parse_datetime("2025-06-15T10:30:00.123456Z")
        assert dt == datetime(2025, 6, 15, 10, 30, 0, 123456)

    def test_iso_no_z(self):
        dt = parse_datetime("2025-06-15T10:30:00")
        assert dt == datetime(2025, 6, 15, 10, 30, 0)

    def test_date_only(self):
        dt = parse_datetime("2025-06-15")
        assert dt == datetime(2025, 6, 15)

    def test_invalid(self):
        assert parse_datetime("not-a-date") is None

    def test_empty(self):
        assert parse_datetime("") is None


# -- TimeSeriesItem sorting ---------------------------------------------------

class TestTimeSeriesItemSorting:
    def test_sort_by_datetime(self):
        items = [
            TimeSeriesItem("2025-06-01T00:00:00Z", "url3", "c"),
            TimeSeriesItem("2025-01-01T00:00:00Z", "url1", "a"),
            TimeSeriesItem("2025-03-01T00:00:00Z", "url2", "b"),
        ]
        items.sort(key=lambda x: x.datetime_str)
        assert [i.item_id for i in items] == ["a", "b", "c"]

    def test_cloud_cover_optional(self):
        item = TimeSeriesItem("2025-01-01T00:00:00Z", "url", "a")
        assert item.cloud_cover is None
        item2 = TimeSeriesItem("2025-01-01T00:00:00Z", "url", "b", 15.3)
        assert item2.cloud_cover == 15.3
