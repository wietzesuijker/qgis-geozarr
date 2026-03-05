"""Tests for STAC time series search."""

from unittest.mock import patch

from qgis_geozarr.stac_search import (
    extract_grid_code,
    query_stac_items,
)


# -- extract_grid_code -------------------------------------------------------

class TestExtractGridCode:
    def test_grid_code_property(self):
        feat = {"properties": {"grid:code": "27XVB"}}
        assert extract_grid_code(feat) == "27XVB"

    def test_s2_mgrs_tile(self):
        feat = {"properties": {"s2:mgrs_tile": "32TQM"}}
        assert extract_grid_code(feat) == "32TQM"

    def test_mgrs_grid_square(self):
        feat = {"properties": {"mgrs:grid_square": "10SEG"}}
        assert extract_grid_code(feat) == "10SEG"

    def test_mgrs_from_item_id(self):
        feat = {"id": "S2B_27XVB_20250101_0_L2A", "properties": {}}
        assert extract_grid_code(feat) == "27XVB"

    def test_wrs2_from_item_id(self):
        # WRS-2 needs slash format in ID to match
        feat = {"id": "LC08_044/034_20250101", "properties": {}}
        assert extract_grid_code(feat) == "044/034"

    def test_no_grid_code(self):
        feat = {"id": "random_item", "properties": {}}
        assert extract_grid_code(feat) == ""

    def test_priority_property_over_id(self):
        """Properties take precedence over ID regex."""
        feat = {
            "id": "S2B_27XVB_20250101",
            "properties": {"grid:code": "OVERRIDE"},
        }
        assert extract_grid_code(feat) == "OVERRIDE"

    def test_empty_feature(self):
        assert extract_grid_code({}) == ""


# -- query_stac_items --------------------------------------------------------

def _make_stac_response(features, next_url=None):
    """Build a minimal STAC FeatureCollection."""
    links = []
    if next_url:
        links.append({"rel": "next", "href": next_url})
    return {"features": features, "links": links}


def _make_feature(item_id, dt, zarr_href, cloud_cover=None, grid_code=None):
    """Build a minimal STAC feature with a Zarr asset."""
    props = {"datetime": dt}
    if cloud_cover is not None:
        props["eo:cloud_cover"] = cloud_cover
    if grid_code:
        props["grid:code"] = grid_code
    return {
        "id": item_id,
        "properties": props,
        "assets": {
            "zarr": {
                "href": zarr_href,
                "type": "application/vnd+zarr",
            },
        },
    }


class TestQueryStacItems:
    @patch("qgis_geozarr.stac_search._http_get_json")
    def test_basic_query(self, mock_http):
        features = [
            _make_feature("item2", "2025-06-01T00:00:00Z", "https://s3/tile2.zarr"),
            _make_feature("item1", "2025-01-01T00:00:00Z", "https://s3/tile1.zarr"),
        ]
        mock_http.return_value = _make_stac_response(features)

        items = query_stac_items("https://api.example.com", "sentinel-2-l2a")

        assert len(items) == 2
        # Should be sorted by datetime
        assert items[0].datetime_str == "2025-01-01T00:00:00Z"
        assert items[1].datetime_str == "2025-06-01T00:00:00Z"
        assert items[0].zarr_url == "https://s3/tile1.zarr"

    @patch("qgis_geozarr.stac_search._http_get_json")
    def test_grid_code_filter(self, mock_http):
        features = [
            _make_feature("a", "2025-01-01T00:00:00Z", "https://s3/a.zarr",
                          grid_code="27XVB"),
            _make_feature("b", "2025-02-01T00:00:00Z", "https://s3/b.zarr",
                          grid_code="32TQM"),
        ]
        mock_http.return_value = _make_stac_response(features)

        items = query_stac_items(
            "https://api.example.com", "sentinel-2-l2a",
            grid_code="27XVB",
        )
        assert len(items) == 1
        assert items[0].item_id == "a"

    @patch("qgis_geozarr.stac_search._http_get_json")
    def test_pagination(self, mock_http):
        page1 = _make_stac_response(
            [_make_feature("a", "2025-01-01T00:00:00Z", "https://s3/a.zarr")],
            next_url="https://api.example.com/page2",
        )
        page2 = _make_stac_response(
            [_make_feature("b", "2025-02-01T00:00:00Z", "https://s3/b.zarr")],
        )
        mock_http.side_effect = [page1, page2]

        items = query_stac_items("https://api.example.com", "sentinel-2-l2a")
        assert len(items) == 2
        assert mock_http.call_count == 2

    @patch("qgis_geozarr.stac_search._http_get_json")
    def test_limit_stops_pagination(self, mock_http):
        features = [
            _make_feature(f"item{i}", f"2025-{i+1:02d}-01T00:00:00Z",
                          f"https://s3/{i}.zarr")
            for i in range(5)
        ]
        mock_http.return_value = _make_stac_response(
            features, next_url="https://api.example.com/page2",
        )

        items = query_stac_items(
            "https://api.example.com", "sentinel-2-l2a", limit=3,
        )
        assert len(items) == 3
        assert mock_http.call_count == 1  # No second page fetched

    @patch("qgis_geozarr.stac_search._http_get_json")
    def test_cloud_cover_preserved(self, mock_http):
        features = [
            _make_feature("a", "2025-01-01T00:00:00Z", "https://s3/a.zarr",
                          cloud_cover=12.5),
        ]
        mock_http.return_value = _make_stac_response(features)

        items = query_stac_items("https://api.example.com", "sentinel-2-l2a")
        assert items[0].cloud_cover == 12.5

    @patch("qgis_geozarr.stac_search._http_get_json")
    def test_deduplicates_item_ids(self, mock_http):
        feat = _make_feature("dup", "2025-01-01T00:00:00Z", "https://s3/a.zarr")
        mock_http.return_value = _make_stac_response([feat, feat])

        items = query_stac_items("https://api.example.com", "sentinel-2-l2a")
        assert len(items) == 1

    @patch("qgis_geozarr.stac_search._http_get_json")
    def test_skips_items_without_datetime(self, mock_http):
        features = [
            {"id": "no_dt", "properties": {}, "assets": {
                "zarr": {"href": "https://s3/a.zarr", "type": "application/vnd+zarr"},
            }},
        ]
        mock_http.return_value = _make_stac_response(features)

        items = query_stac_items("https://api.example.com", "sentinel-2-l2a")
        assert len(items) == 0

    @patch("qgis_geozarr.stac_search._http_get_json")
    def test_skips_items_without_zarr_asset(self, mock_http):
        features = [
            {"id": "no_zarr", "properties": {"datetime": "2025-01-01T00:00:00Z"},
             "assets": {"thumbnail": {"href": "https://s3/thumb.png"}}},
        ]
        mock_http.return_value = _make_stac_response(features)

        items = query_stac_items("https://api.example.com", "sentinel-2-l2a")
        assert len(items) == 0

    @patch("qgis_geozarr.stac_search._http_get_json")
    def test_http_failure_returns_empty(self, mock_http):
        mock_http.return_value = None

        items = query_stac_items("https://api.example.com", "sentinel-2-l2a")
        assert items == []

    @patch("qgis_geozarr.stac_search._http_get_json")
    def test_bbox_and_datetime_in_url(self, mock_http):
        mock_http.return_value = _make_stac_response([])

        query_stac_items(
            "https://api.example.com", "s2",
            bbox=(-10.0, 40.0, 10.0, 50.0),
            datetime_range="2025-01-01T00:00:00Z/2025-12-31T00:00:00Z",
        )

        call_url = mock_http.call_args[0][0]
        assert "bbox=-10.0%2C40.0%2C10.0%2C50.0" in call_url
        assert "datetime=2025-01-01" in call_url
