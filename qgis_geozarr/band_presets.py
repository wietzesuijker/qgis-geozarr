"""RGB composite band presets for common satellite platforms."""

from typing import Dict, List, Optional

# {satellite: {preset_name: (r, g, b)}}
PRESETS: Dict[str, Dict[str, tuple]] = {
    "sentinel-2": {
        "true_color": ("B04", "B03", "B02"),
        "false_color": ("B08", "B04", "B03"),
        "swir": ("B12", "B8A", "B04"),
        "agriculture": ("B11", "B08", "B02"),
        "geology": ("B12", "B11", "B02"),
        "urban": ("B12", "B11", "B04"),
        "moisture": ("B8A", "B11", "B12"),
        "bathymetric": ("B04", "B03", "B01"),
    },
    "landsat-8": {
        "true_color": ("B4", "B3", "B2"),
        "false_color": ("B5", "B4", "B3"),
        "swir": ("B7", "B5", "B4"),
        "agriculture": ("B6", "B5", "B2"),
        "geology": ("B7", "B6", "B2"),
        "urban": ("B7", "B6", "B4"),
    },
    "landsat-9": {
        "true_color": ("B4", "B3", "B2"),
        "false_color": ("B5", "B4", "B3"),
        "swir": ("B7", "B5", "B4"),
    },
    "modis": {
        "true_color": ("B1", "B4", "B3"),
        "false_color": ("B7", "B2", "B1"),
    },
    "sentinel-3": {
        "true_color": ("Oa08", "Oa06", "Oa04"),
        "false_color": ("Oa17", "Oa08", "Oa06"),
    },
}

# Collection ID substring -> satellite key
_COLLECTION_MAP = {
    "sentinel-2": "sentinel-2", "s2": "sentinel-2",
    "landsat-8": "landsat-8", "lc08": "landsat-8",
    "landsat-9": "landsat-9", "lc09": "landsat-9",
    "modis": "modis", "mod09": "modis",
    "sentinel-3": "sentinel-3", "s3_olci": "sentinel-3",
}


def detect_satellite(collection_id: str) -> Optional[str]:
    """Detect satellite from STAC collection ID."""
    cid = collection_id.lower()
    for pattern, satellite in _COLLECTION_MAP.items():
        if pattern in cid:
            return satellite
    return None


def get_presets(satellite: str) -> Optional[Dict[str, tuple]]:
    """Get band presets for a satellite. Returns None if unknown."""
    return PRESETS.get(satellite.lower())


def default_preset(satellite: str) -> Optional[tuple]:
    """Get the default (true_color) preset for a satellite."""
    presets = get_presets(satellite)
    if presets:
        return presets.get("true_color")
    return None


def preset_names(satellite: str) -> List[str]:
    """List available preset names."""
    presets = get_presets(satellite)
    return list(presets.keys()) if presets else []
