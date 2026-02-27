"""RGB composite band presets and metadata for common satellite platforms."""

from typing import Dict, List, Optional, Tuple

# {satellite: {preset_name: (r, g, b)}}
PRESETS: Dict[str, Dict[str, tuple]] = {
    "sentinel-2": {
        "true_color": ("B04", "B03", "B02"),
        "false_color": ("B08", "B04", "B03"),
        "swir": ("B12", "B8A", "B04"),
        "classification": ("SCL",),
        "agriculture": ("B11", "B08", "B02"),
        "geology": ("B12", "B11", "B02"),
        "urban": ("B12", "B11", "B04"),
        "moisture": ("B8A", "B11", "B12"),
    },
    "landsat-8": {
        "true_color": ("B4", "B3", "B2"),
        "false_color": ("B5", "B4", "B3"),
        "swir": ("B7", "B5", "B4"),
        "quality": ("QA_PIXEL",),
        "agriculture": ("B6", "B5", "B2"),
        "geology": ("B7", "B6", "B2"),
    },
    "landsat-9": {
        "true_color": ("B4", "B3", "B2"),
        "false_color": ("B5", "B4", "B3"),
        "swir": ("B7", "B5", "B4"),
        "quality": ("QA_PIXEL",),
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

# {satellite: {band_id: (name, central_wavelength_nm)}}
BAND_INFO: Dict[str, Dict[str, Tuple[str, int]]] = {
    "sentinel-2": {
        "B01": ("Coastal aerosol", 443),
        "B02": ("Blue", 490),
        "B03": ("Green", 560),
        "B04": ("Red", 665),
        "B05": ("Red edge 1", 705),
        "B06": ("Red edge 2", 740),
        "B07": ("Red edge 3", 783),
        "B08": ("NIR", 842),
        "B8A": ("NIR narrow", 865),
        "B09": ("Water vapour", 945),
        "B10": ("Cirrus", 1375),
        "B11": ("SWIR 1", 1610),
        "B12": ("SWIR 2", 2190),
    },
    "landsat-8": {
        "B1": ("Coastal", 443),
        "B2": ("Blue", 482),
        "B3": ("Green", 561),
        "B4": ("Red", 655),
        "B5": ("NIR", 865),
        "B6": ("SWIR 1", 1609),
        "B7": ("SWIR 2", 2201),
        "B8": ("Pan", 590),
        "B9": ("Cirrus", 1373),
        "B10": ("TIR 1", 10895),
        "B11": ("TIR 2", 12005),
    },
    "landsat-9": {
        "B1": ("Coastal", 443),
        "B2": ("Blue", 482),
        "B3": ("Green", 561),
        "B4": ("Red", 655),
        "B5": ("NIR", 865),
        "B6": ("SWIR 1", 1609),
        "B7": ("SWIR 2", 2201),
        "B8": ("Pan", 590),
        "B9": ("Cirrus", 1373),
    },
    "sentinel-3": {
        "Oa01": ("Aerosol", 400),
        "Oa02": ("Blue", 412),
        "Oa03": ("Blue-green", 443),
        "Oa04": ("Blue-green", 490),
        "Oa05": ("Green", 510),
        "Oa06": ("Green", 560),
        "Oa07": ("Red", 620),
        "Oa08": ("Red", 665),
        "Oa09": ("Red", 674),
        "Oa10": ("Red edge", 681),
        "Oa11": ("Red edge", 709),
        "Oa12": ("NIR", 754),
        "Oa17": ("NIR", 865),
        "Oa21": ("SWIR", 1020),
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


def get_band_label(
    satellite: Optional[str], band_id: str, description: str = "",
) -> str:
    """Human-readable band label. Falls back to description, then raw band_id."""
    if satellite:
        info = BAND_INFO.get(satellite.lower(), {}).get(band_id.upper())
        if info:
            return f"{band_id.upper()} - {info[0]} ({info[1]} nm)"
    if description:
        return f"{band_id} - {description}"
    return band_id


def get_band_tooltip(satellite: Optional[str], band_id: str) -> str:
    """Detailed tooltip for a band checkbox."""
    if satellite:
        info = BAND_INFO.get(satellite.lower(), {}).get(band_id.upper())
        if info:
            return f"{info[0]}\nCentral wavelength: {info[1]} nm"
    return band_id


def get_preset_tooltip(satellite: Optional[str], preset_name: str) -> str:
    """Tooltip showing band composition of a preset."""
    if not satellite:
        return ""
    presets = get_presets(satellite)
    if not presets:
        return ""
    bands = presets.get(preset_name)
    if not bands:
        return ""
    if len(bands) == 1:
        info = BAND_INFO.get(satellite.lower(), {}).get(bands[0].upper())
        name = info[0] if info else bands[0]
        return f"{bands[0]} ({name})"
    parts = []
    for role, band_id in zip(("R", "G", "B"), bands):
        info = BAND_INFO.get(satellite.lower(), {}).get(band_id.upper())
        name = info[0] if info else band_id
        parts.append(f"{role}: {band_id} ({name})")
    return "\n".join(parts)
