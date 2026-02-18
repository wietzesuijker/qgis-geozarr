def classFactory(iface):
    from .plugin import GeoZarrPlugin
    return GeoZarrPlugin(iface)
