# Publishing GeoZarr to plugins.qgis.org

## Prerequisites

- OSGeo account: https://www.osgeo.org/community/getting-started-osgeo/osgeo_userid/
- QGIS 3.44+ with GDAL 3.13+ for local testing
- Plugin installed locally and verified working

## Pre-flight checklist

```bash
# 1. Tests pass
make test

# 2. Lint clean
make lint

# 3. Version bumped in metadata.txt
grep '^version=' qgis_geozarr/metadata.txt

# 4. Changelog updated in metadata.txt (under changelog= field)

# 5. Build clean zip
make clean && make zip
```

Verify the zip contains only source files (no .pyc, __pycache__, .git):
```bash
unzip -l qgis_geozarr.zip
```

## Upload to plugins.qgis.org

1. Log in at https://plugins.qgis.org/ with your OSGeo ID
2. Go to https://plugins.qgis.org/plugins/add/
3. Upload `qgis_geozarr.zip`
4. The system validates metadata.txt automatically
5. First submission goes through manual review (allow 1-2 weeks)
6. Subsequent version updates are auto-published

## GitHub release

After the plugin is accepted:

```bash
VERSION=$(grep '^version=' qgis_geozarr/metadata.txt | cut -d= -f2)

# Tag the release
git tag -a "v${VERSION}" -m "v${VERSION}"
git push origin "v${VERSION}"

# Create GitHub release (attaches the zip)
gh release create "v${VERSION}" qgis_geozarr.zip \
  --title "v${VERSION}" \
  --notes "See changelog in metadata.txt"
```

## Post-publish verification

1. Open QGIS > Plugins > Manage and Install Plugins
2. Search "GeoZarr" - should appear in the list
3. Install, restart QGIS if prompted
4. Verify: STAC browser right-click shows "Load GeoZarr..."
5. Verify: toolbar icon opens URL loader dialog

## Updating an existing version

1. Bump `version=` in `qgis_geozarr/metadata.txt`
2. Add changelog entry under `changelog=`
3. Run pre-flight checklist above
4. Upload new zip at https://plugins.qgis.org/plugins/qgis_geozarr/version/add/
5. Tag and release on GitHub

## metadata.txt reference

Required fields (already set):

| Field | Purpose |
|-------|---------|
| `name` | Display name in plugin manager |
| `version` | Dotted version (triggers update notifications) |
| `qgisMinimumVersion` | Minimum QGIS version (3.44) |
| `description` | One-line summary (shown in search results) |
| `about` | Full description (shown on plugin page) |
| `author` | Author name |
| `email` | Contact (only visible to logged-in users) |
| `repository` | Source code URL |
| `tracker` | Issue tracker URL |
| `icon` | Path to icon file within the zip |
| `tags` | Comma-separated for discoverability |
| `category` | One of: Raster, Vector, Database, Web, Sketching |

Optional fields:
- `experimental=True` - marks as experimental (yellow warning in plugin manager)
- `deprecated=True` - marks as deprecated
- `supportsQt6=yes` - confirms Qt6 compatibility (QGIS 3.40+)
- `hasProcessingProvider=no` - no Processing toolbox algorithms
