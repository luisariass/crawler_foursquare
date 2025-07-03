import h3
from h3 import LatLngPoly, LatLngMultiPoly

def get_h3_cells_from_polygon(polygon, resolution):
    """
    Convierte un polígono de Shapely a LatLngPoly o LatLngMultiPoly y usa polygon_to_cells.
    Compatible con H3 v4.x.
    """
    geo = polygon.__geo_interface__
    all_cells = set()

    if geo["type"] == "Polygon":
        rings = geo["coordinates"]
        outer = [(lat, lon) for lon, lat in rings[0]]
        holes = [[(lat, lon) for lon, lat in ring] for ring in rings[1:]] if len(rings) > 1 else []
        poly = LatLngPoly(outer, holes)  # holes como segundo argumento posicional
        cells = h3.polygon_to_cells(poly, resolution)
        all_cells.update(cells)
    elif geo["type"] == "MultiPolygon":
        polys = []
        for rings in geo["coordinates"]:
            outer = [(lat, lon) for lon, lat in rings[0]]
            holes = [[(lat, lon) for lon, lat in ring] for ring in rings[1:]] if len(rings) > 1 else []
            polys.append(LatLngPoly(outer, holes))
        mpoly = LatLngMultiPoly(polys)
        cells = h3.polygon_to_cells(mpoly, resolution)
        all_cells.update(cells)
    return list(all_cells)

def h3_cell_to_center(cell):
    return h3.cell_to_latlng(cell)

def h3_cell_to_bbox(cell):
    vertices = h3.cell_to_boundary(cell)
    lats = [v[0] for v in vertices]
    lons = [v[1] for v in vertices]
    return max(lats), max(lons), min(lats), min(lons)