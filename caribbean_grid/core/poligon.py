import requests
from shapely.geometry import shape, Point
import numpy as np

def get_municipio_polygon(nombre_municipio, departamento):
    """Obtiene el polígono de un municipio usando Nominatim (OpenStreetMap)"""
    url = f"https://nominatim.openstreetmap.org/search"
    params = {
        "q": f"{nombre_municipio}, {departamento}, Colombia",
        "format": "json",
        "polygon_geojson": 1
    }
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, params=params, headers=headers)
    data = resp.json()
    if data and "geojson" in data[0]:
        return shape(data[0]["geojson"])
    return None

def generar_zonas_en_poligono(polygon, n_zonas=4):
    """Genera una cuadrícula de puntos dentro de un polígono dado"""
    minx, miny, maxx, maxy = polygon.bounds
    lats = np.linspace(miny, maxy, n_zonas+1)
    lons = np.linspace(minx, maxx, n_zonas+1)
    zonas = []
    for i in range(n_zonas):
        for j in range(n_zonas):
            lat_centro = (lats[i] + lats[i+1]) / 2
            lon_centro = (lons[j] + lons[j+1]) / 2
            punto = Point(lon_centro, lat_centro)
            if polygon.contains(punto):
                zonas.append((lat_centro, lon_centro))
    return zonas