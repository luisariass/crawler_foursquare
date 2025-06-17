import requests
import numpy as np
from shapely.geometry import shape, Point
from config.settings import NOMINATIM_API

# Enfoque recomendado: funciones a nivel de módulo
def get_polygon(nombre_municipio, departamento):
    """Obtiene el polígono de un municipio usando Nominatim (OpenStreetMap).
    
    Args:
        nombre_municipio (str): Nombre del municipio a consultar.
        departamento (str): Departamento donde se ubica el municipio.
        
    Returns:
        shapely.geometry.Polygon or None: Polígono del municipio o None si no se encuentra.
    """
    params = {
        "q": f"{nombre_municipio}, {departamento}, Colombia",
        "format": "json",
        "polygon_geojson": 1
    }
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        resp = requests.get(NOMINATIM_API, params=params, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        
        if data and "geojson" in data[0]:
            return shape(data[0]["geojson"])
        return None
    except (requests.RequestException, IndexError, KeyError) as e:
        # Considera usar un logger aquí
        print(f"Error obteniendo el polígono: {e}")
        return None

def generate_zones(polygon, n_zonas=4):
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