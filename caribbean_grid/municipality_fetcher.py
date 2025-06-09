import requests
import pandas as pd
import time
from pathlib import Path

CARIBBEAN_DEPARTMENTS = [
    "Atlántico", "Bolívar"
]

def fetch_municipalities_overpass(department_name, sleep_time=1.5):
    """
    Consulta la Overpass API para obtener todos los municipios de un departamento.
    Devuelve una lista de diccionarios con nombre, lat, lon y departamento.
    """
    print(f"Consultando municipios de {department_name}...")
    overpass_url = "https://overpass-api.de/api/interpreter"
    query = f"""
    [out:json][timeout:60];
    area["name"="{department_name}"]["admin_level"="4"]["boundary"="administrative"];
    rel(area)["admin_level"="6"]["boundary"="administrative"];
    out center;
    """
    response = requests.post(overpass_url, data={'data': query})
    if response.status_code != 200:
        print(f"Error en Overpass API: {response.status_code}")
        return []
    data = response.json()
    results = []
    for element in data.get('elements', []):
        if element.get('type') == 'relation' and 'tags' in element and 'center' in element:
            muni_name = element['tags'].get('name', 'Unknown')
            lat = element['center']['lat']
            lon = element['center']['lon']
            results.append({
                'municipio': muni_name,
                'departamento': department_name,
                'latitude': lat,
                'longitude': lon,
                #'url_municipio': f"https://es.foursquare.com/explore?ll={lat},{lon}&radius=1000",
                'url_municipio_detalle': f"https://es.foursquare.com/explore?mode=url&ne={lat}%2C{lon}&sw={lat}%2C{lon}"
            })
    time.sleep(sleep_time)  # Para no saturar la API
    return results

def save_municipalities_to_csv(municipalities, department_name, output_dir="municipios_csv"):
    """
    Guarda la lista de municipios en un archivo CSV.
    """
    Path(output_dir).mkdir(exist_ok=True)
    filename = f"{output_dir}/municipios_{department_name.lower().replace(' ', '_')}.csv"
    df = pd.DataFrame(municipalities)
    df.to_csv(filename, index=False, encoding='utf-8')
    print(f"Archivo generado: {filename}")

def generate_all_caribbean_departments():
    """
    Genera un CSV por cada departamento de la región Caribe.
    """
    for dept in CARIBBEAN_DEPARTMENTS:
        municipios = fetch_municipalities_overpass(dept)
        if municipios:
            save_municipalities_to_csv(municipios, dept)
        else:
            print(f"No se encontraron municipios para {dept}")

if __name__ == "__main__":
    generate_all_caribbean_departments()