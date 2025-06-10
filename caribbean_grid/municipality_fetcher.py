import requests
import pandas as pd
from urllib.parse import quote
from pathlib import Path

CARIBBEAN_DEPARTMENTS = [
    "BOLÍVAR", "ATLÁNTICO", "LA GUAJIRA", "SUCRE",
    "ARCHIPIÉLAGO DE SAN ANDRÉS, PROVIDENCIA Y SANTA CATALINA",
    "CÓRDOBA", "CESAR", "MAGDALENA"
]

API_URL = "https://www.datos.gov.co/resource/gdxc-w37w.json"

def fetch_official_municipalities(department):
    params = {
        "$query": f"SELECT cod_dpto, dpto, cod_mpio, nom_mpio, tipo_municipio, longitud, latitud WHERE dpto = '{department}'"
    }
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    return response.json()

def save_municipalities_to_csv(municipalities, department, output_dir="municipios_csv"):
    Path(output_dir).mkdir(exist_ok=True)
    rows = []
    for m in municipalities:
        municipio = m['nom_mpio'].strip().title()
        departamento = m['dpto'].strip().title()
        lat = m['latitud'].replace(',', '.') if 'latitud' in m else ''
        lon = m['longitud'].replace(',', '.') if 'longitud' in m else ''
        url_municipio = f"https://es.foursquare.com/explore?near={quote(municipio + ', ' + departamento + ', Colombia')}"
        rows.append({
            "municipio": municipio,
            "departamento": departamento,
            "latitude": lat,
            "longitude": lon,
            "url_municipio": url_municipio
        })
    df = pd.DataFrame(rows)
    filename = f"{output_dir}/municipios_{department.lower().replace(' ', '_').replace(',', '')}.csv"
    df.to_csv(filename, index=False, encoding='utf-8')
    print(f"Archivo generado: {filename}")

def generate_all_caribbean_departments():
    for dept in CARIBBEAN_DEPARTMENTS:
        municipios = fetch_official_municipalities(dept)
        if municipios:
            save_municipalities_to_csv(municipios, dept)
        else:
            print(f"No se encontraron municipios oficiales para {dept}")

if __name__ == "__main__":
    generate_all_caribbean_departments()