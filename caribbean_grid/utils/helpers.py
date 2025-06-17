import pandas as pd
from pathlib import Path
from urllib.parse import quote

def save_municipalities_to_csv(municipalities, department, output_dir):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    rows = []
    for m in municipalities:
        municipio = m['nom_mpio'].strip().title()
        departamento = m['dpto'].strip().title()
        rows.append({
            "municipio": municipio,
            "departamento": departamento,
        })
    filename = f"{output_dir}/municipios_{department.lower().replace(' ', '_').replace(',', '')}.csv"
    df = pd.DataFrame(rows)
    df.to_csv(filename, index=False, encoding='utf-8')
    print(f"Archivo generado: {filename}")
    
def generate_url(lat_centro, lon_centro, delta=0.04):
    """
    Genera URL de Foursquare para un área rectangular (bounding box)
    alrededor de un punto central
    """
    lat_ne = lat_centro + delta/2
    lon_ne = lon_centro + delta/2
    lat_sw = lat_centro - delta/2
    lon_sw = lon_centro - delta/2
    return f"https://es.foursquare.com/explore?mode=url&ne={lat_ne},{lon_ne}&sw={lat_sw},{lon_sw}"