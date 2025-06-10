import pandas as pd
from pathlib import Path
from urllib.parse import quote

def save_municipalities_to_csv(municipalities, department, output_dir):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    rows = []
    for m in municipalities:
        municipio = m['nom_mpio'].strip().title()
        departamento = m['dpto'].strip().title()
        lat = m.get('latitud', '').replace(',', '.')
        lon = m.get('longitud', '').replace(',', '.')
        url_municipio = f"https://es.foursquare.com/explore?near={quote(municipio + ', ' + departamento + ', Colombia')}"
        rows.append({
            "municipio": municipio,
            "departamento": departamento,
            "latitude": lat,
            "longitude": lon,
            "url_municipio": url_municipio
        })
    filename = f"{output_dir}/municipios_{department.lower().replace(' ', '_').replace(',', '')}.csv"
    df = pd.DataFrame(rows)
    df.to_csv(filename, index=False, encoding='utf-8')
    print(f"Archivo generado: {filename}")