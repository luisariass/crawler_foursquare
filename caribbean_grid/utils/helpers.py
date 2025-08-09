import pandas as pd
from pathlib import Path

def save_municipalities_to_csv(municipalities, department, output_dir):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    rows = []
    for m in municipalities:
        municipio = m['nom_mpio'].strip().title()
        departamento = m['dpto'].strip().title()
        lat = m.get('latitud', '').replace(',', '.')
        lon = m.get('longitud', '').replace(',', '.')
        # Extraemos los códigos del diccionario de la API
        cod_dpto = m.get('cod_dpto')
        cod_mpio = m.get('cod_mpio')
        
        rows.append({
            "municipio": municipio,
            "departamento": departamento,
            "latitud": lat,
            "longitud": lon,
            # Añadimos los códigos al diccionario que se guardará
            "cod_dpto": cod_dpto,
            "cod_mpio": cod_mpio,
        })
    filename = f"{output_dir}/municipios_{department.lower().replace(' ', '_').replace(',', '')}.csv"
    df = pd.DataFrame(rows)
    df.to_csv(filename, index=False, encoding='utf-8')
    print(f"Archivo generado: {filename}")