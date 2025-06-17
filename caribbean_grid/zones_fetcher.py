import pandas as pd
import glob
import os
import time
from .config.settings import ZONES_OUTPUT_DIR
from .core.polygon import get_polygon, generate_zones
from .utils.helpers import generate_url

def generate_zones():
    csv_files = glob.glob("caribbean_grid/data/municipios_*.csv")
    for csv_file in csv_files:
        # Extrae el nombre del departamento del nombre del archivo
        departamento = os.path.basename(csv_file).replace("municipios_", "").replace(".csv", "").replace("_", " ").title()
        df = pd.read_csv(csv_file)
        rows = []
        for _, row in df.iterrows():
            municipio = row["municipio"]
            dep = row["departamento"]
            print(f"Procesando {municipio}, {dep}...")
            polygon = get_polygon(municipio, dep)
            if polygon:
                zonas = generate_zones(polygon, n_zonas=4)
                for lat, lon in zonas:
                    # Generamos la URL usando el formato de bounding box
                    url = generate_url(lat, lon, delta=0.04)
                    rows.append({
                        "municipio": municipio,
                        "departamento": dep,
                        "latitude": lat,
                        "longitude": lon,
                        "url_municipio": url,
                        # Incluimos también las coordenadas de las esquinas
                        "lat_ne": lat + 0.02,
                        "lon_ne": lon + 0.02,
                        "lat_sw": lat - 0.02,
                        "lon_sw": lon - 0.02
                    })
                print(f"  → {len(zonas)} zonas generadas para {municipio}")
            else:
                print(f"  → No se encontró polígono para {municipio}")
            time.sleep(1)  # Respetar el rate limit de Nominatim

        # Guarda un solo CSV por departamento
        out_path = os.path.join(
            ZONES_OUTPUT_DIR,
            f"zonas_{departamento.lower().replace(' ', '_').replace(',', '')}.csv"
        )
        df_out = pd.DataFrame(rows)
        df_out.to_csv(out_path, index=False, encoding="utf-8")
        print(f"Archivo generado: {out_path}")

if __name__ == "__main__":
    generate_zones()