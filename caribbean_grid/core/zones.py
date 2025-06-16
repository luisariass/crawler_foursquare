import pandas as pd
import glob
import os
import time
from urllib.parse import quote
from ..core.poligon import get_municipio_polygon, generar_zonas_en_poligono

OUTPUT_DIR = "caribbean_grid/data/zonas_departamentos"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def generar_url_foursquare_bbox(lat_centro, lon_centro, delta=0.04):
    """
    Genera URL de Foursquare para un área rectangular (bounding box)
    alrededor de un punto central
    """
    lat_ne = lat_centro + delta/2
    lon_ne = lon_centro + delta/2
    lat_sw = lat_centro - delta/2
    lon_sw = lon_centro - delta/2
    return f"https://es.foursquare.com/explore?mode=url&ne={lat_ne},{lon_ne}&sw={lat_sw},{lon_sw}"

def main():
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
            polygon = get_municipio_polygon(municipio, dep)
            if polygon:
                zonas = generar_zonas_en_poligono(polygon, n_zonas=4)
                for lat, lon in zonas:
                    # Generamos la URL usando el formato de bounding box
                    url = generar_url_foursquare_bbox(lat, lon, delta=0.04)
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
            OUTPUT_DIR,
            f"zonas_{departamento.lower().replace(' ', '_').replace(',', '')}.csv"
        )
        df_out = pd.DataFrame(rows)
        df_out.to_csv(out_path, index=False, encoding="utf-8")
        print(f"Archivo generado: {out_path}")

if __name__ == "__main__":
    main()