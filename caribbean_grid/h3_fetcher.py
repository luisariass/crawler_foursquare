import pandas as pd
import glob
import os
from config.settings import ZONES_OUTPUT_DIR
from utils.h3_helpers import get_h3_cells, h3_cell_to_center
from utils.helpers import generate_url

def generate_zone_h3():
    csv_files = glob.glob("caribbean_grid/data/municipios_*.csv")
    for csv_file in csv_files:
        departamento = os.path.basename(csv_file).replace("municipios_", "").replace(".csv", "").replace("_", " ").title()
        df = pd.read_csv(csv_file)
        rows = []
        for _, row in df.iterrows():
            municipio = row["municipio"]
            dep = row["departamento"]
            lat = float(row["latitud"])
            lon = float(row["longitud"])
            print(f"Procesando {municipio}, {dep}...")
            # Ajusta resolution y k según el tamaño del municipio si lo deseas
            h3_cells = get_h3_cells(lat, lon, resolution=8, k=1)
            for cell in h3_cells:
                lat_centro, lon_centro = h3_cell_to_center(cell)
                url = generate_url(lat_centro, lon_centro, delta=0.04)
                rows.append({
                    "municipio": municipio,
                    "departamento": dep,
                    "latitude": lat_centro,
                    "longitude": lon_centro,
                    "h3_cell": cell,
                    "url_municipio": url,
                    "origen_poligono": "h3",
                    "lat_ne": lat_centro + 0.02,
                    "lon_ne": lon_centro + 0.02,
                    "lat_sw": lat_centro - 0.02,
                    "lon_sw": lon_centro - 0.02
                })
            print(f"  → {len(h3_cells)} zonas H3 generadas para {municipio}")
        out_path = os.path.join(
            ZONES_OUTPUT_DIR,
            f"zonas_{departamento.lower().replace(' ', '_').replace(',', '')}_h3.csv"
        )
        df_out = pd.DataFrame(rows)
        df_out.to_csv(out_path, index=False, encoding="utf-8")
        print(f"Archivo generado: {out_path}")

if __name__ == "__main__":
    generate_zone_h3()