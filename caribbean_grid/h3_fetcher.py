import pandas as pd
import glob
import os
from config.settings import ZONES_OUTPUT_DIR
# Importamos la nueva función de búsqueda por código
from utils.shapefile_helpers import load_municipal_boundaries, get_polygon_from_gdf_by_code
from utils.h3_helpers import get_h3_cells_from_polygon, h3_cell_to_center, h3_cell_to_bbox

def generate_zones_from_shapefile():
    # --- ¡ACCIÓN REQUERIDA! ---
    # 1. Confirma la ruta a tu Shapefile. Geopandas puede leer la carpeta directamente.
    # 2. Usa los nombres de las COLUMNAS DE CÓDIGOS que encontraste en tu Shapefile.
    SHAPEFILE_PATH = "caribbean_grid/data/MGN2020_URB_AREA_CENSAL"
    MUNICIPIO_CODE_COL = "COD_MPIO"      # Columna con el código del municipio
    DEPARTAMENTO_CODE_COL = "COD_DPTO"   # Columna con el código del departamento
    
    # Carga el Shapefile una sola vez al inicio
    try:
        gdf = load_municipal_boundaries(SHAPEFILE_PATH)
    except Exception as e:
        print(f"Error fatal: No se pudo cargar el Shapefile. Verifica la ruta. Error: {e}")
        return

    csv_files = glob.glob("caribbean_grid/data/municipios_*.csv")
    for csv_file in csv_files:
        departamento = os.path.basename(csv_file).replace("municipios_", "").replace(".csv", "").replace("_", " ").title()
        df = pd.read_csv(csv_file)
        rows = []
        print(f"\n--- Procesando Departamento: {departamento} ---")
        for _, row in df.iterrows():
            municipio = row["municipio"]
            # Leemos los códigos desde el CSV
            cod_dpto = row["cod_dpto"]
            cod_mpio = row["cod_mpio"]
            
            print(f"Procesando {municipio} (Código: {cod_mpio})...")
            
            # Busca el polígono usando los códigos
            polygon = get_polygon_from_gdf_by_code(
                gdf, DEPARTAMENTO_CODE_COL, MUNICIPIO_CODE_COL, cod_dpto, cod_mpio
            )
            
            if polygon:
                # Rellena el polígono exacto con celdas H3
                h3_cells = get_h3_cells_from_polygon(polygon, resolution=8)
                for cell in h3_cells:
                    lat_centro, lon_centro = h3_cell_to_center(cell)
                    lat_ne, lon_ne, lat_sw, lon_sw = h3_cell_to_bbox(cell)
                    url = f"https://es.foursquare.com/explore?mode=url&ne={lat_ne},{lon_ne}&sw={lat_sw},{lon_sw}"
                    rows.append({
                        "municipio": municipio, "departamento": departamento, "latitude": lat_centro,
                        "longitude": lon_centro, "h3_cell": cell, "url_municipio": url,
                        "origen_poligono": "shapefile", "lat_ne": lat_ne, "lon_ne": lon_ne,
                        "lat_sw": lat_sw, "lon_sw": lon_sw
                    })
                print(f"  → {len(h3_cells)} zonas H3 generadas para {municipio} desde el Shapefile.")
            else:
                print(f"  → ADVERTENCIA: No se encontró polígono para {municipio} (Código: {cod_mpio}) en el Shapefile.")

        out_path = os.path.join(
            ZONES_OUTPUT_DIR,
            f"zonas_{departamento.lower().replace(' ', '_').replace(',', '')}_h3.csv"
        )
        if rows:
            df_out = pd.DataFrame(rows)
            df_out.to_csv(out_path, index=False, encoding="utf-8")
            print(f"Archivo generado: {out_path}")

if __name__ == "__main__":
    generate_zones_from_shapefile()