import pandas as pd
import glob
import os
import unicodedata
import geopandas as gpd
from config.settings import ZONES_OUTPUT_DIR
from utils.shapefile_helpers import load_municipal_boundaries, find_cabecera_polygon
from utils.h3_helpers import get_h3_cells_from_polygon, h3_cell_to_center, h3_cell_to_bbox

def normalize_text(text):
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
    return text.strip()

def generate_zones_from_shapefile():
    SHAPEFILE_PATH = "caribbean_grid/data/MGN2020_URB_AREA_CENSAL"
    
    try:
        gdf = load_municipal_boundaries(SHAPEFILE_PATH)
        gdf = gdf.to_crs("EPSG:4326")
    except Exception as e:
        print(f"Error fatal: No se pudo cargar o procesar el Shapefile. {e}")
        return

    csv_files = glob.glob("caribbean_grid/data/municipios_*.csv")
    for csv_file in csv_files:
        departamento = os.path.basename(csv_file).replace("municipios_", "").replace(".csv", "").replace("_", " ").title()
        #df = pd.read_csv(csv_file)
        df = pd.read_csv(csv_file, dtype={'cod_dpto': str, 'cod_mpio': str})
        rows = []
        print(f"\n--- Procesando Departamento: {departamento} ---")
        
        for _, row in df.iterrows():
            municipio = str(row["municipio"]).strip()
            cod_dpto = str(row.get("cod_dpto", "")).strip()
            cod_mpio = str(row.get("cod_mpio", "")).strip()
            
            print(f"Procesando {municipio} (Código: {cod_mpio})...")
            
            polygon = find_cabecera_polygon(gdf, cod_dpto, cod_mpio)
            
            if polygon and not polygon.is_empty:
                resolution = 10
                
                if polygon.area < 0.001:
                    resolution = 11
                
                h3_cells = get_h3_cells_from_polygon(polygon, resolution=resolution)
                
                if len(h3_cells) > 250 and resolution > 8:
                    resolution -= 1
                    h3_cells = get_h3_cells_from_polygon(polygon, resolution=resolution)
                
                if len(h3_cells) < 3 and resolution < 11:
                    resolution += 1
                    h3_cells = get_h3_cells_from_polygon(polygon, resolution=resolution)
                
                for cell in h3_cells:
                    lat_centro, lon_centro = h3_cell_to_center(cell)
                    lat_ne, lon_ne, lat_sw, lon_sw = h3_cell_to_bbox(cell)
                    url = f"https://es.foursquare.com/explore?mode=url&ne={lat_ne},{lon_ne}&sw={lat_sw},{lon_sw}"
                    rows.append({
                        "municipio": municipio, "departamento": departamento, "latitude": lat_centro,
                        "longitude": lon_centro, "h3_cell": cell, "url_municipio": url,
                        "origen_poligono": "shapefile_cabecera", "lat_ne": lat_ne, "lon_ne": lon_ne,
                        "lat_sw": lat_sw, "lon_sw": lon_sw, "resolution": resolution
                    })
                print(f"  → {len(h3_cells)} zonas H3 generadas para la CABECERA de {municipio} (resolución {resolution}).")
            else:
                print(f"  → ADVERTENCIA: No se encontró cabecera para {municipio} (Código: {cod_mpio}) en el Shapefile.")

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