import geopandas as gpd

def load_boundaries(shapefile_path):
    """
    Carga un Shapefile desde la ruta especificada y lo prepara para el análisis.
    """
    print(f"Cargando Shapefile desde: {shapefile_path}")
    gdf = gpd.read_file(shapefile_path)
    
    # Corregir cualquier geometría inválida que pueda existir en el archivo
    gdf = gdf[gdf.geometry.is_valid]
    
    print("Shapefile cargado y validado correctamente.")
    return gdf

def find_polygon(gdf, cod_dpto, cod_mpio):
    """
    Busca el polígono de una cabecera municipal de forma precisa.
    1. Busca por código de departamento y municipio.
    2. Si hay múltiples resultados, prioriza la cabecera oficial (COD_CLAS='1').
    3. Si no, elige el polígono de mayor área.
    """
    if not cod_dpto or not cod_mpio:
        return None

    # --- INICIO DEL BLOQUE A AGREGAR ---
    # Estandarizar las columnas del GeoDataFrame para asegurar la comparación correcta
    gdf["COD_DPTO"] = gdf["COD_DPTO"].astype(str).str.strip().str.zfill(2)
    gdf["COD_MPIO"] = gdf["COD_MPIO"].astype(str).str.strip().str.zfill(5)

    # Estandarizar también los códigos de búsqueda
    cod_dpto = cod_dpto.zfill(2)
    cod_mpio = cod_mpio.zfill(5)
    # --- FIN DEL BLOQUE A AGREGAR ---

    matches = gdf[(gdf["COD_DPTO"] == cod_dpto) & (gdf["COD_MPIO"] == cod_mpio)]

    if matches.empty:
        return None

    # Si hay múltiples polígonos, buscar la cabecera oficial
    if 'COD_CLAS' in gdf.columns:
        cabecera = matches[matches['COD_CLAS'].astype(str) == '1']
        if not cabecera.empty:
            return cabecera.iloc[0].geometry

    # Si no hay cabecera oficial, devolver el de mayor área
    if 'SHAPE_AREA' in gdf.columns:
        matches = matches.sort_values('SHAPE_AREA', ascending=False)
    
    return matches.iloc[0].geometry