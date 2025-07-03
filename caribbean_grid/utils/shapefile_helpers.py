import geopandas as gpd

def load_municipal_boundaries(shapefile_path):
    """
    Carga un Shapefile desde la ruta especificada.
    """
    print(f"Cargando Shapefile desde: {shapefile_path}")
    gdf = gpd.read_file(shapefile_path)
    print("Shapefile cargado correctamente.")
    return gdf

def get_polygon_from_gdf_by_code(gdf, dept_code_col, mpio_code_col, dept_code, mpio_code):
    """
    Busca y devuelve el polígono (geometría) de un municipio en un GeoDataFrame
    usando los códigos de departamento y municipio.
    """
    # Asegurarse de que los códigos en el GeoDataFrame sean del mismo tipo que los de búsqueda (strings)
    gdf[dept_code_col] = gdf[dept_code_col].astype(str)
    gdf[mpio_code_col] = gdf[mpio_code_col].astype(str)
    
    # Busca la fila que coincida en código de departamento y municipio
    match = gdf[(gdf[dept_code_col] == str(dept_code)) & (gdf[mpio_code_col] == str(mpio_code))]
    
    if not match.empty:
        # Devuelve el primer objeto de geometría encontrado
        return match.iloc[0].geometry
    
    return None