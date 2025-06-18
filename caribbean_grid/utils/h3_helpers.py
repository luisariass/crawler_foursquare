import h3

def get_h3_cells(lat, lon, resolution=8, k=1):
    """
    Devuelve una lista de celdas H3 (hexágonos) alrededor de una coordenada central.
    resolution: granularidad del hexágono (8 ≈ 0.7 km²)
    k: distancia de anillo (1 = hexágono central + vecinos)
    """
    central_cell = h3.latlng_to_cell(lat, lon, resolution)
    return list(h3.grid_disk(central_cell, k))

def h3_cell_to_center(cell):
    """
    Devuelve el centro (lat, lon) de una celda H3.
    """
    return h3.cell_to_latlng(cell)