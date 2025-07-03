import folium
import pandas as pd
import glob
import os
import h3

def plot_hexagons_from_csv(csv_path, m=None, color='blue'):
    """
    Dibuja todos los hexágonos H3 de un archivo CSV en un mapa folium.
    """
    df = pd.read_csv(csv_path)
    if m is None:
        # Centra el mapa en el primer hexágono
        lat, lon = df.iloc[0]['latitude'], df.iloc[0]['longitude']
        m = folium.Map(location=[lat, lon], zoom_start=12)
    for _, row in df.iterrows():
        cell = row['h3_cell']
        boundary = h3.cell_to_boundary(cell)
        folium.Polygon(
            locations=boundary,
            color=color,
            fill_color=color,
            fill_opacity=0.2,
            tooltip=cell
        ).add_to(m)
    return m

if __name__ == "__main__":
    # Cambia este patrón para visualizar todos los archivos que quieras
    csv_folder = "caribbean_grid/data/zonas_departamentos"
    csv_pattern = os.path.join(csv_folder, "zonas_*_h3.csv")
    csv_files = glob.glob(csv_pattern)

    # Puedes cambiar el color para cada departamento si lo deseas
    colors = ['blue', 'green', 'red', 'purple', 'orange', 'darkred', 'lightred', 'beige', 'darkblue', 'darkgreen', 'cadetblue']
    
    m = None
    for idx, csv_file in enumerate(csv_files):
        color = colors[idx % len(colors)]
        m = plot_hexagons_from_csv(csv_file, m, color=color)
        print(f"Agregado: {os.path.basename(csv_file)}")
    
    if m:
        m.save("hexagonos_todos_departamentos.html")
        print("Mapa generado: hexagonos_todos_departamentos.html")
    else:
        print("No se encontraron archivos CSV de zonas para visualizar.")