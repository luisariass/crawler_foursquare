import os

# Ruta base del proyecto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Departamentos del Caribe
CARIBBEAN_DEPARTMENTS = [
    "BOLÍVAR", "ATLÁNTICO", "LA GUAJIRA", "SUCRE",
    "ARCHIPIÉLAGO DE SAN ANDRÉS, PROVIDENCIA Y SANTA CATALINA",
    "CÓRDOBA", "CESAR", "MAGDALENA"
]

# URLs de APIs
DATOSGOV_API = "https://www.datos.gov.co/resource/gdxc-w37w.json"

# Directorios de datos
DATA_DIR = os.path.join(BASE_DIR, "data")
ZONES_OUTPUT_DIR = os.path.join(DATA_DIR, "zonas_departamentos")