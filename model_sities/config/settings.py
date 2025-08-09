"""
Configuración para el scraper de Foursquare
"""
import os
import glob

# Ruta base del proyecto (carpeta model_sities)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Ruta a la carpeta de datos
DATA_DIR = os.path.join(BASE_DIR, "data")


# Ruta a los CSVs generados por caribbean_grid
# El subdirectorio 'zonas_departamentos' es configurable para facilitar la integración con caribbean_grid.

class Settings:
    """Configuración centralizada para el scraper"""
    # Subdirectorio configurable para los CSVs generados por caribbean_grid
    CARIBBEAN_SUBDIR = "zonas_departamentos"
    # Ruta a los CSVs generados por caribbean_grid
    CARIBBEAN_CSV_DIR = os.path.join(os.path.dirname(BASE_DIR), "caribbean_grid", "data", CARIBBEAN_SUBDIR)
    
    # Archivos y directorios con rutas absolutas
    COOKIES_JSON = os.path.join(DATA_DIR, "cookies_foursquare.json")
    SITIES_OUTPUT_DIR = os.path.join(DATA_DIR, "sities")
    REVIEWS_OUTPUT_DIR = os.path.join(DATA_DIR, "reviewers_sities")
    PROGRESO_PATH = os.path.join(REVIEWS_OUTPUT_DIR, "progress_reviews.json")
    FAILED_MUNICIPALITIES_PATH = os.path.join(DATA_DIR, "failed_municipalities.txt")

    # URLs de Foursquare
    BASE_URL = "https://es.foursquare.com"
    LOGIN_URL = f"{BASE_URL}/login"
    
    # Configuración del navegador
    BROWSER_TYPE = "chromium"  # firefox, chromium, webkit 
    HEADLESS = True # False = firefox con interfaz gráfica, True = sin interfaz gráfica
    
    # Tiempos de espera (en milisegundos)
    WAIT_SHORT_MIN = 1000
    WAIT_SHORT_MAX = 3000
    WAIT_MEDIUM_MIN = 5000
    WAIT_MEDIUM_MAX = 6000
    WAIT_LONG_MIN = 15000
    WAIT_LONG_MAX = 25000
    WAIT_EXTRA_LONG_MIN = 30000
    WAIT_EXTRA_LONG_MAX = 40000
    
    # Configuración de procesamiento
    SAVE_INTERVAL = 5  # Guardar cada 5 URLs procesadas
    RETRIES = 3  # Número de reintentos al fallar una URL
    TIMEOUT = 60000
    
    # Selectores CSS
    SELECTORS = {
        'content_holder': '.contentHolder',
        'venue_score': '.venueScore.positive',
        'venue_name': 'h2',
        'venue_category': '.venueDataItem',
        'venue_address': '.venueAddress',
        'more_results_button': 'button:has-text("Ver más resultados")',
        'login_username': 'input[id="username"]',
        'login_password': 'input[id="password"]',
        'login_button': 'input[id="loginFormButton"]'
    }
    
    @classmethod
    def get_caribbean_csvs(cls):
        """Devuelve la lista de archivos CSV generados por caribbean_grid"""
        return glob.glob(os.path.join(Settings.CARIBBEAN_CSV_DIR, "*.csv"))