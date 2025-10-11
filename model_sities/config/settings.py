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
    PROGRESS_SITIES = os.path.join(SITIES_OUTPUT_DIR, "progress_sities.json")
    PROGRESS_REVIEWER = os.path.join(REVIEWS_OUTPUT_DIR, "progress_reviewer.json")
    STOP_FILE_PATH = os.path.join(DATA_DIR, "stop_scraping.flag") # Archivo para controlar la pausa

    USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/113.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
    ]
    VIEWPORTS = [
        {'width': 1920, 'height': 1080},
        {'width': 1366, 'height': 768},
        {'width': 1536, 'height': 864}
    ]
    
    # URLs de Foursquare
    BASE_URL = "https://es.foursquare.com"
    LOGIN_URL = f"{BASE_URL}/login"
    HUMAN_DELAY_MIN = 0.8  # segundos
    HUMAN_DELAY_MAX = 2.5  # segundos
    # Configuración del navegador
    BROWSER_TYPE = "chromium"  # firefox, chromium, webkit 
    HEADLESS = True # False = firefox con interfaz gráfica, True = sin interfaz gráfica
    
    # Tiempos de espera (en milisegundos) basados en tu script de reviews
    # Pausa corta para acciones como paginación o clics secundarios
    WAIT_SHORT_MIN = 3000
    WAIT_SHORT_MAX = 5000
    
    # Pausa media después de cargar una página principal
    WAIT_MEDIUM_MIN = 5000
    WAIT_MEDIUM_MAX = 6000
    
    # Pausa larga para usar entre lotes de tareas o en backoff
    WAIT_LONG_MIN = 15000
    WAIT_LONG_MAX = 25000
    
    BLOCK_COOLDOWN_MIN_SECONDS = 30 * 60  # 30 minutos
    BLOCK_COOLDOWN_MAX_SECONDS = 40 * 60  # 40 minutos

    # Pausa de "enfriamiento" extra larga para evitar bloqueos
    WAIT_EXTRA_LONG_MIN = 30000 # USADO EN REVIEWS.PY PARA BUSCAR LAS RESEÑAS DE LOS RESEÑANTES
    WAIT_EXTRA_LONG_MAX = 40000
    
    # Pausa aleatoria post-carga (usaremos la pausa media)
    POST_LOAD_WAIT_MIN = 5000
    POST_LOAD_WAIT_MAX = 6000
    # Configuración de procesamiento
    SAVE_INTERVAL = 5  # Guardar cada 5 URLs procesadas
    RETRIES = 3  # Número de reintentos al fallar una URL
    TIMEOUT = 60000
    PARALLEL_PROCESSES = os.cpu_count()  # Número de procesos a ejecutar en paralelo
    BACKOFF_FACTOR = 10 # Factor para el backoff progresivo en reintentos

    RATE_LIMIT_PER_HOUR = 45  # Límite prudente (oficial es 500)
    RATE_LIMIT_WINDOW_SECONDS = 3600 
    
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
        'login_button': 'input[id="loginFormButton"]',
        'no_results_card': 'li.card.noResults',
        'generic_error_card': 'li.card.genericError',
        'block_error_h1': 'div#container > h1',
        'map_search_button': 'div.leaflet-control-requery.leaflet-control.active' # <-- ¡NUEVO! Selector para "Buscar en esta área"
        
    }
    
    @classmethod
    def get_caribbean_csvs(cls):
        """Devuelve la lista de archivos CSV generados por caribbean_grid"""
        # Usamos la variable de clase CARIBBEAN_CSV_DIR
        return glob.glob(os.path.join(cls.CARIBBEAN_CSV_DIR, "*.csv"))