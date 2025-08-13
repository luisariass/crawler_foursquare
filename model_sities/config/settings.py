import os

"""
Configuración para el scraper de Foursquare
"""

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

class Settings:
    """Configuración centralizada para el scraper"""
    CARIBBEAN_SUBDIR = "zonas_departamentos"
    CARIBBEAN_CSV_DIR = os.path.join(os.path.dirname(BASE_DIR), "caribbean_grid", "data", CARIBBEAN_SUBDIR)
    
    COOKIES_JSON = os.path.join(DATA_DIR, "cookies_foursquare.json")
    SITIES_OUTPUT_DIR = os.path.join(DATA_DIR, "sities")
    REVIEWS_OUTPUT_DIR = os.path.join(DATA_DIR, "reviewers_sities")
    PROGRESO_PATH = os.path.join(REVIEWS_OUTPUT_DIR, "progress_reviews.json")
    FAILED_MUNICIPALITIES_PATH = os.path.join(DATA_DIR, "failed_municipalities.txt")

    BASE_URL = "https://es.foursquare.com"
    LOGIN_URL = f"{BASE_URL}/login"
    
    BROWSER_TYPE = "chromium"
    HEADLESS = True
    
    # Tiempos de espera y timeout para Playwright (en milisegundos)
    NAV_TIMEOUT = 60000
    WAIT_TIMEOUT = 15000
    CLICK_TIMEOUT = 5000
    
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


    # Scroll para cargar todos los resultados
    SCROLL_PAUSE = 2000
    MAX_SCROLL_NO_CHANGE = 3

    SAVE_INTERVAL = 5
    RETRIES = 3
    TIMEOUT = 60000
    PARALLEL_PROCESSES = 2
    REQUEST_DELAY = 2

    # Selectores CSS mínimos necesarios
    SELECTORS = {
        'venue_card': '[data-testid="venue-card"]',
        'venue_name': 'a[data-testid="venue-name"]',
        'venue_address': 'div[class*="address-line"]',
        'search_zone_button': 'button:has-text("Buscar en esta zona")',
        'results_container': 'div[aria-label="Resultados de la búsqueda"] >> xpath=..'
    }

    def get_caribbean_csvs(self) -> list:
        """Obtiene la lista de archivos CSV del directorio de caribbean_grid"""
        if not os.path.exists(self.CARIBBEAN_CSV_DIR):
            print(f"[ERROR] El directorio {self.CARIBBEAN_CSV_DIR} no existe.")
            return []
        return [os.path.join(self.CARIBBEAN_CSV_DIR, f) for f in os.listdir(self.CARIBBEAN_CSV_DIR) if f.endswith('.csv')]