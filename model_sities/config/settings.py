"""
Configuración para el scraper de Foursquare
"""
import os

# Ruta base del proyecto (carpeta model_sities)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Ruta a la carpeta de datos
DATA_DIR = os.path.join(BASE_DIR, "data")

class Settings:
    """Configuración centralizada para el scraper"""
    
    # Archivos y directorios con rutas absolutas
    CREDENTIALS_FILE = os.path.join(DATA_DIR, "credentials.txt")
    CSV_URLS_FILE = os.path.join(DATA_DIR, "merge_sities_bolivar.csv")
    OUTPUT_DIR = os.path.join(BASE_DIR, "sitios_turisticos_bolivar")
    
    SITIES_ATLANTICO_CSV = os.path.join(DATA_DIR, "sities_atlantico.csv")
    SITIES_BOLIVAR_CSV = os.path.join(DATA_DIR, "sities_bolivar.csv")
    
    # URLs de Foursquare
    BASE_URL = "https://es.foursquare.com"
    LOGIN_URL = f"{BASE_URL}/login"
    
    # Configuración del navegador
    BROWSER_TYPE = "firefox"  # firefox, chromium, webkit
    HEADLESS = False
    
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
    def validate_files(cls):
        """Verifica que los archivos necesarios existan"""
        files_to_check = [cls.CREDENTIALS_FILE, cls.CSV_URLS_FILE]
        missing_files = []
        
        for file_path in files_to_check:
            if not os.path.exists(file_path):
                missing_files.append(file_path)
                
        if missing_files:
            print("ERROR: No se encontraron los siguientes archivos:")
            for file in missing_files:
                print(f"  - {file}")
            return False
            
        print("✓ Todos los archivos necesarios encontrados")
        return True