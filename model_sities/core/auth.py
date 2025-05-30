"""
Manejo de autenticación para Foursquare
"""
from playwright.sync_api import Page
from config.settings import Settings
from utils.helpers import load_credentials

class FoursquareAuth:
    """Maneja la autenticación en Foursquare"""
    
    def __init__(self, credentials_file: str = None):
        """Inicializa el autenticador"""
        self.credentials_file = credentials_file or Settings.CREDENTIALS_FILE
        self.credentials = {}
        self._load_credentials()
    
    def _load_credentials(self) -> bool:
        """Carga las credenciales desde el archivo"""
        self.credentials = load_credentials(self.credentials_file)
        return len(self.credentials) > 0
    
    def login(self, page: Page) -> bool:
        """Realiza el proceso de login en Foursquare"""
        try:
            print("Iniciando proceso de login...")
            
            # Ir a la página de login
            page.goto(Settings.LOGIN_URL)
            page.wait_for_timeout(Settings.WAIT_MEDIUM_MIN)
            
            # Completar formulario de login
            page.fill(Settings.SELECTORS['login_username'], self.credentials.get('username', ''))
            page.fill(Settings.SELECTORS['login_password'], self.credentials.get('password', ''))
            page.click(Settings.SELECTORS['login_button'])
            
            # Pausa para autenticación de dos factores (2FA)
            print("Si se requiere autenticación de dos factores, ingrésala ahora en el navegador")
            page.pause()  # Pausa para 2FA
            
            print("Proceso de login completado")
            return True
            
        except Exception as e:
            print(f"Error durante el proceso de login: {e}")
            return False