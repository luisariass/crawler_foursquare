from playwright.sync_api import Page
from config.settings import Settings
from utils.helpers import load_credentials
from utils.cookies_helper import cargar_cookies_playwright, guardar_cookies_playwright


class FoursquareAuth:
    """Maneja la autenticación en Foursquare"""

    def __init__(self, credentials_file: str = None, cookies_path: str = "cookies_foursquare.json"):
        self.credentials_file = credentials_file or Settings.CREDENTIALS_FILE
        self.credentials = {}
        self.cookies_path = cookies_path
        self._load_credentials()

    def _load_credentials(self) -> bool:
        self.credentials = load_credentials(self.credentials_file)
        return len(self.credentials) > 0

    def login(self, page: Page) -> bool:
        """Intenta cargar cookies, si falla hace login manual"""
        # 1. Intentar cargar cookies
        if cargar_cookies_playwright(page, self.cookies_path):
            page.goto(Settings.BASE_URL)
            page.wait_for_timeout(Settings.WAIT_SHORT_MIN)
            # Verifica si la sesión es válida (no redirige a login)
            if "login" not in page.url:
                print("Sesión restaurada con cookies.")
                return True
            else:
                print("Las cookies no son válidas, se requiere login manual.")

        # 2. Si no hay cookies válidas, hacer login manual
        try:
            print("Iniciando proceso de login manual...")
            page.goto(Settings.LOGIN_URL)
            page.wait_for_timeout(Settings.WAIT_MEDIUM_MIN)
            page.fill(Settings.SELECTORS['login_username'], self.credentials.get('username', ''))
            page.fill(Settings.SELECTORS['login_password'], self.credentials.get('password', ''))
            page.click(Settings.SELECTORS['login_button'])
            print("Si se requiere autenticación de dos factores, ingrésala ahora en el navegador")
            page.pause()  # Pausa para 2FA
            # Guardar cookies después del login exitoso
            guardar_cookies_playwright(page, self.cookies_path)
            print("Proceso de login completado y cookies guardadas.")
            return True
        except Exception as e:
            print(f"Error durante el proceso de login: {e}")
            return False