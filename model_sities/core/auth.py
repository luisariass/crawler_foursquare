from playwright.sync_api import Page
from config.settings import Settings
from utils.cookies_helper import save_cookies, load_cookies

class FoursquareAuth:
    """Maneja la autenticación en Foursquare"""

    def __init__(self, cookies_path=Settings.COOKIES_JSON):
        self.cookies_path = cookies_path

    def login(self, page: Page) -> bool:
        """Intenta cargar cookies, si falla hace login manual"""
        # 1. Intentar cargar cookies
        if load_cookies(page, self.cookies_path):
            page.goto(Settings.BASE_URL)
            page.wait_for_timeout(Settings.WAIT_SHORT_MIN)
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
            page.fill(Settings.SELECTORS['login_username'], input("Usuario Foursquare: "))
            page.fill(Settings.SELECTORS['login_password'], input("Contraseña Foursquare: "))
            page.click(Settings.SELECTORS['login_button'])
            print("Si se requiere autenticación de dos factores, ingrésala ahora en el navegador")
            page.pause()  # Pausa para 2FA
            save_cookies(page, self.cookies_path)
            print("Proceso de login completado y cookies guardadas.")
            return True
        except Exception as e:
            print(f"Error durante el proceso de login: {e}")
            return False