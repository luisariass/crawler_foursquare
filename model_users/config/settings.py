import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
RESULTADOS_DIR = os.path.join(BASE_DIR, "resultados")
TIPS_DIR = os.path.join(RESULTADOS_DIR, "tips")
USERS_DIR = os.path.join(RESULTADOS_DIR, "users")
LOGS_ERROR_DIR = os.path.join(BASE_DIR, "logs_error")
ERROR_TIPS_PATH = os.path.join(LOGS_ERROR_DIR, "error_tips.json")
PROGRESO_PATH = os.path.join("progreso_resenas_usuarios.json")
COOKIES_PATH = os.path.join("cookies_foursquare.json")
USERS_CSV = os.path.join("merge_user_altlantico_bolivar_no_duplicates.csv")

class Settings:
    BASE_DIR = BASE_DIR
    DATA_DIR = DATA_DIR
    RESULTADOS_DIR = RESULTADOS_DIR
    TIPS_DIR = TIPS_DIR
    USERS_DIR = USERS_DIR
    LOGS_ERROR_DIR = LOGS_ERROR_DIR
    ERROR_TIPS_PATH = ERROR_TIPS_PATH
    PROGRESO_PATH = PROGRESO_PATH
    COOKIES_PATH = COOKIES_PATH
    USERS_CSV = USERS_CSV
    BROWSER_TYPE = "firefox"
    HEADLESS = True

    @classmethod
    def create_output_dirs(cls):
        os.makedirs(cls.RESULTADOS_DIR, exist_ok=True)
        os.makedirs(cls.TIPS_DIR, exist_ok=True)
        os.makedirs(cls.USERS_DIR, exist_ok=True)
        os.makedirs(cls.LOGS_ERROR_DIR, exist_ok=True)
        os.makedirs(cls.DATA_DIR, exist_ok=True)