"""
Módulo que define los workers para el scraping.
"""
import random
from abc import ABC, abstractmethod
from multiprocessing import Event
from typing import Dict, Any, List, Tuple

from playwright.sync_api import sync_playwright, Error as PlaywrightError

from ..config.settings import Settings
from ..core.auth import FoursquareAuth
from ..core.reviewer import ReviewerLogic
from ..core.sities import SitiesLogic

# --- Lógica para apagado controlado ---
shutdown_event = Event()


class BaseScraperWorker(ABC):
    """Clase base abstracta para workers de scraping."""

    def __init__(self):
        """Inicializa las clases base del worker."""
        self.settings = Settings()
        self.auth = FoursquareAuth()

    def _setup_browser(self, playwright):
        """Configuración común del browser."""
        browser = getattr(playwright, self.settings.BROWSER_TYPE).launch(
            headless=self.settings.HEADLESS
        )
        context = browser.new_context(
            user_agent=random.choice(self.settings.USER_AGENTS),
            viewport=random.choice(self.settings.VIEWPORTS)
        )
        page = context.new_page()
        return browser, context, page

    def _login(self, page) -> bool:
        """Login común para todos los workers."""
        return self.auth.login(page)

    @abstractmethod
    def scrape(self, page, task_info: Dict[str, Any]) -> Tuple[str, List]:
        """Método abstracto que debe implementar cada worker específico."""
        pass

    @abstractmethod
    def get_default_result(self, task_info: Dict[str, Any]) -> Dict[str, Any]:
        """Resultado por defecto en caso de error."""
        pass

    def execute(self, task_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ejecución del worker con manejo robusto de recursos para evitar
        fugas de memoria y procesos colgados.
        """
        if shutdown_event.is_set():
            return {**self.get_default_result(task_info), "status": "shutdown"}

        try:
            with sync_playwright() as p:
                browser = None
                context = None
                page = None
                try:
                    browser, context, page = self._setup_browser(p)

                    if not self._login(page):
                        return {
                            **self.get_default_result(task_info),
                            "status": "auth_error"
                        }

                    status, data = self.scrape(page, task_info)

                    result = self.get_default_result(task_info)
                    result["status"] = status
                    if isinstance(self, SiteScraperWorker):
                        result["sites"] = data
                    elif isinstance(self, ReviewerScraperWorker):
                        result["users"] = data
                    return result
                finally:
                    # Limpieza DENTRO del bloque 'with'
                    if page:
                        page.close()
                    if context:
                        context.close()
                    if browser:
                        browser.close()
        except PlaywrightError as e:
            print(f"[WORKER CRASH] Error grave de Playwright: {e}")
            return {
                **self.get_default_result(task_info),
                "status": "playwright_crash"
            }
        except Exception as e:
            print(f"[WORKER ERROR] Error inesperado en worker: {e}")
            return {
                **self.get_default_result(task_info),
                "status": "worker_error"
            }


class ReviewerScraperWorker(BaseScraperWorker):
    """Worker para extraer perfiles de usuario."""

    def scrape(self, page, task_info: Dict[str, Any]) -> Tuple[str, List]:
        """Extrae las reseñas de un sitio."""
        site_data = task_info['site_data']
        site_url = site_data.get('url_sitio', '')
        site_id = site_data.get('id', 'unknown_id')

        reviewer = ReviewerLogic()
        return reviewer.extract_reviews(page, site_url, str(site_id))

    def get_default_result(self, task_info: Dict[str, Any]) -> Dict[str, Any]:
        """Genera un resultado por defecto para el worker de reviewers."""
        site_data = task_info['site_data']
        return {
            "site_id": site_data.get('id', 'unknown_id'),
            "site_url": site_data.get('url_sitio', ''),
            "site_name": site_data.get('nombre', 'nombre_desconocido'),
            "municipio": site_data.get('municipio', 'municipio_desconocido'),
            "departamento": site_data.get('departamento', 'departamento_desconocido'),
            "users": []
        }


class SiteScraperWorker(BaseScraperWorker):
    """Worker para extraer sitios de Foursquare."""

    def scrape(self, page, task_info: Dict[str, Any]) -> Tuple[str, List]:
        """Extrae los sitios de una URL de Foursquare."""
        sities = SitiesLogic()
        return sities.extract_sites(
            page,
            task_info['url_municipio'],
            task_info['municipio'],
            task_info['departamento']
        )

    def get_default_result(self, task_info: Dict[str, Any]) -> Dict[str, Any]:
        """Genera un resultado por defecto para el worker de sitios."""
        return {
            "municipio": task_info['municipio'],
            "departamento": task_info['departamento'],
            "sites": []
        }


def worker_users(task_info: Dict[str, Any]) -> Dict[str, Any]:
    """Wrapper para el worker de usuarios."""
    worker = ReviewerScraperWorker()
    return worker.execute(task_info)


def worker_sities(task_info: Dict[str, Any]) -> Dict[str, Any]:
    """Wrapper para el worker de sitios."""
    worker = SiteScraperWorker()
    return worker.execute(task_info)