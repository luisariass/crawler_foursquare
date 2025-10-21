import random
import signal
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Tuple
from multiprocessing import Event
from playwright.sync_api import sync_playwright
from ..core.auth import FoursquareAuth
from ..core.reviewer import ReviewerLogic
from ..core.sities import SitiesLogic
from ..config.settings import Settings

# --- Lógica para apagado controlado ---
shutdown_event = Event()

def init_worker():
    """Inicializador para que cada worker ignore la señal de interrupción."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)

class BaseScraperWorker(ABC):
    """Clase base abstracta para workers de scraping."""
    
    def __init__(self):
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
        return browser, context.new_page()
    
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
        """Ejecución común del worker."""
        if shutdown_event.is_set():
            return {**self.get_default_result(task_info), "status": "shutdown"}
        
        try:
            with sync_playwright() as p:
                browser, page = self._setup_browser(p)
                
                if not self._login(page):
                    return {**self.get_default_result(task_info), "status": "auth_error"}
                
                status, data = self.scrape(page, task_info)
                browser.close()
                
                return {
                    **self.get_default_result(task_info),
                    "status": status,
                    "data": data
                }
                
        except Exception as e:
            print(f"[WORKER ERROR] Error en {self.__class__.__name__}: {e}")
            return {**self.get_default_result(task_info), "status": "worker_error"}

class ReviewerScraperWorker(BaseScraperWorker):
    """Worker para extraer perfiles de usuario."""
    
    def scrape(self, page, task_info: Dict[str, Any]) -> Tuple[str, List]:
        site_data = task_info['site_data']
        site_url = site_data.get('url_sitio', '')
        site_id = site_data.get('id', 'unknown_id')
        
        reviewer = ReviewerLogic()
        return reviewer.extract_reviews(page, site_url, str(site_id))

    def get_default_result(self, task_info: Dict[str, Any]) -> Dict[str, Any]:
        site_data = task_info['site_data']
        return {
            "site_id": site_data.get('id', 'unknown_id'),
            "site_url": site_data.get('url_sitio', ''),
            "site_name": site_data.get('nombre', 'nombre_desconocido'),
            "municipio": site_data.get('municipio', 'municipio_desconocido'),
            "users": []
        }

class SiteScraperWorker(BaseScraperWorker):
    """Worker para extraer sitios de Foursquare."""
    
    def scrape(self, page, task_info: Dict[str, Any]) -> Tuple[str, List]:
        url = task_info['url_municipio']
        municipio = task_info['municipio']
        departamento = task_info['departamento']
        
        sities = SitiesLogic()
        return sities.extract_sites(page, url, municipio, departamento)

    def get_default_result(self, task_info: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "municipio": task_info['municipio'],
            "departamento": task_info['departamento'],
            "sites": []
        }

# Funciones originales (ahora son wrappers simples)
def worker_users(task_info: Dict[str, Any]) -> Dict[str, Any]:
    worker = ReviewerScraperWorker()
    result = worker.execute(task_info)
    # Mantener compatibilidad con la interfaz original
    if "data" in result:
        result["users"] = result.pop("data")
    return result

def worker_sities(task_info: Dict[str, Any]) -> Dict[str, Any]:
    worker = SiteScraperWorker()
    result = worker.execute(task_info)
    # Mantener compatibilidad con la interfaz original
    if "data" in result:
        result["sites"] = result.pop("data")
    return result