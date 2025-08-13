"""
Clase principal para realizar scraping en Foursquare.
Contiene la lógica para cargar URLs y extraer sitios de una zona geográfica.
"""
import pandas as pd
from typing import Dict, Any, List
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

from ..config.settings import Settings
from ..utils.helpers import current_timestamp

class FoursquareScraper:
    """Realiza el scraping de sitios turísticos en Foursquare."""

    def __init__(self):
        """Inicializa el scraper."""
        self.settings = Settings()

    def load_urls_from_csvs(self, csv_files: list) -> pd.DataFrame:
        """Carga y concatena las URLs desde una lista de archivos CSV."""
        frames = []
        for csv_path in csv_files:
            try:
                df = pd.read_csv(csv_path, sep=',')
                print(f"[INFO] {len(df)} URLs cargadas de {csv_path}")
                frames.append(df)
            except Exception as e:
                print(f"[ERROR] No se pudo cargar {csv_path}: {e}")
        if frames:
            return pd.concat(frames, ignore_index=True)
        return pd.DataFrame()

    def extract_sites_from_zone(self, page: Page, url: str, municipio: str) -> List[Dict[str, Any]]:
        """
        Navega a una URL de un municipio, hace clic en 'Buscar en esta zona'
        y extrae todos los sitios encontrados haciendo scroll.

        Args:
            page: El objeto Page de Playwright.
            url: La URL del municipio a procesar.
            municipio: El nombre del municipio (para logging).

        Returns:
            Una lista de diccionarios, donde cada diccionario representa un sitio.
        """
        print(f"[SCRAPER] Extrayendo sitios para {municipio} desde {url}")
        sites = []
        try:
            page.goto(url, timeout=self.settings.NAV_TIMEOUT)
            page.wait_for_load_state('domcontentloaded')

            # Hacer clic en "Buscar en esta zona" para asegurar que se carguen los sitios
            try:
                search_button = page.locator('button:has-text("Buscar en esta zona")')
                search_button.click(timeout=self.settings.CLICK_TIMEOUT)
                print(f"[SCRAPER] Clic en 'Buscar en esta zona' para {municipio}.")
                # Esperar a que la nueva lista de resultados se cargue
                page.wait_for_selector('[data-testid="venue-card"]', timeout=self.settings.WAIT_TIMEOUT)
            except PlaywrightTimeoutError:
                print(f"[WARN] No se encontró o no fue necesario hacer clic en 'Buscar en esta zona' para {municipio}.")

            # Lógica de scroll para cargar todos los sitios
            scroll_container = page.locator('div[aria-label="Resultados de la búsqueda"] >> xpath=..')
            if not scroll_container.is_visible():
                 print(f"[WARN] Contenedor de resultados no visible para {municipio}.")
                 return []

            previous_height = -1
            consecutive_no_change = 0
            
            while consecutive_no_change < self.settings.MAX_SCROLL_NO_CHANGE:
                current_height = scroll_container.evaluate('(element) => element.scrollHeight')
                scroll_container.evaluate('(element) => element.scrollTo(0, element.scrollHeight)')
                page.wait_for_timeout(self.settings.SCROLL_PAUSE)

                if current_height == previous_height:
                    consecutive_no_change += 1
                else:
                    consecutive_no_change = 0
                
                previous_height = current_height

            # Extraer la información de todos los sitios cargados
            site_cards = page.locator('[data-testid="venue-card"]').all()
            print(f"[SCRAPER] Se encontraron {len(site_cards)} tarjetas de sitios para {municipio}.")

            for card in site_cards:
                try:
                    name_element = card.locator('a[data-testid="venue-name"]')
                    site_name = name_element.inner_text()
                    site_url = name_element.get_attribute('href')
                    
                    address_element = card.locator('div[class*="address-line"]')
                    address = address_element.inner_text() if address_element.count() > 0 else 'N/A'

                    site_data = {
                        'name': site_name,
                        'address': address,
                        'url': f"https://foursquare.com{site_url}" if site_url else 'N/A',
                        'municipio_busqueda': municipio,
                        'fecha_extraccion': current_timestamp()
                    }
                    sites.append(site_data)
                except Exception as e:
                    print(f"[WARN] No se pudo procesar una tarjeta de sitio en {municipio}: {e}")
            
            return sites

        except PlaywrightTimeoutError:
            print(f"[ERROR] Timeout durante la navegación o extracción en {municipio}.")
            self.register_failed_municipality(municipio, url, "timeout_error")
            return []
        except Exception as e:
            print(f"[ERROR] Error inesperado extrayendo sitios para {municipio}: {e}")
            self.register_failed_municipality(municipio, url, f"unexpected_error: {e}")
            return []

    def register_failed_municipality(self, municipio: str, url: str, reason: str):
        """Registra un municipio que falló en un archivo de texto."""
        try:
            with open(self.settings.FAILED_MUNICIPALITIES_PATH, "a", encoding="utf-8") as f:
                f.write(f"{municipio},{url},{reason},{current_timestamp()}\n")
            print(f"[FAILED] Municipio fallido registrado: {municipio} - Razón: {reason}")
        except Exception as e:
            print(f"[ERROR] No se pudo escribir en el archivo de fallos: {e}")