"""
Clase principal para realizar scraping en Foursquare.
Contiene la lógica para cargar URLs, extraer sitios de una página y manejar reintentos.
"""
import pandas as pd
import numpy as np
import time
from typing import Dict, Any, List
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError
from ..config.settings import Settings
from ..utils.helpers import current_timestamp

class FoursquareScraper:
    """Realiza el scraping de sitios turísticos en Foursquare"""
    
    def __init__(self):
        """Inicializa el scraper"""
        self.settings = Settings()
    
    def load_urls_from_csvs(self, csv_files: list) -> pd.DataFrame:
        """Carga y concatena las URLs desde una lista de archivos CSV"""
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

    def extract_sites_zone(self, page: Page, url: str, municipio: str = "") -> List[Dict[str, Any]]:
        """
        Orquesta la extracción para una zona completa: fuerza refresh, itera categorías y deduplica.
        """
        for attempt in range(1, self.settings.RETRIES + 1):
            try:
                print(f"Procesando zona: {municipio} (Intento {attempt}/{self.settings.RETRIES})")
                page.goto(url, timeout=self.settings.TIMEOUT)
                
                # 1. Forzar actualización inicial si el botón está disponible
                self._force_requery_if_available(page)
                
                # 2. Comprobar si la zona está completamente vacía desde el inicio
                initial_state = self._wait_results_or_empty(page, timeout_ms=15000)
                if initial_state == "empty":
                    print(f"[INFO] Zona vacía detectada para {municipio}. No se encontraron sitios.")
                    return []

                # 3. Extraer sitios de la vista inicial y luego iterar por categorías
                all_sites = []
                all_sites.extend(self._extract_current_list(page, municipio, "__default__"))

                if self._open_category_menu(page):
                    for label in self.settings.CATEGORY_LABELS:
                        if self._select_category(page, label):
                            self._force_requery_if_available(page)
                            state = self._wait_results_or_empty(page, timeout_ms=10000)
                            if state == "results":
                                sites_in_category = self._extract_current_list(page, municipio, label)
                                all_sites.extend(sites_in_category)
                            else:
                                print(f"[INFO] Categoría '{label}' vacía o con timeout en {municipio}.")
                        # Reabrir menú si se cierra
                        self._open_category_menu(page)
                
                # 4. Deduplicar resultados por URL del sitio para evitar repetidos
                unique_sites = {site["url_sitio"]: site for site in all_sites if site.get("url_sitio")}
                final_list = list(unique_sites.values())
                
                print(f"[SUCCESS] {municipio}: {len(final_list)} sitios únicos encontrados en total.")
                time.sleep(self.settings.REQUEST_DELAY)
                return final_list

            except Exception as e:
                print(f"[ERROR] Fallo grave en el intento {attempt} para {municipio}: {e}")
                if attempt == self.settings.RETRIES:
                    self.register_failed_municipality(municipio, url, f"error_final: {e}")
                else:
                    time.sleep(10 * attempt) # Espera más larga entre reintentos
        
        print(f"[FAIL] No se pudieron extraer datos para {municipio} después de {self.settings.RETRIES} intentos.")
        return []

    def _force_requery_if_available(self, page: Page):
        """Intenta hacer clic en el botón 'Buscar en esta área' si es visible."""
        try:
            requery_button = page.locator(self.settings.SELECTORS['requery_button'])
            if requery_button.is_visible(timeout=4000):
                print("[INFO] Botón 'Buscar en esta área' encontrado. Haciendo clic.")
                requery_button.click()
                page.wait_for_timeout(int(np.random.uniform(1500, 2500)))
        except Exception:
            pass # Es normal que no esté, no es un error.

    def _wait_results_or_empty(self, page: Page, timeout_ms: int) -> str:
        """Espera a que aparezcan resultados o el indicador de vacío."""
        sel_results = self.settings.SELECTORS['content_holder']
        sel_empty = self.settings.SELECTORS['no_results_indicator']
        try:
            page.wait_for_selector(f"{sel_results}, {sel_empty}", timeout=timeout_ms)
            if page.locator(sel_empty).is_visible():
                return "empty"
            return "results"
        except PlaywrightTimeoutError:
            return "timeout"

    def _open_category_menu(self, page: Page) -> bool:
        """Abre el menú de categorías si no está ya abierto."""
        try:
            panel = page.locator(self.settings.SELECTORS['category_menu_panel'])
            if panel.is_visible():
                return True # Ya está abierto
            trigger = page.locator(self.settings.SELECTORS['category_menu_trigger']).first
            if trigger.is_visible(timeout=3000):
                trigger.click()
                page.wait_for_timeout(500)
                return panel.is_visible(timeout=3000)
        except Exception:
            return False
        return False

    def _select_category(self, page: Page, label: str) -> bool:
        """Selecciona una categoría por su texto visible."""
        try:
            item = page.locator(f"{self.settings.SELECTORS['category_item']}:has-text('{label}')").first
            if item.is_visible(timeout=2000):
                item.click()
                page.wait_for_timeout(int(np.random.uniform(800, 1200)))
                return True
        except Exception:
            print(f"[WARN] No se pudo hacer clic en la categoría '{label}'.")
        return False

    def _extract_current_list(self, page: Page, municipio: str, categoria_base: str) -> List[Dict[str, Any]]:
        """Extrae la lista de sitios actualmente visibles en la página."""
        print(f"[INFO] Extrayendo sitios para categoría '{categoria_base}' en {municipio}...")
        self._load_all_results(page)
        elements = page.query_selector_all(self.settings.SELECTORS['content_holder'])
        sites_found = []
        for idx, el in enumerate(elements, start=1):
            try:
                data = self._extract_site_data(el, idx)
                data["categoria_base"] = categoria_base # Añade la categoría de origen
                sites_found.append(data)
            except Exception as e:
                print(f"[WARN] Error extrayendo item {idx} en {municipio}: {e}")
        return sites_found

    def register_failed_municipality(self, municipio: str, url: str, reason: str):
        """Registra un municipio que falló en un archivo de texto."""
        try:
            with open(self.settings.FAILED_MUNICIPALITIES_PATH, "a", encoding="utf-8") as f:
                f.write(f"{municipio},{url},{reason},{current_timestamp()}\n")
            print(f"[FAILED] Municipio registrado: {municipio} - Razón: {reason}")
        except Exception as e:
            print(f"[ERROR] No se pudo escribir en el archivo de fallos: {e}")
    
    def _load_all_results(self, page: Page) -> None:
        """Hace clic en 'Ver más resultados' hasta que el botón desaparece."""
        while True:
            try:
                more_results_button = page.locator(self.settings.SELECTORS['more_results_button'])
                if more_results_button.is_visible(timeout=5000):
                    more_results_button.click()
                    page.wait_for_timeout(int(np.random.uniform(self.settings.WAIT_SHORT_MIN, self.settings.WAIT_SHORT_MAX)))
                else:
                    break
            except PlaywrightTimeoutError:
                break
            except Exception as e:
                print(f"[WARN] Ocurrió un error al intentar cargar más resultados: {e}")
                break
    
    def _extract_site_data(self, sitio_element, index: int) -> Dict[str, Any]:
        """Extrae y estructura los datos de un único elemento de sitio."""
        sitio_data = {
            "id": index,
            "puntuacion": "N/A",
            "nombre": "N/A",
            "categoria": "N/A",
            "direccion": "N/A",
            "url_sitio": "",
            "fecha_extraccion": current_timestamp()
        }
        
        if (puntuacion_element := sitio_element.query_selector(self.settings.SELECTORS['venue_score'])):
            sitio_data["puntuacion"] = puntuacion_element.inner_text().strip()
        
        if (nombre_element := sitio_element.query_selector(self.settings.SELECTORS['venue_name'])):
            if (nombre_link := nombre_element.query_selector('a')):
                sitio_data["nombre"] = nombre_link.inner_text().strip()
                if href := nombre_link.get_attribute('href'):
                    sitio_data["url_sitio"] = href if href.startswith('http') else f"{self.settings.BASE_URL}{href}"
            else:
                sitio_data["nombre"] = nombre_element.inner_text().strip()
        
        if (categoria_element := sitio_element.query_selector(self.settings.SELECTORS['venue_category'])):
            sitio_data["categoria"] = categoria_element.inner_text().replace('•', '').strip()
        
        if (direccion_element := sitio_element.query_selector(self.settings.SELECTORS['venue_address'])):
            sitio_data["direccion"] = direccion_element.inner_text().strip()
        
        return sitio_data