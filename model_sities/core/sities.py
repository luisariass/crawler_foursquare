"""
Clase principal para realizar scraping en Foursquare
"""
import pandas as pd
import numpy as np
import time
from typing import Dict, Any
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError
from ..config.settings import Settings
from ..utils.helpers import current_timestamp

class FoursquareSitiesScraper:
    """Realiza el scraping de sitios turísticos en Foursquare"""
    
    def __init__(self):
        """Inicializa el scraper"""
        self.settings = Settings()
    
    def load_urls_from_csvs(self, csv_files: list) -> 'pd.DataFrame':
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
    
    def extract_sites(self, page, url, municipio="", max_retries=None, timeout=None) -> tuple:
        max_retries = max_retries or self.settings.RETRIES
        timeout = timeout or self.settings.TIMEOUT

        for attempt in range(1, max_retries + 1):
            try:
                print(f"Intento {attempt}/{max_retries} para {municipio} ({url})")
                page.goto(url, timeout=timeout)
                
                # --- INICIO DE LA LÓGICA PARA EL BOTÓN DEL MAPA ---
                
                # Paso 1: Buscar y presionar el botón "Buscar en esta área" si existe.
                map_search_button_selector = self.settings.SELECTORS['map_search_button']
                
                # Damos un tiempo corto para que el botón aparezca si es que va a aparecer.
                try:
                    page.locator(map_search_button_selector).wait_for(timeout=7000) # Espera hasta 7 segundos
                    if page.is_visible(map_search_button_selector):
                        print(f"[INFO] Botón 'Buscar en esta área' encontrado. Presionando...")
                        page.click(map_search_button_selector)
                        # Esperamos un poco después del clic para que los resultados empiecen a cargar.
                        page.wait_for_timeout(3000)
                except PlaywrightTimeoutError:
                    # Si el botón no aparece en 7 segundos, asumimos que no es necesario y continuamos.
                    print(f"[INFO] Botón 'Buscar en esta área' no encontrado, continuando con la carga normal.")
                
                # --- FIN DE LA LÓGICA PARA EL BOTÓN DEL MAPA ---

                content_selector = self.settings.SELECTORS['content_holder']
                no_results_selector = self.settings.SELECTORS['no_results_card']
                generic_error_selector = self.settings.SELECTORS['generic_error_card']

                # Espera a que cargue el contenido principal, el mensaje de "sin resultados" o el error genérico
                page.locator(f"{content_selector}, {no_results_selector}, {generic_error_selector}").first.wait_for(timeout=20000)

                # Early exit: error genérico (bloqueo del servidor)
                if page.is_visible(generic_error_selector):
                    print(f"[BLOCK] Bloqueo del servidor detectado en {municipio}.")
                    self.register_failed_municipality(municipio, url, "generic_error")
                    return ("generic_error", [])

                # Early exit: zona vacía
                elif page.is_visible(no_results_selector):
                    print(f"[INFO] Zona vacía para {municipio}. No se encontraron sitios.")
                    return ("no_results", [])

                # Si hay resultados, procede con el scraping
                self._load_all_results(page)
                sitios_elements = page.query_selector_all(content_selector)
                
                sitios_list = []
                for i, sitio_element in enumerate(sitios_elements):
                    try:
                        site_data = self._extract_site_data(sitio_element, i + 1)
                        sitios_list.append(site_data)
                    except Exception as e:
                        print(f"[WARN] No se pudo procesar un sitio en {municipio}: {e}")
                return ("success", sitios_list)

            except PlaywrightTimeoutError:
                print(f"[TIMEOUT] Timeout en intento {attempt} para {municipio}.")
                if attempt == max_retries:
                    self.register_failed_municipality(municipio, url, "timeout_final")
                    return ("timeout", [])
                else:
                    wait_time = self.settings.BACKOFF_FACTOR * attempt
                    print(f"Esperando {wait_time} segundos antes de reintentar...")
                    time.sleep(wait_time)
            except Exception as e:
                print(f"[ERROR] Error inesperado en intento {attempt} para {municipio}: {e}")
                self.register_failed_municipality(municipio, url, f"error_inesperado: {e}")
                return ("error", [])

        return ("error", [])

    def register_failed_municipality(self, municipio, url, reason):
        failed_path = self.settings.FAILED_MUNICIPALITIES_PATH
        with open(failed_path, "a", encoding="utf-8") as f:
            f.write(f"{municipio},{url},{reason},{current_timestamp()}\n")
        print(f"[FAILED] Municipio registrado: {municipio} ({url}) - Razón: {reason}")
    
    def _load_all_results(self, page: Page) -> None:
        """Hace clic en 'Ver más resultados' hasta que no haya más"""
        try:
            while True:
                boton = page.query_selector(self.settings.SELECTORS['more_results_button'])
                if boton and boton.is_visible():
                    boton.click()
                    page.wait_for_timeout(int(np.random.uniform(self.settings.WAIT_SHORT_MIN, self.settings.WAIT_SHORT_MAX)))
                else:
                    break
        except Exception:
            pass
    
    def _extract_site_data(self, sitio, index: int) -> Dict[str, Any]:
        """Extrae datos de un sitio individual"""
        sitio_data = {
            "id": index,
            "puntuacion": "N/A",
            "nombre": "N/A",
            "categoria": "N/A",
            "direccion": "N/A",
            "url_sitio": "",
            "fecha_extraccion": current_timestamp()
        }
        
        puntuacion_element = sitio.query_selector(self.settings.SELECTORS['venue_score'])
        if puntuacion_element:
            sitio_data["puntuacion"] = puntuacion_element.inner_text().strip()
        
        nombre_element = sitio.query_selector(self.settings.SELECTORS['venue_name'])
        if nombre_element:
            nombre_link = nombre_element.query_selector('a')
            if nombre_link:
                sitio_data["nombre"] = nombre_link.inner_text().strip()
                href = nombre_link.get_attribute('href')
                if href:
                    sitio_data["url_sitio"] = f"{self.settings.BASE_URL}{href}" if href.startswith('/') else href
            else:
                sitio_data["nombre"] = nombre_element.inner_text().strip()
        
        categoria_element = sitio.query_selector(self.settings.SELECTORS['venue_category'])
        if categoria_element:
            sitio_data["categoria"] = categoria_element.inner_text().strip().replace('•', '').strip()
        
        direccion_element = sitio.query_selector(self.settings.SELECTORS['venue_address'])
        if direccion_element:
            sitio_data["direccion"] = direccion_element.inner_text().strip()
        
        return sitio_data