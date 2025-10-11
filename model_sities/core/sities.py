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

class SitiesLogic:
    """Realiza el scraping de sitios turísticos en Foursquare"""

    def __init__(self):
        """Inicializa el scraper"""
        self.settings = Settings()
    
    def extract_sites(self, page, url, municipio: str = "") -> tuple:
        """
        Realiza el scraping de sitios turísticos en Foursquare con manejo de reintentos y errores.
        """
        max_retries = self.settings.RETRIES
        timeout = self.settings.TIMEOUT

        for attempt in range(1, max_retries + 1):
            try:
                print(f"Intento {attempt}/{max_retries} para {municipio}")
                page.goto(url, timeout=timeout)
                self._handle_map_search_button(page)
                result = self._wait_and_check_early_exit(page, municipio)
                if result is not None:
                    return result
                sitios_list = self._scrape_sites(page)
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

    def _handle_map_search_button(self, page: Page) -> None:
        """
        Busca y presiona el botón 'Buscar en esta área' si existe.
        """
        map_search_button_selector = self.settings.SELECTORS['map_search_button']
        try:
            page.locator(map_search_button_selector).wait_for(timeout=7000)
            if page.is_visible(map_search_button_selector):
                print("[INFO] Botón 'Buscar en esta área' encontrado. Presionando...")
                page.click(map_search_button_selector)
                page.wait_for_timeout(3000)
        except PlaywrightTimeoutError:
            print("[INFO] Botón 'Buscar en esta área' no encontrado, continuando con la carga normal.")

    def _wait_and_check_early_exit(self, page: Page, municipio: str):
        """
        Espera la carga de contenido y verifica condiciones de salida temprana.
        """
        content_selector = self.settings.SELECTORS['content_holder']
        no_results_selector = self.settings.SELECTORS['no_results_card']
        generic_error_selector = self.settings.SELECTORS['generic_error_card']

        page.locator(
            f"{content_selector}, {no_results_selector}, {generic_error_selector}"
        ).first.wait_for(timeout=20000)

        if page.is_visible(generic_error_selector):
            print(f"[BLOCK] Bloqueo del servidor detectado en {municipio}.")
            return ("generic_error", [])
        elif page.is_visible(no_results_selector):
            print(f"[INFO] Zona vacía para {municipio}. No se encontraron sitios.")
            return ("no_results", [])
        return None

    def _scrape_sites(self, page: Page) -> list:
        """
        Realiza el scraping de los sitios turísticos listados en la página.
        """
        self._load_all_results(page)
        content_selector = self.settings.SELECTORS['content_holder']
        sitios_elements = page.query_selector_all(content_selector)
        sitios_list = []
        for sitio_element in sitios_elements:
            try:
                site_data = self._extract_site_data(sitio_element)
                if site_data.get("id") != "N/A":
                    sitios_list.append(site_data)
            except Exception as e:
                print(f"[WARN] No se pudo procesar un sitio: {e}")
        return sitios_list

    def _load_all_results(self, page: Page) -> None:
        """Hace clic en 'Ver más resultados' hasta que no haya más"""
        rng = np.random.default_rng(42)
        try:
            while True:
                boton = page.query_selector(self.settings.SELECTORS['more_results_button'])
                if boton and boton.is_visible():
                    boton.click()
                    page.wait_for_timeout(
                        int(rng.uniform(self.settings.WAIT_SHORT_MIN, self.settings.WAIT_SHORT_MAX))
                    )
                else:
                    break
        except Exception:
            pass

    def _extract_site_data(self, sitio) -> Dict[str, Any]:
        """Extrae datos de un sitio individual, usando el ID de la URL."""
        sitio_data = {
            "id": "N/A",
            "puntuacion": "N/A",
            "nombre": "N/A",
            "categoria": "N/A",
            "direccion": "N/A",
            "url_sitio": "",
            "fecha_extraccion": current_timestamp()
        }

        self._extract_nombre_y_url(sitio, sitio_data)

        puntuacion_element = sitio.query_selector(self.settings.SELECTORS['venue_score'])
        if puntuacion_element:
            sitio_data["puntuacion"] = puntuacion_element.inner_text().strip()

        categoria_element = sitio.query_selector(self.settings.SELECTORS['venue_category'])
        if categoria_element:
            sitio_data["categoria"] = categoria_element.inner_text().strip().replace('•', '').strip()

        direccion_element = sitio.query_selector(self.settings.SELECTORS['venue_address'])
        if direccion_element:
            sitio_data["direccion"] = direccion_element.inner_text().strip()

        return sitio_data

    def _extract_nombre_y_url(self, sitio, sitio_data: Dict[str, Any]) -> None:
        """Extrae el nombre y la URL del sitio, y el ID si es posible."""
        nombre_element = sitio.query_selector(self.settings.SELECTORS['venue_name'])
        if not nombre_element:
            return

        nombre_link = nombre_element.query_selector('a')
        if nombre_link:
            sitio_data["nombre"] = nombre_link.inner_text().strip()
            href = nombre_link.get_attribute('href')
            if href:
                full_url = (
                    f"{self.settings.BASE_URL}{href}" if href.startswith('/') else href
                )
                sitio_data["url_sitio"] = full_url
                try:
                    sitio_data["id"] = full_url.strip('/').split('/')[-1]
                except IndexError:
                    sitio_data["id"] = "N/A"
        else:
            sitio_data["nombre"] = nombre_element.inner_text().strip()