"""
Clase principal para realizar scraping en Foursquare
"""
import pandas as pd
import numpy as np
import time
from typing import Dict, Any
from playwright.sync_api import Page
from ..config.settings import Settings
from ..utils.helpers import current_timestamp

class FoursquareScraper:
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
    
    def extract_sites(self, page, url, municipio="", max_retries=None, timeout=None) -> list:
        max_retries = max_retries or Settings.RETRIES
        timeout = timeout or Settings.TIMEOUT

        for attempt in range(1, max_retries + 1):
            try:
                print(f"Intento {attempt} de {max_retries} para {municipio} ({url})")
                page.goto(url, timeout=timeout)
                page.wait_for_timeout(int(np.random.uniform(2000, 4000)))
                # ... scraping real ...
                sitios_list = []
                page.wait_for_selector(Settings.SELECTORS['content_holder'], timeout=10000)
                self._load_all_results(page)
                sitios = page.query_selector_all(Settings.SELECTORS['content_holder'])
                for i, sitio in enumerate(sitios):
                    try:
                        site_data = self._extract_site_data(sitio, i + 1)
                        sitios_list.append(site_data)
                    except Exception:
                        print(f"[WARN] Sitio {i + 1} no procesado.")
                        continue
                return sitios_list
            except Exception as e:
                # Detecta bloqueo por Foursquare (puedes mejorar esta lógica)
                error_str = str(e).lower()
                if "429" in error_str or "captcha" in error_str or "blocked" in error_str:
                    print(f"[BLOCKED] Bloqueo detectado para {municipio} ({url})")
                    self.register_failed_municipality(municipio, url, "bloqueo")
                    break  # No reintentes si es bloqueo
                elif "timeout" in error_str:
                    print(f"[TIMEOUT] Timeout para {municipio} ({url})")
                    if attempt == max_retries:
                        self.register_failed_municipality(municipio, url, "timeout")
                else:
                    print(f"[ERROR] Error en intento {attempt} para {municipio} ({url}): {e}")
                if attempt < max_retries:
                    wait_time = 10 * attempt
                    print(f"Reintentando en {wait_time} segundos...")
                    time.sleep(wait_time)
        return []
    def register_failed_municipality(self, municipio, url, reason):
        failed_path = Settings.FAILED_MUNICIPALITIES_PATH
        with open(failed_path, "a", encoding="utf-8") as f:
            f.write(f"{municipio},{url},{reason}\n")
        print(f"[FAILED] Municipio registrado: {municipio} ({url}) - Razón: {reason}")
    
    def _load_all_results(self, page: Page) -> None:
        """Hace clic en 'Ver más resultados' hasta que no haya más"""
        try:
            while True:
                boton = page.query_selector(Settings.SELECTORS['more_results_button'])
                if boton:
                    boton.click()
                    page.wait_for_timeout(int(np.random.uniform(Settings.WAIT_SHORT_MIN, Settings.WAIT_SHORT_MAX)))
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
        
        # Extraer puntuación
        puntuacion_element = sitio.query_selector(Settings.SELECTORS['venue_score'])
        if puntuacion_element:
            sitio_data["puntuacion"] = puntuacion_element.inner_text().strip()
        
        # Extraer nombre
        nombre_element = sitio.query_selector(Settings.SELECTORS['venue_name'])
        if nombre_element:
            nombre_link = nombre_element.query_selector('a')
            if nombre_link:
                sitio_data["nombre"] = nombre_link.inner_text().strip()
                href = nombre_link.get_attribute('href')
                if href:
                    if href.startswith('/'):
                        sitio_data["url_sitio"] = f"{Settings.BASE_URL}{href}"
                    else:
                        sitio_data["url_sitio"] = href
            else:
                sitio_data["nombre"] = nombre_element.inner_text().strip()
        
        # Extraer categoría
        categoria_element = sitio.query_selector(Settings.SELECTORS['venue_category'])
        if categoria_element:
            categoria_text = categoria_element.inner_text().strip()
            sitio_data["categoria"] = categoria_text.replace('•', '').strip()
        
        # Extraer dirección
        direccion_element = sitio.query_selector(Settings.SELECTORS['venue_address'])
        if direccion_element:
            sitio_data["direccion"] = direccion_element.inner_text().strip()
        
        return sitio_data