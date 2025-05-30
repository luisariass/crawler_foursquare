"""
Clase principal para realizar scraping en Foursquare
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from playwright.sync_api import Page
from config.settings import Settings
from utils.helpers import current_timestamp

class FoursquareScraper:
    """Realiza el scraping de sitios turísticos en Foursquare"""
    
    def __init__(self):
        """Inicializa el scraper"""
        self.settings = Settings()
    
    def load_urls_from_csv(self, csv_file: str = None) -> pd.DataFrame:
        """Carga las URLs desde un archivo CSV"""
        csv_path = csv_file or self.settings.CSV_URLS_FILE
        try:
            df = pd.read_csv(csv_path, sep=',')
            print(f"Cargadas {len(df)} URLs desde {csv_path}")
            return df
        except Exception as e:
            print(f"Error al cargar el archivo CSV {csv_path}: {e}")
            return pd.DataFrame()
    
    def extract_sites(self, page: Page, url: str) -> List[Dict[str, Any]]:
        """Extrae sitios turísticos de una página de Foursquare"""
        try:
            # Navegar a la URL
            page.goto(url)
            page.wait_for_timeout(int(np.random.uniform(Settings.WAIT_MEDIUM_MIN, Settings.WAIT_MEDIUM_MAX)))
            
            # Lista para almacenar los sitios turísticos
            sitios_list = []
            
            # Esperar a que carguen los elementos
            page.wait_for_selector(Settings.SELECTORS['content_holder'], timeout=10000)
            
            # Cargar todos los resultados disponibles
            self._load_all_results(page)
            
            # Buscar todos los elementos que contienen información sobre sitios turísticos
            sitios = page.query_selector_all(Settings.SELECTORS['content_holder'])
            
            # Extraer información de cada sitio
            for i, sitio in enumerate(sitios):
                try:
                    site_data = self._extract_site_data(sitio, i + 1)
                    sitios_list.append(site_data)
                except Exception as e:
                    print(f"Error al procesar sitio {i + 1}: {e}")
                    continue
            
            return sitios_list
            
        except Exception as e:
            print(f"Error al acceder a la página {url}: {e}")
            return []
    
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
            # Si hay algún error, simplemente continuar con los resultados cargados
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
                # Extraer URL del sitio
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