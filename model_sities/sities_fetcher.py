"""
Archivo principal que orquesta el proceso de scraping de Foursquare
"""
import argparse
import sys
import numpy as np
from playwright.sync_api import sync_playwright

from config.settings import Settings
from core.auth import FoursquareAuth
from core.scraper import FoursquareScraper
from core.data_handler import DataHandler
from utils.helpers import print_progress

class FoursquareScraperApp:
    """Aplicación principal para scraping de Foursquare"""
    
    def __init__(self):
        """Inicializa la aplicación"""
        self.settings = Settings()
        self.auth = FoursquareAuth()
        self.scraper = FoursquareScraper()
        self.data_handler = DataHandler(output_dir=self.settings.OUTPUT_DIR)
    
    def run(self, start_index: int = 0, end_index: int = None, process_all: bool = False) -> bool:
        """
        Ejecuta el proceso principal de scraping
        """
        try:
            # Cargar todos los CSVs generados por caribbean_grid
            csv_files = self.settings.get_caribbean_csvs()
            if not csv_files:
                print("No se encontraron archivos CSV en caribbean_grid/data/. Abortando.")
                return False
            urls_data = self.scraper.load_urls_from_csvs(csv_files)
            
            if urls_data.empty:
                print("No se pudieron cargar URLs. Abortando.")
                return False
            
            # Establecer rango de URLs a procesar
            if end_index is None:
                end_index = len(urls_data) - 1  # Por defecto procesar todas las URLs

            total_urls = end_index - start_index + 1
            print(f"Se procesarán {total_urls} URLs (desde índice {start_index} hasta {end_index})")
            
            with sync_playwright() as p:
                # Iniciar navegador
                browser = getattr(p, self.settings.BROWSER_TYPE).launch(headless=self.settings.HEADLESS)
                page = browser.new_page()
                
                try:
                    # Realizar login
                    if not self.auth.login(page):
                        print("Error en el proceso de login. Abortando.")
                        return False
                    
                    # Procesar cada URL del CSV
                    for idx, (_, info) in enumerate(urls_data.iloc[start_index:end_index+1].iterrows()):
                        url = info['url_municipio']
                        municipio = info['municipio']
                        
                        print_progress(idx + 1, total_urls, "Procesando municipios")
                        print(f"Procesando municipio: {municipio} - {url}")
                        
                        # Extraer sitios turísticos de la página
                        #sitios_encontrados = self.scraper.extract_sites(page, url)
                        sitios_encontrados = self.scraper.extract_sites(page, url, municipio)
                        
                        if sitios_encontrados:
                            # Añadir sitios y obtener estadísticas
                            stats = self.data_handler.add_sites(municipio, sitios_encontrados, idx + 1)
                            
                            # Actualizar información de URL procesada
                            self.data_handler.update_processed_url(municipio, url, {
                                'sitios_encontrados': stats['new_sites'],
                                'sitios_duplicados_omitidos': stats['duplicates_omitted'],
                                'total_sitios_municipio': stats['total_sites']
                            })
                            
                            print(f"Se encontraron {stats['new_sites']} sitios nuevos en {municipio}")
                            print(f"Total de sitios únicos en {municipio}: {stats['total_sites']}")
                            
                            # Guardar datos del municipio inmediatamente
                            self.data_handler.save_municipio_data(municipio)
                            
                        else:
                            print(f"No se encontraron sitios en {municipio}")
                            self.data_handler.update_processed_url(municipio, url, {
                                'sitios_encontrados': 0,
                                'error': 'No se encontraron sitios'
                            })
                        
                        # Guardar resumen cada N URLs procesadas
                        if (idx + 1) % self.settings.SAVE_INTERVAL == 0:
                            print(f"Guardando resumen cada {self.settings.SAVE_INTERVAL} URLs procesadas")
                            self.data_handler.save_all_data()
                            page.wait_for_timeout(int(np.random.uniform(
                                self.settings.WAIT_EXTRA_LONG_MIN, 
                                self.settings.WAIT_EXTRA_LONG_MAX
                            )))
                        
                        # Espera entre URLs (comportamiento humano)
                        if idx < total_urls - 1:  # No esperar después de la última URL
                            page.wait_for_timeout(int(np.random.uniform(
                                self.settings.WAIT_LONG_MIN, 
                                self.settings.WAIT_LONG_MAX
                            )))
                    
                    return True
                    
                finally:
                    browser.close()
                    
        except Exception as e:
            print(f"Error general: {e}")
            return False
        finally:
            # Guardar todos los datos al finalizar
            print("Guardando datos finales")
            self.data_handler.save_all_data()
            
            # Mostrar estadísticas finales
            stats = self.data_handler.get_statistics()
            print(f"Fin del programa. Total de sitios extraídos: {stats['total_sites']}")
            print(f"Municipios procesados: {stats['municipalities']}")

def main():
    """Punto de entrada principal con argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(description='Scraper de Foursquare')
    parser.add_argument('--start', type=int, default=0, help='Índice de inicio para procesar URLs')
    parser.add_argument('--end', type=int, help='Índice final para procesar URLs')
    parser.add_argument('--all', action='store_true', help='Procesar todas las URLs')
    
    args = parser.parse_args()
    
    app = FoursquareScraperApp()
    success = app.run(args.start, args.end, args.all)
    
    if success:
        print("Scraping completado exitosamente!")
        sys.exit(0)
    else:
        print("El proceso de scraping falló.")
        sys.exit(1)

if __name__ == "__main__":
    main()