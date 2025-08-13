"""
Archivo principal que orquesta el proceso de scraping de Foursquare en paralelo.
"""
import argparse
import sys
from multiprocessing import Pool
from playwright.sync_api import sync_playwright

from .config.settings import Settings
from .core.auth import FoursquareAuth
from .core.scraper import FoursquareScraper
from .core.data_handler import DataHandler
from .utils.helpers import print_progress

def process_municipio_worker(task_info: dict):
    """
    Función "trabajadora" que se ejecuta en un proceso aislado.
    Inicia su propio navegador, hace login, extrae datos de una zona y se cierra.
    """
    municipio = task_info['municipio']
    url = task_info['url']
    
    print(f"[WORKER] Iniciando proceso para: {municipio}")
    
    settings = Settings()
    auth = FoursquareAuth()
    scraper = FoursquareScraper()
    
    with sync_playwright() as p:
        browser = getattr(p, settings.BROWSER_TYPE).launch(headless=settings.HEADLESS)
        page = browser.new_page()
        try:
            if not auth.login(page):
                print(f"[ERROR] Login fallido para el worker de {municipio}.")
                return {'municipio': municipio, 'url': url, 'sites': [], 'status': 'login_failed'}
            
            # Usamos la función simplificada del scraper
            sitios_encontrados = scraper.extract_sites_from_zone(page, url, municipio)
            
            return {'municipio': municipio, 'url': url, 'sites': sitios_encontrados, 'status': 'success'}
        
        except Exception as e:
            print(f"[ERROR] Worker para {municipio} falló con un error inesperado: {e}")
            return {'municipio': municipio, 'url': url, 'sites': [], 'status': 'failed'}
        finally:
            browser.close()


class FoursquareScraperApp:
    """Aplicación principal que gestiona el pool de procesos de scraping."""
    
    def __init__(self):
        """Inicializa la aplicación"""
        self.settings = Settings()
        self.scraper = FoursquareScraper()
        self.data_handler = DataHandler(output_dir=self.settings.SITIES_OUTPUT_DIR)
    
    def run(self, csv_files: list = None) -> bool:
        """
        Ejecuta el proceso principal de scraping distribuyendo el trabajo en paralelo.
        """
        try:
            if not csv_files:
                csv_files = self.settings.get_caribbean_csvs()
                if not csv_files:
                    print("[ERROR] No se encontraron archivos CSV. Abortando.")
                    return False

            print(f"[INFO] Archivos CSV a procesar: {len(csv_files)}")
            urls_data = self.scraper.load_urls_from_csvs(csv_files)
            
            if urls_data.empty:
                print("[ERROR] No se pudieron cargar URLs. Abortando.")
                return False
            
            tasks = [
                {'municipio': info['municipio'], 'url': info['url_municipio']}
                for _, info in urls_data.iterrows()
            ]
            total_tasks = len(tasks)
            processed_count = 0
            print(f"[INFO] Se procesarán {total_tasks} municipios en paralelo con {self.settings.PARALLEL_PROCESSES} procesos.")

            with Pool(processes=self.settings.PARALLEL_PROCESSES) as pool:
                results_iterator = pool.imap_unordered(process_municipio_worker, tasks)
                
                for result in results_iterator:
                    processed_count += 1
                    print_progress(processed_count, total_tasks, "Procesando y guardando municipios")
                    
                    if result and result['status'] == 'success':
                        municipio = result['municipio']
                        sitios_encontrados = result['sites']
                        if sitios_encontrados:
                            stats = self.data_handler.add_sites(municipio, sitios_encontrados, processed_count)
                            self.data_handler.update_processed_url(municipio, result['url'], {
                                'sitios_encontrados': stats['new_sites'],
                                'sitios_duplicados_omitidos': stats['duplicates_omitted'],
                                'total_sitios_municipio': stats['total_sites']
                            })
                            self.data_handler.save_municipio_data(municipio)
                        else:
                            self.data_handler.update_processed_url(municipio, result['url'], {
                                'sitios_encontrados': 0, 'error': 'No se encontraron sitios'
                            })
                        # Guardar siempre el resumen para registrar el progreso
                        self.data_handler.save_all_data()
                            
                    else:
                        municipio_fallido = result.get('municipio', 'desconocido')
                        url_fallida = result.get('url', 'desconocida')
                        estado_fallo = result.get('status', 'desconocido')
                        print(f"\n[WARN] La tarea para {municipio_fallido} falló con estado: {estado_fallo}")
                        self.data_handler.update_processed_url(municipio_fallido, url_fallida, {
                            'sitios_encontrados': 0, 'error': f'Fallo del worker: {estado_fallo}'
                        })
                        self.data_handler.save_all_data()

            return True
                    
        except Exception as e:
            print(f"\n[ERROR] Error general en el orquestador principal: {e}")
            return False
        finally:
            print("\n[INFO] Guardando resumen final de la extracción.")
            self.data_handler.save_all_data()
            stats = self.data_handler.get_statistics()
            print(f"[INFO] Fin del programa. Total de sitios únicos extraídos: {stats['total_sites']}")
            print(f"[INFO] Total de municipios procesados: {stats['municipalities']}")

def main():
    """Punto de entrada principal con argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(description='Scraper de Foursquare en Paralelo')
    parser.add_argument('--csv', type=str, nargs='*', help='Ruta(s) de archivo(s) CSV a procesar. Si no se especifica, se usan todos los de la carpeta de datos.')

    args = parser.parse_args()
    
    app = FoursquareScraperApp()
    success = app.run(csv_files=args.csv)
    
    if success:
        print("\n[INFO] Scraping completado exitosamente!")
        sys.exit(0)
    else:
        print("\n[ERROR] El proceso de scraping falló.")
        sys.exit(1)

if __name__ == "__main__":
    main()