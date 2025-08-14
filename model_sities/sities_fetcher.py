"""
Archivo principal que orquesta el proceso de scraping de Foursquare en paralelo.
"""
import argparse
import sys
import os
from multiprocessing import Pool
from playwright.sync_api import sync_playwright

from .config.settings import Settings
from .core.auth import FoursquareAuth
from .core.scraper import FoursquareScraper
from .core.data_handler import DataHandler
from .utils.helpers import print_progress

def worker_process(task_info: dict):
    """
    Función ejecutada por cada proceso del pool.
    Es autónoma: inicia su propio navegador, hace login, extrae datos y se cierra.
    """
    url = task_info['url']
    municipio = task_info['municipio']
    
    print(f"[WORKER] Iniciando para {municipio}")
    
    # Cada worker necesita sus propias instancias
    settings = Settings()
    auth = FoursquareAuth()
    scraper = FoursquareScraper()

    with sync_playwright() as p:
        browser = getattr(p, settings.BROWSER_TYPE).launch(headless=settings.HEADLESS)
        page = browser.new_page()
        try:
            if not auth.login(page):
                print(f"[ERROR][WORKER] Login fallido para {municipio}")
                return {'municipio': municipio, 'url': url, 'sites': [], 'status': 'login_failed'}
            
            sitios = scraper.extract_sites(page, url, municipio)
            return {'municipio': municipio, 'url': url, 'sites': sitios, 'status': 'success'}
        except Exception as e:
            print(f"[ERROR][WORKER] Proceso para {municipio} falló: {e}")
            return {'municipio': municipio, 'url': url, 'sites': [], 'status': 'failed'}
        finally:
            browser.close()


class FoursquareScraperApp:
    """Aplicación principal para scraping de Foursquare"""
    
    def __init__(self):
        """Inicializa la aplicación"""
        self.settings = Settings()
        self.scraper = FoursquareScraper()
        self.data_handler = DataHandler(output_dir=self.settings.SITIES_OUTPUT_DIR)
    
    def run(self, start_index: int = 0, end_index: int = None, process_all: bool = False, csv_files: list = None) -> bool:
        """
        Ejecuta el proceso principal de scraping en paralelo.
        """
        try:
            if csv_files:
                csv_files = [f for f in csv_files if os.path.isfile(f)]
                if not csv_files:
                    print("[ERROR] No se encontraron los archivos CSV especificados.")
                    return False
            else:
                csv_files = self.settings.get_caribbean_csvs()
                if not csv_files:
                    print("[ERROR] No se encontraron archivos CSV.")
                    return False

            print(f"[INFO] Archivos CSV a procesar: {len(csv_files)}")
            urls_data = self.scraper.load_urls_from_csvs(csv_files)
            
            if urls_data.empty:
                print("[ERROR] No se pudieron cargar URLs.")
                return False
            
            if end_index is None or process_all:
                end_index = len(urls_data) - 1

            urls_to_process = urls_data.iloc[start_index:end_index+1]
            tasks = [
                {'url': info['url_municipio'], 'municipio': info['municipio']}
                for _, info in urls_to_process.iterrows()
            ]
            total_tasks = len(tasks)
            print(f"[INFO] Se procesarán {total_tasks} URLs (índices {start_index} a {end_index}) usando {self.settings.PARALLEL_PROCESSES} procesos.")
            
            processed_count = 0
            with Pool(processes=self.settings.PARALLEL_PROCESSES) as pool:
                # imap_unordered procesa los resultados a medida que están listos
                results_iterator = pool.imap_unordered(worker_process, tasks)

                for result in results_iterator:
                    processed_count += 1
                    print_progress(processed_count, total_tasks, "Procesando municipios")

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
                            print(f"\n[INFO] {municipio}: {stats['new_sites']} sitios nuevos, {stats['duplicates_omitted']} duplicados omitidos.")
                            self.data_handler.save_municipio_data(municipio)
                        else:
                            print(f"\n[WARN] {municipio}: No se encontraron sitios.")
                            self.data_handler.update_processed_url(municipio, result['url'], {
                                'sitios_encontrados': 0,
                                'error': 'Zona vacia o sin sitios'
                            })
                    else:
                        municipio_fallido = result.get('municipio', 'Desconocido')
                        print(f"\n[WARN] Tarea fallida para {municipio_fallido}. Estado: {result.get('status', 'error')}")
                        self.data_handler.update_processed_url(municipio_fallido, result.get('url', ''), {
                            'sitios_encontrados': 0,
                            'error': f"Fallo del worker: {result.get('status', 'error')}"
                        })

                    # Guardado incremental del resumen general
                    if processed_count % self.settings.SAVE_INTERVAL == 0 or processed_count == total_tasks:
                        print(f"\n[INFO] Guardando resumen tras {processed_count} URLs procesadas.")
                        self.data_handler.save_all_data()
            
            return True
                    
        except Exception as e:
            print(f"[ERROR] Error general en el orquestador: {e}")
            return False
        finally:
            print("\n[INFO] Guardando datos finales.")
            self.data_handler.save_all_data()
            stats = self.data_handler.get_statistics()
            print(f"[INFO] Fin del programa. Total de sitios extraídos: {stats['total_sites']}")
            print(f"[INFO] Municipios procesados: {stats['municipalities']}")

def main():
    """Punto de entrada principal con argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(description='Scraper de Foursquare en Paralelo')
    parser.add_argument('--start', type=int, default=0, help='Índice de inicio para procesar URLs')
    parser.add_argument('--end', type=int, help='Índice final para procesar URLs')
    parser.add_argument('--all', action='store_true', help='Procesar todas las URLs')
    parser.add_argument('--csv', type=str, nargs='*', help='Ruta(s) de archivo(s) CSV a procesar')

    args = parser.parse_args()
    
    app = FoursquareScraperApp()
    success = app.run(args.start, args.end, args.all, csv_files=args.csv)
    
    if success:
        print("[INFO] Scraping completado exitosamente!")
        sys.exit(0)
    else:
        print("[ERROR] El proceso de scraping falló.")
        sys.exit(1)

if __name__ == "__main__":
    main()