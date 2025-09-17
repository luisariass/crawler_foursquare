"""
Archivo principal que orquesta el proceso de scraping de Foursquare en paralelo.
Implementa manejo de estados, "Circuit Breaker" para bloqueos y rotación de User-Agents.
"""
import argparse
import sys
import os
import time
import random
from multiprocessing import Pool
from playwright.sync_api import sync_playwright

from .config.settings import Settings
from .core.auth import FoursquareAuth
from .core.scraper import FoursquareScraper
from .core.data_handler import DataHandler
from .utils.helpers import print_progress
from .utils.block_helper import handle_cooldown


def worker_process(task_info: dict):
    """
    Función ejecutada por cada proceso del pool. Es autónoma y robusta.
    """
    url = task_info['url']
    municipio = task_info['municipio']
    settings = Settings()
    
    # Antes de hacer nada, comprobar si el sistema está en modo de enfriamiento.
    handle_cooldown(settings)
    
    print(f"[WORKER] Iniciando para {municipio}")
    
    auth = FoursquareAuth()
    scraper = FoursquareScraper()
    context = None
    browser = None

    with sync_playwright() as p:
        try:
            browser = getattr(p, settings.BROWSER_TYPE).launch(headless=settings.HEADLESS)
            context = browser.new_context(
                user_agent=random.choice(settings.USER_AGENTS),
                viewport=random.choice(settings.VIEWPORTS)
            )
            page = context.new_page()

            if not auth.login(page):
                return {'municipio': municipio, 'url': url, 'sites': [], 'status': 'login_failed'}
            
            status, sitios = scraper.extract_sites(page, url, municipio)
            
            # Lógica del "Circuit Breaker"
            if status == 'generic_error':
                print(f"[BLOCK HANDLER] Worker para {municipio} activa el protocolo de parada.")
                # Crear el archivo de parada para alertar a otros workers.
                if not os.path.exists(settings.STOP_FILE_PATH):
                    with open(settings.STOP_FILE_PATH, 'w') as f:
                        f.write(str(time.time()))
                
                # Este worker también entra en pausa.
                handle_cooldown(settings)
                
                # Una vez que la pausa termina, este worker es responsable de limpiar el archivo.
                if os.path.exists(settings.STOP_FILE_PATH):
                    try:
                        os.remove(settings.STOP_FILE_PATH)
                        print("[BLOCK HANDLER] Protocolo de parada finalizado. Reanudando operaciones.")
                    except OSError:
                        pass # Otro proceso pudo haberlo borrado, está bien.

            return {'municipio': municipio, 'url': url, 'sites': sitios, 'status': status}
        
        except Exception as e:
            print(f"[ERROR][WORKER] Proceso para {municipio} falló con error inesperado: {e}")
            return {'municipio': municipio, 'url': url, 'sites': [], 'status': 'error'}
        
        finally:
            if context:
                context.close()
            if browser:
                browser.close()


class FoursquareScraperApp:
    """Aplicación principal para scraping de Foursquare"""
    
    def __init__(self):
        """Inicializa la aplicación"""
        self.settings = Settings()
        self.scraper = FoursquareScraper()
        self.data_handler = DataHandler()
    
    def run(self, start_index: int = 0, end_index: int = None, process_all: bool = False, csv_files: list = None) -> bool:
        """
        Ejecuta el proceso principal de scraping en paralelo.
        """
        # Limpiar el archivo de parada de una ejecución anterior fallida.
        if os.path.exists(self.settings.STOP_FILE_PATH):
            os.remove(self.settings.STOP_FILE_PATH)

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
            tasks = [{'url': info['url_municipio'], 'municipio': info['municipio']} for _, info in urls_to_process.iterrows()]
            total_tasks = len(tasks)
            print(f"[INFO] Se procesarán {total_tasks} URLs (índices {start_index} a {end_index}) usando {self.settings.PARALLEL_PROCESSES} procesos.")
            
            processed_count = 0
            with Pool(processes=self.settings.PARALLEL_PROCESSES) as pool:
                results_iterator = pool.imap_unordered(worker_process, tasks)

                for result in results_iterator:
                    processed_count += 1
                    print_progress(processed_count, total_tasks, "Procesando municipios")

                    status = result.get('status', 'error')
                    municipio = result.get('municipio', 'Desconocido')
                    url = result.get('url', '')
                    sitios_encontrados = result.get('sites', [])

                    if status == 'success':
                        stats = self.data_handler.add_sites(municipio, sitios_encontrados)
                        self.data_handler.update_processed_url(municipio, url, {
                            'sitios_encontrados': stats['new_sites'],
                            'sitios_duplicados_omitidos': stats['duplicates_omitted']
                        })
                        print(f"\n[INFO] {municipio}: {stats['new_sites']} sitios nuevos.")
                        self.data_handler.save_municipio_data(municipio)
                    
                    elif status == 'no_results':
                        print(f"\n[INFO] {municipio}: Zona sin sitios encontrados.")
                        self.data_handler.update_processed_url(municipio, url, {'sitios_encontrados': 0, 'error': 'Zona vacía'})
                    
                    elif status == 'generic_error':
                        print(f"\n[CRITICAL] {municipio}: Error genérico/bloqueo detectado. Se activó el protocolo de parada.")
                        self.data_handler.update_processed_url(municipio, url, {'sitios_encontrados': 0, 'error': 'Bloqueo del servidor'})
                    
                    else: # timeout, login_failed, error
                        print(f"\n[WARN] Tarea fallida para {municipio}. Estado: {status}")
                        self.data_handler.update_processed_url(municipio, url, {'sitios_encontrados': 0, 'error': f"Fallo del worker: {status}"})

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
            # Acceder a las estadísticas de sitios correctamente
            sites_stats = stats.get('sites_stats', {})
            total_sites = sites_stats.get('total_sites', 0)
            municipalities = sites_stats.get('municipalities', [])
            print(f"[INFO] Fin del programa. Total de sitios extraídos: {total_sites}")
            print(f"[INFO] Municipios procesados: {municipalities}")

def main():
    """Punto de entrada principal con argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(description='Scraper de Foursquare en Paralelo')
    parser.add_argument('--start', type=int, default=0, help='Índice de inicio para procesar URLs')
    parser.add_argument('--end', type=int, help='Índice final para procesar URLs')
    parser.add_argument('--all', action='store_true', help='Procesar todas las URLs')
    parser.add_argument('--csv', type=str, nargs='*', help='Ruta(s) de archivo(s) CSV a procesar')
    #arg = python -m model_sities.sities_fetcher --csv path
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