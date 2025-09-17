"""
Orquestador principal para la extracción de sitios turísticos de Foursquare.

Lee un archivo CSV con URLs de búsqueda por municipio, distribuye el trabajo
en paralelo y guarda los resultados de forma incremental en archivos JSON.
"""

import os
import signal
import random
import argparse
import pandas as pd
from multiprocessing import Pool, Event
from playwright.sync_api import sync_playwright

from .config.settings import Settings
from .core.auth import FoursquareAuth
from .core.scraper import FoursquareScraper
from .core.data_handler import DataHandler
from .utils.helpers import print_progress, current_timestamp

# --- Lógica para apagado controlado (graceful shutdown) ---
shutdown_event = Event()


def init_worker():
    """
    Inicializador para cada worker. Ignora la señal de KeyboardInterrupt.
    El proceso padre se encargará del apagado.
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def worker_process(task_info: dict):
    """
    Proceso worker que extrae sitios de una URL de búsqueda de Foursquare.
    """
    if shutdown_event.is_set():
        return {
            "status": "shutdown",
            "municipio": task_info['municipio'],
            "sites": []
        }

    settings = Settings()
    municipio = task_info['municipio']
    url = task_info['url_municipio']

    try:
        with sync_playwright() as p:
            browser = getattr(p, settings.BROWSER_TYPE).launch(
                headless=settings.HEADLESS
            )
            context = browser.new_context(
                user_agent=random.choice(settings.USER_AGENTS),
                viewport=random.choice(settings.VIEWPORTS)
            )
            page = context.new_page()

            auth = FoursquareAuth()
            if not auth.login(page):
                return {"status": "auth_error", "municipio": municipio, "sites": []}

            scraper = FoursquareScraper()
            status, sites = scraper.extract_sites(page, url, municipio)

            browser.close()
            return {"status": status, "municipio": municipio, "sites": sites}
    except Exception as e:
        print(f"[WORKER ERROR] Error fatal en {municipio}: {e}")
        return {"status": "worker_error", "municipio": municipio, "sites": []}


class FoursquareScraperApp:
    """Aplicación principal para coordinar el scraping de sitios."""

    def __init__(self):
        self.settings = Settings()
        self.data_handler = DataHandler()
        self._original_sigint_handler = None

    def _setup_signal_handler(self):
        """Configura el manejador para una interrupción controlada."""
        self._original_sigint_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        """Manejador de la señal de apagado (Ctrl+C)."""
        print("\n[SHUTDOWN] Señal de apagado recibida. Terminando procesos...")
        shutdown_event.set()
        if self._original_sigint_handler:
            signal.signal(signal.SIGINT, self._original_sigint_handler)

    def run(self, csv_path: str, start_index: int = 0, end_index: int = None):
        """Ejecuta el proceso de scraping en paralelo."""
        self._setup_signal_handler()

        try:
            df_urls = pd.read_csv(csv_path).iloc[start_index:end_index]
            tasks = [
                {'municipio': row['municipio'], 'url_municipio': row['url_municipio']}
                for _, row in df_urls.iterrows()
            ]
            total_tasks = len(tasks)
            processed_count = 0

            print(f"Iniciando scraping para {total_tasks} municipios.")

            with Pool(
                processes=self.settings.PARALLEL_PROCESSES,
                initializer=init_worker
            ) as pool:
                results_iterator = pool.imap_unordered(worker_process, tasks)

                for result in results_iterator:
                    if shutdown_event.is_set():
                        break

                    processed_count += 1
                    print_progress(
                        processed_count, total_tasks, "Procesando municipios"
                    )

                    status = result.get("status")
                    municipio = result.get("municipio")
                    sites_found = result.get("sites", [])

                    if status == "success" and sites_found:
                        # --- MODIFICACIÓN CLAVE ---
                        # Capturamos y usamos las estadísticas devueltas
                        stats = self.data_handler.add_sites(
                            municipio, sites_found
                        )
                        print(
                            f"\n[INFO] Municipio {municipio}: "
                            f"{stats['new_sites']} sitios nuevos, "
                            f"{stats['duplicates_omitted']} duplicados omitidos. "
                            f"Total actual: {stats['total_sites']}."
                        )
                        self.data_handler.update_processed_url(
                            municipio,
                            result.get('url', ''),
                            stats
                        )
                        self.data_handler.save_municipio_data(municipio)

                    elif status == "no_results":
                        print(f"\n[INFO] Municipio {municipio}: Sin resultados.")

                    elif status == "generic_error":
                        print(f"\n[BLOCK] Municipio {municipio}: Bloqueado.")
                        if not os.path.exists(self.settings.STOP_FILE_PATH):
                            with open(self.settings.STOP_FILE_PATH, 'w') as f:
                                f.write(f"Block detected at {current_timestamp()}")

                    else:
                        print(f"\n[WARN] Tarea fallida para {municipio}: {status}")

            if shutdown_event.is_set():
                pool.terminate()
                pool.join()

        finally:
            print("\n[INFO] Guardando datos finales antes de salir.")
            self.data_handler.save_all_data()
            stats = self.data_handler.get_statistics()
            sites_stats = stats.get('sites_stats', {})
            total_sites = sites_stats.get('total_sites', 0)
            print(f"Proceso finalizado. Total de sitios únicos: {total_sites}")


def main():
    """Punto de entrada principal para el scraper de sitios."""
    parser = argparse.ArgumentParser(
        description='Scraper de Sitios Turísticos de Foursquare'
    )
    parser.add_argument(
        '--csv', required=True, help='Ruta al archivo CSV con las URLs'
    )
    parser.add_argument(
        '--start', type=int, default=0, help='Índice de inicio (fila)'
    )
    parser.add_argument(
        '--end', type=int, default=None, help='Índice de fin (fila)'
    )
    args = parser.parse_args()

    app = FoursquareScraperApp()
    app.run(csv_path=args.csv, start_index=args.start, end_index=args.end)


if __name__ == "__main__":
    main()