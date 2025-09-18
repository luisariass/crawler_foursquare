"""
Orquestador principal para extraer perfiles de usuario de los sitios turísticos.
Lee sitios desde sitios_*.json, procesa en paralelo y guarda resultados incrementales.
"""

import os
import glob
import json
import random
import argparse
import signal
from typing import List, Dict, Any
from multiprocessing import Pool, Event
from playwright.sync_api import sync_playwright

from .config.settings import Settings
from .core.auth import FoursquareAuth
from .core.reviews import FoursquareReviewerScraper
from .core.data_handler import DataHandler
from .utils.helpers import print_progress, current_timestamp

# --- Lógica para apagado controlado (graceful shutdown) ---
shutdown_event = Event()


def init_worker():
    """Inicializador para que cada worker ignore la señal de interrupción."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def worker_process(task_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Proceso worker que extrae los perfiles de usuario de un sitio.
    """
    if shutdown_event.is_set():
        return {"status": "shutdown", "site_id": task_info.get('id', 'unknown')}

    settings = Settings()
    site_data = task_info['site_data']
    site_id = site_data.get('id', 'unknown_id')
    site_url = site_data.get('url_sitio', '')
    site_name = site_data.get('nombre', 'nombre_desconocido')

    if not site_url:
        return {"status": "no_url", "site_id": site_id, "reviews": []}

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

            # Si tu scraping requiere login, mantenlo; si no, podrías omitirlo.
            auth = FoursquareAuth()
            if not auth.login(page):
                return {"status": "auth_error", "site_id": site_id, "reviews": []}

            scraper = FoursquareReviewerScraper()
            status, reviews = scraper.extract_reviews(
                page, site_url, str(site_id), site_name
            )

            browser.close()
            return {
                "status": status,
                "site_id": str(site_id),
                "site_url": site_url,
                "reviews": reviews  # [{user_name, user_url}, ...]
            }
    except Exception as e:
        print(f"[WORKER ERROR] Error fatal en sitio {site_id}: {e}")
        return {"status": "worker_error", "site_id": site_id, "reviews": []}


class ReviewerFetcherApp:
    """Aplicación para coordinar la extracción."""

    def __init__(self):
        self.settings = Settings()
        self.data_handler = DataHandler()
        self._original_sigint_handler = None

    def _setup_signal_handler(self):
        self._original_sigint_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        print("\n[SHUTDOWN] Señal de apagado recibida. Terminando procesos...")
        shutdown_event.set()
        if self._original_sigint_handler:
            signal.signal(signal.SIGINT, self._original_sigint_handler)

    def _load_sites_from_json_files(
        self, json_files_paths: List[str]
    ) -> List[Dict]:
        """Carga todos los sitios desde los archivos sitios_*.json."""
        if not json_files_paths:
            json_pattern = os.path.join(
                self.settings.SITIES_OUTPUT_DIR, "sitios_*.json"
            )
            json_files_paths = glob.glob(json_pattern)

        all_sites = []
        print(f"Encontrados {len(json_files_paths)} archivos JSON a procesar.")

        for file_path in json_files_paths:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    sites = data.get("sitios_turisticos", [])
                    all_sites.extend(sites)
            except Exception as e:
                print(f"[WARN] No se pudo leer el archivo {file_path}: {e}")
        return all_sites

    def run(self, json_files: List[str]) -> None:
        """Ejecuta el proceso de scraping en paralelo."""
        self._setup_signal_handler()

        try:
            sites_to_process = self._load_sites_from_json_files(json_files)
            if not sites_to_process:
                print("[ERROR] No se encontraron sitios para procesar.")
                return

            tasks = [{"site_data": site} for site in sites_to_process]
            total_tasks = len(tasks)
            print(f"Iniciando extracción de perfiles para {total_tasks} sitios...")

            processed_count = 0
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
                        processed_count, total_tasks, "Extrayendo perfiles"
                    )

                    status = result.get("status")
                    site_id = result.get("site_id")
                    site_url = result.get("site_url", "")
                    users_found = result.get("reviews", [])  # [{user_name, user_url}, ...]

                    if status == "success" and users_found:
                        # Agrega usuarios al contexto 'site_id'
                        stats = self.data_handler.add_users(
                            site_id, users_found
                        )
                        # OJO: DataHandler devuelve new_users (no new_items)
                        self.data_handler.update_processed_user_context(
                            site_id, site_url, {
                                'usuarios_encontrados': stats.get('new_users', 0),
                                'duplicados_omitidos': stats.get('duplicates_omitted', 0)
                            }
                        )
                        print(
                            f"\n[INFO] Sitio {site_id}: "
                            f"{stats.get('new_users', 0)} usuarios nuevos."
                        )
                        self.data_handler.save_user_data(site_id)
                    else:
                        print(f"\n[INFO] Tarea para sitio {site_id}: {status}")

            if shutdown_event.is_set():
                pool.terminate()
                pool.join()

        finally:
            print("\n[INFO] Guardando datos finales antes de salir.")
            self.data_handler.save_all_data()
            stats = self.data_handler.get_statistics()
            users_stats = stats.get('users_stats', {})
            total_users = users_stats.get('total_users', 0)
            print(f"Proceso finalizado. Total de perfiles únicos: {total_users}")


def main():
    """Punto de entrada principal."""
    parser = argparse.ArgumentParser(
        description='Extractor de Perfiles de Usuario desde Foursquare'
    )
    parser.add_argument(
        '--json',
        nargs='*',
        help='Ruta(s) a archivos JSON de sitios a procesar. '
             'Si no se pasa, procesa todos sitios_*.json en SITIES_OUTPUT_DIR.'
    )
    args = parser.parse_args()

    app = ReviewerFetcherApp()
    app.run(json_files=args.json)


if __name__ == "__main__":
    main()
