"""
Orquestador para extraer perfiles de usuario, respetando el rate limit.

Lee sitios desde archivos JSON, distribuye el trabajo en paralelo y controla
el ritmo de las peticiones para no superar el límite por hora, utilizando
el DataHandler para una persistencia de datos robusta y organizada.
"""

import os
import glob
import json
import time
import argparse
import signal
import random
from typing import List, Dict
from multiprocessing import Pool

from .config.settings import Settings
from .core.data_handler import DataHandler
from .utils.helpers import print_progress
from .utils.worker_helper import init_worker, worker_users, shutdown_event


class ReviewerFetcher:
    """
    Aplicación para coordinar la extracción de perfiles de usuario con
    control de rate limit y persistencia centralizada.
    """

    def __init__(self):
        """Inicializa el orquestador y los controles de rate limit."""
        self.settings = Settings()
        self.data_handler = DataHandler()
        self._original_sigint_handler = None
        self.requests_this_window = 0
        self.window_start_time = time.time()
        self.block_cooldown_until = 0

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
                    sites = data.get("sitios_turisticos", []) #LEER LISTA DENTRO DEL JSON PARA SITIES
                    municipio_name = data.get("municipio", "desconocido") #DEFINIR QUE ATRIBUTO SE BUSCARA EN LA LISTA
                    for site in sites:
                        site['municipio'] = municipio_name #COMPARAR SI EL ATRIBUTO COINCIDE CON ALGUNO EN LA LISTA
                    all_sites.extend(sites)
            except Exception as e:
                print(f"[WARN] No se pudo leer el archivo {file_path}: {e}")
        return all_sites

    def _rate_limit_guard(self):
        """
        Verifica si se ha alcanzado el límite de peticiones. Si es así,
        pausa la ejecución hasta que se reinicie la ventana de tiempo.
        """
        if time.time() < self.block_cooldown_until:
            wait_time = self.block_cooldown_until - time.time()
            print(
                f"\n[COOLDOWN] Pausa por bloqueo previo. "
                f"Reanudando en {int(wait_time)} segundos..."
            )
            time.sleep(wait_time)
            self.block_cooldown_until = 0

        self.requests_this_window += 1
        elapsed_time = time.time() - self.window_start_time

        if elapsed_time > self.settings.RATE_LIMIT_WINDOW_SECONDS:
            print("\n[INFO] Reiniciando ventana de rate limit.")
            self.window_start_time = time.time()
            self.requests_this_window = 1
            return

        if self.requests_this_window >= self.settings.RATE_LIMIT_PER_HOUR:
            wait_time = (
                self.settings.RATE_LIMIT_WINDOW_SECONDS - elapsed_time
            )
            if wait_time > 0:
                print(
                    f"\n[RATE LIMIT] Límite alcanzado. "
                    f"Esperando {int(wait_time)} segundos..."
                )
                time.sleep(wait_time)

            self.window_start_time = time.time()
            self.requests_this_window = 1

    def _handle_result(self, result: Dict):
        """Procesa el resultado de un worker."""
        status = result.get("status")
        site_id = result.get("site_id", "ID no disponible")

        if status == "blocked":
            print(f"\n[BLOCK DETECTED] Tarea para sitio {site_id} fue bloqueada.")
            cooldown_period = random.uniform(120, 300)
            self.block_cooldown_until = time.time() + cooldown_period
            print(f"Iniciando cooldown de {int(cooldown_period)} segundos.")
            return

        if status == "success":
            users_found = result.get("users", [])
            if users_found:
                context_info = { #DICCCIONARIO QUE GUARDA EL CONTEXTO DENTRO DEL JSON 
                    "municipio": result.get("municipio", "desconocido"), #cambio de municipality a municipio
                    "site_id": site_id,
                    "site_name": result.get("site_name", "desconocido"),
                }
                stats = self.data_handler.add_reviewers(
                    context_info, users_found
                )
                print(
                    f"\n[INFO] Sitio {context_info['site_id']}: "
                    f"{stats['new_reviewers']} perfiles nuevos."
                )
                self.data_handler.save_reviewers_data(context_info)
            else:
                print(f"\n[INFO] Tarea para sitio {site_id}: éxito sin perfiles.")
        else:
            print(f"\n[INFO] Tarea para sitio {site_id}: {status}")

    def run(self, json_files: List[str]) -> None:
        """Ejecuta el proceso de scraping en paralelo con control de ritmo."""
        self._setup_signal_handler()

        try:
            sites_to_process = self._load_sites_from_json_files(json_files)
            if not sites_to_process:
                print("[ERROR] No se encontraron sitios para procesar.")
                return

            tasks = [{"site_data": site} for site in sites_to_process]
            total_tasks = len(tasks)
            print(f"Iniciando extracción para {total_tasks} sitios...")

            processed_count = 0
            with Pool(
                processes=self.settings.PARALLEL_PROCESSES,
                initializer=init_worker
            ) as pool:
                results_iterator = pool.imap_unordered(worker_users, tasks)

                for result in results_iterator:
                    if shutdown_event.is_set():
                        break

                    self._rate_limit_guard()
                    processed_count += 1
                    print_progress(
                        processed_count, total_tasks, "Extrayendo perfiles"
                    )
                    self._handle_result(result)

            if shutdown_event.is_set():
                pool.terminate()
                pool.join()

        finally:
            print("\n[INFO] Proceso de extracción finalizado.")
            self.data_handler.save_all_data()


def main():
    """Punto de entrada principal."""
    parser = argparse.ArgumentParser(
        description='Extractor de Perfiles de Usuario de Foursquare'
    )
    parser.add_argument(
        '--json',
        nargs='*',
        help='Ruta(s) a archivos JSON de sitios a procesar. '
             'Si no se pasa, procesa todos sitios_*.json en SITIES_OUTPUT_DIR.'
    )
    args = parser.parse_args()

    app = ReviewerFetcher()
    app.run(json_files=args.json)


if __name__ == "__main__":
    main()