"""
Orquestador principal para extraer perfiles de usuario de los sitios turísticos.
Lee sitios desde sitios_*.json, procesa en paralelo y guarda resultados incrementales
en subdirectorios por municipio, cada archivo JSON por sitio.
"""

import os
import glob
import json
import argparse
import signal
from typing import List, Dict
from multiprocessing import Pool
from .config.settings import Settings
from .utils.helpers import print_progress, current_timestamp
from .utils.worker_helper import init_worker, worker_process, shutdown_event

class ReviewerFetcherApp:
    """Aplicación para coordinar la extracción y guardado por municipio/sitio."""

    def __init__(self):
        self.settings = Settings()
        self.output_dir = os.path.join(self.settings.REVIEWS_OUTPUT_DIR)
        os.makedirs(self.output_dir, exist_ok=True)
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

    def _save_users_json(self, municipio: str, site_id: str, site_name: str, users: List[Dict[str, str]]) -> None:
        """
        Guarda los usuarios encontrados en un archivo JSON dentro del subdirectorio
        del municipio, con nombre del sitio.
        """
        municipio_dir = os.path.join(self.output_dir, municipio)
        os.makedirs(municipio_dir, exist_ok=True)
        safe_site_name = site_name.replace(" ", "_").replace("/", "_")
        filename = f"reviewers_sitio_{site_id}_{safe_site_name}.json"
        path_out = os.path.join(municipio_dir, filename)
        data = {
            "site_id": site_id,
            "site_name": site_name,
            "municipio": municipio,
            "fecha_extraccion": current_timestamp(),
            "perfiles_usuarios": users
        }
        with open(path_out, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"[GUARDADO] {len(users)} usuarios en {path_out}")

    def run(self, json_files: List[str]) -> None:
        """Ejecuta el proceso de scraping en paralelo y guarda por municipio/sitio."""
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
                    users_found = result.get("users", [])
                    municipio = result.get("municipio", "municipio_desconocido")
                    site_name = result.get("site_name", "nombre_desconocido")

                    if status == "success" and users_found:
                        self._save_users_json(municipio, site_id, site_name, users_found)
                    else:
                        print(f"\n[INFO] Tarea para sitio {site_id}: {status}")

            if shutdown_event.is_set():
                pool.terminate()
                pool.join()

        finally:
            print("\n[INFO] Proceso finalizado.")

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