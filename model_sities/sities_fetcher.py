"""
Orquestador principal para la extracción de sitios turísticos de Foursquare.

Lee un archivo CSV con URLs de búsqueda por municipio, distribuye el trabajo
en paralelo y guarda los resultados de forma incremental en archivos JSON.
"""

import os
import signal
import argparse
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from .config.settings import Settings
from .core.data_handler import DataHandler
from .utils.helpers import print_progress, current_timestamp, save_progress, load_progress
from .utils.worker_helper import worker_sities, shutdown_event


class SitiesFetcher:
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

    def run(self, csv_path: str, start_index: int = 0, end_index: Optional[int] = None):
        """Ejecuta el proceso de scraping en paralelo con reanudación de progreso."""
        self._setup_signal_handler()

        try:
            self._load_initial_data()
            start_index = self._get_resume_index(csv_path, start_index)
            df_urls = pd.read_csv(csv_path)
            df_urls = df_urls.iloc[start_index:end_index]
            tasks = [
                {
                    'municipio': row['municipio'],
                    'url_municipio': row['url_municipio']
                }
                for _, row in df_urls.iterrows()
            ]
            total_tasks = len(tasks)
            print(f"Iniciando scraping para {total_tasks} zonas.")

            with ThreadPoolExecutor(
                max_workers=self.settings.PARALLEL_PROCESSES
            ) as pool:
                results_iterator = pool.map(worker_sities, tasks)
                self._process_results(
                    results_iterator, start_index, total_tasks, csv_path
                )
        finally:
            self._finalize_data()

    def _load_initial_data(self) -> None:
        """Carga los datos existentes y muestra estadísticas iniciales."""
        self.data_handler.load_data_sities()
        initial_stats = self.data_handler.get_statistics()
        initial_sites = initial_stats.get('sites_stats', {}).get('total_sites', 0)
        print(f"[INFO] Se cargaron {initial_sites} sitios existentes.")

    def _get_resume_index(self, csv_path: str, start_index: int) -> int:
        """Determina el índice de inicio considerando el progreso guardado."""
        progreso = load_progress()
        if progreso and progreso.get("csv_path") == csv_path:
            resume_index = progreso.get("idx_actual", 0) + 1
            if resume_index > start_index:
                start_index = resume_index
            print(f"[INFO] Reanudando desde índice {start_index}")
        else:
            print(f"[INFO] Iniciando desde índice {start_index}")
        return start_index

    def _process_results(
        self,
        results_iterator,
        start_index: int,
        total_tasks: int,
        csv_path: str
    ) -> None:
        """Procesa los resultados del scraping y maneja el guardado incremental."""
        processed_count = 0
        for idx, result in enumerate(results_iterator, start=start_index):
            if shutdown_event.is_set():
                break

            processed_count += 1
            print_progress(
                processed_count, total_tasks, "Procesando municipios"
            )

            self._handle_result(result, csv_path, idx)

    def _handle_result(self, result: dict, csv_path: str, idx: int) -> None:
        """Maneja el resultado de cada tarea individual."""
        status = result.get("status")
        municipio = result.get("municipio")
        sites_found = result.get("sites", [])

        if status == "success" and sites_found:
            stats = self.data_handler.add_sites(
                municipio, sites_found
            )
            print(
                f"\n[INFO] Municipio {municipio}: "
                f"{stats['new_sites']} sitios nuevos, "
                f"{stats['duplicates_omitted']} duplicados omitidos. "
                f"Total actual: {stats['total_items']}."
            )
            self.data_handler.save_sites_data(municipio)

        elif status == "no_results":
            print(f"\n[INFO] Municipio {municipio}: Sin resultados.")

        elif status == "generic_error":
            print(f"\n[BLOCK] Municipio {municipio}: Bloqueado.")
            if not os.path.exists(self.settings.STOP_FILE_PATH):
                with open(self.settings.STOP_FILE_PATH, 'w') as f:
                    f.write(f"Block detected at {current_timestamp()}")

        else:
            print(f"\n[WARN] Tarea fallida para {municipio}: {status}")

        save_progress(idx, csv_path, [])

    def _finalize_data(self) -> None:
        """Guarda los datos finales y muestra estadísticas al finalizar."""
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
        "python -m model_sities.sities_fetcher --csv data/municipios_urls.csv"
    )
    parser.add_argument(
        '--start', type=int, default=0, help='Índice de inicio (fila)'
        "python -m model_sities.sities_fetcher --start 100"
    )
    parser.add_argument(
        '--end', type=int, default=None, help='Índice de fin (fila)'
    )
    args = parser.parse_args()

    app = SitiesFetcher()
    app.run(csv_path=args.csv, start_index=args.start, end_index=args.end)


if __name__ == "__main__":
    main()
