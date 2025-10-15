"""
Orquestador principal para la extracción de sitios turísticos.
Versión actualizada con MongoDB Atlas.
"""

import os
import signal
import argparse
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, Any, Iterator

from .config.settings import Settings
from .core.data_handler import MongoDataHandler
from .config.database import MongoDBConfig
from .utils.helpers import print_progress, current_timestamp
from .utils.worker_helper import worker_sities, shutdown_event


class SitiesFetcher:
    """Aplicación principal para coordinar el scraping con MongoDB Atlas."""
    
    def __init__(self) -> None:
        self.settings = Settings()
        self.data_handler = MongoDataHandler()
        self._original_sigint_handler = None
    
    def _setup_signal_handler(self) -> None:
        """Configura el manejador para Ctrl+C."""
        self._original_sigint_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self._handle_shutdown)
    
    def _handle_shutdown(self, signum: int, frame: Any) -> None:
        """Manejador de señal de apagado."""
        print("\n[SHUTDOWN] Señal de apagado recibida. Terminando hilos...")
        shutdown_event.set()
        if self._original_sigint_handler:
            signal.signal(signal.SIGINT, self._original_sigint_handler)
    
    def run(
        self,
        csv_path: str,
        start_index: int = 0,
        end_index: Optional[int] = None
    ) -> None:
        """Ejecuta el proceso de scraping en paralelo."""
        self._setup_signal_handler()
        
        try:
            self._load_initial_data()
            start_index = self._get_resume_index(csv_path, start_index)
            
            df_urls = pd.read_csv(csv_path)
            df_to_process = df_urls.iloc[start_index:end_index]
            tasks = df_to_process[
                ['municipio', 'url_municipio']
            ].to_dict('records')
            total_tasks = len(tasks)
            
            if total_tasks == 0:
                print("[INFO] No hay nuevas zonas para procesar.")
                return
            
            print(f"Iniciando scraping para {total_tasks} zonas.")
            
            with ThreadPoolExecutor(
                max_workers=self.settings.PARALLEL_PROCESSES
            ) as executor:
                results_iterator = executor.map(worker_sities, tasks)
                self._process_results(
                    results_iterator,
                    start_index,
                    total_tasks,
                    csv_path
                )
        finally:
            self._finalize_data()
    
    def _load_initial_data(self) -> None:
        """Carga los datos existentes y muestra estadísticas."""
        self.data_handler.load_data_sities()
        stats = self.data_handler.get_statistics()
        total_sites = stats.get('sites_stats', {}).get('total_sites', 0)
        print(f"[INFO] Sitios existentes en MongoDB: {total_sites}")
    
    def _get_resume_index(self, csv_path: str, start_index: int) -> int:
        """Determina el índice de inicio considerando progreso guardado."""
        progress = self.data_handler.load_progress('sities_fetcher')
        
        if progress and progress.get("csv_path") == csv_path:
            resume_index = progress.get("idx_actual", -1) + 1
            if resume_index > start_index:
                print(f"[INFO] Reanudando desde el índice {resume_index}")
                return resume_index
        
        print(f"[INFO] Iniciando desde el índice {start_index}")
        return start_index
    
    def _process_results(
        self,
        results_iterator: Iterator[Dict[str, Any]],
        start_index: int,
        total_tasks: int,
        csv_path: str
    ) -> None:
        """Procesa los resultados del scraping."""
        for i, result in enumerate(results_iterator):
            if shutdown_event.is_set():
                print("[INFO] Deteniendo procesamiento de resultados.")
                break
            
            current_index_in_df = start_index + i
            print_progress(i + 1, total_tasks, "Procesando municipios")
            self._handle_result(result)
            self.data_handler.save_progress(
                'sities_fetcher',
                csv_path,
                current_index_in_df
            )
    
    def _handle_result(self, result: Dict[str, Any]) -> None:
        """Maneja el resultado de una tarea individual."""
        status = result.get("status")
        municipio = result.get("municipio")
        sites_found = result.get("sites", [])
        
        if status == "success" and sites_found:
            stats = self.data_handler.add_sites(municipio, sites_found)
            print(
                f"\n[INFO] Municipio {municipio}: "
                f"{stats['new_sites']} sitios nuevos, "
                f"{stats['duplicates_omitted']} duplicados omitidos. "
                f"Total: {stats['total_items']}."
            )
        elif status == "no_results":
            print(f"\n[INFO] Municipio {municipio}: Sin resultados.")
        elif status == "generic_error":
            print(f"\n[BLOCK] Municipio {municipio}: Bloqueado.")
            if not os.path.exists(self.settings.STOP_FILE_PATH):
                with open(self.settings.STOP_FILE_PATH, 'w') as f:
                    f.write(f"Block detected at {current_timestamp()}")
        else:
            print(f"\n[WARN] Tarea fallida para {municipio}: {status}")
    
    def _finalize_data(self) -> None:
        """Finaliza el proceso y muestra estadísticas."""
        print("\n[INFO] Finalizando proceso de scraping.")
        stats = self.data_handler.get_statistics()
        total_sites = stats.get('sites_stats', {}).get('total_sites', 0)
        print(
            f"Proceso finalizado. "
            f"Total de sitios únicos en MongoDB: {total_sites}"
        )
        MongoDBConfig.close_connection()


def main() -> None:
    """Punto de entrada principal."""
    parser = argparse.ArgumentParser(
        description='Scraper de Sitios Turísticos con MongoDB Atlas'
    )
    parser.add_argument(
        '--csv',
        required=True,
        help='Ruta al archivo CSV con las URLs.'
    )
    parser.add_argument(
        '--start',
        type=int,
        default=0,
        help='Índice de inicio en el CSV.'
    )
    parser.add_argument(
        '--end',
        type=int,
        default=None,
        help='Índice de fin en el CSV.'
    )
    args = parser.parse_args()
    
    app = SitiesFetcher()
    app.run(csv_path=args.csv, start_index=args.start, end_index=args.end)


if __name__ == "__main__":
    main()