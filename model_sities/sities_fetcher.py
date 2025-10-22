"""
Orquestador principal para la extracción de sitios turísticos.
Versión actualizada con MongoDB Atlas y soporte para directorios.
"""
import os
import signal
import argparse
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, Any, List
from pathlib import Path

from .config.settings import Settings
from .core.data_handler import MongoDataHandler
from .utils.helpers import print_progress
from .utils.worker_helper import worker_sities, shutdown_event


class SitiesFetcher:
    """Aplicación principal para coordinar el scraping con MongoDB Atlas."""

    def __init__(self) -> None:
        self.settings = Settings()
        self.data_handler = MongoDataHandler()
        self._setup_signal_handler()

    def _setup_signal_handler(self) -> None:
        """Configura manejadores de señales para apagado controlado."""
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum: int, frame: Any) -> None:
        """Maneja señales de apagado de forma controlada."""
        print(f"\n[SHUTDOWN] Señal {signum} recibida. Apagado en curso...")
        shutdown_event.set()

    def _get_csv_files(self, path: str) -> List[str]:
        """Obtiene lista de archivos CSV desde un directorio o archivo."""
        path_obj = Path(path)
        if path_obj.is_file() and path_obj.suffix == '.csv':
            return [str(path_obj)]
        elif path_obj.is_dir():
            return sorted([str(f) for f in path_obj.glob('*.csv')])
        raise ValueError(f"Ruta inválida o sin archivos CSV: {path}")

    def run(
        self,
        csv_path: str,
        start_index: int = 0,
        end_index: Optional[int] = None
    ) -> None:
        """Ejecuta el scraping para uno o múltiples CSVs."""
        csv_files = self._get_csv_files(csv_path)
        print(f"[INFO] Se procesarán {len(csv_files)} archivo(s) CSV.")

        for csv_file in csv_files:
            if shutdown_event.is_set():
                print("[SHUTDOWN] Proceso detenido antes de iniciar nuevo CSV.")
                break
            print(f"\n[INFO] Procesando archivo: {os.path.basename(csv_file)}")
            self._process_single_csv(csv_file, start_index, end_index)

        self._finalize_data()

    def _process_single_csv(
        self,
        csv_path: str,
        start_index: int = 0,
        end_index: Optional[int] = None
    ) -> None:
        """
        Procesa un CSV usando un patrón robusto con as_completed para
        evitar bloqueos y manejar timeouts individuales.
        """
        self._load_initial_data()
        resume_index = self._get_resume_index(csv_path, start_index)

        df_urls = pd.read_csv(csv_path)
        df_to_process = df_urls.iloc[resume_index:end_index]
        tasks = df_to_process[
            ['municipio', 'departamento', 'url_municipio']
        ].to_dict('records')
        total_tasks = len(tasks)

        if not tasks:
            print(f"[INFO] No hay tareas nuevas para procesar en {csv_path}")
            return

        print(f"[INFO] {total_tasks} tareas para procesar.")
        completed_count = 0

        with ThreadPoolExecutor(
            max_workers=self.settings.PARALLEL_PROCESSES
        ) as executor:
            future_to_task = {
                executor.submit(worker_sities, task): (i + resume_index, task)
                for i, task in enumerate(tasks)
            }

            for future in as_completed(future_to_task):
                if shutdown_event.is_set():
                    break

                current_index, task_info = future_to_task[future]
                try:
                    result = future.result(timeout=600)
                    self._handle_result(result)
                    # Guardar progreso después de un manejo exitoso
                    self.data_handler.save_progress(
                        'sities', os.path.basename(csv_path), current_index
                    )
                except Exception as exc:
                    print(
                        f"\n[ERROR] Tarea para {task_info['municipio']} "
                        f"(índice {current_index}) falló: {exc}"
                    )
                finally:
                    completed_count += 1
                    print_progress(
                        completed_count, total_tasks, "Procesando"
                    )

    def _load_initial_data(self) -> None:
        """Carga datos iniciales de MongoDB."""
        self.data_handler.load_data_sities()

    def _get_resume_index(self, csv_path: str, start_index: int) -> int:
        """
        Obtiene el índice desde donde reanudar, usando el nombre del archivo
        como clave única para la persistencia.
        """
        csv_filename = os.path.basename(csv_path)
        progress = self.data_handler.load_progress('sities', csv_filename)

        if progress and progress.get('idx_actual') is not None:
            resume_idx = progress['idx_actual'] + 1
            if resume_idx > start_index:
                print(f"[RESUME] Reanudando desde el índice {resume_idx}")
                return resume_idx
        print(f"[INFO] Iniciando desde el índice {start_index}")
        return start_index

    def _handle_result(self, result: Dict[str, Any]) -> None:
        """Maneja el resultado de una tarea individual."""
        status = result.get("status")
        municipio = result.get("municipio")
        departamento = result.get("departamento")
        sites = result.get("sites", [])

        if status == "success" and sites:
            stats = self.data_handler.add_sites(municipio, departamento, sites)
            print(
                f"\n[DB] {municipio}: {stats['new_sites']} nuevos, "
                f"{stats['duplicates_omitted']} duplicados."
            )
        elif status == "no_results":
            print(f"\n[INFO] {municipio}: Sin resultados.")
        else:
            print(f"\n[WARN] {municipio}: Tarea finalizada con estado '{status}'.")

    def _finalize_data(self) -> None:
        """Finaliza y guarda datos pendientes."""
        print("\n[FINALIZE] Finalizando proceso y actualizando estadísticas.")
        self.data_handler.refresh_stats()


def main() -> None:
    """Punto de entrada principal."""
    parser = argparse.ArgumentParser(
        description='Scraper de Sitios Turísticos con MongoDB Atlas'
    )
    parser.add_argument(
        '--csv', required=True,
        help='Ruta al archivo CSV o directorio con archivos CSV.'
    )
    parser.add_argument(
        '--start', type=int, default=0, help='Índice de inicio.'
    )
    parser.add_argument(
        '--end', type=int, default=None, help='Índice final (opcional).'
    )
    args = parser.parse_args()

    try:
        fetcher = SitiesFetcher()
        fetcher.run(
            csv_path=args.csv, start_index=args.start, end_index=args.end
        )
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Proceso interrumpido por el usuario.")
    except Exception as e:
        print(f"\n[FATAL] Error no controlado: {e}")


if __name__ == "__main__":
    main()