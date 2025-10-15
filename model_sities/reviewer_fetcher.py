"""
Orquestador para extraer perfiles de usuario usando MongoDB.
Lee sitios desde MongoDB en lugar de archivos JSON.
"""

import time
import argparse
import signal
import random
from typing import List, Dict
from multiprocessing import Pool

from .config.settings import Settings
from .core.data_handler import MongoDataHandler
from .config.database import MongoDBConfig
from .utils.helpers import print_progress
from .utils.worker_helper import init_worker, worker_users, shutdown_event


class ReviewerFetcher:
    """
    Aplicación para coordinar la extracción de perfiles de usuario con
    control de rate limit y persistencia en MongoDB.
    """
    
    def __init__(self):
        """Inicializa el orquestador y los controles de rate limit."""
        self.settings = Settings()
        self.data_handler = MongoDataHandler()
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
    
    def _load_sites_from_mongodb(self) -> List[Dict]:
        """
        Carga todos los sitios desde MongoDB para procesar sus reviewers.
        """
        print("[INFO] Cargando sitios desde MongoDB...")
        sites = self.data_handler.get_all_sites_for_reviewers()
        print(f"[INFO] Encontrados {len(sites)} sitios en MongoDB.")
        return sites
    
    def _rate_limit_guard(self):
        """Controla el rate limit por ventana de tiempo."""
        now = time.time()
        
        if now < self.block_cooldown_until:
            wait_time = self.block_cooldown_until - now
            print(f"[COOLDOWN] Esperando {int(wait_time)}s por bloqueo previo...")
            time.sleep(wait_time)
            self.window_start_time = time.time()
            self.requests_this_window = 0
            return
        
        elapsed = now - self.window_start_time
        
        if elapsed >= self.settings.RATE_LIMIT_WINDOW_SECONDS:
            self.window_start_time = now
            self.requests_this_window = 0
        
        if self.requests_this_window >= self.settings.RATE_LIMIT_PER_HOUR:
            sleep_time = self.settings.RATE_LIMIT_WINDOW_SECONDS - elapsed
            if sleep_time > 0:
                print(f"[RATE LIMIT] Esperando {int(sleep_time)}s...")
                time.sleep(sleep_time)
                self.window_start_time = time.time()
                self.requests_this_window = 0
        
        self.requests_this_window += 1
    
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
                context_info = {
                    "municipio": result.get("municipio", "desconocido"),
                    "site_id": site_id,
                    "site_name": result.get("site_name", "desconocido"),
                }
                stats = self.data_handler.add_reviewers(context_info, users_found)
                print(
                    f"\n[INFO] Sitio {context_info['site_id']}: "
                    f"{stats['new_reviewers']} perfiles nuevos."
                )
            else:
                print(f"\n[INFO] Tarea para sitio {site_id}: éxito sin perfiles.")
        else:
            print(f"\n[INFO] Tarea para sitio {site_id}: {status}")
    
    def run(self, filter_municipio: str = None) -> None:
        """
        Ejecuta el proceso de scraping en paralelo con control de ritmo.
        
        Args:
            filter_municipio: Si se proporciona, solo procesa sitios de ese municipio.
        """
        self._setup_signal_handler()
        
        try:
            sites_to_process = self._load_sites_from_mongodb()
            
            # Filtrar por municipio si se especifica
            if filter_municipio:
                sites_to_process = [
                    s for s in sites_to_process 
                    if s.get('municipio') == filter_municipio
                ]
                print(f"[INFO] Filtrando por municipio: {filter_municipio}")
            
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
                    print_progress(processed_count, total_tasks, "Extrayendo perfiles")
                    self._handle_result(result)
                
                if shutdown_event.is_set():
                    pool.terminate()
                    pool.join()
        
        finally:
            print("\n[INFO] Proceso de extracción finalizado.")
            MongoDBConfig.close_connection()


def main():
    """Punto de entrada principal."""
    parser = argparse.ArgumentParser(
        description='Extractor de Perfiles de Usuario de Foursquare con MongoDB'
    )
    parser.add_argument(
        '--municipio',
        type=str,
        default=None,
        help='Filtrar por municipio específico (opcional)'
    )
    args = parser.parse_args()
    
    app = ReviewerFetcher()
    app.run(filter_municipio=args.municipio)


if __name__ == "__main__":
    main()