"""
Funciones auxiliares para el scraper
"""
import time, json, os
from ..config.settings import Settings


def current_timestamp() -> str:
    """Devuelve la fecha y hora actual como string"""
    return time.strftime("%Y-%m-%d %H:%M:%S")

def print_progress(current: int, total: int, prefix: str = "Progreso") -> None:
    percentage = (current / total) * 100 if total > 0 else 0
    print(f"{prefix}: {current}/{total} ({percentage:.1f}%)")

def save_progress(idx_actual, csv_path, sitios_bloqueados):
    progreso = {
        "csv_path": csv_path,
        "idx_actual": idx_actual,
        "sitios_bloqueados": sitios_bloqueados
    }
    with open(Settings.PROGRESS_SITIES, "w", encoding="utf-8") as f:
        json.dump(progreso, f, ensure_ascii=False, indent=4)
    #print(f"Progreso guardado en {Settings.PROGRESS_SITIES}")

def load_progress():
    if os.path.exists(Settings.PROGRESS_SITIES):
        with open(Settings.PROGRESS_SITIES, "r", encoding="utf-8") as f:
            return json.load(f)
    return None