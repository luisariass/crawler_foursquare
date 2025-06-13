"""
Funciones auxiliares para el scraper
"""
import time

def current_timestamp() -> str:
    """Devuelve la fecha y hora actual como string"""
    return time.strftime("%Y-%m-%d %H:%M:%S")

def print_progress(current: int, total: int, prefix: str = "Progreso") -> None:
    percentage = (current / total) * 100 if total > 0 else 0
    print(f"{prefix}: {current}/{total} ({percentage:.1f}%)")